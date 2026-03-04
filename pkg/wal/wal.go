// Package wal implements a write-ahead log modeled after etcd's WAL.
//
// The WAL is a sequence of binary segment files in a directory. Each
// segment contains framed protobuf records with CRC-32C integrity.
//
// Record types:
//
//	MetadataType (1) — cluster/member metadata
//	EntryType    (2) — raft log entries
//	StateType    (3) — raft hard state
//	CrcType      (4) — CRC chain continuation at segment boundaries
//	SnapshotType (5) — snapshot marker (term + index)
//
// Segment files are pre-allocated to 64 MB and named with 16-hex-char
// sequence and index numbers: {seq:016x}-{index:016x}.wal
package wal

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/harshithgowdakt/raftlabkv/pkg/fileutil"
	"github.com/harshithgowdakt/raftlabkv/pkg/raft"
	"github.com/harshithgowdakt/raftlabkv/pkg/wal/walpb"
	"google.golang.org/protobuf/proto"
)

// Record types (1-indexed, matching etcd).
const (
	MetadataType int64 = iota + 1 // 1
	EntryType                     // 2
	StateType                     // 3
	CrcType                       // 4
	SnapshotType                  // 5
)

// SegmentSizeBytes is the target size for WAL segments. When a segment
// exceeds this after a Save, it is rotated.
var SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64 MB (etcd uses 64*1000*1000)

// WAL is a write-ahead log with binary segmented files, CRC integrity,
// file locking, pre-allocation, and a background file pipeline.
type WAL struct {
	mu sync.Mutex

	dir      string
	dirFile  *os.File               // open handle on dir for fsync
	metadata []byte                 // cluster/member metadata
	state    raft.HardState         // latest hard state (written at segment rotation)
	start    *walpb.Snapshot        // snapshot the WAL was opened from
	locks    []*fileutil.LockedFile // locked segment files (oldest first)
	fp       *filePipeline          // background pre-allocation

	encoder *encoder // current segment encoder
}

// Exists returns true if dir contains .wal files.
func Exists(dir string) bool {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, name := range names {
		if strings.HasSuffix(name, ".wal") {
			return true
		}
	}
	return false
}

// Create initializes a new WAL in dir. It creates the directory, writes
// the first segment with metadata and an initial snapshot record
// (index=0, term=0), and starts the file pipeline.
func Create(dir string, metadata []byte) (*WAL, error) {
	if Exists(dir) {
		return nil, fmt.Errorf("wal: directory %s already contains WAL files", dir)
	}

	// Create a temporary directory, write the first segment there,
	// then rename atomically.
	tmpdirpath := filepath.Clean(dir) + ".tmp"
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		return nil, err
	}

	// Create first segment file.
	fpath := filepath.Join(tmpdirpath, walName(0, 0))
	f, err := fileutil.LockFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileutil.PrivateFileMode)
	if err != nil {
		os.RemoveAll(tmpdirpath)
		return nil, err
	}
	// Pre-allocate.
	if err := fileutil.Preallocate(f.File, SegmentSizeBytes); err != nil {
		f.Close()
		os.RemoveAll(tmpdirpath)
		return nil, err
	}
	// Seek to beginning for writing.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.RemoveAll(tmpdirpath)
		return nil, err
	}

	w := &WAL{
		dir:      dir,
		metadata: metadata,
	}
	enc := newFileEncoder(f.File, 0)
	w.encoder = enc
	w.locks = append(w.locks, f)

	// Write metadata record.
	if err := w.saveMeta(MetadataType, metadata); err != nil {
		w.cleanupTmp(tmpdirpath)
		return nil, err
	}
	// Write initial snapshot record (index=0, term=0) — establishes
	// the invariant that all entries are preceded by a snapshot.
	if err := w.SaveSnapshot(&walpb.Snapshot{}); err != nil {
		w.cleanupTmp(tmpdirpath)
		return nil, err
	}

	// Rename temp dir to final dir atomically.
	if err := os.Rename(tmpdirpath, dir); err != nil {
		w.cleanupTmp(tmpdirpath)
		return nil, err
	}
	// Fsync parent directory.
	if err := fileutil.Fsync(filepath.Dir(dir)); err != nil {
		return nil, err
	}

	// Re-open the file with the final path for the lock.
	newPath := filepath.Join(dir, walName(0, 0))
	newF, err := fileutil.LockFile(newPath, os.O_WRONLY|os.O_APPEND, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}
	// Close the old locked file (it was in tmp dir).
	w.locks[0].Close()
	w.locks[0] = newF

	// Repoint encoder to the new file.
	w.encoder = newFileEncoder(newF.File, 0)
	// Replay to bring encoder CRC up to date by re-reading what we wrote.
	w.encoder = newFileEncoder(newF.File, 0)
	// Actually, since the encoder was writing sequentially and we reopened,
	// we need to seek to end and create a fresh encoder at the right offset.
	off, err := newF.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	w.encoder = newEncoder(newF.File, 0, int(off)%walPageBytes)

	// Open directory handle.
	w.dirFile, err = os.Open(dir)
	if err != nil {
		return nil, err
	}

	// Start file pipeline.
	w.fp = newFilePipeline(dir, SegmentSizeBytes)

	return w, nil
}

// Open opens an existing WAL for recovery/appending. The snap parameter
// indicates the snapshot point; only segments from that point onward
// will be opened. After Open, call ReadAll to recover state, which
// transitions the WAL from read mode to append mode.
func Open(dir string, snap *walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(dir, snap)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func openAtIndex(dir string, snap *walpb.Snapshot) (*WAL, error) {
	names, err := readWALNames(dir)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("wal: no segment files in %s", dir)
	}

	// Find the segment containing the snapshot index.
	nameIdx, err := searchIndex(names, snap.Index)
	if err != nil {
		return nil, err
	}

	// Open and lock all segments from nameIdx onward.
	var locks []*fileutil.LockedFile
	for _, name := range names[nameIdx:] {
		fpath := filepath.Join(dir, name)
		lf, err := fileutil.TryLockFile(fpath, os.O_RDWR, fileutil.PrivateFileMode)
		if err != nil {
			for _, l := range locks {
				l.Close()
			}
			return nil, fmt.Errorf("wal: lock %s: %w", name, err)
		}
		locks = append(locks, lf)
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		for _, l := range locks {
			l.Close()
		}
		return nil, err
	}

	w := &WAL{
		dir:     dir,
		dirFile: dirFile,
		start:   snap,
		locks:   locks,
	}
	return w, nil
}

// ReadAll reads all WAL segments from the snapshot point onward. It
// returns metadata, the latest hard state, and all raft entries.
// After reading, it transitions to append mode by creating an encoder
// on the last segment and starting the file pipeline.
func (w *WAL) ReadAll() ([]byte, raft.HardState, []raft.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build readers from all locked segments.
	readers := make([]io.Reader, len(w.locks))
	for i, lf := range w.locks {
		if _, err := lf.Seek(0, io.SeekStart); err != nil {
			return nil, raft.HardState{}, nil, err
		}
		readers[i] = lf.File
	}

	dec := newDecoder(readers...)

	var (
		metadata []byte
		hs       raft.HardState
		entries  []raft.Entry
		matchSn  bool // have we found the matching snapshot record?
	)

	var rec walpb.Record
	for {
		err := dec.decode(&rec)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			// Torn write in the last segment — zero it out for repair.
			log.Printf("wal: torn write detected, zeroing from offset %d", dec.lastOffset())
			lastFile := w.locks[len(w.locks)-1]
			if err := fileutil.ZeroToEnd(lastFile.File, dec.lastOffset()); err != nil {
				return nil, raft.HardState{}, nil, fmt.Errorf("wal: zero torn: %w", err)
			}
			break
		}
		if err != nil {
			return nil, raft.HardState{}, nil, err
		}

		switch rec.Type {
		case MetadataType:
			if metadata != nil && string(metadata) != string(rec.Data) {
				return nil, raft.HardState{}, nil, fmt.Errorf("wal: metadata conflict")
			}
			metadata = rec.Data

		case CrcType:
			// Re-seed CRC chain from previous segment.
			crc := dec.lastCRC()
			dec.updateCRC(crc)

		case SnapshotType:
			var snap walpb.Snapshot
			if err := proto.Unmarshal(rec.Data, &snap); err != nil {
				return nil, raft.HardState{}, nil, fmt.Errorf("wal: unmarshal snap: %w", err)
			}
			if w.start != nil && snap.Index == w.start.Index && snap.Term == w.start.Term {
				matchSn = true
			}

		case EntryType:
			var e raft.Entry
			if err := json.Unmarshal(rec.Data, &e); err != nil {
				return nil, raft.HardState{}, nil, fmt.Errorf("wal: unmarshal entry: %w", err)
			}
			// Last-write-wins: if index already in slice, replace.
			if len(entries) > 0 && e.Index == entries[len(entries)-1].Index {
				entries[len(entries)-1] = e
			} else {
				entries = append(entries, e)
			}

		case StateType:
			if err := json.Unmarshal(rec.Data, &hs); err != nil {
				return nil, raft.HardState{}, nil, fmt.Errorf("wal: unmarshal state: %w", err)
			}
			w.state = hs

		default:
			log.Printf("wal: unknown record type %d", rec.Type)
		}
	}

	// Validate that we found the starting snapshot.
	if !matchSn && w.start != nil && (w.start.Index > 0 || w.start.Term > 0) {
		return nil, raft.HardState{}, nil, fmt.Errorf("wal: snapshot record not found for index=%d term=%d", w.start.Index, w.start.Term)
	}

	w.metadata = metadata

	// --- Transition to append mode ---

	// Close read-only file handles except the last one (which we'll write to).
	lastLock := w.locks[len(w.locks)-1]

	// Seek to the end of valid data in the last segment.
	if _, err := lastLock.Seek(dec.lastOffset(), io.SeekStart); err != nil {
		return nil, raft.HardState{}, nil, err
	}

	// Create encoder on the last segment, seeded with the decoder's CRC.
	w.encoder = newFileEncoder(lastLock.File, dec.lastCRC())

	// Start file pipeline for future segments.
	w.fp = newFilePipeline(w.dir, SegmentSizeBytes)

	return metadata, hs, entries, nil
}

// Save persists hard state and entries. Entries are written first, then
// the state record, followed by a flush + fsync. Triggers segment
// rotation if the current segment exceeds SegmentSizeBytes.
func (w *WAL) Save(st raft.HardState, ents []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write entry records.
	for _, e := range ents {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("wal: marshal entry: %w", err)
		}
		rec := &walpb.Record{Type: EntryType, Data: data}
		if err := w.encoder.encode(rec); err != nil {
			return fmt.Errorf("wal: encode entry: %w", err)
		}
	}

	// Write hard state if non-empty.
	if !raft.IsEmptyHardState(st) {
		data, err := json.Marshal(st)
		if err != nil {
			return fmt.Errorf("wal: marshal state: %w", err)
		}
		rec := &walpb.Record{Type: StateType, Data: data}
		if err := w.encoder.encode(rec); err != nil {
			return fmt.Errorf("wal: encode state: %w", err)
		}
		w.state = st
	}

	// Flush and fsync.
	if err := w.encoder.flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	curFile := w.locks[len(w.locks)-1]
	if err := curFile.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	// Check segment rotation.
	info, err := curFile.Stat()
	if err != nil {
		return err
	}
	if info.Size() < SegmentSizeBytes {
		return nil
	}
	return w.cut()
}

// SaveSnapshot writes a snapshot marker to the WAL.
func (w *WAL) SaveSnapshot(snap *walpb.Snapshot) error {
	data, err := proto.Marshal(snap)
	if err != nil {
		return err
	}
	rec := &walpb.Record{Type: SnapshotType, Data: data}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// Flush + sync to ensure the snapshot marker is durable before
	// we tell the snapshotter to write the actual snapshot file.
	if err := w.encoder.flush(); err != nil {
		return err
	}
	if len(w.locks) > 0 {
		curFile := w.locks[len(w.locks)-1]
		return curFile.Sync()
	}
	return nil
}

// ReleaseLockTo releases file locks on segments whose entries are all
// before the given index. Keeps the largest segment lock below the
// threshold to avoid gaps. This allows the purge goroutine to delete
// old segment files.
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) <= 1 {
		return nil
	}

	// Find the last lock to release. We keep at least one lock below
	// the threshold and always keep the current (last) segment.
	var toRelease int
	for i, lf := range w.locks[:len(w.locks)-1] {
		_, segIndex, err := parseWalName(filepath.Base(lf.Name()))
		if err != nil {
			continue
		}
		if segIndex >= index {
			break
		}
		toRelease = i
	}

	// Release locks [0, toRelease).
	for i := 0; i < toRelease; i++ {
		if err := w.locks[i].Close(); err != nil {
			log.Printf("wal: release lock: %v", err)
		}
	}
	w.locks = w.locks[toRelease:]
	return nil
}

// Close flushes, closes all segment files, and shuts down the pipeline.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.encoder != nil {
		w.encoder.flush()
	}
	if w.fp != nil {
		w.fp.Close()
	}
	for _, lf := range w.locks {
		lf.Close()
	}
	w.locks = nil
	if w.dirFile != nil {
		w.dirFile.Close()
	}
	return nil
}

// cut rotates to a new segment. The new segment starts with a CRC record
// (chaining from previous segment), a metadata record, and a state record.
func (w *WAL) cut() error {
	// Flush and sync current segment.
	if err := w.encoder.flush(); err != nil {
		return err
	}
	curFile := w.locks[len(w.locks)-1]

	// Truncate current segment to actual written size (reclaim pre-allocated space).
	off, err := curFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := curFile.Truncate(off); err != nil {
		return err
	}
	if err := curFile.Sync(); err != nil {
		return err
	}

	// Get a pre-allocated file from the pipeline.
	newFile, err := w.fp.Open()
	if err != nil {
		return fmt.Errorf("wal: pipeline open: %w", err)
	}

	// Determine new segment name. Use last entry index if available.
	_, lastIdx := w.lastEntryInfo()
	newSeq := w.currentSeq() + 1
	newName := walName(newSeq, lastIdx+1)
	newPath := filepath.Join(w.dir, newName)

	// Seek to beginning of the new file.
	if _, err := newFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Create encoder on the new file, chaining CRC.
	w.encoder = newFileEncoder(newFile.File, w.encoder.crc.Sum32())

	// Write CRC record to chain integrity.
	if err := w.saveCRC(w.encoder.crc.Sum32()); err != nil {
		return err
	}
	// Write metadata record.
	if err := w.saveMeta(MetadataType, w.metadata); err != nil {
		return err
	}
	// Write current hard state.
	if !raft.IsEmptyHardState(w.state) {
		stateData, err := json.Marshal(w.state)
		if err != nil {
			return err
		}
		rec := &walpb.Record{Type: StateType, Data: stateData}
		if err := w.encoder.encode(rec); err != nil {
			return err
		}
	}
	if err := w.encoder.flush(); err != nil {
		return err
	}

	// Rename temp file to final WAL name.
	if err := os.Rename(newFile.Name(), newPath); err != nil {
		return err
	}
	if err := fileutil.Fsync(w.dir); err != nil {
		return err
	}

	// Close the old pipeline file and re-open with lock under the new name.
	newFile.Close()
	newLF, err := fileutil.TryLockFile(newPath, os.O_WRONLY|os.O_APPEND, fileutil.PrivateFileMode)
	if err != nil {
		return fmt.Errorf("wal: lock new segment: %w", err)
	}
	w.locks = append(w.locks, newLF)

	// Repoint encoder to the new locked file.
	endOff, _ := newLF.Seek(0, io.SeekEnd)
	w.encoder = newEncoder(newLF.File, w.encoder.crc.Sum32(), int(endOff)%walPageBytes)

	return nil
}

// --- Internal helpers ---

func (w *WAL) saveMeta(recType int64, data []byte) error {
	rec := &walpb.Record{Type: recType, Data: data}
	return w.encoder.encode(rec)
}

func (w *WAL) saveCRC(prevCrc uint32) error {
	rec := &walpb.Record{Type: CrcType, Crc: prevCrc}
	return w.encoder.encode(rec)
}

func (w *WAL) currentSeq() uint64 {
	if len(w.locks) == 0 {
		return 0
	}
	last := w.locks[len(w.locks)-1]
	seq, _, _ := parseWalName(filepath.Base(last.Name()))
	return seq
}

func (w *WAL) lastEntryInfo() (term, index uint64) {
	// Best-effort: parse from the current segment name.
	if len(w.locks) == 0 {
		return 0, 0
	}
	last := w.locks[len(w.locks)-1]
	_, idx, _ := parseWalName(filepath.Base(last.Name()))
	return 0, idx
}

func (w *WAL) cleanupTmp(dir string) {
	for _, lf := range w.locks {
		lf.Close()
	}
	os.RemoveAll(dir)
}

func readWALNames(dir string) ([]string, error) {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read dir: %w", err)
	}
	var walNames []string
	for _, name := range names {
		if strings.HasSuffix(name, ".wal") && isValidWALName(name) {
			walNames = append(walNames, name)
		}
	}
	return walNames, nil
}
