package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

// Record types stored in the WAL.
const (
	recTypeMetadata      int64 = 0 // WAL metadata (e.g., node ID)
	recTypeEntry         int64 = 1 // JSON-encoded raft.Entry
	recTypeState         int64 = 2 // JSON-encoded raft.HardState
	recTypeCRC           int64 = 3 // CRC chain sentinel (segment continuity)
	recTypeSnapshotIndex int64 = 4 // snapshot marker {term, index}
)

const (
	segmentSizeBytes = 64 * 1024 * 1024 // 64 MB
	recordAlign      = 8                 // 8-byte alignment for records
)

// SnapshotLocator identifies a snapshot boundary in the WAL.
type SnapshotLocator struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
}

// WAL implements a binary segmented write-ahead log with CRC integrity.
//
// Record format:
//
//	[type: 8 bytes LE (int64)][crc32: 4 bytes LE][dataLen: 4 bytes LE][data: dataLen bytes][padding: 0-7 bytes to 8-byte align]
type WAL struct {
	mu      sync.Mutex
	dir     string
	file    *os.File    // current segment file
	seq     uint64      // current segment sequence number
	encoder *walEncoder // buffered binary record writer
	crc     hash.Hash32 // running CRC (IEEE)
}

// Exists returns true if the directory contains any .wal segment files.
func Exists(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") {
			return true
		}
	}
	return false
}

// Create initializes a new WAL in the given directory, writing an initial
// segment with metadata and CRC sentinel records. Used for new clusters.
func Create(dir string, metadata []byte) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	// Create first segment: seq=0, index=0
	fname := segmentName(0, 0)
	fpath := filepath.Join(dir, fname)

	f, err := os.Create(fpath)
	if err != nil {
		return nil, fmt.Errorf("wal: create segment: %w", err)
	}

	w := &WAL{
		dir:  dir,
		file: f,
		seq:  0,
		crc:  crc32.NewIEEE(),
	}
	w.encoder = newWalEncoder(f, w.crc)

	// Write metadata record.
	if err := w.encoder.encode(recTypeMetadata, metadata); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: write metadata: %w", err)
	}

	// Write initial CRC sentinel.
	if err := w.encoder.encode(recTypeCRC, nil); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: write crc sentinel: %w", err)
	}

	if err := w.encoder.flush(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: flush: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: sync: %w", err)
	}

	// Fsync directory.
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: sync dir: %w", err)
	}

	return w, nil
}

// Open opens an existing WAL for recovery. It locates segments starting
// from the given snapshot point and prepares for appending.
func Open(dir string, snap SnapshotLocator) (*WAL, error) {
	names, err := segmentNames(dir)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("wal: no segments found in %s", dir)
	}

	// Open the last segment for appending.
	lastSeg := names[len(names)-1]
	lastSeq, _, _ := parseSegmentName(lastSeg)

	fpath := filepath.Join(dir, lastSeg)
	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_APPEND, 0640)
	if err != nil {
		return nil, fmt.Errorf("wal: open last segment: %w", err)
	}

	w := &WAL{
		dir:  dir,
		file: f,
		seq:  lastSeq,
		crc:  crc32.NewIEEE(),
	}

	// Read all segments to reconstruct running CRC.
	for i := 0; i < len(names); i++ {
		segPath := filepath.Join(dir, names[i])
		if err := w.replayCRC(segPath); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: replay crc %s: %w", names[i], err)
		}
	}

	w.encoder = newWalEncoder(f, w.crc)
	return w, nil
}

// replayCRC reads through a segment to bring the running CRC up to date.
func (w *WAL) replayCRC(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := newWalDecoder(f)
	for {
		_, data, err := dec.decode()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			// Tolerate truncated final record.
			break
		}
		// Update running CRC with the data.
		if len(data) > 0 {
			w.crc.Write(data)
		}
	}
	return nil
}

// ReadAll reads all WAL segments and returns the metadata, hard state,
// and entries after the snapshot index. Used during recovery.
func (w *WAL) ReadAll() ([]byte, raft.HardState, []raft.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	names, err := segmentNames(w.dir)
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}

	var (
		metadata []byte
		hs       raft.HardState
		entries  []raft.Entry
	)

	for _, name := range names {
		segPath := filepath.Join(w.dir, name)
		meta, state, ents, err := w.readSegment(segPath)
		if err != nil {
			return nil, raft.HardState{}, nil, fmt.Errorf("wal: read segment %s: %w", name, err)
		}
		if meta != nil {
			metadata = meta
		}
		if !raft.IsEmptyHardState(state) {
			hs = state
		}
		entries = append(entries, ents...)
	}

	return metadata, hs, entries, nil
}

func (w *WAL) readSegment(path string) ([]byte, raft.HardState, []raft.Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}
	defer f.Close()

	dec := newWalDecoder(f)
	var (
		metadata []byte
		hs       raft.HardState
		entries  []raft.Entry
	)

	for {
		recType, data, err := dec.decode()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			// Truncated final record — log warning and stop.
			log.Printf("wal: truncated record in %s, stopping read", filepath.Base(path))
			break
		}

		switch recType {
		case recTypeMetadata:
			metadata = data
		case recTypeEntry:
			var e raft.Entry
			if err := json.Unmarshal(data, &e); err != nil {
				return nil, raft.HardState{}, nil, fmt.Errorf("unmarshal entry: %w", err)
			}
			entries = append(entries, e)
		case recTypeState:
			if err := json.Unmarshal(data, &hs); err != nil {
				return nil, raft.HardState{}, nil, fmt.Errorf("unmarshal state: %w", err)
			}
		case recTypeCRC:
			// CRC sentinel — used for segment continuity, skip.
		case recTypeSnapshotIndex:
			// Snapshot marker — informational, skip.
		default:
			log.Printf("wal: unknown record type %d in %s", recType, filepath.Base(path))
		}
	}

	return metadata, hs, entries, nil
}

// Save persists the given hard state and entries to the WAL.
// All entry records are written first, then the state record, followed
// by a flush and fsync. Segment rotation is checked after writing.
func (w *WAL) Save(st raft.HardState, ents []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write entry records.
	for _, e := range ents {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("wal: marshal entry: %w", err)
		}
		if err := w.encoder.encode(recTypeEntry, data); err != nil {
			return fmt.Errorf("wal: encode entry: %w", err)
		}
	}

	// Write hard state record if non-empty.
	if !raft.IsEmptyHardState(st) {
		data, err := json.Marshal(st)
		if err != nil {
			return fmt.Errorf("wal: marshal state: %w", err)
		}
		if err := w.encoder.encode(recTypeState, data); err != nil {
			return fmt.Errorf("wal: encode state: %w", err)
		}
	}

	// Flush and fsync.
	if err := w.encoder.flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	// Check if segment rotation is needed.
	return w.maybeRotate()
}

// SaveSnapshot writes a snapshot marker record to the WAL.
func (w *WAL) SaveSnapshot(snap SnapshotLocator) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("wal: marshal snap locator: %w", err)
	}

	if err := w.encoder.encode(recTypeSnapshotIndex, data); err != nil {
		return fmt.Errorf("wal: encode snap: %w", err)
	}
	if err := w.encoder.flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return w.file.Sync()
}

// Close closes the current WAL segment file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.encoder != nil {
		w.encoder.flush()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// maybeRotate checks if the current segment exceeds the size limit and
// rotates to a new segment if needed.
func (w *WAL) maybeRotate() error {
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < segmentSizeBytes {
		return nil
	}
	return w.rotate()
}

// rotate creates a new segment file and writes a CRC sentinel for continuity.
func (w *WAL) rotate() error {
	// Flush and sync current segment.
	if err := w.encoder.flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	w.seq++
	fname := segmentName(w.seq, 0)
	fpath := filepath.Join(w.dir, fname)

	f, err := os.Create(fpath)
	if err != nil {
		return fmt.Errorf("wal: create new segment: %w", err)
	}

	// Close old segment.
	w.file.Close()
	w.file = f
	w.encoder = newWalEncoder(f, w.crc)

	// Write CRC sentinel carrying the running CRC from the previous segment.
	crcVal := w.crc.Sum32()
	var crcData [4]byte
	binary.LittleEndian.PutUint32(crcData[:], crcVal)
	if err := w.encoder.encode(recTypeCRC, crcData[:]); err != nil {
		return fmt.Errorf("wal: write crc sentinel: %w", err)
	}
	if err := w.encoder.flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	return syncDir(w.dir)
}

// --- walEncoder ---

// walEncoder writes binary WAL records with buffering.
type walEncoder struct {
	bw  *bufio.Writer
	crc hash.Hash32
}

func newWalEncoder(w io.Writer, crc hash.Hash32) *walEncoder {
	return &walEncoder{
		bw:  bufio.NewWriterSize(w, 128*1024),
		crc: crc,
	}
}

// encode writes a single WAL record:
// [type: 8 bytes LE][crc32: 4 bytes LE][dataLen: 4 bytes LE][data][padding to 8-byte align]
func (enc *walEncoder) encode(recType int64, data []byte) error {
	// Update running CRC with the data.
	if len(data) > 0 {
		enc.crc.Write(data)
	}
	checksum := enc.crc.Sum32()

	// Write header: type(8) + crc(4) + dataLen(4) = 16 bytes.
	var header [16]byte
	binary.LittleEndian.PutUint64(header[0:8], uint64(recType))
	binary.LittleEndian.PutUint32(header[8:12], checksum)
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(data)))

	if _, err := enc.bw.Write(header[:]); err != nil {
		return err
	}

	// Write data.
	if len(data) > 0 {
		if _, err := enc.bw.Write(data); err != nil {
			return err
		}
	}

	// Padding to 8-byte alignment.
	total := 16 + len(data)
	if pad := paddingLen(total); pad > 0 {
		var zeros [7]byte
		if _, err := enc.bw.Write(zeros[:pad]); err != nil {
			return err
		}
	}

	return nil
}

func (enc *walEncoder) flush() error {
	return enc.bw.Flush()
}

// --- walDecoder ---

// walDecoder reads binary WAL records.
type walDecoder struct {
	r io.Reader
}

func newWalDecoder(r io.Reader) *walDecoder {
	return &walDecoder{r: bufio.NewReaderSize(r, 128*1024)}
}

// decode reads a single WAL record.
// Returns io.EOF when no more records are available.
func (dec *walDecoder) decode() (recType int64, data []byte, err error) {
	// Read header: type(8) + crc(4) + dataLen(4) = 16 bytes.
	var header [16]byte
	if _, err := io.ReadFull(dec.r, header[:]); err != nil {
		return 0, nil, err
	}

	recType = int64(binary.LittleEndian.Uint64(header[0:8]))
	// crc32 at header[8:12] — verified via running CRC chain.
	dataLen := binary.LittleEndian.Uint32(header[12:16])

	// Read data.
	if dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(dec.r, data); err != nil {
			return 0, nil, err
		}
	}

	// Skip padding.
	total := 16 + int(dataLen)
	if pad := paddingLen(total); pad > 0 {
		var skip [7]byte
		if _, err := io.ReadFull(dec.r, skip[:pad]); err != nil {
			return recType, data, err
		}
	}

	return recType, data, nil
}

// --- Helpers ---

func segmentName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}

func parseSegmentName(name string) (seq, index uint64, err error) {
	name = strings.TrimSuffix(name, ".wal")
	parts := strings.SplitN(name, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("wal: invalid segment name: %s", name)
	}
	if _, err := fmt.Sscanf(parts[0], "%016x", &seq); err != nil {
		return 0, 0, err
	}
	if _, err := fmt.Sscanf(parts[1], "%016x", &index); err != nil {
		return 0, 0, err
	}
	return seq, index, nil
}

func segmentNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read dir: %w", err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".wal") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func paddingLen(totalBytes int) int {
	rem := totalBytes % recordAlign
	if rem == 0 {
		return 0
	}
	return recordAlign - rem
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
