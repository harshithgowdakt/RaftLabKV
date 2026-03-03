package snap

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

const maxRetainedSnapshots = 5

// Snapshotter manages binary snapshot files on disk.
//
// Snapshot file format (binary with CRC):
//
//	[crc32: 4 bytes LE][dataLen: 4 bytes LE][data: dataLen bytes (JSON-encoded raft.Snapshot)]
//
// File naming: {term:016x}-{index:016x}.snap
type Snapshotter struct {
	dir string
}

// New creates a Snapshotter for the given directory, creating it if needed.
func New(dir string) *Snapshotter {
	if err := os.MkdirAll(dir, 0750); err != nil {
		panic(fmt.Sprintf("snap: cannot create dir %s: %v", dir, err))
	}
	return &Snapshotter{dir: dir}
}

// SaveSnap writes a snapshot to disk as a binary file with CRC integrity.
// It also prunes old snapshots beyond the retention limit.
func (s *Snapshotter) SaveSnap(snap raft.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("snap: marshal: %w", err)
	}

	checksum := crc32.ChecksumIEEE(data)
	fname := fmt.Sprintf("%016x-%016x.snap", snap.Metadata.Term, snap.Metadata.Index)
	fpath := filepath.Join(s.dir, fname)

	// Write to temp file, then rename for atomicity.
	tmp := fpath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("snap: create tmp: %w", err)
	}

	// Write header: [crc32 LE][dataLen LE]
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(data)))
	if _, err := f.Write(header[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("snap: write header: %w", err)
	}

	// Write data.
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("snap: write data: %w", err)
	}

	// Fsync file.
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("snap: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("snap: close: %w", err)
	}

	// Atomic rename.
	if err := os.Rename(tmp, fpath); err != nil {
		return fmt.Errorf("snap: rename: %w", err)
	}

	// Fsync directory for durability.
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("snap: sync dir: %w", err)
	}

	// Prune old snapshots.
	s.purge()

	return nil
}

// Load reads the newest valid snapshot from disk.
// Falls back to older snapshots if the newest is corrupt.
func (s *Snapshotter) Load() (*raft.Snapshot, error) {
	names, err := s.SnapNames()
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("snap: no snapshot files")
	}

	for _, name := range names {
		snap, err := s.loadSnap(name)
		if err != nil {
			log.Printf("snap: skipping corrupt snapshot %s: %v", name, err)
			continue
		}
		return snap, nil
	}
	return nil, fmt.Errorf("snap: all snapshot files are corrupt")
}

func (s *Snapshotter) loadSnap(name string) (*raft.Snapshot, error) {
	fpath := filepath.Join(s.dir, name)
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read header.
	var header [8]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	expectedCRC := binary.LittleEndian.Uint32(header[0:4])
	dataLen := binary.LittleEndian.Uint32(header[4:8])

	// Read data.
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}

	// Verify CRC.
	actualCRC := crc32.ChecksumIEEE(data)
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("crc mismatch: got %08x, want %08x", actualCRC, expectedCRC)
	}

	var snap raft.Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &snap, nil
}

// SnapNames returns snapshot filenames sorted newest-first.
func (s *Snapshotter) SnapNames() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("snap: read dir: %w", err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".snap") {
			names = append(names, e.Name())
		}
	}
	// Sort in reverse (newest first by name, which encodes term-index).
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	return names, nil
}

// SnapInfo returns the term and index of the latest snapshot by parsing
// the filename, without reading the file contents.
func (s *Snapshotter) SnapInfo() (term, index uint64, err error) {
	names, err := s.SnapNames()
	if err != nil {
		return 0, 0, err
	}
	if len(names) == 0 {
		return 0, 0, fmt.Errorf("snap: no snapshot files")
	}
	return parseSnapName(names[0])
}

func parseSnapName(name string) (term, index uint64, err error) {
	// Format: {term:016x}-{index:016x}.snap
	name = strings.TrimSuffix(name, ".snap")
	parts := strings.SplitN(name, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("snap: invalid name: %s", name)
	}
	if _, err := fmt.Sscanf(parts[0], "%016x", &term); err != nil {
		return 0, 0, fmt.Errorf("snap: parse term: %w", err)
	}
	if _, err := fmt.Sscanf(parts[1], "%016x", &index); err != nil {
		return 0, 0, fmt.Errorf("snap: parse index: %w", err)
	}
	return term, index, nil
}

// purge removes old snapshots beyond the retention limit.
func (s *Snapshotter) purge() {
	names, err := s.SnapNames()
	if err != nil {
		return
	}
	for i := maxRetainedSnapshots; i < len(names); i++ {
		fpath := filepath.Join(s.dir, names[i])
		if err := os.Remove(fpath); err != nil {
			log.Printf("snap: failed to remove old snapshot %s: %v", names[i], err)
		}
	}
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
