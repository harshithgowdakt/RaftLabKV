package raft

import (
	"errors"
	"fmt"
	"sync"
)

// ErrCompacted is returned by Storage.Entries/Term when the requested index
// is older than the storage's compaction point.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrUnavailable is returned by Storage methods when the requested log entry
// is not available.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapOutOfDate is returned when the snapshot is out of date.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// Storage is the interface that wraps the methods to access log entries and
// raft state from stable storage.
//
// Implementations must be safe for concurrent use.
type Storage interface {
	// InitialState returns the saved HardState and the peer configuration
	// from the most recent snapshot.
	InitialState() (HardState, []uint64, error)

	// Entries returns a slice of consecutive log entries in the range
	// [lo, hi), capped by maxSize (0 = no limit). Must return at least one
	// entry if any exist.
	Entries(lo, hi, maxSize uint64) ([]Entry, error)

	// Term returns the term of entry i. The term of the entry at the
	// compaction index (first dummy entry) is retained.
	Term(i uint64) (uint64, error)

	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)

	// FirstIndex returns the index of the first available log entry (one
	// after the compacted dummy entry).
	FirstIndex() (uint64, error)

	// Snapshot returns the most recent snapshot. If snapshot is temporarily
	// unavailable, it should return ErrSnapshotTemporarilyUnavailable so
	// raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by in-memory arrays.
// It is safe for concurrent use.
type MemoryStorage struct {
	sync.Mutex

	hardState HardState
	snapshot  Snapshot
	// ents[i] has raft log position i + snapshot.Metadata.Index.
	// ents[0] is a dummy entry holding the term/index at the snapshot boundary.
	ents []Entry
}

// NewMemoryStorage creates a new MemoryStorage with a dummy entry at index 0.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		ents: make([]Entry, 1), // dummy entry at index 0
	}
}

// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (HardState, []uint64, error) {
	return ms.hardState, ms.snapshot.Metadata.Peers, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]Entry, error) {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		ms.panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}

	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// SetPeers sets the peer list in the snapshot metadata. This is needed
// during recovery so that newRaft can populate the progress tracker.
func (ms *MemoryStorage) SetPeers(peers []uint64) {
	ms.Lock()
	defer ms.Unlock()
	ms.snapshot.Metadata.Peers = peers
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// Append adds entries to the MemoryStorage. It handles truncation if
// existing entries conflict with the new ones.
func (ms *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// Shortcut if there is nothing new.
	if last < first {
		return nil
	}

	// Truncate compacted entries.
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// Truncate existing entries that conflict.
		ms.ents = append(ms.ents[:offset], entries...)
	case uint64(len(ms.ents)) == offset:
		// Append directly.
		ms.ents = append(ms.ents, entries...)
	default:
		ms.panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}

	return nil
}

// Compact discards all log entries prior to compactIndex. It is the
// application's responsibility to not attempt to compact an index greater
// than applied.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		ms.panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	// Create a new slice and keep everything from compactIndex onward.
	ents := make([]Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// ApplySnapshot overwrites the contents of this Storage with those of the
// given snapshot.
func (ms *MemoryStorage) ApplySnapshot(snap Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap
	ms.ents = []Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot creates a snapshot at the given index with the given
// peer configuration and data.
func (ms *MemoryStorage) CreateSnapshot(i uint64, peers []uint64, data []byte) (Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()

	if i <= ms.snapshot.Metadata.Index {
		return Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		ms.panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if peers != nil {
		ms.snapshot.Metadata.Peers = peers
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

func (ms *MemoryStorage) panicf(format string, v ...any) {
	panic(fmt.Sprintf("MemoryStorage: "+format, v...))
}
