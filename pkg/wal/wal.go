package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

const (
	walFile      = "wal.jsonl"
	snapFile     = "snapshot.json"
	hardStateFile = "hardstate.json"
)

// WAL implements a simple write-ahead log using JSON-lines format.
// Each line is a JSON-encoded raft.Entry. The HardState and snapshot
// are stored in separate files.
type WAL struct {
	dir string
}

// New creates or opens a WAL in the given directory.
func New(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}
	return &WAL{dir: dir}, nil
}

// Save persists the given HardState and entries to the WAL.
func (w *WAL) Save(st raft.HardState, ents []raft.Entry) error {
	if err := w.saveHardState(st); err != nil {
		return err
	}
	return w.appendEntries(ents)
}

// SaveSnapshot persists a snapshot marker.
func (w *WAL) SaveSnapshot(snap raft.Snapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("wal: marshal snapshot: %w", err)
	}
	return os.WriteFile(filepath.Join(w.dir, snapFile), data, 0640)
}

// ReadAll reads the persisted state. Returns the HardState, snapshot,
// and all entries from the WAL. If no WAL exists, returns zero values.
func (w *WAL) ReadAll() (raft.HardState, raft.Snapshot, []raft.Entry, error) {
	var hs raft.HardState
	var snap raft.Snapshot
	var ents []raft.Entry

	// Read hard state.
	hsData, err := os.ReadFile(filepath.Join(w.dir, hardStateFile))
	if err == nil {
		if err := json.Unmarshal(hsData, &hs); err != nil {
			return hs, snap, nil, fmt.Errorf("wal: unmarshal hardstate: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return hs, snap, nil, fmt.Errorf("wal: read hardstate: %w", err)
	}

	// Read snapshot.
	snapData, err := os.ReadFile(filepath.Join(w.dir, snapFile))
	if err == nil {
		if err := json.Unmarshal(snapData, &snap); err != nil {
			return hs, snap, nil, fmt.Errorf("wal: unmarshal snapshot: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return hs, snap, nil, fmt.Errorf("wal: read snapshot: %w", err)
	}

	// Read entries.
	f, err := os.Open(filepath.Join(w.dir, walFile))
	if err != nil {
		if os.IsNotExist(err) {
			return hs, snap, nil, nil
		}
		return hs, snap, nil, fmt.Errorf("wal: open: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Increase scanner buffer for large entries.
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)
	for scanner.Scan() {
		var e raft.Entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return hs, snap, nil, fmt.Errorf("wal: unmarshal entry: %w", err)
		}
		ents = append(ents, e)
	}
	if err := scanner.Err(); err != nil {
		return hs, snap, nil, fmt.Errorf("wal: scan: %w", err)
	}

	return hs, snap, ents, nil
}

// Exists returns true if the WAL directory contains data.
func (w *WAL) Exists() bool {
	_, err := os.Stat(filepath.Join(w.dir, hardStateFile))
	return err == nil
}

func (w *WAL) saveHardState(st raft.HardState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("wal: marshal hardstate: %w", err)
	}
	return os.WriteFile(filepath.Join(w.dir, hardStateFile), data, 0640)
}

func (w *WAL) appendEntries(ents []raft.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	f, err := os.OpenFile(filepath.Join(w.dir, walFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return fmt.Errorf("wal: open for append: %w", err)
	}
	defer f.Close()

	w2 := bufio.NewWriter(f)
	for _, e := range ents {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("wal: marshal entry: %w", err)
		}
		if _, err := w2.Write(data); err != nil {
			return fmt.Errorf("wal: write entry: %w", err)
		}
		if err := w2.WriteByte('\n'); err != nil {
			return fmt.Errorf("wal: write newline: %w", err)
		}
	}
	if err := w2.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return f.Sync()
}
