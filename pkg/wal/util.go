package wal

import (
	"fmt"
	"strings"
)

// walName generates a WAL segment filename.
// Format: {seq:016x}-{index:016x}.wal
func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}

// parseWalName extracts the sequence number and raft index from a segment filename.
func parseWalName(name string) (seq, index uint64, err error) {
	name = strings.TrimSuffix(name, ".wal")
	parts := strings.SplitN(name, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("wal: bad segment name: %s", name)
	}
	if _, err := fmt.Sscanf(parts[0], "%016x", &seq); err != nil {
		return 0, 0, fmt.Errorf("wal: parse seq: %w", err)
	}
	if _, err := fmt.Sscanf(parts[1], "%016x", &index); err != nil {
		return 0, 0, fmt.Errorf("wal: parse index: %w", err)
	}
	return seq, index, nil
}

// searchIndex finds the position in names where the WAL segment for
// the given raft index starts. Returns the index into the names slice
// of the segment whose starting index is <= the target.
func searchIndex(names []string, index uint64) (int, error) {
	for i := len(names) - 1; i >= 0; i-- {
		_, segIndex, err := parseWalName(names[i])
		if err != nil {
			continue
		}
		if segIndex <= index {
			return i, nil
		}
	}
	return 0, nil // default to first segment
}

// isValidWALName returns true if name matches the WAL naming convention.
func isValidWALName(name string) bool {
	if !strings.HasSuffix(name, ".wal") {
		return false
	}
	_, _, err := parseWalName(name)
	return err == nil
}
