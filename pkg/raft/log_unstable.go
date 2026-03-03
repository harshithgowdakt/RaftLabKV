package raft

// unstable contains entries and an optional snapshot that have not yet been
// persisted to Storage. The entries in unstable will be sent in Ready to the
// application, which is responsible for persisting them.
type unstable struct {
	// snapshot is the pending snapshot to persist, or nil.
	snapshot *Snapshot

	// entries are the pending entries to persist.
	// entries[i] has raft log index i + offset.
	entries []Entry
	offset  uint64

	// offsetInProgress is the log position up to which entries have been
	// handed off in a Ready but not yet acknowledged via stableTo.
	offsetInProgress uint64

	// snapshotInProgress is true if the snapshot has been handed off in a
	// Ready but not yet acknowledged.
	snapshotInProgress bool

	logger Logger
}

// maybeFirstIndex returns the index of the first entry in the unstable
// snapshot, if any.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the index of the last entry in unstable, if any.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if it is part of the
// unstable log or snapshot.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

// nextEntries returns the entries that have not yet been handed off in a
// Ready (i.e., from offsetInProgress to the end).
func (u *unstable) nextEntries() []Entry {
	inProgress := int(u.offsetInProgress - u.offset)
	if len(u.entries) == inProgress {
		return nil
	}
	return u.entries[inProgress:]
}

// nextSnapshot returns the snapshot that hasn't been handed off, or nil.
func (u *unstable) nextSnapshot() *Snapshot {
	if u.snapshot == nil || u.snapshotInProgress {
		return nil
	}
	return u.snapshot
}

// acceptInProgress marks all current entries and snapshot as having been
// handed off in a Ready.
func (u *unstable) acceptInProgress() {
	if len(u.entries) > 0 {
		u.offsetInProgress = u.entries[len(u.entries)-1].Index + 1
	}
	if u.snapshot != nil {
		u.snapshotInProgress = true
	}
}

// stableTo marks entries up to and including the given index+term as
// persisted. It removes them from the unstable buffer.
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// Only truncate if term matches, to avoid incorrect truncation
	// on stale acknowledgements.
	if gt != t {
		return
	}

	num := int(i + 1 - u.offset)
	u.entries = u.entries[num:]
	u.offset = i + 1
	// Make sure offsetInProgress doesn't go backwards.
	if u.offsetInProgress < u.offset {
		u.offsetInProgress = u.offset
	}
	u.shrinkEntriesArray()
}

// stableSnapTo marks the snapshot as persisted.
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
		u.snapshotInProgress = false
	}
}

// shrinkEntriesArray discards the underlying array if it is much larger than
// the number of entries, to avoid holding onto memory.
func (u *unstable) shrinkEntriesArray() {
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if cap(u.entries)*lenMultiple < len(u.entries) {
		// This shouldn't happen in practice, but guard against it.
		newEntries := make([]Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

// truncateAndAppend replaces the unstable entries from the given entries
// onward. It handles the common case where the caller's entries overlap
// with existing unstable entries.
func (u *unstable) truncateAndAppend(ents []Entry) {
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// The new entries are appended directly after the current unstable entries.
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset.
		u.offset = after
		u.offsetInProgress = u.offset
		u.entries = ents
	default:
		// Truncate to after and then append.
		u.logger.Infof("truncate the unstable entries before index %d", after)
		keep := u.entries[:after-u.offset]
		u.entries = append(keep[:len(keep):len(keep)], ents...)
		// offsetInProgress might refer to now-removed entries.
		if u.offsetInProgress > after {
			u.offsetInProgress = after
		}
	}
}

// restore sets the snapshot and resets the entry buffer.
func (u *unstable) restore(s Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.offsetInProgress = u.offset
	u.entries = nil
	u.snapshot = &s
	u.snapshotInProgress = false
}
