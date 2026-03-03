package raft

import "fmt"

// raftLog manages the log for the raft consensus module. It combines persisted
// entries in Storage with not-yet-persisted entries in the unstable buffer.
type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// unstable contains all unstable entries and snapshot.
	unstable unstable

	// committed is the highest log position that is known to be in stable
	// storage on a quorum of nodes.
	committed uint64

	// applying is the highest log position that the application has been
	// instructed to apply to its state machine. Some of these entries may
	// have been applied already. Use applied to track those.
	applying uint64

	// applied is the highest log position that the application has
	// successfully applied to its state machine.
	applied uint64

	logger Logger

	// maxNextCommittedEntsSize is the maximum aggregate byte size of
	// committed entries returned from nextCommittedEnts.
	maxNextCommittedEntsSize uint64
}

// newLog returns a new raftLog using the given storage.
func newLog(storage Storage, logger Logger) *raftLog {
	if storage == nil {
		panic("storage must not be nil")
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	log := &raftLog{
		storage: storage,
		unstable: unstable{
			offset:           lastIndex + 1,
			offsetInProgress: lastIndex + 1,
			logger:           logger,
		},
		logger: logger,
		// Initialize committed and applied to the position just before the
		// first stored entry (the compaction point).
		committed: firstIndex - 1,
		applying:  firstIndex - 1,
		applied:   firstIndex - 1,
	}
	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, applying=%d, unstable.offset=%d, len(unstable.Entries)=%d",
		l.committed, l.applied, l.applying, l.unstable.offset, len(l.unstable.entries))
}

// lastIndex returns the index of the last entry in the log.
func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// lastTerm returns the term of the last entry in the log.
func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// term returns the term of entry at the given index.
func (l *raftLog) term(i uint64) (uint64, error) {
	// The valid term range is [index of dummy/snapshot entry, last index].
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		return 0, nil
	}

	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

// firstIndex returns the index of the first available entry.
func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// append adds new entries to the unstable buffer.
func (l *raftLog) append(ents ...Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// maybeAppend returns (lastNewIndex, true) if the entries can be appended.
// This is called on followers receiving MsgApp. If the leader's entry at
// prevLogIndex/prevLogTerm doesn't match our log, returns (0, false).
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...Entry) (lastNewI uint64, ok bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}

	lastNewI = index + uint64(len(ents))
	ci := l.findConflict(ents)
	switch {
	case ci == 0:
		// no conflict, all entries already present
	case ci <= l.committed:
		l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
	default:
		offset := index + 1
		// Append only the entries from the conflict point onward.
		l.append(ents[ci-offset:]...)
	}
	l.commitTo(min(committed, lastNewI))
	return lastNewI, true
}

// findConflict finds the index of the first entry in ents that conflicts
// with the existing log. A conflict is when an entry exists at the same
// index but has a different term. If there is no conflict, and all entries
// in ents are already present in the log, 0 is returned. If there is no
// conflict but new entries are present, the index of the first new entry
// is returned.
func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

// findConflictByTerm returns a best-guess (index, term) for where the
// leader's log and ours diverge. Used as a RejectHint to help the leader
// skip back efficiently.
func (l *raftLog) findConflictByTerm(index uint64, term uint64) (uint64, uint64) {
	if li := l.lastIndex(); index > li {
		// The leader's index is beyond our log; start from our last entry.
		l.logger.Infof("findConflictByTerm(%d, %d) [lastIndex: %d] -> (%d, 0)",
			index, term, li, li)
		return li, 0
	}
	for {
		logTerm, err := l.term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	t, _ := l.term(index)
	return index, t
}

// nextUnstableEnts returns all entries that have not yet been made available
// for persistence.
func (l *raftLog) nextUnstableEnts() []Entry {
	return l.unstable.nextEntries()
}

// nextUnstableSnapshot returns the snapshot if it hasn't been handed off yet.
func (l *raftLog) nextUnstableSnapshot() *Snapshot {
	return l.unstable.nextSnapshot()
}

// hasNextUnstableEnts returns true if there are entries not yet in-progress.
func (l *raftLog) hasNextUnstableEnts() bool {
	return len(l.nextUnstableEnts()) > 0
}

// hasNextUnstableSnapshot returns true if there is a snapshot not yet in-progress.
func (l *raftLog) hasNextUnstableSnapshot() bool {
	return l.unstable.nextSnapshot() != nil
}

// nextCommittedEnts returns the committed entries that have not yet been
// applied.
func (l *raftLog) nextCommittedEnts() []Entry {
	if l.applying >= l.committed {
		return nil
	}
	lo := l.applying + 1
	hi := l.committed + 1
	ents, err := l.slice(lo, hi, l.maxNextCommittedEntsSize)
	if err != nil {
		l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
	}
	return ents
}

// hasNextCommittedEnts returns true if there are committed entries not yet applied.
func (l *raftLog) hasNextCommittedEnts() bool {
	return l.applying < l.committed
}

// acceptApplying marks entries up to the given index as being applied.
func (l *raftLog) acceptApplying(i uint64) {
	if l.committed < i {
		l.logger.Panicf("applying(%d) is out of range [committed(%d)]", i, l.committed)
	}
	l.applying = i
}

// appliedTo marks entries up to the given index as applied.
func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]",
			i, l.applied, l.committed)
	}
	l.applied = i
}

// acceptUnstable marks all current unstable entries as in-progress.
func (l *raftLog) acceptUnstable() {
	l.unstable.acceptInProgress()
}

// stableTo marks entries up to the given index and term as persisted.
func (l *raftLog) stableTo(i, t uint64) {
	l.unstable.stableTo(i, t)
}

// stableSnapTo marks the snapshot as persisted.
func (l *raftLog) stableSnapTo(i uint64) {
	l.unstable.stableSnapTo(i)
}

// commitTo sets the committed index.
func (l *raftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?",
				tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

// matchTerm returns true if the log has an entry at the given index with the
// given term.
func (l *raftLog) matchTerm(i, t uint64) bool {
	term, err := l.term(i)
	if err != nil {
		return false
	}
	return term == t
}

// isUpToDate determines if the given (lastIndex, term) is at least as
// up-to-date as the last entry in this log.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

// entries returns entries starting from index i, capped by maxSize.
func (l *raftLog) entries(i, maxSize uint64) ([]Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxSize)
}

// allEntries returns all available entries.
func (l *raftLog) allEntries() []Entry {
	ents, err := l.entries(l.firstIndex(), 0)
	if err == nil {
		return ents
	}
	if err == ErrCompacted {
		return l.allEntries()
	}
	panic(err)
}

// slice returns a slice of log entries from lo to hi-1, capped by maxSize.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}

	var ents []Entry
	if lo < l.unstable.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err)
		}

		// Check if we got fewer entries than expected due to size limit.
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}

	if hi > l.unstable.offset {
		unstable := l.unstable.entries[max(lo, l.unstable.offset)-l.unstable.offset : hi-l.unstable.offset]
		if len(ents) > 0 {
			combined := make([]Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}
	length := l.lastIndex() + 1 - fi
	if hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

// restore sets the log state to match the given snapshot.
func (l *raftLog) restore(s Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]",
		l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// snapshot returns the current snapshot from unstable, if any, or from storage.
func (l *raftLog) snapshot() (Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
