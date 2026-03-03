package raft

import "sort"

// ProgressStateType is the state of progress for a single follower.
type ProgressStateType int

const (
	// ProgressStateProbe indicates that the leader sends at most one
	// replication message per heartbeat interval. It slowly finds the
	// follower's progress.
	ProgressStateProbe ProgressStateType = iota

	// ProgressStateReplicate is the state when the leader sends entries
	// optimistically.
	ProgressStateReplicate

	// ProgressStateSnapshot indicates that the leader is sending a snapshot.
	ProgressStateSnapshot
)

var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

func (st ProgressStateType) String() string {
	return prstmap[st]
}

// Inflights limits the number of in-flight MsgApp messages to a follower.
// It uses a ring buffer to track the index of each in-flight message.
type Inflights struct {
	start int       // ring buffer start
	count int       // number of in-flight messages
	size  int       // max number of in-flight
	buffer []uint64 // ring buffer of entry indices
}

// NewInflights creates a new Inflights with the given capacity.
func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

// Clone returns a copy of the Inflights.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = append([]uint64(nil), in.buffer...)
	return &ins
}

// Full returns true if no more messages can be sent.
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count returns the number of in-flight messages.
func (in *Inflights) Count() int {
	return in.count
}

// Add adds an inflight message with the given index.
func (in *Inflights) Add(inflight uint64) {
	if in.Full() {
		panic("cannot add into a Full inflights")
	}
	next := in.start + in.count
	if next >= len(in.buffer) {
		// Grow buffer up to capacity.
		if next >= in.size {
			next -= in.size
		}
		if next >= len(in.buffer) {
			in.grow()
		}
	}
	in.buffer[next] = inflight
	in.count++
}

// grow grows the buffer by doubling up to the max size.
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	}
	if newSize > in.size {
		newSize = in.size
	}
	newBuf := make([]uint64, newSize)
	copy(newBuf, in.buffer)
	in.buffer = newBuf
}

// FreeLE frees the inflights smaller or equal to the given index.
func (in *Inflights) FreeLE(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] {
			break
		}
		idx++
		if idx >= len(in.buffer) {
			idx -= len(in.buffer)
		}
	}
	// Free i inflights.
	in.count -= i
	in.start = idx
	if in.count == 0 {
		// Reset to avoid holding onto a large buffer.
		in.start = 0
	}
}

// Reset frees all inflights.
func (in *Inflights) Reset() {
	in.count = 0
	in.start = 0
}

// Progress represents a follower's progress as seen by the leader.
type Progress struct {
	Match, Next uint64

	// State is the current progress state (Probe, Replicate, or Snapshot).
	State ProgressStateType

	// PendingSnapshot is the index of the snapshot being sent (if any).
	PendingSnapshot uint64

	// RecentActive is true if the progress has been active recently.
	// It is reset to false by the leader on each CheckQuorum tick.
	RecentActive bool

	// MsgAppFlowPaused is used in ProgressStateProbe. When true, raft will
	// not send replication messages to this peer until it is reset.
	MsgAppFlowPaused bool

	// Inflights is the in-flight message tracker.
	Inflights *Inflights

	// IsLearner is true if this is a learner node.
	IsLearner bool
}

// ResetState resets the progress to the given state.
func (pr *Progress) ResetState(state ProgressStateType) {
	pr.MsgAppFlowPaused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.Reset()
}

// BecomeProbe transitions into ProgressStateProbe. Next is reset to
// Match+1 or to PendingSnapshot+1 after a snapshot.
func (pr *Progress) BecomeProbe() {
	if pr.State == ProgressStateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.ResetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

// BecomeReplicate transitions into ProgressStateReplicate.
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot transitions into ProgressStateSnapshot with the given
// snapshot index.
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	pr.ResetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// MaybeUpdate returns false if the given index comes from an outdated
// message. Otherwise it updates the progress and returns true.
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.MsgAppFlowPaused = false
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

// MaybeDecrTo adjusts the progress on a rejection. It returns false if
// the rejection is stale (the rejectIndex is not in the probing window).
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if pr.State == ProgressStateReplicate {
		// In replicate state, the rejection must be stale if the rejected
		// index doesn't match next-1.
		if rejected <= pr.Match {
			return false
		}
		pr.Next = pr.Match + 1
		return true
	}

	// The rejection must be stale if it doesn't match our expected next.
	if pr.Next-1 != rejected {
		return false
	}

	pr.Next = max(min(rejected, matchHint+1), pr.Match+1)
	pr.MsgAppFlowPaused = false
	return true
}

// IsPaused returns whether sending log entries to this node has been throttled.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
		return pr.MsgAppFlowPaused
	case ProgressStateReplicate:
		return pr.Inflights.Full()
	case ProgressStateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

// ProgressTracker tracks the progress of all peers.
type ProgressTracker struct {
	Progress map[uint64]*Progress

	Votes map[uint64]bool

	MaxInflight int
}

// NewProgressTracker creates a new ProgressTracker.
func NewProgressTracker(maxInflight int) *ProgressTracker {
	return &ProgressTracker{
		Progress:    make(map[uint64]*Progress),
		Votes:       make(map[uint64]bool),
		MaxInflight: maxInflight,
	}
}

// VoterNodes returns a sorted slice of voter node IDs.
func (p *ProgressTracker) VoterNodes() []uint64 {
	nodes := make([]uint64, 0, len(p.Progress))
	for id := range p.Progress {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// Committed returns the committed index based on the match indices of the
// quorum. It uses the sorted-median approach.
func (p *ProgressTracker) Committed() uint64 {
	mis := make([]uint64, 0, len(p.Progress))
	for _, pr := range p.Progress {
		mis = append(mis, pr.Match)
	}
	sort.Slice(mis, func(i, j int) bool { return mis[i] < mis[j] })

	// The quorum position is the median (for odd) or the lower of the two
	// middle values (for even). That is, index = len/2 for 0-based.
	// For n nodes, quorum is (n/2)+1, so the commit index is at
	// position n - quorum = n - n/2 - 1 when sorted ascending.
	// Equivalently, position (n-1)/2.
	return mis[(len(mis)-1)/2]
}

// QuorumActive returns true if a quorum of voters has been recently active.
func (p *ProgressTracker) QuorumActive() bool {
	active := 0
	for _, pr := range p.Progress {
		if pr.RecentActive {
			active++
		}
	}
	return 2*active > len(p.Progress)
}

// RecordVote records that the node with the given id voted for us (v=true)
// or against us (v=false).
func (p *ProgressTracker) RecordVote(id uint64, v bool) {
	_, ok := p.Votes[id]
	if !ok {
		p.Votes[id] = v
	}
}

// TallyVotes returns the count of granted and rejected votes, and the
// result of the election.
type VoteResult byte

const (
	VotePending VoteResult = iota
	VoteWon
	VoteLost
)

// TallyVotes returns the vote counts and overall result.
func (p *ProgressTracker) TallyVotes() (granted int, rejected int, result VoteResult) {
	for _, v := range p.Votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}
	q := len(p.Progress)/2 + 1
	if granted >= q {
		return granted, rejected, VoteWon
	}
	if rejected >= q {
		return granted, rejected, VoteLost
	}
	return granted, rejected, VotePending
}

// ResetVotes resets the vote tracking.
func (p *ProgressTracker) ResetVotes() {
	p.Votes = make(map[uint64]bool)
}
