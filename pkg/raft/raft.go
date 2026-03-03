package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

// ErrProposalDropped is returned when a proposal is dropped.
var ErrProposalDropped = errors.New("raft proposal dropped")

// stepFunc is the function signature for the per-state step handler.
type stepFunc func(r *raft, m Message) error

// raft is the core consensus state machine. It is a pure function of its
// inputs: it never performs I/O, never spawns goroutines, and never accesses
// the clock. The application drives it through the Node/RawNode interface.
type raft struct {
	id uint64

	Term uint64
	Vote uint64

	readStates []ReadState

	// the log
	raftLog *raftLog

	maxMsgSize         uint64
	maxUncommittedSize uint64

	trk *ProgressTracker

	state StateType
	// isLearner is true if the local raft node is a learner.
	isLearner bool

	// msgs contains messages to be sent immediately (e.g., MsgApp, MsgHeartbeat).
	msgs []Message
	// msgsAfterAppend contains messages that must only be sent after the
	// entries in the same Ready have been persisted (e.g., MsgVoteResp,
	// MsgAppResp).
	msgsAfterAppend []Message

	// the leader id
	lead uint64

	// leadTransferee is id of the leader transfer target.
	leadTransferee uint64

	// Only one conf change may be pending (in the log but not yet applied)
	// at a time. This flag is set when a ConfChange entry is appended and
	// cleared when it is applied.
	pendingConfIndex uint64

	// uncommittedSize is the approximate byte size of uncommitted entries.
	uncommittedSize uint64

	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is
	// leader or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electionTimeout, 2 * electionTimeout - 1].
	randomizedElectionTimeout int

	tick func()
	step stepFunc

	logger Logger

	// rand is used to randomize election timeouts.
	rand *rand.Rand
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftlog := newLog(c.Storage, c.Logger)
	hs, peers, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &raft{
		id:                 c.ID,
		lead:               None,
		isLearner:          false,
		raftLog:            raftlog,
		maxMsgSize:         c.MaxSizePerMsg,
		maxUncommittedSize: 0,
		trk:                NewProgressTracker(c.MaxInflightMsgs),
		electionTimeout:    c.ElectionTick,
		heartbeatTimeout:   c.HeartbeatTick,
		logger:             c.Logger,
		checkQuorum:        c.CheckQuorum,
		preVote:            c.PreVote,
		readOnly:           newReadOnly(c.ReadOnlyOption),
		rand:               rand.New(rand.NewSource(int64(c.ID))),
	}

	// Initialize progress for known peers from storage.
	for _, p := range peers {
		if _, ok := r.trk.Progress[p]; !ok {
			r.trk.Progress[p] = &Progress{
				Next:      1,
				Inflights: NewInflights(r.trk.MaxInflight),
			}
		}
	}

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}

	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
		raftlog.applying = c.Applied
	}

	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.trk.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}
	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied,
		r.raftLog.lastIndex(), r.raftLog.lastTerm())

	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState {
	return &SoftState{Lead: r.lead, RaftState: r.state}
}

func (r *raft) hardState() HardState {
	return HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) quorum() int { return len(r.trk.Progress)/2 + 1 }

// send appends a message to the outgoing message queue. If the message is
// a response type (MsgAppResp, MsgVoteResp, etc.), it goes to msgsAfterAppend
// to ensure it is only sent after state is persisted.
func (r *raft) send(m Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.Type == MsgVote || m.Type == MsgVoteResp || m.Type == MsgPreVote || m.Type == MsgPreVoteResp {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// Do not attach term to MsgProp, MsgReadIndex (they are local only).
		if m.Type != MsgProp && m.Type != MsgReadIndex {
			m.Term = r.Term
		}
	}

	if IsResponseMsg(m.Type) {
		r.msgsAfterAppend = append(r.msgsAfterAppend, m)
	} else {
		r.msgs = append(r.msgs, m)
	}
}

// sendAppend sends an AppendEntries RPC with new entries to the given peer.
// Returns true if a message was sent.
func (r *raft) sendAppend(to uint64) bool {
	return r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an AppendEntries RPC to the given peer.
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.trk.Progress[to]
	if pr.IsPaused() {
		return false
	}

	m := Message{}
	m.To = to

	prevLogIndex := pr.Next - 1
	prevLogTerm, errt := r.raftLog.term(prevLogIndex)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	// If we can't get the term or entries (compacted), send snapshot.
	if errt != nil || erre != nil {
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		m.Type = MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			panic(err)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.BecomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
		r.send(m)
		return true
	}

	m.Type = MsgApp
	m.Index = prevLogIndex
	m.LogTerm = prevLogTerm
	m.Entries = ents
	m.Commit = r.raftLog.committed

	// Update progress to reflect what we're about to send.
	if n := len(m.Entries); n != 0 {
		switch pr.State {
		case ProgressStateReplicate:
			last := m.Entries[n-1].Index
			pr.Next = last + 1
			pr.Inflights.Add(last)
		case ProgressStateProbe:
			pr.MsgAppFlowPaused = true
		default:
			r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
		}
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat to the given peer.
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// commit is min(to.match, r.committed). This is to reduce the number
	// of entries sent in the next MsgApp if the follower rejects.
	commit := min(r.trk.Progress[to].Match, r.raftLog.committed)
	m := Message{
		To:      to,
		Type:    MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}
	r.send(m)
}

// bcastAppend sends AppendEntries to all peers except self.
func (r *raft) bcastAppend() {
	for id := range r.trk.Progress {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// bcastHeartbeat sends heartbeats to all peers except self.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for id := range r.trk.Progress {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id, ctx)
	}
}

// maybeCommit attempts to advance the commit index based on the match
// indices tracked in the ProgressTracker.
func (r *raft) maybeCommit() bool {
	mci := r.trk.Committed()
	return r.raftLog.maybeCommit(mci, r.Term)
}

// raftLog.maybeCommit: commit if entries at mci have the current term.
func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// reset resets the internal state for a new term.
func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.leadTransferee = None

	r.trk.ResetVotes()
	for id, pr := range r.trk.Progress {
		*pr = Progress{
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: NewInflights(r.trk.MaxInflight),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	}

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
	r.readOnly = newReadOnly(r.readOnly.option)
}

// becomeFollower transitions to follower state.
func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transitions to candidate state.
func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomePreCandidate transitions to pre-candidate state. Unlike candidate,
// this does NOT increment the term.
func (r *raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	r.step = stepCandidate
	r.trk.ResetVotes()
	r.tick = r.tickElection
	r.leadTransferee = None
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

// becomeLeader transitions to leader state.
func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending conf change at that index,
	// but if there is, we don't want to allow another one.
	r.pendingConfIndex = r.raftLog.lastIndex()

	// Append an empty entry to establish leadership. This is needed so that
	// the leader can commit entries from previous terms via the empty entry.
	emptyEnt := Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		r.logger.Panic("empty entry was dropped")
	}

	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

// appendEntry appends entries to the leader's log and broadcasts them.
// Returns true if the entries were accepted.
func (r *raft) appendEntry(es ...Entry) bool {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.append(es...)
	// The leader needs to track its own progress.
	r.trk.Progress[r.id].MaybeUpdate(r.raftLog.lastIndex())
	// Commit right away if possible (single-node cluster).
	r.maybeCommit()
	return true
}

func (r *raft) tickElection() {
	r.electionElapsed++

	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(Message{From: r.id, Type: MsgHup})
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			r.Step(Message{From: r.id, Type: MsgCheckQuorum})
		}
	}

	if r.state != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(Message{From: r.id, Type: MsgBeat})
	}
}

// campaign transitions the raft to candidate state and sends vote requests
// to all peers.
func (r *raft) campaign(t CampaignType) {
	if !r.promotable() {
		r.logger.Warningf("%x is unpromotable and can not campaign", r.id)
		return
	}

	var term uint64
	var voteMsg MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = MsgPreVote
		// PreVote RPCs are sent with the term that would be used if the
		// pre-candidate wins the election.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = MsgVote
		term = r.Term
	}

	// Vote for self.
	r.trk.RecordVote(r.id, true)
	if _, _, res := r.trk.TallyVotes(); res == VoteWon {
		// Single node cluster. We won.
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}

	// Send vote requests.
	for id := range r.trk.Progress {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		r.send(Message{
			Term:    term,
			To:      id,
			Type:    voteMsg,
			Index:   r.raftLog.lastIndex(),
			LogTerm: r.raftLog.lastTerm(),
			Context: ctx,
		})
	}
}

// poll records a vote from the given node and returns the tally.
func (r *raft) poll(id uint64, t MessageType, v bool) (granted int, rejected int, result VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.trk.RecordVote(id, v)
	return r.trk.TallyVotes()
}

// Step is the main entry point for the state machine. It handles messages.
func (r *raft) Step(m Message) error {
	// Handle term checks.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			force := (m.Context != nil && string(m.Context) == "campaign")
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum
				// election timeout of hearing from a current leader, it does
				// not update its term or grant its vote.
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
				return nil
			}
		}
		switch {
		case m.Type == MsgPreVote:
			// Don't change term for PreVote.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// We won a pre-vote, but don't change term yet.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}
	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == MsgHeartbeat || m.Type == MsgApp) {
			// We have received messages from a leader at a lower term. It is
			// possible that these messages were simply delayed in the network,
			// but this could also mean that this node has advanced its term
			// number during a network partition, and it is now unable to either
			// win an election or to rejoin the majority on the old term. If
			// checkQuorum is false, this will be handled by incrementing term
			// numbers in response to MsgVote with a higher term, but if
			// checkQuorum is true we may not advance the term on MsgVote and
			// must generate other messages to advance the term. The net result
			// of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration.
			r.send(Message{To: m.From, Type: MsgAppResp})
		} else if m.Type == MsgPreVote {
			// Before Pre-Vote, nodes may send stale MsgPreVote messages after
			// a network partition. We should reject these.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(Message{To: m.From, Term: r.Term, Type: MsgPreVoteResp, Reject: true})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {
	case MsgHup:
		if r.preVote {
			r.campaign(campaignPreElection)
		} else {
			r.campaign(campaignElection)
		}
	case MsgVote, MsgPreVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...or we haven't voted and we don't think there's a leader...
			(r.Vote == None && r.lead == None) ||
			// ...or this is a PreVote for a future term.
			(m.Type == MsgPreVote && m.Term > r.Term)

		// ...and we believe the candidate's log is up-to-date.
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote,
				m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == MsgVote {
				// Only record that we've voted in a real election.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote,
				m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}
	default:
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func stepLeader(r *raft, m Message) error {
	switch m.Type {
	case MsgBeat:
		r.bcastHeartbeat()
		return nil
	case MsgCheckQuorum:
		// The leader should check quorum activity. If a quorum of voters is
		// not active, the leader steps down.
		if !r.trk.QuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		// Mark all as inactive for the next check.
		for _, pr := range r.trk.Progress {
			pr.RecentActive = false
		}
		// Self is always active.
		r.trk.Progress[r.id].RecentActive = true
		return nil
	case MsgProp:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if _, ok := r.trk.Progress[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal",
				r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		r.bcastAppend()
		return nil
	case MsgReadIndex:
		// Only the leader handles ReadIndex. For a single-node cluster, we
		// can respond immediately.
		if r.quorum() <= 1 {
			ri := r.raftLog.committed
			if m.From == None || m.From == r.id {
				r.readStates = append(r.readStates, ReadState{Index: ri, RequestCtx: m.Entries[0].Data})
			} else {
				r.send(Message{To: m.From, Type: MsgReadIndexResp, Index: ri, Entries: m.Entries})
			}
			return nil
		}

		// Use heartbeat responses to confirm leadership for reads.
		switch r.readOnly.option {
		case ReadOnlySafe:
			r.readOnly.addRequest(r.raftLog.committed, m)
			// The leader needs to know that it is still the leader by
			// getting acks from a quorum.
			r.bcastHeartbeatWithCtx(m.Entries[0].Data)
		case ReadOnlyLeaseBased:
			ri := r.raftLog.committed
			if m.From == None || m.From == r.id {
				r.readStates = append(r.readStates, ReadState{Index: ri, RequestCtx: m.Entries[0].Data})
			} else {
				r.send(Message{To: m.From, Type: MsgReadIndexResp, Index: ri, Entries: m.Entries})
			}
		}
		return nil
	}

	// All other message types need to have a target progress.
	pr := r.trk.Progress[m.From]
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}

	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true

		if m.Reject {
			r.logger.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
				r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
			// Use the hint to find a better next index.
			nextProbeIdx := m.RejectHint
			if m.LogTerm > 0 {
				nextProbeIdx, _ = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					pr.BecomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					pr.BecomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot:
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]",
						r.id, m.From, pr)
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == ProgressStateReplicate:
					pr.Inflights.FreeLE(m.Index)
				}

				if r.maybeCommit() {
					// Committed entries changed, broadcast new commit index.
					r.bcastAppend()
				} else if oldPaused {
					// If we were paused before, send more entries now.
					r.sendAppend(m.From)
				}
			}
		}
	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.MsgAppFlowPaused = false

		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}

		ackCount := len(r.readOnly.recvAck(m.From, m.Context))
		if ackCount < r.quorum() {
			return nil
		}

		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			req := rs.req
			if req.From == None || req.From == r.id {
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				r.send(Message{To: req.From, Type: MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case MsgSnapStatus:
		if pr.State != ProgressStateSnapshot {
			return nil
		}
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]",
				r.id, m.From, pr)
		} else {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]",
				r.id, m.From, pr)
		}
		// If snapshot succeeded, the follower will tell us via MsgAppResp.
		// In both cases, resume probing.
		pr.MsgAppFlowPaused = false
	case MsgUnreachable:
		if pr.State == ProgressStateReplicate {
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]",
			r.id, m.From, pr)
	}
	return nil
}

func stepCandidate(r *raft, m Message) error {
	// Only handle vote responses while in (pre-)candidate state.
	var myVoteRespType MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = MsgPreVoteResp
	} else {
		myVoteRespType = MsgVoteResp
	}

	switch m.Type {
	case MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType:
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res {
		case VoteWon:
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case VoteLost:
			r.becomeFollower(r.Term, None)
		}
	case MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func stepFollower(r *raft, m Message) error {
	switch m.Type {
	case MsgProp:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		m.To = r.lead
		r.send(m)
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d",
				r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

// handleAppendEntries handles an AppendEntries RPC (MsgApp).
func (r *raft) handleAppendEntries(m Message) {
	if m.Index < r.raftLog.committed {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(Message{To: m.From, Type: MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)

		// Return a hint to help the leader find the conflict faster.
		hintIndex, hintTerm := r.raftLog.findConflictByTerm(m.Index, m.LogTerm)
		r.send(Message{
			To:         m.From,
			Type:       MsgAppResp,
			Index:      m.Index,
			Reject:     true,
			RejectHint: hintIndex,
			LogTerm:    hintTerm,
		})
	}
}

// handleHeartbeat handles a heartbeat RPC (MsgHeartbeat).
func (r *raft) handleHeartbeat(m Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(Message{To: m.From, Type: MsgHeartbeatResp, Context: m.Context})
}

// handleSnapshot handles an InstallSnapshot RPC (MsgSnap).
func (r *raft) handleSnapshot(m Message) {
	if m.Snapshot == nil {
		return
	}
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(*m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(Message{To: m.From, Type: MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore attempts to restore the given snapshot. Returns true if the
// snapshot was accepted.
func (r *raft) restore(s Snapshot) bool {
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.state != StateFollower {
		r.logger.Warningf("%x attempted to restore snapshot as leader; should not happen", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(),
		s.Metadata.Index, s.Metadata.Term)

	r.raftLog.restore(s)

	// Reset progress for all peers.
	r.trk.Progress = make(map[uint64]*Progress)
	for _, p := range s.Metadata.Peers {
		r.trk.Progress[p] = &Progress{
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: NewInflights(r.trk.MaxInflight),
		}
	}
	return true
}

// promotable returns true if this node can be promoted to leader.
func (r *raft) promotable() bool {
	pr := r.trk.Progress[r.id]
	return pr != nil && !r.isLearner
}

func (r *raft) loadState(state HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]",
			r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)
}

// Sorted helper for ProgressTracker
func (r *raft) nodes() []uint64 {
	nodes := r.trk.VoterNodes()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}
