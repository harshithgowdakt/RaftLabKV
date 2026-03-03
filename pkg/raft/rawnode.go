package raft

import "fmt"

// RawNode is a thread-unsafe wrapper around raft that implements the
// Ready/Advance lifecycle. The application uses RawNode (or Node which
// wraps it) to interact with the raft state machine.
type RawNode struct {
	raft *raft

	prevSoftSt *SoftState
	prevHardSt HardState

	// stepsOnAdvance are messages that are queued to be stepped when
	// Advance() is called. This is used for self-directed messages like
	// MsgAppResp after the leader appends entries.
	stepsOnAdvance []Message
}

// NewRawNode initializes a RawNode from the given config.
func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config)
	rn := &RawNode{raft: r}
	rn.prevSoftSt = r.softState()
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.raft.tick()
}

// Campaign causes the RawNode to transition to candidate state and
// start campaigning to become leader.
func (rn *RawNode) Campaign() error {
	return rn.raft.Step(Message{
		Type: MsgHup,
	})
}

// Propose proposes that data be appended to the log.
func (rn *RawNode) Propose(data []byte) error {
	return rn.raft.Step(Message{
		Type:    MsgProp,
		From:    rn.raft.id,
		Entries: []Entry{{Data: data}},
	})
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m Message) error {
	// Ignore unexpected local messages received over network.
	if IsLocalMsg(m.Type) {
		return nil
	}
	if pr := rn.raft.trk.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
		return rn.raft.Step(m)
	}
	return ErrProposalDropped
}

// ReadIndex requests a read state. The read state will be set in the
// Ready. Read state has a read index. Once the application advances
// further than the read index, any linearizable read requests issued
// before the read request can be processed safely. The read state will
// have the same rctx attached.
func (rn *RawNode) ReadIndex(rctx []byte) {
	_ = rn.raft.Step(Message{
		Type:    MsgReadIndex,
		Entries: []Entry{{Data: rctx}},
	})
}

// HasReady returns true if there is any Ready to consume.
func (rn *RawNode) HasReady() bool {
	r := rn.raft
	if !r.softState().equal(rn.prevSoftSt) {
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	if r.raftLog.hasNextUnstableEnts() || r.raftLog.hasNextCommittedEnts() {
		return true
	}
	if len(r.msgs) > 0 || len(r.msgsAfterAppend) > 0 {
		return true
	}
	if len(r.readStates) > 0 {
		return true
	}
	if r.raftLog.hasNextUnstableSnapshot() {
		return true
	}
	return false
}

// Ready returns the outstanding work that the application needs to handle.
// This includes persisting entries, sending messages, and applying committed
// entries. After handling the Ready, the application must call Advance().
func (rn *RawNode) Ready() Ready {
	rd := rn.readyWithoutAccept()
	rn.acceptReady(rd)
	return rd
}

// readyWithoutAccept returns the Ready without marking entries as accepted.
func (rn *RawNode) readyWithoutAccept() Ready {
	r := rn.raft
	rd := Ready{
		Entries:          r.raftLog.nextUnstableEnts(),
		CommittedEntries: r.raftLog.nextCommittedEnts(),
		// Merge msgs and msgsAfterAppend. In synchronous mode, by the time
		// the application sends these messages, the entries will already be
		// persisted (the app persists first, then sends).
		Messages: r.msgs,
	}

	// Append msgsAfterAppend to Messages.
	if len(r.msgsAfterAppend) > 0 {
		rd.Messages = append(rd.Messages, r.msgsAfterAppend...)
	}

	softSt := r.softState()
	if !softSt.equal(rn.prevSoftSt) {
		rd.SoftState = softSt
	}

	hardSt := r.hardState()
	if !isHardStateEqual(hardSt, rn.prevHardSt) {
		rd.HardState = hardSt
	}

	if r.raftLog.hasNextUnstableSnapshot() {
		rd.Snapshot = *r.raftLog.nextUnstableSnapshot()
	}

	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}

	rd.MustSync = MustSync(rd.HardState, rn.prevHardSt, len(rd.Entries))

	return rd
}

// acceptReady marks the entries and messages as accepted and queues self-
// directed messages to be processed on Advance.
func (rn *RawNode) acceptReady(rd Ready) {
	r := rn.raft

	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}

	if len(rd.ReadStates) != 0 {
		r.readStates = nil
	}

	// Mark unstable entries as in-progress.
	r.raftLog.acceptUnstable()

	// Mark committed entries as being applied.
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		r.raftLog.acceptApplying(last.Index)
	}

	// Clear messages.
	r.msgs = nil
	r.msgsAfterAppend = nil
}

// Advance notifies the RawNode that the application has persisted and
// applied all entries in the last Ready. It signals the RawNode to prepare
// new Ready entries if available.
func (rn *RawNode) Advance(rd Ready) {
	r := rn.raft

	// Update hard state.
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}

	// Stabilize entries.
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}

	// Stabilize snapshot.
	if !IsEmptySnap(rd.Snapshot) {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}

	// Mark committed entries as applied.
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		r.raftLog.appliedTo(last.Index)
	}

	// Process queued self-directed messages (e.g., leader's MsgAppResp to self).
	for _, m := range rn.stepsOnAdvance {
		_ = r.Step(m)
	}
	rn.stepsOnAdvance = nil
}

// Bootstrap initializes the RawNode for first use by appending configuration
// entries. This must be called before starting the node and must only be
// called for new clusters.
func (rn *RawNode) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return errorf("must provide at least one peer")
	}

	lastIndex, err := rn.raft.raftLog.storage.LastIndex()
	if err != nil {
		return err
	}
	if lastIndex != 0 {
		return errorf("can't bootstrap a nonempty Storage")
	}

	// Register all peers in the progress tracker.
	for _, peer := range peers {
		rn.raft.trk.Progress[peer.ID] = &Progress{
			Next:      1,
			Inflights: NewInflights(rn.raft.trk.MaxInflight),
		}
	}

	// Reset now that we know the peers.
	rn.raft.reset(1)
	rn.prevSoftSt = rn.raft.softState()
	rn.prevHardSt = rn.raft.hardState()
	return nil
}

// GetStatus returns the current status of the raft state machine.
func (rn *RawNode) GetStatus() Status {
	return getStatus(rn.raft)
}

func isHardStateEqual(a, b HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func errorf(format string, a ...any) error {
	return fmt.Errorf(format, a...)
}
