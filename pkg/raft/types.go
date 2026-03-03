package raft

import (
	"fmt"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a raft cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// CampaignType represents the type of campaigning.
type CampaignType string

const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (protocol-level) election.
	campaignElection CampaignType = "CampaignElection"
)

// MessageType identifies the type of raft message.
type MessageType int32

const (
	MsgHup           MessageType = 0  // internal: trigger election
	MsgBeat          MessageType = 1  // internal: trigger heartbeat
	MsgProp          MessageType = 2  // propose entry
	MsgApp           MessageType = 3  // append entries
	MsgAppResp       MessageType = 4  // append entries response
	MsgVote          MessageType = 5  // request vote
	MsgVoteResp      MessageType = 6  // request vote response
	MsgSnap          MessageType = 7  // install snapshot
	MsgHeartbeat     MessageType = 8  // heartbeat
	MsgHeartbeatResp MessageType = 9  // heartbeat response
	MsgUnreachable   MessageType = 10 // report unreachable
	MsgSnapStatus    MessageType = 11 // snapshot status report
	MsgCheckQuorum   MessageType = 12 // internal: check quorum
	MsgReadIndex     MessageType = 15 // read index request
	MsgReadIndexResp MessageType = 16 // read index response
	MsgPreVote       MessageType = 17 // pre-vote request
	MsgPreVoteResp   MessageType = 18 // pre-vote response
)

var msgTypeName = map[MessageType]string{
	MsgHup:           "MsgHup",
	MsgBeat:          "MsgBeat",
	MsgProp:          "MsgProp",
	MsgApp:           "MsgApp",
	MsgAppResp:       "MsgAppResp",
	MsgVote:          "MsgVote",
	MsgVoteResp:      "MsgVoteResp",
	MsgSnap:          "MsgSnap",
	MsgHeartbeat:     "MsgHeartbeat",
	MsgHeartbeatResp: "MsgHeartbeatResp",
	MsgUnreachable:   "MsgUnreachable",
	MsgSnapStatus:    "MsgSnapStatus",
	MsgCheckQuorum:   "MsgCheckQuorum",
	MsgReadIndex:     "MsgReadIndex",
	MsgReadIndexResp: "MsgReadIndexResp",
	MsgPreVote:       "MsgPreVote",
	MsgPreVoteResp:   "MsgPreVoteResp",
}

func (mt MessageType) String() string {
	if name, ok := msgTypeName[mt]; ok {
		return name
	}
	return fmt.Sprintf("MsgType(%d)", mt)
}

// EntryType represents the type of log entry.
type EntryType int32

const (
	EntryNormal EntryType = 0
)

// Entry is a raft log entry.
type Entry struct {
	Term  uint64    `json:"term"`
	Index uint64    `json:"index"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data"`
}

// Message is the core communication type in raft. All RPCs are represented as
// Message structs. The raft state machine produces messages; the application
// sends them over the network.
type Message struct {
	Type       MessageType `json:"type"`
	To         uint64      `json:"to"`
	From       uint64      `json:"from"`
	Term       uint64      `json:"term"`
	LogTerm    uint64      `json:"logTerm"`
	Index      uint64      `json:"index"`
	Entries    []Entry     `json:"entries,omitempty"`
	Commit     uint64      `json:"commit"`
	Vote       uint64      `json:"vote"`
	Reject     bool        `json:"reject"`
	RejectHint uint64      `json:"rejectHint"`
	Context    []byte      `json:"context,omitempty"`
	Snapshot   *Snapshot   `json:"snapshot,omitempty"`
}

func (m Message) String() string {
	return fmt.Sprintf("Type:%s From:%d To:%d Term:%d LogTerm:%d Index:%d Commit:%d Reject:%v",
		m.Type, m.From, m.To, m.Term, m.LogTerm, m.Index, m.Commit, m.Reject)
}

// HardState contains the state that must be persisted to stable storage
// before messages are sent.
type HardState struct {
	Term   uint64 `json:"term"`
	Vote   uint64 `json:"vote"`
	Commit uint64 `json:"commit"`
}

// SoftState provides state that is volatile and does not need to be persisted.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// SnapshotMetadata contains metadata for a snapshot.
type SnapshotMetadata struct {
	Index uint64   `json:"index"`
	Term  uint64   `json:"term"`
	Peers []uint64 `json:"peers,omitempty"`
}

// Snapshot is a point-in-time capture of the state machine.
type Snapshot struct {
	Data     []byte           `json:"data,omitempty"`
	Metadata SnapshotMetadata `json:"metadata"`
}

// ReadState provides state for linearizable reads.
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

// Ready encapsulates the entries and messages that are ready to be persisted,
// committed, or sent to other nodes.
//
// The application should:
// 1. Persist Entries and HardState to stable storage.
// 2. Save Entries to MemoryStorage.
// 3. Send Messages to peers.
// 4. Apply CommittedEntries to the state machine.
// 5. Call Advance().
type Ready struct {
	// SoftState will be nil if there is no update.
	*SoftState

	// HardState will be empty if there is no update.
	HardState HardState

	// ReadStates states can be used for linearizable reads.
	ReadStates []ReadState

	// Entries contains entries to persist to stable storage BEFORE Messages
	// are sent.
	Entries []Entry

	// Snapshot specifies the snapshot to save to stable storage.
	Snapshot Snapshot

	// CommittedEntries contains entries that have been committed and are
	// ready to apply to the state machine.
	CommittedEntries []Entry

	// Messages contains messages to send AFTER Entries are persisted.
	Messages []Message

	// MustSync indicates whether the HardState and Entries must be
	// synchronously written to disk.
	MustSync bool
}

// Peer represents a node in the raft cluster.
type Peer struct {
	ID uint64 `json:"id"`
}

// SnapshotStatus represents the status of a snapshot send.
type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)
