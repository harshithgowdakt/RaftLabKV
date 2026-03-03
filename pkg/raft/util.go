package raft

import "encoding/json"

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st HardState) bool {
	return st.Term == 0 && st.Vote == 0 && st.Commit == 0
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp Snapshot) bool {
	return sp.Metadata.Index == 0
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst HardState, entsnum int) bool {
	// Persist if we have entries, or the hard state changed.
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

// voteRespMsgType maps a vote message type to its corresponding response type.
func voteRespMsgType(msgt MessageType) MessageType {
	switch msgt {
	case MsgVote:
		return MsgVoteResp
	case MsgPreVote:
		return MsgPreVoteResp
	default:
		panic("not a vote message")
	}
}

// IsLocalMsg returns true if the message type is internal to the node.
func IsLocalMsg(msgt MessageType) bool {
	return msgt == MsgHup || msgt == MsgBeat || msgt == MsgCheckQuorum
}

// IsResponseMsg returns true if the message type is a response.
func IsResponseMsg(msgt MessageType) bool {
	return msgt == MsgAppResp || msgt == MsgVoteResp || msgt == MsgHeartbeatResp || msgt == MsgPreVoteResp
}

// limitSize limits the total size of entries by the maxSize. It always returns
// at least one entry if there are any.
func limitSize(ents []Entry, maxSize uint64) []Entry {
	if len(ents) == 0 {
		return ents
	}
	if maxSize == 0 {
		return ents
	}
	size := entrySize(ents[0])
	for i := 1; i < len(ents); i++ {
		size += entrySize(ents[i])
		if size > maxSize {
			return ents[:i]
		}
	}
	return ents
}

// entrySize returns the approximate size of an entry.
func entrySize(e Entry) uint64 {
	// Use JSON encoding size as an approximation.
	b, _ := json.Marshal(e)
	return uint64(len(b))
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
