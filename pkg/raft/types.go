package raft

import (
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	Index   int         `json:"index"`
	Term    int         `json:"term"`
	Command interface{} `json:"command"`
}

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

type Node struct {
	mu          sync.RWMutex
	id          string
	peers       []string
	state       NodeState
	currentTerm int
	votedFor    string
	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time

	applyCh chan LogEntry
	stopCh  chan struct{}

	transport Transport
}

type Transport interface {
	RequestVote(target string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(target string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}