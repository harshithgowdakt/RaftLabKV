package raft

import (
	"errors"
	"fmt"
)

// Config contains the parameters to start a raft node.
type Config struct {
	// ID is the identity of the local raft node. ID cannot be 0.
	ID uint64

	// ElectionTick is the number of Node.Tick invocations that must pass
	// between elections. That is, if a follower does not receive any message
	// from the leader of current term before ElectionTick has elapsed, it
	// will become candidate and start an election.
	// ElectionTick must be greater than HeartbeatTick.
	ElectionTick int

	// HeartbeatTick is the number of Node.Tick invocations that must pass
	// between heartbeats. That is, a leader sends heartbeat messages to
	// maintain its leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to
	// be stored in storage. raft reads the persisted entries and states out
	// of Storage when it needs to restart.
	Storage Storage

	// Applied is the last applied index. It should only be set when
	// restarting raft. raft will not return entries to the application
	// smaller or equal to Applied.
	Applied uint64

	// MaxSizePerMsg limits the max byte size of each append message.
	// 0 means no limit.
	MaxSizePerMsg uint64

	// MaxInflightMsgs limits the max number of in-flight append messages
	// during optimistic replication phase. The application transportation
	// layer usually has its own sending buffer over TCP/UDP. Set to avoid
	// overflowing that sending buffer.
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity.
	// When CheckQuorum is enabled, the leader steps down if it has not
	// heard from a quorum of voters within an election timeout.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in section 9.6 of
	// the Raft thesis. This prevents disruption when a node that has been
	// partitioned away rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. If nil, a default logger
	// writing to stderr will be used.
	Logger Logger
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxInflightMsgs <= 0 {
		return fmt.Errorf("max inflight messages must be greater than 0, got %d", c.MaxInflightMsgs)
	}

	if c.Logger == nil {
		c.Logger = defaultLogger
	}

	return nil
}
