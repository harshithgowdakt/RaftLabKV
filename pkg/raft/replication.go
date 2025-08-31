package raft

import (
	"log"
	"sync"
	"time"
)

func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.RUnlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer == n.id {
			continue
		}

		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.sendAppendEntries(peer, term, leaderID, leaderCommit)
		}(peer)
	}
	wg.Wait()
}

func (n *Node) replicateToFollowers() {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.RUnlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer == n.id {
			continue
		}

		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.sendAppendEntries(peer, term, leaderID, leaderCommit)
		}(peer)
	}
	wg.Wait()

	n.updateCommitIndex()
}

func (n *Node) sendAppendEntries(peer string, term int, leaderID string, leaderCommit int) {
	n.mu.RLock()
	nextIndex := n.nextIndex[peer]
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0

	if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	entries := make([]LogEntry, 0)
	if nextIndex < len(n.log) {
		entries = n.log[nextIndex:]
	}
	n.mu.RUnlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	reply, err := n.transport.AppendEntries(peer, args)
	if err != nil {
		log.Printf("Node %s: Failed to send AppendEntries to %s: %v", n.id, peer, err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		n.nextIndex[peer] = nextIndex + len(entries)
		n.matchIndex[peer] = n.nextIndex[peer] - 1
		log.Printf("Node %s: Successfully replicated to %s, nextIndex: %d", n.id, peer, n.nextIndex[peer])
	} else {
		if n.nextIndex[peer] > 0 {
			n.nextIndex[peer]--
		}
		log.Printf("Node %s: Failed to replicate to %s, decremented nextIndex to %d", n.id, peer, n.nextIndex[peer])
	}
}

func (n *Node) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	if args.Term < n.currentTerm {
		return reply
	}

	n.lastHeartbeat = time.Now()

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}

	n.state = Follower
	reply.Term = n.currentTerm

	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(n.log) {
			return reply
		}
		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return reply
		}
	}

	if len(args.Entries) > 0 {
		n.log = n.log[:args.PrevLogIndex+1]
		n.log = append(n.log, args.Entries...)
		log.Printf("Node %s: Appended %d entries, log length: %d", n.id, len(args.Entries), len(n.log))
	}

	if args.LeaderCommit > n.commitIndex {
		newCommitIndex := args.LeaderCommit
		if len(n.log)-1 < newCommitIndex {
			newCommitIndex = len(n.log) - 1
		}
		n.commitIndex = newCommitIndex
		n.applyEntries()
	}

	reply.Success = true
	return reply
}