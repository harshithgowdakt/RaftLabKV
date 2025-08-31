package raft

import (
	"log"
	"sync"
	"time"
)

func (n *Node) startElection() {
	n.becomeCandidate()

	n.mu.RLock()
	term := n.currentTerm
	candidateID := n.id
	lastLogIndex, lastLogTerm := n.getLastLogInfo()
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.RUnlock()

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1
	var mu sync.Mutex
	var wg sync.WaitGroup
	majority := (len(peers)+1)/2 + 1 // +1 for self

	for _, peer := range peers {
		if peer == n.id {
			continue
		}

		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			reply, err := n.transport.RequestVote(peer, args)
			if err != nil {
				log.Printf("Node %s: Failed to request vote from %s: %v", n.id, peer, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if reply.Term > term {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				votes++
				log.Printf("Node %s: Received vote from %s (total: %d)", n.id, peer, votes)
			}
		}(peer)
	}

	wg.Wait()

	mu.Lock()
	if votes >= majority {
		n.becomeLeader()
		go n.sendHeartbeats()
	}
	mu.Unlock()
}

func (n *Node) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.state = Follower
	}

	reply.Term = n.currentTerm

	lastLogIndex, lastLogTerm := n.getLastLogInfo()

	logOk := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOk {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.lastHeartbeat = time.Now()
		log.Printf("Node %s: Granted vote to %s for term %d", n.id, args.CandidateID, args.Term)
	}

	return reply
}