package raft

import (
	"log"
	"math/rand"
	"time"
)

func NewNode(id string, peers []string, transport Transport) *Node {
	n := &Node{
		id:               id,
		peers:            peers,
		state:            Follower,
		currentTerm:      0,
		votedFor:         "",
		log:              make([]LogEntry, 0),
		commitIndex:      -1,
		lastApplied:      -1,
		nextIndex:        make(map[string]int),
		matchIndex:       make(map[string]int),
		electionTimeout:  time.Duration(150+rand.Intn(150)) * time.Millisecond,
		heartbeatTimeout: 50 * time.Millisecond,
		lastHeartbeat:    time.Now(),
		applyCh:          make(chan LogEntry, 100),
		stopCh:           make(chan struct{}),
		transport:        transport,
	}

	for _, peer := range peers {
		n.nextIndex[peer] = 0
		n.matchIndex[peer] = -1
	}

	return n
}

func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) GetState() (int, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm, n.state == Leader
}

func (n *Node) GetApplyCh() <-chan LogEntry {
	return n.applyCh
}

func (n *Node) Submit(command interface{}) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return false
	}

	entry := LogEntry{
		Index:   len(n.log),
		Term:    n.currentTerm,
		Command: command,
	}

	n.log = append(n.log, entry)
	log.Printf("Node %s: Added entry %+v", n.id, entry)

	go n.replicateToFollowers()
	return true
}

func (n *Node) electionLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
			n.mu.RLock()
			state := n.state
			lastHeartbeat := n.lastHeartbeat
			electionTimeout := n.electionTimeout
			n.mu.RUnlock()

			if state != Leader && time.Since(lastHeartbeat) > electionTimeout {
				n.startElection()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (n *Node) heartbeatLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
			n.mu.RLock()
			state := n.state
			n.mu.RUnlock()

			if state == Leader {
				n.sendHeartbeats()
			}
			time.Sleep(n.heartbeatTimeout)
		}
	}
}


func (n *Node) becomeFollower(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.lastHeartbeat = time.Now()
	log.Printf("Node %s: Became follower for term %d", n.id, term)
}

func (n *Node) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.lastHeartbeat = time.Now()
	n.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	log.Printf("Node %s: Became candidate for term %d", n.id, n.currentTerm)
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Leader
	for peer := range n.nextIndex {
		n.nextIndex[peer] = len(n.log)
		n.matchIndex[peer] = -1
	}
	log.Printf("Node %s: Became leader for term %d", n.id, n.currentTerm)
}

func (n *Node) resetElectionTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lastHeartbeat = time.Now()
}

func (n *Node) getLastLogInfo() (int, int) {
	if len(n.log) == 0 {
		return -1, 0
	}
	lastEntry := n.log[len(n.log)-1]
	return lastEntry.Index, lastEntry.Term
}

func (n *Node) updateCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return
	}

	for i := n.commitIndex + 1; i < len(n.log); i++ {
		if n.log[i].Term != n.currentTerm {
			continue
		}

		count := 1
		for peer := range n.matchIndex {
			if n.matchIndex[peer] >= i {
				count++
			}
		}

		if count > len(n.peers)/2 {
			n.commitIndex = i
			n.applyEntries()
		}
	}
}

func (n *Node) applyEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		select {
		case n.applyCh <- entry:
		default:
		}
	}
}