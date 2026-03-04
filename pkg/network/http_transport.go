package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/harshithgowdakt/raftlabkv/pkg/raft"
)

// Transport sends raft messages to peers via HTTP. It maps uint64 node IDs
// to network addresses and POSTs JSON-encoded messages to /raft/message.
type Transport struct {
	mu    sync.RWMutex
	peers map[uint64]string // id -> "host:port"

	client *http.Client
}

// NewTransport creates a new Transport.
func NewTransport() *Transport {
	return &Transport{
		peers: make(map[uint64]string),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// AddPeer registers a peer's address.
func (t *Transport) AddPeer(id uint64, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addr
}

// RemovePeer removes a peer.
func (t *Transport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
}

// Send sends a batch of raft messages to their respective peers.
func (t *Transport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		t.send(m)
	}
}

func (t *Transport) send(m raft.Message) {
	t.mu.RLock()
	addr, ok := t.peers[m.To]
	t.mu.RUnlock()

	if !ok {
		log.Printf("transport: no address for node %d", m.To)
		return
	}

	data, err := json.Marshal(m)
	if err != nil {
		log.Printf("transport: marshal error: %v", err)
		return
	}

	url := fmt.Sprintf("http://%s/raft/message", addr)
	resp, err := t.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		// Don't log every failure - network partitions are expected.
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

// Handler returns an http.Handler that receives raft messages from peers
// and feeds them into the given raft.Node.
func Handler(n raft.Node) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var m raft.Message
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := n.Step(r.Context(), m); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}
