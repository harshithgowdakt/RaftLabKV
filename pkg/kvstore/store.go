package kvstore

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

// Operation represents a KV store operation.
type Operation struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// KVStore is a simple in-memory key-value store backed by raft. Writes go
// through raft.Node.Propose(); the main loop calls ApplyCommitted() with
// committed entries from Ready.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
	node raft.Node

	// pending tracks in-flight proposals. When Put/Delete is called, we
	// create a channel and block until the entry is committed and applied.
	pendingMu sync.Mutex
	pending   map[string]chan error
}

// NewKVStore creates a new KVStore.
func NewKVStore(node raft.Node) *KVStore {
	return &KVStore{
		data:    make(map[string]string),
		node:    node,
		pending: make(map[string]chan error),
	}
}

// Get returns the value for the given key.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.data[key]
	return value, exists
}

// Put proposes a PUT operation through raft. It blocks until the entry is
// committed and applied, or returns an error.
func (kv *KVStore) Put(key, value string) error {
	op := Operation{Type: "PUT", Key: key, Value: value}
	return kv.propose(op)
}

// Delete proposes a DELETE operation through raft.
func (kv *KVStore) Delete(key string) error {
	op := Operation{Type: "DELETE", Key: key}
	return kv.propose(op)
}

// GetAll returns a copy of all key-value pairs.
func (kv *KVStore) GetAll() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	result := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		result[k] = v
	}
	return result
}

func (kv *KVStore) propose(op Operation) error {
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	// Create a unique key for this proposal so we can wait for it.
	// We use the serialized op as the key - this is simple but sufficient.
	proposalKey := string(data)

	ch := make(chan error, 1)
	kv.pendingMu.Lock()
	kv.pending[proposalKey] = ch
	kv.pendingMu.Unlock()

	defer func() {
		kv.pendingMu.Lock()
		delete(kv.pending, proposalKey)
		kv.pendingMu.Unlock()
	}()

	// Propose through raft.
	if err := kv.node.Propose(nil, data); err != nil {
		return fmt.Errorf("propose failed: %w", err)
	}

	// Wait for the entry to be committed and applied.
	return <-ch
}

// ApplyCommitted applies committed entries from the raft log to the
// KV store. This is called by the main application loop when processing
// Ready.CommittedEntries.
func (kv *KVStore) ApplyCommitted(entries []raft.Entry) {
	for _, entry := range entries {
		if entry.Data == nil {
			// Empty entry (e.g., leader's initial empty entry).
			continue
		}

		var op Operation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			log.Printf("kvstore: failed to unmarshal entry: %v", err)
			continue
		}

		kv.mu.Lock()
		switch op.Type {
		case "PUT":
			kv.data[op.Key] = op.Value
		case "DELETE":
			delete(kv.data, op.Key)
		default:
			log.Printf("kvstore: unknown op type: %s", op.Type)
		}
		kv.mu.Unlock()

		// Notify the pending proposal, if any.
		proposalKey := string(entry.Data)
		kv.pendingMu.Lock()
		if ch, ok := kv.pending[proposalKey]; ok {
			ch <- nil
			delete(kv.pending, proposalKey)
		}
		kv.pendingMu.Unlock()
	}
}

// TakeSnapshot returns a JSON snapshot of the current data.
func (kv *KVStore) TakeSnapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return json.Marshal(kv.data)
}

// RestoreSnapshot restores the KV store from a snapshot.
func (kv *KVStore) RestoreSnapshot(data []byte) error {
	var newData map[string]string
	if err := json.Unmarshal(data, &newData); err != nil {
		return err
	}
	kv.mu.Lock()
	kv.data = newData
	kv.mu.Unlock()
	return nil
}
