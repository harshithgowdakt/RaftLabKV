package kvstore

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

type Operation struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
	raft *raft.Node
}

func NewKVStore(raftNode *raft.Node) *KVStore {
	kv := &KVStore{
		data: make(map[string]string),
		raft: raftNode,
	}

	go kv.applyLoop()
	return kv
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	
	value, exists := kv.data[key]
	return value, exists
}

func (kv *KVStore) Put(key, value string) error {
	op := Operation{
		Type:  "PUT",
		Key:   key,
		Value: value,
	}

	if !kv.raft.Submit(op) {
		return fmt.Errorf("not leader")
	}

	return nil
}

func (kv *KVStore) Delete(key string) error {
	op := Operation{
		Type: "DELETE",
		Key:  key,
	}

	if !kv.raft.Submit(op) {
		return fmt.Errorf("not leader")
	}

	return nil
}

func (kv *KVStore) GetAll() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	
	result := make(map[string]string)
	for k, v := range kv.data {
		result[k] = v
	}
	return result
}

func (kv *KVStore) applyLoop() {
	applyCh := kv.raft.GetApplyCh()
	for entry := range applyCh {
		kv.applyEntry(entry)
	}
}

func (kv *KVStore) applyEntry(entry raft.LogEntry) {
	var op Operation
	
	switch cmd := entry.Command.(type) {
	case Operation:
		op = cmd
	case map[string]interface{}:
		jsonData, err := json.Marshal(cmd)
		if err != nil {
			log.Printf("Failed to marshal command: %v", err)
			return
		}
		if err := json.Unmarshal(jsonData, &op); err != nil {
			log.Printf("Failed to unmarshal command: %v", err)
			return
		}
	default:
		log.Printf("Unknown command type: %T", cmd)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op.Type {
	case "PUT":
		kv.data[op.Key] = op.Value
		log.Printf("Applied PUT: %s = %s", op.Key, op.Value)
	case "DELETE":
		delete(kv.data, op.Key)
		log.Printf("Applied DELETE: %s", op.Key)
	default:
		log.Printf("Unknown operation type: %s", op.Type)
	}
}