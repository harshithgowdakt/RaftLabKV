package kvstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("key")

// Operation represents a KV store operation.
type Operation struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// KVStore is a persistent key-value store backed by bbolt and raft.
// Writes go through raft.Node.Propose(); the main loop calls
// ApplyCommitted() with committed entries from Ready.
type KVStore struct {
	db     *bolt.DB
	node   raft.Node
	dbPath string

	pendingMu sync.Mutex
	pending   map[string]chan error
}

// NewKVStore opens (or creates) a bbolt database at dbPath and returns
// a new KVStore. The "key" bucket is created if it does not exist.
func NewKVStore(dbPath string, node raft.Node) (*KVStore, error) {
	db, err := bolt.Open(dbPath, 0640, nil)
	if err != nil {
		return nil, fmt.Errorf("kvstore: open db: %w", err)
	}

	// Ensure the bucket exists.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("kvstore: create bucket: %w", err)
	}

	return &KVStore{
		db:      db,
		node:    node,
		dbPath:  dbPath,
		pending: make(map[string]chan error),
	}, nil
}

// Get returns the value for the given key using a bbolt read transaction.
func (kv *KVStore) Get(key string) (string, bool) {
	var val string
	var found bool
	kv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		v := b.Get([]byte(key))
		if v != nil {
			val = string(v)
			found = true
		}
		return nil
	})
	return val, found
}

// Put proposes a PUT operation through raft. It blocks until the entry
// is committed and applied, or returns an error.
func (kv *KVStore) Put(key, value string) error {
	op := Operation{Type: "PUT", Key: key, Value: value}
	return kv.propose(op)
}

// Delete proposes a DELETE operation through raft.
func (kv *KVStore) Delete(key string) error {
	op := Operation{Type: "DELETE", Key: key}
	return kv.propose(op)
}

// GetAll returns a copy of all key-value pairs using a bbolt read transaction.
func (kv *KVStore) GetAll() map[string]string {
	result := make(map[string]string)
	kv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		b.ForEach(func(k, v []byte) error {
			result[string(k)] = string(v)
			return nil
		})
		return nil
	})
	return result
}

func (kv *KVStore) propose(op Operation) error {
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

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

	if err := kv.node.Propose(context.TODO(), data); err != nil {
		return fmt.Errorf("propose failed: %w", err)
	}

	return <-ch
}

// ApplyCommitted applies committed entries from the raft log to the
// KV store. All entries are applied in a single bbolt transaction
// (one fsync). Called by the main loop when processing Ready.CommittedEntries.
func (kv *KVStore) ApplyCommitted(entries []raft.Entry) {
	// Collect operations to apply.
	type pendingOp struct {
		op          Operation
		proposalKey string
	}
	var ops []pendingOp

	for _, entry := range entries {
		if entry.Data == nil {
			continue
		}

		var op Operation
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			log.Printf("kvstore: failed to unmarshal entry: %v", err)
			continue
		}
		ops = append(ops, pendingOp{op: op, proposalKey: string(entry.Data)})
	}

	if len(ops) == 0 {
		return
	}

	// Apply all ops in a single bbolt write transaction.
	err := kv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, po := range ops {
			switch po.op.Type {
			case "PUT":
				if err := b.Put([]byte(po.op.Key), []byte(po.op.Value)); err != nil {
					return err
				}
			case "DELETE":
				if err := b.Delete([]byte(po.op.Key)); err != nil {
					return err
				}
			default:
				log.Printf("kvstore: unknown op type: %s", po.op.Type)
			}
		}
		return nil
	})

	if err != nil {
		log.Printf("kvstore: bbolt update failed: %v", err)
	}

	// Notify pending proposals.
	for _, po := range ops {
		kv.pendingMu.Lock()
		if ch, ok := kv.pending[po.proposalKey]; ok {
			ch <- err
			delete(kv.pending, po.proposalKey)
		}
		kv.pendingMu.Unlock()
	}
}

// TakeSnapshot returns a copy of the entire bbolt database file.
// This is the snapshot data that gets stored in raft snapshot files.
func (kv *KVStore) TakeSnapshot() ([]byte, error) {
	var buf bytes.Buffer
	err := kv.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buf)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("kvstore: snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// RestoreSnapshot replaces the bbolt database with the given snapshot data.
// It closes the current db, writes the data to a temp file, renames it
// over the original, and reopens.
func (kv *KVStore) RestoreSnapshot(data []byte) error {
	if err := kv.db.Close(); err != nil {
		return fmt.Errorf("kvstore: close for restore: %w", err)
	}

	dir := filepath.Dir(kv.dbPath)
	tmp := filepath.Join(dir, "db.tmp")

	if err := os.WriteFile(tmp, data, 0640); err != nil {
		return fmt.Errorf("kvstore: write tmp: %w", err)
	}
	if err := os.Rename(tmp, kv.dbPath); err != nil {
		return fmt.Errorf("kvstore: rename: %w", err)
	}

	db, err := bolt.Open(kv.dbPath, 0640, nil)
	if err != nil {
		return fmt.Errorf("kvstore: reopen: %w", err)
	}
	kv.db = db
	return nil
}

// Close closes the bbolt database.
func (kv *KVStore) Close() error {
	if kv.db != nil {
		return kv.db.Close()
	}
	return nil
}
