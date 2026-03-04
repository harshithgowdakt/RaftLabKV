package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/harshithgowdakt/raftlabkv/pkg/fileutil"
	"github.com/harshithgowdakt/raftlabkv/pkg/kvstore"
	"github.com/harshithgowdakt/raftlabkv/pkg/network"
	"github.com/harshithgowdakt/raftlabkv/pkg/raft"
	"github.com/harshithgowdakt/raftlabkv/pkg/snap"
	"github.com/harshithgowdakt/raftlabkv/pkg/wal"
	"github.com/harshithgowdakt/raftlabkv/pkg/wal/walpb"
)

const snapshotInterval uint64 = 10000

// Maximum number of old WAL/snap files to keep before purging.
const (
	maxWALFiles  uint = 5
	maxSnapFiles uint = 5
)

func main() {
	var (
		id      = flag.Uint64("id", 0, "Node ID (must be > 0)")
		addr    = flag.String("addr", ":8080", "Listen address")
		peers   = flag.String("peers", "", "Comma-separated list of id=addr pairs (e.g., 1=:8080,2=:8081,3=:8082)")
		dataDir = flag.String("data-dir", "", "Directory for persistent data (default: /tmp/raft-{id})")
	)
	flag.Parse()

	if *id == 0 {
		log.Fatal("Node ID is required (use -id=N where N > 0)")
	}

	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/raft-%d", *id)
	}

	// Parse peers.
	peerMap := parsePeers(*peers)
	if len(peerMap) == 0 {
		log.Fatal("At least one peer required (use -peers=1=:8080,2=:8081,...)")
	}

	// Ensure this node is in the peer map.
	if _, ok := peerMap[*id]; !ok {
		log.Fatalf("Node %d is not in the peer list", *id)
	}

	// Directory layout: {data-dir}/member/wal/ and {data-dir}/member/snap/
	walDir := filepath.Join(*dataDir, "member", "wal")
	snapDir := filepath.Join(*dataDir, "member", "snap")
	dbPath := filepath.Join(snapDir, "db")

	// Initialize MemoryStorage.
	storage := raft.NewMemoryStorage()

	// Build peer list for transport and bootstrap.
	transport := network.NewTransport()
	var raftPeers []raft.Peer
	for pid, paddr := range peerMap {
		if pid != *id {
			transport.AddPeer(pid, paddr)
		}
		raftPeers = append(raftPeers, raft.Peer{ID: pid})
	}

	// Raft configuration.
	cfg := &raft.Config{
		ID:              *id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		PreVote:         true,
	}

	// Initialize snapshotter (creates snapDir if needed).
	snapshotter := snap.New(snapDir)

	var (
		node      raft.Node
		w         *wal.WAL
		store     *kvstore.KVStore
		snapIndex uint64 // index of last snapshot, for triggering new ones
	)

	if wal.Exists(walDir) {
		// --- Recovery path ---
		log.Println("Recovering from existing WAL...")

		// 1. Load latest snapshot from disk.
		var snapshot *raft.Snapshot
		walSnap := &walpb.Snapshot{} // default: index=0, term=0
		snapshot, err := snapshotter.Load()
		if err != nil {
			log.Printf("No snapshot found: %v", err)
		} else {
			log.Printf("Loaded snapshot at term=%d index=%d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			walSnap = &walpb.Snapshot{
				Term:  snapshot.Metadata.Term,
				Index: snapshot.Metadata.Index,
			}
			snapIndex = snapshot.Metadata.Index
		}

		// 2. Open WAL from snapshot point.
		w, err = wal.Open(walDir, walSnap)
		if err != nil {
			log.Fatalf("Failed to open WAL: %v", err)
		}

		// 3. Replay WAL.
		_, hs, ents, err := w.ReadAll()
		if err != nil {
			log.Fatalf("Failed to read WAL: %v", err)
		}

		// 4. Apply snapshot to MemoryStorage.
		if snapshot != nil {
			if err := storage.ApplySnapshot(*snapshot); err != nil {
				log.Fatalf("Failed to apply snapshot: %v", err)
			}
		}

		// 5. Restore hard state.
		if !raft.IsEmptyHardState(hs) {
			if err := storage.SetHardState(hs); err != nil {
				log.Fatalf("Failed to set hard state: %v", err)
			}
		}

		// 6. Replay entries.
		if len(ents) > 0 {
			if err := storage.Append(ents); err != nil {
				log.Fatalf("Failed to append entries: %v", err)
			}
			cfg.Applied = ents[len(ents)-1].Index
		}

		// 7. Set peer list so the progress tracker is populated on restart.
		var peerIDs []uint64
		for pid := range peerMap {
			peerIDs = append(peerIDs, pid)
		}
		storage.SetPeers(peerIDs)

		// 8. Restart raft node.
		node = raft.RestartNode(cfg)

		// 8. Open bbolt KV store.
		store, err = kvstore.NewKVStore(dbPath, node)
		if err != nil {
			log.Fatalf("Failed to open KV store: %v", err)
		}

		// If we loaded a snapshot with data, restore KV from it.
		if snapshot != nil && len(snapshot.Data) > 0 {
			if err := store.RestoreSnapshot(snapshot.Data); err != nil {
				log.Fatalf("Failed to restore KV from snapshot: %v", err)
			}
		}

	} else {
		// --- New cluster path ---
		log.Println("Starting new node...")

		// Encode node ID as metadata.
		metadata := make([]byte, 8)
		binary.LittleEndian.PutUint64(metadata, *id)

		var err error
		w, err = wal.Create(walDir, metadata)
		if err != nil {
			log.Fatalf("Failed to create WAL: %v", err)
		}

		node = raft.StartNode(cfg, raftPeers)

		store, err = kvstore.NewKVStore(dbPath, node)
		if err != nil {
			log.Fatalf("Failed to create KV store: %v", err)
		}
	}

	// Start HTTP server.
	httpServer := network.NewServer(node, store, *addr)
	go func() {
		log.Printf("Node %d listening on %s", *id, *addr)
		if err := httpServer.Start(); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Start periodic purge goroutines for old WAL and snapshot files.
	purgeCtx, purgeCancel := context.WithCancel(context.Background())
	var purgeWg sync.WaitGroup
	fileutil.PurgeFile(purgeCtx, &purgeWg, walDir, ".wal", maxWALFiles)
	fileutil.PurgeFileWithoutFlock(purgeCtx, &purgeWg, snapDir, ".snap", maxSnapFiles)

	// Signal handling.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	// Main loop: Tick -> Ready -> Persist -> Send -> Apply -> Advance
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastApplied uint64
	log.Printf("Node %d started. Peers: %v", *id, peerMap)

	for {
		select {
		case <-ticker.C:
			node.Tick()

		case rd := <-node.Ready():
			// 1. Persist Entries and HardState to WAL.
			if err := w.Save(rd.HardState, rd.Entries); err != nil {
				log.Fatalf("Failed to save to WAL: %v", err)
			}

			// 2. Handle snapshot if any.
			if !raft.IsEmptySnap(rd.Snapshot) {
				walSnap := &walpb.Snapshot{
					Term:  rd.Snapshot.Metadata.Term,
					Index: rd.Snapshot.Metadata.Index,
				}

				// Save snapshot marker in WAL.
				if err := w.SaveSnapshot(walSnap); err != nil {
					log.Fatalf("Failed to save snapshot to WAL: %v", err)
				}

				// Save snapshot file to disk.
				if err := snapshotter.SaveSnap(rd.Snapshot); err != nil {
					log.Fatalf("Failed to save snapshot file: %v", err)
				}

				// Apply to MemoryStorage.
				if err := storage.ApplySnapshot(rd.Snapshot); err != nil {
					log.Fatalf("Failed to apply snapshot to storage: %v", err)
				}

				// Restore KV state from snapshot data.
				if len(rd.Snapshot.Data) > 0 {
					if err := store.RestoreSnapshot(rd.Snapshot.Data); err != nil {
						log.Fatalf("Failed to restore KV from snapshot: %v", err)
					}
				}

				snapIndex = rd.Snapshot.Metadata.Index
			}

			// 3. Save entries to MemoryStorage.
			if err := storage.Append(rd.Entries); err != nil {
				log.Fatalf("Failed to append entries to storage: %v", err)
			}

			// 4. Send messages to peers.
			transport.Send(rd.Messages)

			// 5. Apply committed entries to the state machine.
			store.ApplyCommitted(rd.CommittedEntries)

			// Track last applied index for snapshot triggering.
			if n := len(rd.CommittedEntries); n > 0 {
				lastApplied = rd.CommittedEntries[n-1].Index
			}

			// 6. Advance.
			node.Advance()

			// 7. Trigger snapshot if enough entries have been applied.
			if lastApplied > 0 && lastApplied-snapIndex >= snapshotInterval {
				data, err := store.TakeSnapshot()
				if err != nil {
					log.Printf("Failed to take snapshot: %v", err)
				} else {
					compactSnap, err := storage.CreateSnapshot(lastApplied, nil, data)
					if err != nil {
						log.Printf("Failed to create snapshot: %v", err)
					} else {
						if err := snapshotter.SaveSnap(compactSnap); err != nil {
							log.Printf("Failed to save snapshot: %v", err)
						} else {
							walSnap := &walpb.Snapshot{
								Term:  compactSnap.Metadata.Term,
								Index: compactSnap.Metadata.Index,
							}
							if err := w.SaveSnapshot(walSnap); err != nil {
								log.Printf("Failed to save snapshot to WAL: %v", err)
							}
							// Release old WAL segment locks.
							w.ReleaseLockTo(lastApplied)
							if err := storage.Compact(lastApplied); err != nil {
								log.Printf("Failed to compact storage: %v", err)
							}
							snapIndex = lastApplied
							log.Printf("Snapshot taken at index %d", lastApplied)
						}
					}
				}
			}

		case <-sigc:
			log.Println("Shutting down...")

			// Gracefully shut down the HTTP server (5s deadline for in-flight requests).
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := httpServer.Shutdown(shutdownCtx); err != nil {
				log.Printf("HTTP server shutdown error: %v", err)
			}
			shutdownCancel()

			purgeCancel()
			purgeWg.Wait()
			node.Stop()
			store.Close()
			w.Close()
			return
		}
	}
}

// parsePeers parses a comma-separated list of id=addr pairs.
func parsePeers(s string) map[uint64]string {
	result := make(map[uint64]string)
	if s == "" {
		return result
	}
	for _, p := range strings.Split(s, ",") {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid peer format: %q (expected id=addr)", p)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			log.Fatalf("Invalid peer ID: %q", parts[0])
		}
		result[id] = strings.TrimSpace(parts[1])
	}
	return result
}
