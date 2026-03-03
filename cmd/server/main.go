package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/harshithgowda/distributed-key-value-store/pkg/kvstore"
	"github.com/harshithgowda/distributed-key-value-store/pkg/network"
	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
	"github.com/harshithgowda/distributed-key-value-store/pkg/wal"
)

func main() {
	var (
		id      = flag.Uint64("id", 0, "Node ID (must be > 0)")
		addr    = flag.String("addr", ":8080", "Listen address")
		peers   = flag.String("peers", "", "Comma-separated list of id=addr pairs (e.g., 1=:8080,2=:8081,3=:8082)")
		dataDir = flag.String("data-dir", "", "Directory for WAL data (default: /tmp/raft-{id})")
	)
	flag.Parse()

	if *id == 0 {
		log.Fatal("Node ID is required (use -id=N where N > 0)")
	}

	if *dataDir == "" {
		*dataDir = fmt.Sprintf("/tmp/raft-%d", *id)
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

	// Initialize WAL.
	w, err := wal.New(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}

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

	var node raft.Node

	// Check if we are recovering from an existing WAL.
	if w.Exists() {
		log.Println("Recovering from existing WAL...")
		hs, snap, ents, err := w.ReadAll()
		if err != nil {
			log.Fatalf("Failed to read WAL: %v", err)
		}

		// Restore snapshot.
		if !raft.IsEmptySnap(snap) {
			if err := storage.ApplySnapshot(snap); err != nil {
				log.Fatalf("Failed to apply snapshot: %v", err)
			}
		}

		// Restore hard state.
		if !raft.IsEmptyHardState(hs) {
			if err := storage.SetHardState(hs); err != nil {
				log.Fatalf("Failed to set hard state: %v", err)
			}
		}

		// Replay entries.
		if len(ents) > 0 {
			if err := storage.Append(ents); err != nil {
				log.Fatalf("Failed to append entries: %v", err)
			}
			cfg.Applied = ents[len(ents)-1].Index
		}

		node = raft.RestartNode(cfg)
	} else {
		log.Println("Starting new node...")
		node = raft.StartNode(cfg, raftPeers)
	}

	// Initialize KV store.
	store := kvstore.NewKVStore(node)

	// Start HTTP server.
	server := network.NewServer(node, store, *addr)
	go func() {
		log.Printf("Node %d listening on %s", *id, *addr)
		if err := server.Start(); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Signal handling.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	// Main loop: Tick -> Ready -> Persist -> Send -> Apply -> Advance
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

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

			// 2. Save snapshot if any.
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := w.SaveSnapshot(rd.Snapshot); err != nil {
					log.Fatalf("Failed to save snapshot: %v", err)
				}
				if err := storage.ApplySnapshot(rd.Snapshot); err != nil {
					log.Fatalf("Failed to apply snapshot to storage: %v", err)
				}
				// Restore KV state from snapshot.
				store.RestoreSnapshot(rd.Snapshot.Data)
			}

			// 3. Save entries to MemoryStorage.
			if err := storage.Append(rd.Entries); err != nil {
				log.Fatalf("Failed to append entries to storage: %v", err)
			}

			// 4. Send messages to peers.
			transport.Send(rd.Messages)

			// 5. Apply committed entries to the state machine.
			store.ApplyCommitted(rd.CommittedEntries)

			// 6. Advance.
			node.Advance()

		case <-sigc:
			log.Println("Shutting down...")
			node.Stop()
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
