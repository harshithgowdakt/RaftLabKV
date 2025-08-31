package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/harshithgowda/distributed-key-value-store/pkg/kvstore"
	"github.com/harshithgowda/distributed-key-value-store/pkg/network"
	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

func main() {
	var (
		nodeID = flag.String("id", "node1", "Node ID")
		addr   = flag.String("addr", ":8080", "Server address")
		peers  = flag.String("peers", "", "Comma-separated list of peer addresses")
	)
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Node ID is required")
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	transport := network.NewHTTPTransport("")
	raftNode := raft.NewNode(*nodeID, peerList, transport)
	
	store := kvstore.NewKVStore(raftNode)
	server := network.NewServer(raftNode, store, *addr)

	raftNode.Start()

	go func() {
		log.Printf("Starting distributed key-value store server...")
		log.Printf("Node ID: %s", *nodeID)
		log.Printf("Address: %s", *addr)
		log.Printf("Peers: %v", peerList)
		
		if err := server.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Println("Shutting down...")
	raftNode.Stop()
}