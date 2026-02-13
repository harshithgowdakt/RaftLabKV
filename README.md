# RaftLabKV: Distributed Key-Value Store with Raft Consensus

A simple distributed key-value store implementation using the Raft consensus algorithm, similar to etcd but simplified for educational purposes.

> Developed using Claude Code and Codex.
> Not recommended for production use.

## Architecture

The system consists of several key components:

- **Raft Layer**: Handles consensus, leader election, and log replication
- **Key-Value Store**: Applies committed operations to maintain consistent state
- **Network Layer**: HTTP-based communication between nodes
- **Client Interface**: Simple API for interacting with the cluster

## Features

- **Consensus**: Raft algorithm ensures consistency across nodes
- **Leader Election**: Automatic leader election with configurable timeouts
- **Log Replication**: Reliable replication of operations across the cluster
- **REST API**: HTTP endpoints for both Raft and KV operations
- **Client Library**: Simple client for interacting with the cluster

## Quick Start

### Build the Project

```bash
# Build server and client binaries
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

### Start a 3-Node Cluster

```bash
# Option 1: Use the provided script
./examples/start_cluster.sh

# Option 2: Start nodes manually
./bin/server -id=node1 -addr=:8080 -peers=localhost:8081,localhost:8082 &
./bin/server -id=node2 -addr=:8081 -peers=localhost:8080,localhost:8082 &
./bin/server -id=node3 -addr=:8082 -peers=localhost:8080,localhost:8081 &
```

### Use the Client

```bash
# Interactive client
./bin/client localhost:8080,localhost:8081,localhost:8082

# Example commands:
> put name Alice
> put age 25
> get name
> getall
> delete age
> exit
```

### Test the Cluster

```bash
./examples/test_cluster.sh
```

## API Endpoints

### Key-Value Operations
- `GET /kv/get/{key}` - Get a value by key
- `POST /kv/put` - Put a key-value pair
- `DELETE /kv/delete/{key}` - Delete a key
- `GET /kv/all` - Get all key-value pairs

### Cluster Status
- `GET /status` - Get node status (term, leader info)

### Raft Internal (used by nodes)
- `POST /raft/requestVote` - Request vote for leader election
- `POST /raft/appendEntries` - Append entries / heartbeat

## Architecture Details

### Raft Implementation

The Raft implementation includes:

1. **Leader Election**: Nodes start as followers, become candidates during elections
2. **Log Replication**: Leaders replicate log entries to followers
3. **Safety**: Only committed entries are applied to the state machine

### Key Components

- `pkg/raft/`: Core Raft implementation
  - `node.go`: Main Raft node logic
  - `election.go`: Leader election algorithms
  - `replication.go`: Log replication and heartbeats
  - `types.go`: Data structures and interfaces

- `pkg/kvstore/`: Key-value store implementation
  - `store.go`: State machine that applies Raft log entries

- `pkg/network/`: Network communication
  - `server.go`: HTTP server handling requests
  - `http_transport.go`: HTTP transport for Raft RPCs

- `pkg/client/`: Client library
  - `client.go`: Client for interacting with the cluster

### Testing

You can test various scenarios:

1. **Basic Operations**: Put/Get/Delete operations
2. **Leader Election**: Stop the leader and observe re-election
3. **Network Partitions**: Simulate network failures
4. **Data Consistency**: Verify data consistency across nodes

## Understanding Raft

This implementation demonstrates key Raft concepts:

1. **Term**: Logical clock that increases during elections
2. **Log Index**: Position of entries in the replicated log
3. **Commit Index**: Index of the highest committed log entry
4. **Majority Quorum**: Operations require majority of nodes

## Limitations

This is a simplified implementation for educational purposes:

- No persistent storage (data lost on restart)
- Basic HTTP transport (no TLS, authentication)
- Simple conflict resolution
- No membership changes
- No log compaction/snapshots

## Next Steps

To make this production-ready, consider adding:

- Persistent storage for logs and state
- TLS encryption and authentication  
- Log compaction and snapshots
- Dynamic membership changes
- Better error handling and monitoring
- Performance optimizations
