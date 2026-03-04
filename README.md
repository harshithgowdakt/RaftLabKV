# RaftLabKV: Distributed Key-Value Store with Raft Consensus

A distributed key-value store built from scratch using the Raft consensus algorithm, modeled after etcd's architecture. Implements etcd-style binary WAL, bbolt-backed persistent storage, and binary snapshots with CRC integrity.

> **Disclaimer:** This project was developed with AI assistance (Claude Code and Codex). It is intended for **learning and educational purposes only** and is **not recommended for production use cases**.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   HTTP Server                    │
│         /kv/put  /kv/get  /kv/delete             │
├─────────────────────────────────────────────────┤
│                   KV Store (bbolt)               │
│            Applies committed entries             │
├─────────────────────────────────────────────────┤
│                 Raft Consensus                   │
│     Leader Election · Log Replication            │
│     PreVote · CheckQuorum · ReadIndex            │
├─────────────────────────────────────────────────┤
│              HTTP Transport Layer                │
│           /raft/message (peer RPCs)              │
├─────────────────────────────────────────────────┤
│                Persistence Layer                 │
│    WAL (binary, CRC)  ·  Snapshots (binary)      │
│    File Locking  ·  Pre-allocation  ·  Purging   │
└─────────────────────────────────────────────────┘
```

### On-Disk Layout

```
{data-dir}/member/
├── wal/
│   ├── 0000000000000000-0000000000000000.wal   # Binary segmented WAL files
│   └── ...
└── snap/
    ├── db                                       # bbolt database (persistent KV state)
    └── 0000000000000001-0000000000000002.snap   # Binary snapshot files
```

## Features

- **Raft Consensus** — Full implementation with leader election, log replication, PreVote, and CheckQuorum
- **etcd-Style Binary WAL** — Protobuf-encoded records, CRC-32C integrity chains, 64 MB pre-allocated segments
- **Persistent KV Store** — bbolt (B+ tree embedded database) for durable key-value storage
- **Binary Snapshots** — CRC-protected snapshot files with automatic compaction
- **Crash Recovery** — WAL replay from last snapshot point, torn write detection, WAL repair
- **File Pipeline** — Background goroutine pre-allocates next WAL segment for fast rotation
- **File Purging** — Automatic cleanup of old WAL and snapshot files
- **Graceful Shutdown** — Context-based cancellation with `sync.WaitGroup` across all goroutines
- **REST API** — HTTP endpoints for KV operations and cluster status
- **Interactive Client** — CLI client with auto leader discovery

## Quick Start

### Prerequisites

- Go 1.24+

### Build

```bash
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

### Start a 3-Node Cluster

```bash
# Terminal 1
./bin/server -id=1 -addr=:8080 -peers=1=:8080,2=:8081,3=:8082 -data-dir=/tmp/raft-1

# Terminal 2
./bin/server -id=2 -addr=:8081 -peers=1=:8080,2=:8081,3=:8082 -data-dir=/tmp/raft-2

# Terminal 3
./bin/server -id=3 -addr=:8082 -peers=1=:8080,2=:8081,3=:8082 -data-dir=/tmp/raft-3
```

### Use the Client

```bash
./bin/client localhost:8080,localhost:8081,localhost:8082

> put name Alice
> put age 25
> get name
> getall
> delete age
> status localhost:8080
> exit
```

### Use curl

```bash
# Write a key
curl -X POST localhost:8080/kv/put -d '{"key":"name","value":"Alice"}'

# Read a key (from any node)
curl localhost:8081/kv/get/name

# Delete a key
curl -X DELETE localhost:8080/kv/delete/name

# Get all keys
curl localhost:8080/kv/all

# Check node status
curl localhost:8080/status
```

### Verify Persistence

```bash
# Write some data, then kill a node (Ctrl+C)
# Restart the same node — data survives via WAL recovery:
./bin/server -id=1 -addr=:8080 -peers=1=:8080,2=:8081,3=:8082 -data-dir=/tmp/raft-1
# Node logs: "Recovering from existing WAL..."

# Inspect on-disk files:
ls -la /tmp/raft-1/member/wal/
ls -la /tmp/raft-1/member/snap/
```

## Server Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-id` | (required) | Node ID (must be > 0) |
| `-addr` | `:8080` | HTTP listen address |
| `-peers` | (required) | Comma-separated `id=addr` pairs |
| `-data-dir` | `/tmp/raft-{id}` | Directory for persistent data |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/kv/get/{key}` | GET | Get value by key |
| `/kv/put` | POST | Put key-value pair (JSON body: `{"key":"...","value":"..."}`) |
| `/kv/delete/{key}` | DELETE | Delete a key |
| `/kv/all` | GET | Get all key-value pairs |
| `/status` | GET | Node status (term, leader, commit/applied index) |
| `/raft/message` | POST | Raft peer-to-peer messages (internal) |

## Project Structure

```
cmd/
├── server/main.go          Main server entry point, Ready/Advance loop
└── client/main.go          Interactive CLI client

pkg/
├── raft/                   Core Raft consensus engine
│   ├── raft.go             State machine (follower/candidate/leader transitions)
│   ├── node.go             Node interface with channel-based event loop
│   ├── rawnode.go           Low-level RawNode API
│   ├── log.go              Raft log management
│   ├── log_unstable.go      Unstable (not yet persisted) log entries
│   ├── storage.go          MemoryStorage implementation
│   ├── config.go           Raft configuration
│   ├── types.go            Messages, entries, hard/soft state, snapshots
│   ├── tracker.go          Follower progress tracking
│   ├── read_only.go        Linearizable read handling
│   ├── status.go           Status reporting
│   ├── logger.go           Logging
│   └── util.go             Helpers
│
├── wal/                    etcd-style Write-Ahead Log
│   ├── wal.go              Create, Open, Save, ReadAll, cut (segment rotation)
│   ├── encoder.go          Protobuf record encoding, CRC-32C, frame format
│   ├── decoder.go          Multi-segment decoding, torn write detection
│   ├── pagewriter.go       4096-byte page-aligned buffered writer
│   ├── file_pipeline.go    Background segment pre-allocation
│   ├── repair.go           WAL recovery from corruption
│   ├── util.go             Segment naming and parsing
│   └── walpb/              Protobuf definitions (Record, Snapshot)
│
├── snap/
│   └── snapshotter.go      Binary snapshot files with CRC integrity
│
├── kvstore/
│   └── store.go            bbolt-backed KV store (state machine)
│
├── network/
│   ├── server.go           HTTP server with graceful shutdown
│   └── http_transport.go   HTTP transport for Raft RPCs
│
├── fileutil/
│   ├── fileutil.go         Directory reading, file validation
│   ├── lock.go             File locking (flock)
│   ├── preallocate.go      Disk pre-allocation
│   └── purge.go            Periodic old file cleanup
│
└── client/
    └── client.go           HTTP client with leader auto-discovery
```

## How It Works

### Write Path

1. Client sends `PUT` request to any node
2. If not leader, request is rejected (client retries on leader)
3. Leader proposes entry through Raft (`node.Propose()`)
4. Raft replicates entry to followers via `MsgApp` messages
5. Once a quorum acknowledges, the entry is committed
6. The `Ready` struct delivers committed entries to the application
7. Application persists entries to WAL (`w.Save()`)
8. Application applies committed entries to bbolt KV store
9. Application calls `node.Advance()` to signal completion

### Recovery Path

1. Load latest snapshot from `member/snap/`
2. Open WAL from the snapshot point (`wal.Open()`)
3. Replay all WAL records (`w.ReadAll()`) to get hard state + entries
4. Populate `MemoryStorage` with snapshot, hard state, and entries
5. Restore bbolt KV state from snapshot data (if any)
6. Restart Raft node (`raft.RestartNode()`)

### Snapshot Triggering

Every 10,000 committed entries:
1. Copy bbolt database via `tx.WriteTo()` for a consistent snapshot
2. Save snapshot file to `member/snap/`
3. Write snapshot marker to WAL
4. Release locks on old WAL segments
5. Compact MemoryStorage to free old entries

## Dependencies

| Dependency | Purpose |
|------------|---------|
| [bbolt](https://github.com/etcd-io/bbolt) | Embedded B+ tree database for persistent KV storage |
| [protobuf](https://pkg.go.dev/google.golang.org/protobuf) | WAL record serialization |

## Limitations

- No TLS or authentication
- No dynamic membership changes (cluster size fixed at startup)
- No client-side read consistency (reads served from local bbolt, may be stale on followers)
- Basic HTTP transport (no connection pooling or streaming)
- Single-bucket KV model (no ranges, watches, or transactions)

## License

This project is for educational purposes.
