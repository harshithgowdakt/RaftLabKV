#!/bin/bash

# Start a 3-node cluster

echo "Starting distributed key-value store cluster..."

# Build the server
echo "Building server..."
go build -o bin/server ./cmd/server

# Start node1 (port 8080)
echo "Starting node1 on :8080..."
./bin/server -id=node1 -addr=:8080 -peers=localhost:8081,localhost:8082 &
NODE1_PID=$!

# Start node2 (port 8081)
echo "Starting node2 on :8081..."
./bin/server -id=node2 -addr=:8081 -peers=localhost:8080,localhost:8082 &
NODE2_PID=$!

# Start node3 (port 8082)
echo "Starting node3 on :8082..."
./bin/server -id=node3 -addr=:8082 -peers=localhost:8080,localhost:8081 &
NODE3_PID=$!

echo "Cluster started!"
echo "Node1: localhost:8080 (PID: $NODE1_PID)"
echo "Node2: localhost:8081 (PID: $NODE2_PID)"
echo "Node3: localhost:8082 (PID: $NODE3_PID)"
echo ""
echo "To test the cluster, run: go run ./cmd/client localhost:8080,localhost:8081,localhost:8082"
echo ""
echo "Press Ctrl+C to stop all nodes"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Stopping all nodes..."
    kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null
    wait $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null
    echo "All nodes stopped."
    exit 0
}

# Trap Ctrl+C
trap cleanup INT

# Wait for all background processes
wait