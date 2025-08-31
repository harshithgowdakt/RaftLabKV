#!/bin/bash

# Test script for the distributed key-value store

SERVERS="localhost:8080,localhost:8081,localhost:8082"

echo "Testing distributed key-value store..."

# Build the client
go build -o bin/client ./cmd/client

# Function to run client command
run_cmd() {
    echo "$ $1"
    echo "$1" | ./bin/client $SERVERS
    echo ""
}

echo "1. Checking cluster status..."
curl -s http://localhost:8080/status | jq .
curl -s http://localhost:8081/status | jq .
curl -s http://localhost:8082/status | jq .
echo ""

echo "2. Testing basic operations..."
run_cmd "put name Alice"
run_cmd "put age 25"
run_cmd "put city NewYork"

echo "3. Reading values..."
run_cmd "get name"
run_cmd "get age"
run_cmd "get city"

echo "4. Getting all data..."
run_cmd "getall"

echo "5. Deleting a key..."
run_cmd "delete age"

echo "6. Verifying deletion..."
run_cmd "get age"
run_cmd "getall"

echo "Test completed!"