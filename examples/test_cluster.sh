#!/bin/bash

# Test script for the distributed key-value store
# Assumes a 3-node cluster is already running on ports 8080, 8081, 8082

set -e

echo "Testing distributed key-value store..."
echo ""

echo "1. Checking cluster status..."
echo "--- Node 1 ---"
curl -s http://localhost:8080/status | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8080/status
echo ""
echo "--- Node 2 ---"
curl -s http://localhost:8081/status | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8081/status
echo ""
echo "--- Node 3 ---"
curl -s http://localhost:8082/status | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8082/status
echo ""

sleep 1

echo "2. Writing key-value pairs..."
curl -s -X POST localhost:8080/kv/put -d '{"key":"name","value":"Alice"}'
echo ""
curl -s -X POST localhost:8080/kv/put -d '{"key":"age","value":"25"}'
echo ""
curl -s -X POST localhost:8080/kv/put -d '{"key":"city","value":"NewYork"}'
echo ""

sleep 1

echo "3. Reading values from different nodes..."
echo "--- get name (from node 1) ---"
curl -s localhost:8080/kv/get/name
echo ""
echo "--- get age (from node 2) ---"
curl -s localhost:8081/kv/get/age
echo ""
echo "--- get city (from node 3) ---"
curl -s localhost:8082/kv/get/city
echo ""

echo "4. Getting all data..."
curl -s localhost:8080/kv/all | python3 -m json.tool 2>/dev/null || curl -s localhost:8080/kv/all
echo ""

echo "5. Deleting a key..."
curl -s -X DELETE localhost:8080/kv/delete/age
echo ""

sleep 1

echo "6. Verifying deletion..."
echo "--- get age (should be 404) ---"
curl -s -w "\nHTTP %{http_code}\n" localhost:8080/kv/get/age
echo ""
echo "--- getall (age should be gone) ---"
curl -s localhost:8080/kv/all | python3 -m json.tool 2>/dev/null || curl -s localhost:8080/kv/all
echo ""

echo "Test completed!"
