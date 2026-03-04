package network

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/harshithgowda/distributed-key-value-store/pkg/kvstore"
	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

// Server is the HTTP server that handles both raft messages and KV API
// requests. It has a single /raft/message endpoint for all raft communication,
// plus /kv/get/{key}, /kv/put, /kv/delete/{key}, and /status endpoints.
type Server struct {
	node   raft.Node
	store  *kvstore.KVStore
	addr   string
	server *http.Server
}

// NewServer creates a new HTTP server.
func NewServer(node raft.Node, store *kvstore.KVStore, addr string) *Server {
	return &Server{
		node:  node,
		store: store,
		addr:  addr,
	}
}

// Start starts the HTTP server. It blocks until the server exits.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Raft message endpoint - all raft RPCs are Message structs.
	mux.Handle("/raft/message", Handler(s.node))

	// KV API endpoints.
	mux.HandleFunc("/kv/get/", s.handleGet)
	mux.HandleFunc("/kv/put", s.handlePut)
	mux.HandleFunc("/kv/delete/", s.handleDelete)
	mux.HandleFunc("/kv/all", s.handleGetAll)
	mux.HandleFunc("/status", s.handleStatus)

	s.server = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	log.Printf("Starting server on %s", s.addr)
	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server, waiting for in-flight
// requests to complete within the given context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Path[len("/kv/get/"):]
	if key == "" {
		http.Error(w, "Key required", http.StatusBadRequest)
		return
	}

	value, exists := s.store.Get(key)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	response := map[string]string{"key": key, "value": value}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Key required", http.StatusBadRequest)
		return
	}

	if err := s.store.Put(req.Key, req.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]string{"status": "ok"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Path[len("/kv/delete/"):]
	if key == "" {
		http.Error(w, "Key required", http.StatusBadRequest)
		return
	}

	if err := s.store.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]string{"status": "ok"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data := s.store.GetAll()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := s.node.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
