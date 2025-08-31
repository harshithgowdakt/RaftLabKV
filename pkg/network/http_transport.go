package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/harshithgowda/distributed-key-value-store/pkg/raft"
)

type HTTPTransport struct {
	client  *http.Client
	baseURL string
}

func NewHTTPTransport(baseURL string) *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		baseURL: baseURL,
	}
}

func (t *HTTPTransport) RequestVote(target string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	url := fmt.Sprintf("http://%s/raft/requestVote", target)
	
	jsonData, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var reply raft.RequestVoteReply
	if err := json.Unmarshal(body, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}

func (t *HTTPTransport) AppendEntries(target string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	url := fmt.Sprintf("http://%s/raft/appendEntries", target)
	
	jsonData, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var reply raft.AppendEntriesReply
	if err := json.Unmarshal(body, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}