package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	servers []string
	client  *http.Client
	leader  string
}

func NewClient(servers []string) *Client {
	return &Client{
		servers: servers,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) Get(key string) (string, error) {
	for _, server := range c.servers {
		url := fmt.Sprintf("http://%s/kv/get/%s", server, key)
		
		resp, err := c.client.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				continue
			}

			var response struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}

			if err := json.Unmarshal(body, &response); err != nil {
				continue
			}

			return response.Value, nil
		} else if resp.StatusCode == http.StatusNotFound {
			return "", fmt.Errorf("key not found")
		}
	}

	return "", fmt.Errorf("failed to get key from any server")
}

func (c *Client) Put(key, value string) error {
	req := map[string]string{
		"key":   key,
		"value": value,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return err
	}

	if c.leader != "" {
		if err := c.tryPut(c.leader, jsonData); err == nil {
			return nil
		}
	}

	for _, server := range c.servers {
		if server == c.leader {
			continue
		}
		
		if err := c.tryPut(server, jsonData); err == nil {
			c.leader = server
			return nil
		}
	}

	return fmt.Errorf("failed to put key to any server")
}

func (c *Client) tryPut(server string, jsonData []byte) error {
	url := fmt.Sprintf("http://%s/kv/put", server)
	
	resp, err := c.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("put failed: %s", string(body))
}

func (c *Client) Delete(key string) error {
	if c.leader != "" {
		if err := c.tryDelete(c.leader, key); err == nil {
			return nil
		}
	}

	for _, server := range c.servers {
		if server == c.leader {
			continue
		}
		
		if err := c.tryDelete(server, key); err == nil {
			c.leader = server
			return nil
		}
	}

	return fmt.Errorf("failed to delete key from any server")
}

func (c *Client) tryDelete(server, key string) error {
	url := fmt.Sprintf("http://%s/kv/delete/%s", server, key)
	
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("delete failed: %s", string(body))
}

func (c *Client) GetAll() (map[string]string, error) {
	for _, server := range c.servers {
		url := fmt.Sprintf("http://%s/kv/all", server)
		
		resp, err := c.client.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				continue
			}

			var data map[string]string
			if err := json.Unmarshal(body, &data); err != nil {
				continue
			}

			return data, nil
		}
	}

	return nil, fmt.Errorf("failed to get all data from any server")
}

func (c *Client) GetStatus(server string) (map[string]interface{}, error) {
	url := fmt.Sprintf("http://%s/status", server)
	
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var status map[string]interface{}
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, err
	}

	return status, nil
}