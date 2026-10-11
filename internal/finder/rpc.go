package finder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// ClusterNode is one entry of getClusterNodes.
type ClusterNode struct {
	Pubkey  string  `json:"pubkey"`
	Gossip  *string `json:"gossip"`
	RPC     *string `json:"rpc"`
	Version *string `json:"version"`
}

// RPCClient talks JSON-RPC to the cluster entrypoint(s). With several URLs
// each call tries them in order until one answers.
type RPCClient struct {
	URLs []string
	HTTP *http.Client
}

// NewRPCClient builds a client for a comma-separated list of RPC URLs.
func NewRPCClient(urls string, timeout time.Duration) *RPCClient {
	var list []string
	for _, u := range strings.Split(urls, ",") {
		if u = strings.TrimSpace(u); u != "" {
			list = append(list, u)
		}
	}
	return &RPCClient{
		URLs: list,
		HTTP: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: timeout,
			},
		},
	}
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (c *RPCClient) call(ctx context.Context, method string, params any, out any) error {
	if len(c.URLs) == 0 {
		return errors.New("no RPC URL configured")
	}
	req := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method}
	if params != nil {
		req["params"] = params
	}
	body, _ := json.Marshal(req)
	var errs []error
	for _, u := range c.URLs {
		err := c.callOne(ctx, u, body, out)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", u, err))
		if ctx.Err() != nil {
			break
		}
	}
	return fmt.Errorf("%s failed: %w", method, errors.Join(errs...))
}

func (c *RPCClient) callOne(ctx context.Context, url string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// getClusterNodes on mainnet is ~1.5 MB; 64 MB is a sanity cap, not a limit.
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(data), 200))
	}
	var r rpcResponse
	if err := json.Unmarshal(data, &r); err != nil {
		return fmt.Errorf("bad JSON-RPC response: %w", err)
	}
	if r.Error != nil {
		return fmt.Errorf("RPC error %d: %s", r.Error.Code, r.Error.Message)
	}
	if len(r.Result) == 0 || string(r.Result) == "null" {
		return errors.New("empty result")
	}
	return json.Unmarshal(r.Result, out)
}

// GetSlot returns the cluster's current slot.
func (c *RPCClient) GetSlot(ctx context.Context) (uint64, error) {
	var slot uint64
	err := c.call(ctx, "getSlot", nil, &slot)
	return slot, err
}

// GetClusterNodes returns the gossip view of the cluster.
func (c *RPCClient) GetClusterNodes(ctx context.Context) ([]ClusterNode, error) {
	var nodes []ClusterNode
	err := c.call(ctx, "getClusterNodes", nil, &nodes)
	return nodes, err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
