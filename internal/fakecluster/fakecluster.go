// Package fakecluster is a small in-process Solana cluster for tests: a
// JSON-RPC endpoint answering getSlot/getClusterNodes, and snapshot nodes
// that behave like Agave's RPC HTTP service (303 redirects with a lowercase
// "location" header, Content-Length, Range ignored unless enabled), plus
// knobs for the failure modes the finder must survive.
package fakecluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Hashes usable in archive names (base58, 44 chars).
const (
	HashA = "7jMmeXZSNcWPrB2RsTdeXfXrsyW5c1BfPjqoLW2X5T7V"
	HashB = "RJVE6prxeCAjvaWffrmrUoeHe58jwX3QqmvWeKxA96q2"
	HashC = "DXUyfhMVbLQUeoRTw1J1fria8DE4JDJ5mX6btFsTgP4v"
)

// NodeSpec describes one snapshot node.
type NodeSpec struct {
	FullSlot uint64
	FullHash string // default HashA
	FullSize int64  // default 8 MiB
	IncBase  uint64 // default FullSlot
	IncSlot  uint64 // 0 = no incremental
	IncHash  string // default HashC
	IncSize  int64  // default 1 MiB
	Ext      string // default "tar.zst"

	RateBps      float64       // throttle archive bodies (0 = unlimited)
	HeadDelay    time.Duration // added to redirect responses (latency)
	Range        bool          // honour Range requests (Agave does not)
	FailAfter    int64         // first FailTimes GETs of an archive drop the connection after this many bytes
	FailTimes    int
	StallAfter   int64 // first archive GET stops sending (connection kept open) after this many bytes
	NoLength     bool  // omit Content-Length
	BadLocation  string
	Version      string
	PrivateOnly  bool  // advertise no RPC address (reachable only via --with_private_rpc)
	NoSnapshot   bool  // 404 on the redirect paths
	Unreachable  bool  // listed in gossip, but nothing listens
	ShortContent int64 // send this many bytes fewer than Content-Length, once
}

// Node is a running snapshot node.
type Node struct {
	Spec NodeSpec
	Addr string
	srv  *httptest.Server

	mu       sync.Mutex
	full     string
	inc      string
	sizes    map[string]int64
	fails    int
	stalled  bool
	shorted  bool
	Gets     map[string]int // archive GETs by path
	RangeReq int
	Served   atomic.Int64
	// AfterRedirect, if set, runs after a redirect path was answered (under
	// no lock), e.g. to rotate the full snapshot right after discovery.
	AfterRedirect func(path string)
}

// SetAfterRedirect installs the AfterRedirect hook.
func (n *Node) SetAfterRedirect(f func(path string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.AfterRedirect = f
}

// Cluster is an RPC endpoint plus nodes.
type Cluster struct {
	RPC   *httptest.Server
	Nodes []*Node
	Slot  atomic.Uint64
	// RPCDown makes the RPC endpoint answer HTTP 503.
	RPCDown atomic.Bool
}

func defaults(s NodeSpec) NodeSpec {
	if s.FullHash == "" {
		s.FullHash = HashA
	}
	if s.IncHash == "" {
		s.IncHash = HashC
	}
	if s.FullSize == 0 {
		s.FullSize = 8 << 20
	}
	if s.IncSize == 0 {
		s.IncSize = 1 << 20
	}
	if s.IncBase == 0 {
		s.IncBase = s.FullSlot
	}
	if s.Ext == "" {
		s.Ext = "tar.zst"
	}
	if s.Version == "" {
		s.Version = "3.0.0"
	}
	return s
}

// FullName / IncName build archive names.
func FullName(slot uint64, hash, ext string) string {
	return fmt.Sprintf("snapshot-%d-%s.%s", slot, hash, ext)
}
func IncName(base, slot uint64, hash, ext string) string {
	return fmt.Sprintf("incremental-snapshot-%d-%d-%s.%s", base, slot, hash, ext)
}

// New starts a cluster at the given slot.
func New(slot uint64, specs ...NodeSpec) *Cluster {
	c := &Cluster{}
	c.Slot.Store(slot)
	for _, s := range specs {
		c.Nodes = append(c.Nodes, newNode(defaults(s)))
	}
	c.RPC = httptest.NewServer(http.HandlerFunc(c.serveRPC))
	return c
}

// Close stops every server.
func (c *Cluster) Close() {
	c.RPC.Close()
	for _, n := range c.Nodes {
		if n.srv != nil {
			n.srv.CloseClientConnections()
			n.srv.Close()
		}
	}
}

func newNode(s NodeSpec) *Node {
	n := &Node{Spec: s, sizes: map[string]int64{}, Gets: map[string]int{}}
	n.full = FullName(s.FullSlot, s.FullHash, s.Ext)
	n.sizes[n.full] = s.FullSize
	if s.IncSlot > 0 {
		n.inc = IncName(s.IncBase, s.IncSlot, s.IncHash, s.Ext)
		n.sizes[n.inc] = s.IncSize
	}
	if s.Unreachable {
		// Reserve a port and release it: connections are refused.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err == nil {
			n.Addr = l.Addr().String()
			l.Close()
		}
		return n
	}
	n.srv = httptest.NewServer(http.HandlerFunc(n.serve))
	n.Addr = strings.TrimPrefix(n.srv.URL, "http://")
	return n
}

// FullName / IncName return the node's current archive names.
func (n *Node) FullName() string { n.mu.Lock(); defer n.mu.Unlock(); return n.full }
func (n *Node) IncName() string  { n.mu.Lock(); defer n.mu.Unlock(); return n.inc }

// Rotate replaces the node's full snapshot (the old one is deleted, i.e. 404).
func (n *Node) Rotate(fullSlot uint64, hash string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.sizes, n.full)
	n.full = FullName(fullSlot, hash, n.Spec.Ext)
	n.sizes[n.full] = n.Spec.FullSize
}

// SetIncremental replaces the node's incremental snapshot.
func (n *Node) SetIncremental(base, slot uint64, hash string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.inc != "" {
		delete(n.sizes, n.inc)
	}
	n.inc = IncName(base, slot, hash, n.Spec.Ext)
	n.sizes[n.inc] = n.Spec.IncSize
}

// GetCount returns how many archive GETs a path received.
func (n *Node) GetCount(name string) int { n.mu.Lock(); defer n.mu.Unlock(); return n.Gets["/"+name] }

func (c *Cluster) serveRPC(w http.ResponseWriter, r *http.Request) {
	if c.RPCDown.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Method string `json:"method"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	var result any
	switch req.Method {
	case "getSlot":
		result = c.Slot.Load()
	case "getClusterNodes":
		var nodes []map[string]any
		for i, n := range c.Nodes {
			host, port, _ := net.SplitHostPort(n.Addr)
			m := map[string]any{
				"pubkey":  fmt.Sprintf("Node%02d1111111111111111111111111111111111", i),
				"gossip":  net.JoinHostPort(host, "8001"),
				"version": n.Spec.Version,
				"rpc":     nil,
			}
			if !n.Spec.PrivateOnly {
				m["rpc"] = net.JoinHostPort(host, port)
			}
			nodes = append(nodes, m)
		}
		// A gossip-only node without RPC, as most of mainnet is.
		nodes = append(nodes, map[string]any{"pubkey": "Gossip1111111111111111111111111111111111111", "gossip": "127.0.0.1:1", "rpc": nil, "version": nil})
		result = nodes
	default:
		http.Error(w, "unknown method", http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
}

func (n *Node) redirect(w http.ResponseWriter, name string) {
	if n.Spec.HeadDelay > 0 {
		time.Sleep(n.Spec.HeadDelay)
	}
	if name == "" || n.Spec.NoSnapshot {
		http.NotFound(w, nil)
		return
	}
	// Agave (hyper) sends the header name in lower case; keep it that way.
	w.Header()["location"] = []string{"/" + name}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusSeeOther)
}

func (n *Node) serve(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	full, inc, hook := n.full, n.inc, n.AfterRedirect
	n.mu.Unlock()
	if hook != nil && (r.URL.Path == "/snapshot.tar.bz2" || r.URL.Path == "/incremental-snapshot.tar.bz2") {
		defer hook(r.URL.Path)
	}
	switch r.URL.Path {
	case "/snapshot.tar.bz2":
		if n.Spec.BadLocation != "" {
			w.Header()["location"] = []string{n.Spec.BadLocation}
			w.WriteHeader(http.StatusSeeOther)
			return
		}
		n.redirect(w, full)
		return
	case "/incremental-snapshot.tar.bz2":
		n.redirect(w, inc)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/")
	n.mu.Lock()
	size, ok := n.sizes[name]
	if ok && r.Method == http.MethodGet {
		n.Gets[r.URL.Path]++
	}
	n.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	var start int64
	status := http.StatusOK
	if rg := r.Header.Get("Range"); rg != "" {
		n.mu.Lock()
		n.RangeReq++
		n.mu.Unlock()
		if n.Spec.Range && strings.HasPrefix(rg, "bytes=") && strings.HasSuffix(rg, "-") {
			if s, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(rg, "bytes="), "-"), 10, 64); err == nil && s < size {
				start, status = s, http.StatusPartialContent
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, size-1, size))
			}
		}
	}
	if !n.Spec.NoLength {
		w.Header().Set("Content-Length", strconv.FormatInt(size-start, 10))
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}

	limit := size
	n.mu.Lock()
	drop := false
	if n.Spec.FailAfter > 0 && n.fails < n.Spec.FailTimes {
		n.fails++
		drop = true
		limit = min(size, start+n.Spec.FailAfter)
	}
	stall := false
	if n.Spec.StallAfter > 0 && !n.stalled {
		n.stalled, stall = true, true
		limit = min(size, start+n.Spec.StallAfter)
	}
	if n.Spec.ShortContent > 0 && !n.shorted {
		n.shorted = true
		limit = size - n.Spec.ShortContent
	}
	n.mu.Unlock()

	buf := make([]byte, 64<<10)
	t0 := time.Now()
	var sent int64
	for pos := start; pos < limit; {
		k := min(int64(len(buf)), limit-pos)
		Fill(name, pos, buf[:k])
		if _, err := w.Write(buf[:k]); err != nil {
			return
		}
		pos += k
		sent += k
		n.Served.Add(k)
		if n.Spec.RateBps > 0 {
			if ahead := time.Duration(float64(sent)/n.Spec.RateBps*1e9) - time.Since(t0); ahead > 0 {
				time.Sleep(ahead)
			}
		}
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	if stall {
		<-r.Context().Done() // hold the connection open, send nothing
		return
	}
	if drop || limit < size {
		// Abort the connection mid-body (net/http closes it; nothing is logged).
		panic(http.ErrAbortHandler)
	}
}

// Fill writes the deterministic content of archive name at offset off into p.
// Every byte depends on its absolute position, so a resumed download that was
// appended at the wrong offset does not verify.
func Fill(name string, off int64, p []byte) {
	var seed byte
	for i := 0; i < len(name); i++ {
		seed = seed*31 + name[i]
	}
	for i := range p {
		pos := off + int64(i)
		p[i] = byte(pos%251) ^ byte(pos>>16) ^ seed
	}
}

// Verify checks that the file at path is exactly archive name of the given size.
func Verify(path, name string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() != size {
		return fmt.Errorf("%s: size %d, want %d", path, st.Size(), size)
	}
	buf := make([]byte, 1<<20)
	want := make([]byte, 1<<20)
	var off int64
	for {
		k, err := io.ReadFull(f, buf)
		if k > 0 {
			Fill(name, off, want[:k])
			if !bytes.Equal(buf[:k], want[:k]) {
				return fmt.Errorf("%s: content differs near offset %d", path, off)
			}
			off += int64(k)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
