package finder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// Agave serves these two paths as 303 redirects to the current archive names.
// The ".tar.bz2" suffix is historical; the archives are usually .tar.zst.
const (
	fullRedirectPath = "/snapshot.tar.bz2"
	incRedirectPath  = "/incremental-snapshot.tar.bz2"
)

// ProbeResult is what one node advertises.
type ProbeResult struct {
	Target
	Full    *Archive      // nil if the node serves no (valid) full snapshot
	Inc     *Archive      // nil if the node serves no (valid) incremental snapshot
	Latency time.Duration // time to response headers of the node's first request (incl. TCP connect)
	Err     error         // set when the node is unusable; Reason classifies it
	Reason  string
}

// Prober checks many nodes concurrently with short timeouts.
type Prober struct {
	Concurrency int
	Timeout     time.Duration // per request, connect + headers
	client      *http.Client
	transport   *http.Transport
}

// NewProber builds a prober. Redirects are never followed: the Location
// header is the answer.
func NewProber(concurrency int, timeout time.Duration) *Prober {
	if concurrency < 1 {
		concurrency = 1
	}
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: timeout}).DialContext,
		ResponseHeaderTimeout: timeout,
		MaxIdleConns:          concurrency,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       5 * time.Second,
		DisableCompression:    true,
	}
	return &Prober{
		Concurrency: concurrency,
		Timeout:     timeout,
		transport:   tr,
		client: &http.Client{
			Transport:     tr,
			Timeout:       2 * timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// ProbeAll probes every target with bounded concurrency. Results are in the
// order of targets.
func (p *Prober) ProbeAll(ctx context.Context, targets []Target, onDone func()) []ProbeResult {
	results := make([]ProbeResult, len(targets))
	sem := make(chan struct{}, p.Concurrency)
	var wg sync.WaitGroup
	for i, t := range targets {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			results[i] = ProbeResult{Target: t, Err: ctx.Err(), Reason: "canceled"}
			continue
		}
		wg.Add(1)
		go func(i int, t Target) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = p.Probe(ctx, t)
			if onDone != nil {
				onDone()
			}
		}(i, t)
	}
	wg.Wait()
	p.transport.CloseIdleConnections()
	return results
}

// Probe asks one node for its incremental and full snapshot names. Both
// requests share one keep-alive connection; the latency is that of the first
// request and therefore includes the TCP handshake, like upstream's.
func (p *Prober) Probe(ctx context.Context, t Target) ProbeResult {
	r := ProbeResult{Target: t}
	incLoc, incLat, incErr := p.resolve(ctx, t.Addr, incRedirectPath)
	r.Latency = incLat
	if incErr != nil && isNetErr(incErr) {
		// The node did not answer at all; do not wait for a second timeout.
		r.Err, r.Reason = incErr, "unreachable"
		return r
	}
	fullLoc, fullLat, fullErr := p.resolve(ctx, t.Addr, fullRedirectPath)
	if incErr != nil {
		r.Latency = fullLat
	}
	if fullErr != nil {
		r.Err = fullErr
		if isNetErr(fullErr) {
			r.Reason = "unreachable"
		} else {
			r.Reason = "no_snapshot"
		}
		return r
	}
	full, err := ParseLocation(fullLoc, KindFull)
	if err != nil {
		r.Err, r.Reason = err, "bad_location"
		return r
	}
	r.Full = &full
	if incErr == nil {
		if inc, err := ParseLocation(incLoc, KindIncremental); err == nil {
			r.Inc = &inc
		}
	}
	return r
}

// ProbeIncremental re-resolves only the incremental snapshot of a node.
func (p *Prober) ProbeIncremental(ctx context.Context, addr string) (*Archive, time.Duration, error) {
	loc, lat, err := p.resolve(ctx, addr, incRedirectPath)
	if err != nil {
		return nil, lat, err
	}
	a, err := ParseLocation(loc, KindIncremental)
	if err != nil {
		return nil, lat, err
	}
	return &a, lat, nil
}

// ProbeFull re-resolves only the full snapshot of a node.
func (p *Prober) ProbeFull(ctx context.Context, addr string) (*Archive, error) {
	loc, _, err := p.resolve(ctx, addr, fullRedirectPath)
	if err != nil {
		return nil, err
	}
	a, err := ParseLocation(loc, KindFull)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

var errNoRedirect = errors.New("no snapshot redirect")

func (p *Prober) resolve(ctx context.Context, addr, path string) (string, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, "http://"+addr+path, nil)
	if err != nil {
		return "", 0, err
	}
	start := time.Now()
	resp, err := p.client.Do(req)
	lat := time.Since(start)
	if err != nil {
		return "", lat, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode < 300 || resp.StatusCode > 399 {
		return "", lat, fmt.Errorf("%w: HTTP %d", errNoRedirect, resp.StatusCode)
	}
	loc := resp.Header.Get("Location") // canonicalised: matches "location" too
	if loc == "" {
		return "", lat, fmt.Errorf("%w: empty Location", errNoRedirect)
	}
	return loc, lat, nil
}

// isNetErr tells a dead or silent node (dial error, timeout, reset) from one
// that answered with something we do not like.
func isNetErr(err error) bool {
	if err == nil || errors.Is(err, errNoRedirect) {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	var oe *net.OpError
	return errors.As(err, &oe) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded)
}
