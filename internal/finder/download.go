package finder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Download errors. Only errShortBody and transport errors are retried on the
// same node; the others make the caller move on to another node (or give up).
var (
	ErrTooSlow   = errors.New("download speed below --min_download_speed")
	ErrGone      = errors.New("archive is no longer served by the node")
	ErrNoSpace   = errors.New("not enough free disk space")
	ErrStalled   = errors.New("download stalled")
	ErrSlowdown  = errors.New("node slowed down: finishing here would take longer than starting over at --min_download_speed")
	errShortBody = errors.New("connection closed before the whole archive was received")
	errNoLength  = errors.New("server sent no Content-Length; the size cannot be verified")
)

// FetchOptions controls one archive download.
type FetchOptions struct {
	MinSpeed         float64       // bytes/s; 0 disables the speed gate
	MeasureTime      time.Duration // speed gate window (from the first body byte)
	MaxSpeed         float64       // bytes/s; 0 = unlimited
	StallTimeout     time.Duration // abort when no byte arrives for this long
	SwitchOnSlowdown bool          // see ErrSlowdown
	SlowdownWindow   time.Duration
	DryRun           bool // measure the speed only, write nothing
	ProgressEvery    time.Duration
	ReserveBytes     uint64 // free space to keep on top of this archive (e.g. for the incremental)
	MaxAttempts      int    // attempts on this node for broken transfers
}

// FetchResult describes a finished (or measured, with DryRun) download.
type FetchResult struct {
	Addr         string  `json:"source"`
	Archive      Archive `json:"archive"`
	Kind         string  `json:"kind"`
	Path         string  `json:"path,omitempty"`
	Size         int64   `json:"size"`
	Transferred  int64   `json:"bytes_transferred"`
	Seconds      float64 `json:"seconds"`
	AvgMBps      float64 `json:"avg_mb_per_s"`
	MeasuredMBps float64 `json:"measured_mb_per_s,omitempty"`
	ResumedFrom  int64   `json:"resumed_from,omitempty"`
	Restarts     int     `json:"restarts,omitempty"` // Range ignored -> started again from byte 0
}

// Downloader fetches archives over plain HTTP.
type Downloader struct {
	client    *http.Client
	log       *Logger
	FreeSpace func(dir string) (uint64, error)
}

// NewDownloader builds a downloader. Connection setup and response headers
// have timeouts; the body has none — the stall watchdog covers it.
func NewDownloader(log *Logger) *Downloader {
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    true,
		ReadBufferSize:        1 << 20,
		WriteBufferSize:       64 << 10,
	}
	return &Downloader{
		client: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("unexpected redirect while downloading an archive")
		}},
		log:       log,
		FreeSpace: freeSpace,
	}
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range []error{ErrTooSlow, ErrGone, ErrNoSpace, ErrStalled, ErrSlowdown, errNoLength, context.Canceled} {
		if errors.Is(err, e) {
			return false
		}
	}
	return true
}

// Fetch downloads archive a from addr into dir. The data goes to
// "tmp-<name>" and is renamed to "<name>" only after its size matched the
// server's Content-Length, so a final name always means a complete file.
//
// Resume: a "tmp-<name>" left by an earlier attempt is continued with a Range
// request if it came from the same node (recorded in "tmp-<name>.source").
// A server that ignores Range (Agave does, as of 2026-10) answers 200 with
// the whole file; the partial file is then truncated and the download starts
// over — never appended to, which would corrupt it.
func (d *Downloader) Fetch(ctx context.Context, addr string, a Archive, dir string, o FetchOptions) (FetchResult, error) {
	res := FetchResult{Addr: addr, Archive: a, Kind: a.Kind.String()}
	if !o.DryRun {
		res.Path = archivePath(dir, a)
	}
	tmp := tmpPath(dir, a)
	meta := tmp + ".source"
	if o.MaxAttempts < 1 {
		o.MaxAttempts = 1
	}
	if !o.DryRun {
		removeStaleTmp(dir, a, d.log)
	}
	start := time.Now()
	var err error
	for attempt := 1; attempt <= o.MaxAttempts; attempt++ {
		var offset int64
		if !o.DryRun {
			offset = resumeOffset(tmp, meta, addr, a)
			if offset > 0 {
				d.log.Infof("%s: resuming %s from byte %d", addr, a.Name, offset)
			}
		}
		err = d.fetchOnce(ctx, addr, a, tmp, meta, offset, o, &res)
		if err == nil {
			break
		}
		if !retryable(err) || ctx.Err() != nil || attempt == o.MaxAttempts {
			break
		}
		d.log.Warnf("%s: %s: %v — retrying (%d/%d)", addr, a.Name, err, attempt+1, o.MaxAttempts)
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
	res.Seconds = time.Since(start).Seconds()
	if res.Seconds > 0 {
		res.AvgMBps = float64(res.Transferred) / res.Seconds / 1e6
	}
	if o.DryRun {
		return res, err
	}
	if err != nil {
		if ctx.Err() == nil {
			// Moving on to another node: a partial file from this one is useless
			// (archives of the same slot are not byte-identical across nodes).
			_ = os.Remove(tmp)
			_ = os.Remove(meta)
		}
		return res, err
	}
	if err := os.Rename(tmp, res.Path); err != nil {
		return res, fmt.Errorf("rename %s: %w", tmp, err)
	}
	_ = os.Remove(meta)
	return res, nil
}

// removeStaleTmp deletes partial downloads of other archives (left by an
// interrupted run): they can never be resumed — archives of the same slot
// differ byte-wise between nodes — and a full one is ~120 GB.
func removeStaleTmp(dir string, keep Archive, log *Logger) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "tmp-") || !e.Type().IsRegular() {
			continue
		}
		base := strings.TrimSuffix(strings.TrimPrefix(name, "tmp-"), ".source")
		if _, err := ParseArchiveName(base); err != nil || base == keep.Name {
			continue
		}
		if err := os.Remove(dir + "/" + name); err == nil {
			log.Infof("Removed stale partial download %s", name)
		}
	}
}

func resumeOffset(tmp, meta, addr string, a Archive) int64 {
	m, err := os.ReadFile(meta)
	if err != nil {
		_ = os.Remove(tmp)
		return 0
	}
	fields := strings.Fields(string(m))
	st, err := os.Stat(tmp)
	if err != nil || len(fields) != 3 || fields[0] != addr || fields[1] != a.Name {
		_ = os.Remove(tmp)
		_ = os.Remove(meta)
		return 0
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || st.Size() >= size {
		_ = os.Remove(tmp)
		_ = os.Remove(meta)
		return 0
	}
	return st.Size()
}

// parseContentRange parses "bytes <start>-<end>/<total>".
func parseContentRange(v string) (start, end, total int64, err error) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "bytes ") {
		return 0, 0, 0, fmt.Errorf("bad Content-Range %q", v)
	}
	se, tot, ok := strings.Cut(v[len("bytes "):], "/")
	s, e, ok2 := strings.Cut(se, "-")
	if !ok || !ok2 {
		return 0, 0, 0, fmt.Errorf("bad Content-Range %q", v)
	}
	if start, err = strconv.ParseInt(s, 10, 64); err == nil {
		if end, err = strconv.ParseInt(e, 10, 64); err == nil {
			total, err = strconv.ParseInt(tot, 10, 64)
		}
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("bad Content-Range %q", v)
	}
	return start, end, total, nil
}

func (d *Downloader) fetchOnce(ctx context.Context, addr string, a Archive, tmp, meta string, offset int64, o FetchOptions, res *FetchResult) error {
	reqCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+"/"+a.Name, nil)
	if err != nil {
		return err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var total int64
	switch resp.StatusCode {
	case http.StatusOK:
		if offset > 0 {
			d.log.Infof("%s: server ignored the Range request — downloading %s from the start", addr, a.Name)
			offset = 0
			res.Restarts++
		}
		total = resp.ContentLength
	case http.StatusPartialContent:
		start, _, tot, err := parseContentRange(resp.Header.Get("Content-Range"))
		if err != nil || start != offset || resp.ContentLength < 0 || tot != offset+resp.ContentLength {
			_ = os.Remove(meta) // next attempt starts from 0
			return fmt.Errorf("inconsistent partial response (Content-Range %q, Content-Length %d, offset %d)",
				resp.Header.Get("Content-Range"), resp.ContentLength, offset)
		}
		total = tot
		res.ResumedFrom = offset
	case http.StatusNotFound, http.StatusGone:
		return fmt.Errorf("%w (HTTP %d)", ErrGone, resp.StatusCode)
	default:
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if total < 0 {
		return errNoLength
	}
	if res.Size > 0 && res.Size != total {
		return fmt.Errorf("%w: size changed from %d to %d between attempts", ErrGone, res.Size, total)
	}
	res.Size = total

	var dst io.Writer = io.Discard
	if !o.DryRun {
		need := uint64(total-offset) + o.ReserveBytes
		dir := dirOf(tmp)
		if free, err := d.FreeSpace(dir); err == nil && free < need {
			return fmt.Errorf("%w in %s: need %s, have %s", ErrNoSpace, dir, HumanBytes(int64(need)), HumanBytes(int64(free)))
		}
		flags := os.O_WRONLY | os.O_CREATE
		if offset == 0 {
			flags |= os.O_TRUNC
		}
		f, err := os.OpenFile(tmp, flags, 0o644)
		if err != nil {
			return err
		}
		// The success path closes f itself and checks the error; this only
		// covers the early returns.
		defer f.Close()
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		if err := os.WriteFile(meta, []byte(fmt.Sprintf("%s %s %d\n", addr, a.Name, total)), 0o644); err != nil {
			return err
		}
		dst = f
	}

	// Stall watchdog: cancel the request when no byte arrived for StallTimeout.
	var lastByte atomic.Int64
	lastByte.Store(time.Now().UnixNano())
	done := make(chan struct{})
	defer close(done)
	if o.StallTimeout > 0 {
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case now := <-t.C:
					if now.Sub(time.Unix(0, lastByte.Load())) > o.StallTimeout {
						cancel(ErrStalled)
						return
					}
				}
			}
		}()
	}

	gate := o.MinSpeed > 0 || o.DryRun
	warmup := time.Duration(0)
	if o.MeasureTime > 2*time.Second {
		warmup = time.Second // skip TCP slow start
	}
	window := o.SlowdownWindow
	if window <= 0 {
		window = 30 * time.Second
	}
	buf := make([]byte, 4<<20)
	var got, warmBytes, winBytes int64
	warmTaken := warmup == 0
	start := time.Now()
	winStart := start
	nextLog := start.Add(o.ProgressEvery)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return fmt.Errorf("write %s: %w", tmp, werr)
			}
			got += int64(n)
			res.Transferred += int64(n)
			lastByte.Store(time.Now().UnixNano())
		}
		if rerr != nil && rerr != io.EOF {
			if cause := context.Cause(reqCtx); errors.Is(cause, ErrStalled) {
				return fmt.Errorf("%w: no data for %s", ErrStalled, o.StallTimeout)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: %v", errShortBody, rerr)
		}
		now := time.Now()
		el := now.Sub(start)
		if !warmTaken && el >= warmup {
			warmBytes, warmTaken = got, true
		}
		if gate && o.MeasureTime > 0 && el >= o.MeasureTime {
			gate = false
			speed := float64(got-warmBytes) / (el - warmup).Seconds()
			res.MeasuredMBps = speed / 1e6
			if o.MinSpeed > 0 && speed < o.MinSpeed {
				return fmt.Errorf("%w: %.1f MB/s < %.1f MB/s", ErrTooSlow, speed/1e6, o.MinSpeed/1e6)
			}
			d.log.Infof("%s: speed %.1f MB/s over %s — OK", addr, speed/1e6, (el - warmup).Round(100*time.Millisecond))
			if o.DryRun {
				return nil
			}
			winStart, winBytes = now, got
		}
		if o.MaxSpeed > 0 {
			if ahead := time.Duration(float64(got)/o.MaxSpeed*1e9) - el; ahead > 0 {
				select {
				case <-time.After(ahead):
				case <-reqCtx.Done():
				}
			}
		}
		if o.SwitchOnSlowdown && o.MinSpeed > 0 && !gate && now.Sub(winStart) >= window {
			ws := float64(got-winBytes) / now.Sub(winStart).Seconds()
			remaining := float64(total - offset - got)
			if ws > 0 && remaining/ws > float64(total)/o.MinSpeed {
				return fmt.Errorf("%w (%.1f MB/s over the last %s, %s left)", ErrSlowdown, ws/1e6, window, HumanBytes(int64(remaining)))
			}
			winStart, winBytes = now, got
		}
		if o.ProgressEvery > 0 && now.After(nextLog) {
			nextLog = now.Add(o.ProgressEvery)
			pos := offset + got
			avg := float64(got) / el.Seconds()
			eta := "?"
			if avg > 0 {
				eta = time.Duration(float64(total-pos) / avg * float64(time.Second)).Round(time.Second).String()
			}
			d.log.Infof("%s: %s / %s (%.1f%%), avg %.0f MB/s, ETA %s", a.Name, HumanBytes(pos), HumanBytes(total), 100*float64(pos)/float64(max(total, 1)), avg/1e6, eta)
		}
		if rerr == io.EOF {
			break
		}
	}
	if gate {
		// The whole file arrived before the measurement window ended.
		res.MeasuredMBps = float64(got) / max(time.Since(start).Seconds(), 1e-3) / 1e6
	}
	if pos := offset + got; pos != total {
		return fmt.Errorf("%w: got %d of %d bytes", errShortBody, pos, total)
	}
	if f, ok := dst.(*os.File); ok {
		if err := f.Close(); err != nil {
			return fmt.Errorf("close %s: %w", tmp, err)
		}
		st, err := os.Stat(tmp)
		if err != nil {
			return err
		}
		if st.Size() != total {
			return fmt.Errorf("%w: %s is %d bytes on disk, expected %d", errShortBody, tmp, st.Size(), total)
		}
	}
	return nil
}

func dirOf(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		if i == 0 {
			return "/"
		}
		return p[:i]
	}
	return "."
}

// HumanBytes formats a byte count with decimal units (MB = 10^6, like the
// MB/s speeds).
func HumanBytes(n int64) string {
	const unit = 1000
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "kMGTPE"[exp])
}
