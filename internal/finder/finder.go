package finder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Exit codes. Scripts can rely on these; anything non-zero means "no usable
// snapshot was produced by this run".
const (
	ExitOK          = 0
	ExitFailure     = 1   // unexpected/internal error
	ExitUsage       = 2   // bad command line
	ExitRPC         = 3   // cluster RPC unreachable (getSlot / getClusterNodes)
	ExitNoSnapshot  = 4   // no node matches the filters or meets --min_download_speed
	ExitDownload    = 5   // a suitable node was found but the download failed
	ExitLocal       = 6   // local problem: snapshot path not writable, not enough disk space
	ExitInterrupted = 130 // SIGINT/SIGTERM
)

// errAllKnownBad: discovery found only nodes that already failed in this run.
var errAllKnownBad = errors.New("every suitable node already failed earlier in this run")

// ExitError carries the process exit code for an error.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

func exitErr(code int, format string, args ...any) *ExitError {
	return &ExitError{Code: code, Err: fmt.Errorf(format, args...)}
}

// CodeOf maps an error returned by Run to an exit code.
func CodeOf(err error) int {
	if err == nil {
		return ExitOK
	}
	if errors.Is(err, context.Canceled) {
		return ExitInterrupted
	}
	var ee *ExitError
	if errors.As(err, &ee) {
		return ee.Code
	}
	return ExitFailure
}

// Config is the complete run configuration (see cmd/snapshot-finder for the flags).
type Config struct {
	RPC              string
	Threads          int
	Slot             uint64
	Version          string
	WildcardVersion  string
	MaxAge           uint64
	MinSpeedMB       float64
	MinIncSpeedMB    float64 // incremental downloads; capped at MinSpeedMB and MaxSpeedMB, 0 = no gate
	MaxSpeedMB       float64
	MaxLatency       time.Duration
	WithPrivateRPC   bool
	MeasureTime      time.Duration
	SnapshotPath     string
	Retries          int
	Sleep            time.Duration
	SortOrder        string
	IPBlacklist      IPSet
	Blacklist        []string
	DryRun           bool
	ProbeTimeout     time.Duration
	RPCTimeout       time.Duration
	MaxSpeedTests    int
	StallTimeout     time.Duration
	MinAgreement     int
	SwitchOnSlowdown bool
	ProgressEvery    time.Duration
	ReserveBytes     uint64 // kept free after the full snapshot, for the incremental
	IgnoreLocal      bool   // never reuse a full snapshot already in SnapshotPath
	WriteCandidates  bool   // write <snapshot_path>/snapshot.json like upstream
}

// SpeedTest records one node's speed gate result.
type SpeedTest struct {
	Kind   string  `json:"kind,omitempty"` // "incremental"; empty for the full snapshot
	Addr   string  `json:"addr"`
	MBps   float64 `json:"mb_per_s"`
	Result string  `json:"result"`
}

// Summary is the machine-readable outcome of a run.
type Summary struct {
	Tool            string         `json:"tool"`
	Version         string         `json:"version"`
	Status          string         `json:"status"` // "ok" or "error"
	ExitCode        int            `json:"exit_code"`
	Error           string         `json:"error,omitempty"`
	DryRun          bool           `json:"dry_run"`
	StartedAt       time.Time      `json:"started_at"`
	FinishedAt      time.Time      `json:"finished_at"`
	Seconds         float64        `json:"seconds"`
	RPC             string         `json:"rpc"`
	SnapshotPath    string         `json:"snapshot_path"`
	CurrentSlot     uint64         `json:"current_slot"`
	Attempts        int            `json:"attempts"`
	Targets         TargetStats    `json:"targets"`
	ProbeSeconds    float64        `json:"probe_seconds"`
	Discarded       map[string]int `json:"discarded"`
	Candidates      int            `json:"candidates"`
	Warnings        []string       `json:"warnings,omitempty"`
	SpeedTests      []SpeedTest    `json:"speed_tests"`
	ReusedLocalFull string         `json:"reused_local_full,omitempty"`
	Full            *FetchResult   `json:"full,omitempty"`
	Incremental     *FetchResult   `json:"incremental,omitempty"`
	Files           []string       `json:"files"`
}

type runner struct {
	cfg         Config
	log         *Logger
	rpc         *RPCClient
	prober      *Prober
	dl          *Downloader
	sum         *Summary
	withPrivate bool
	bad         map[string]string // nodes that failed us this run, never retried
	currentSlot uint64
}

// Run finds and downloads a full + matching incremental snapshot. It always
// returns a summary; the error (if any) maps to an exit code via CodeOf.
func Run(ctx context.Context, cfg Config, log *Logger, version string, dl *Downloader) (*Summary, error) {
	sum := &Summary{
		Tool: "snapshot-finder", Version: version, DryRun: cfg.DryRun, StartedAt: time.Now().UTC(),
		RPC: cfg.RPC, SnapshotPath: cfg.SnapshotPath, Discarded: map[string]int{}, SpeedTests: []SpeedTest{}, Files: []string{},
	}
	if dl == nil {
		dl = NewDownloader(log)
	}
	r := &runner{
		cfg: cfg, log: log, sum: sum, dl: dl, withPrivate: cfg.WithPrivateRPC, bad: map[string]string{},
		rpc:    NewRPCClient(cfg.RPC, cfg.RPCTimeout),
		prober: NewProber(cfg.Threads, cfg.ProbeTimeout),
	}
	err := r.run(ctx)
	sum.FinishedAt = time.Now().UTC()
	sum.Seconds = sum.FinishedAt.Sub(sum.StartedAt).Seconds()
	sum.ExitCode = CodeOf(err)
	if err != nil {
		sum.Status, sum.Error = "error", err.Error()
	} else {
		sum.Status = "ok"
	}
	return sum, err
}

func (r *runner) run(ctx context.Context) error {
	retries := max(r.cfg.Retries, 1)
	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		r.sum.Attempts = attempt
		r.log.Infof("Attempt %d of %d", attempt, retries)
		err := r.attempt(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		code := CodeOf(err)
		if ctx.Err() != nil || code == ExitLocal || code == ExitInterrupted {
			return err
		}
		r.log.Errorf("Attempt %d failed: %v", attempt, err)
		if attempt == retries {
			break
		}
		if errors.Is(err, errAllKnownBad) && r.withPrivate {
			// Nothing new turned up even with private RPCs; more attempts
			// would only re-test the same nodes. Let the caller lower its
			// --min_download_speed instead of waiting.
			break
		}
		if !r.withPrivate && code == ExitNoSnapshot {
			r.log.Infof("Next attempt also probes nodes without a public RPC (--with_private_rpc)")
			r.withPrivate = true
		}
		r.log.Infof("Sleeping %s before the next attempt", r.cfg.Sleep)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.cfg.Sleep):
		}
	}
	return lastErr
}

func (r *runner) selectOptions() SelectOptions {
	return SelectOptions{
		CurrentSlot: r.currentSlot, MaxAge: r.cfg.MaxAge, SpecificSlot: r.cfg.Slot,
		MaxLatency: r.cfg.MaxLatency, Blacklist: r.cfg.Blacklist, SortOrder: r.cfg.SortOrder,
		MinAgreement: r.cfg.MinAgreement,
	}
}

func (r *runner) attempt(ctx context.Context) error {
	if r.cfg.Slot != 0 {
		r.currentSlot = r.cfg.Slot
	} else {
		slot, err := r.rpc.GetSlot(ctx)
		if err != nil {
			return &ExitError{ExitRPC, fmt.Errorf("cannot get the current slot: %w", err)}
		}
		r.currentSlot = slot
	}
	r.sum.CurrentSlot = r.currentSlot
	nodes, err := r.rpc.GetClusterNodes(ctx)
	if err != nil {
		return &ExitError{ExitRPC, fmt.Errorf("cannot get cluster nodes: %w", err)}
	}
	targets, st := BuildTargets(nodes, TargetFilter{
		Version: r.cfg.Version, WildcardVersion: r.cfg.WildcardVersion,
		WithPrivateRPC: r.withPrivate, IPBlacklist: r.cfg.IPBlacklist,
	})
	r.sum.Targets = st
	r.log.Infof("Cluster nodes: %d, RPC endpoints to probe: %d (version filter: %d, no public RPC: %d, ip blacklist: %d) | current slot: %d",
		st.ClusterNodes, st.Targets, st.ByVersion, st.NoRPC, st.IPBlacklist, r.currentSlot)
	if len(targets) == 0 {
		return exitErr(ExitNoSnapshot, "no RPC endpoints to probe")
	}

	t0 := time.Now()
	results := r.prober.ProbeAll(ctx, targets, nil)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	r.sum.ProbeSeconds = time.Since(t0).Seconds()
	opts := r.selectOptions()

	if done, err := r.tryLocalFull(ctx, results, opts); done || err != nil {
		return err
	}

	sel := SelectCandidates(results, opts)
	for k, v := range sel.Discarded {
		r.sum.Discarded[k] = v
	}
	r.sum.Candidates = len(sel.Candidates)
	r.sum.Warnings = sel.Warnings
	for _, w := range sel.Warnings {
		r.log.Warnf("%s", w)
	}
	r.log.Infof("Probed %d endpoints in %.1fs: %d suitable | discarded: %s",
		len(targets), r.sum.ProbeSeconds, len(sel.Candidates), formatCounts(sel.Discarded))
	if r.cfg.WriteCandidates && !r.cfg.DryRun {
		if err := r.writeCandidates(len(targets), sel.Candidates); err != nil {
			r.log.Warnf("cannot write snapshot.json: %v", err)
		}
	}
	if len(sel.Candidates) == 0 {
		return exitErr(ExitNoSnapshot, "no node serves a snapshot matching the filters (max age %d slots, max latency %s)", r.cfg.MaxAge, r.cfg.MaxLatency)
	}
	return r.downloadFromCandidates(ctx, results, sel.Candidates, opts)
}

// tryLocalFull reuses a complete full snapshot already in the snapshot path
// (typically left by an earlier run whose incremental step failed) and only
// fetches a fresh incremental for it.
func (r *runner) tryLocalFull(ctx context.Context, results []ProbeResult, opts SelectOptions) (bool, error) {
	if r.cfg.IgnoreLocal {
		return false, nil
	}
	fulls, _, err := LocalArchives(r.cfg.SnapshotPath)
	if err != nil || len(fulls) == 0 {
		return false, nil
	}
	lf := fulls[0]
	if Blacklisted(lf, r.cfg.Blacklist) {
		return false, nil
	}
	if r.cfg.Slot != 0 && lf.Slot == r.cfg.Slot {
		r.log.Infof("Full snapshot for slot %d is already present: %s", lf.Slot, lf.Name)
		r.sum.ReusedLocalFull = lf.Name
		r.sum.Files = append(r.sum.Files, archivePath(r.cfg.SnapshotPath, lf))
		return true, nil
	}
	var fresh []IncSource
	for _, s := range rankIncrementalSources(IncrementalSources(results, lf, opts), "", opts, r.bad) {
		if incFresh(s.Inc, opts) {
			fresh = append(fresh, s)
		}
	}
	if len(fresh) == 0 {
		r.log.Infof("Local full snapshot %s has no fresh incremental on the network — looking for a new full snapshot", lf.Name)
		return false, nil
	}
	r.log.Infof("Reusing local full snapshot %s; only the incremental is needed", lf.Name)
	r.sum.ReusedLocalFull = lf.Name
	if r.cfg.DryRun {
		r.sum.Incremental = &FetchResult{Addr: fresh[0].Addr, Archive: fresh[0].Inc, Kind: "incremental"}
		r.log.Infof("Dry run: would download %s from %s", fresh[0].Inc.Name, fresh[0].Addr)
		return true, nil
	}
	if err := r.fetchIncremental(ctx, lf, fresh, ""); err != nil {
		if CodeOf(err) == ExitLocal || ctx.Err() != nil {
			return true, err
		}
		r.log.Warnf("%v — looking for a new full snapshot instead", err)
		return false, nil
	}
	r.sum.Files = append([]string{archivePath(r.cfg.SnapshotPath, lf)}, r.sum.Files...)
	return true, nil
}

func (r *runner) fetchOptions(gate bool) FetchOptions {
	o := FetchOptions{
		MaxSpeed: r.cfg.MaxSpeedMB * 1e6, StallTimeout: r.cfg.StallTimeout, ProgressEvery: r.cfg.ProgressEvery,
		DryRun: r.cfg.DryRun, MaxAttempts: 3,
	}
	if gate {
		o.MinSpeed = r.cfg.MinSpeedMB * 1e6
		o.MeasureTime = r.cfg.MeasureTime
		o.SwitchOnSlowdown = r.cfg.SwitchOnSlowdown
		o.ReserveBytes = r.cfg.ReserveBytes
	}
	return o
}

// minIncSpeedMB is the speed an incremental source must keep. It never asks
// more of a node than the full snapshot download did.
func (r *runner) minIncSpeedMB() float64 {
	s := r.cfg.MinIncSpeedMB
	if r.cfg.MinSpeedMB > 0 {
		s = min(s, r.cfg.MinSpeedMB)
	}
	if r.cfg.MaxSpeedMB > 0 {
		s = min(s, r.cfg.MaxSpeedMB)
	}
	return s
}

// incFetchOptions: the incremental gets a speed gate too, enforced over the
// whole transfer, not only its first seconds. Without it a node trickling
// bytes never tripped the stall watchdog and the run sat for an hour on a
// 0.3 GB file (mainnet, 2026-10-19 and 2026-10-29).
func (r *runner) incFetchOptions() FetchOptions {
	o := r.fetchOptions(false)
	o.MinSpeed = r.minIncSpeedMB() * 1e6
	o.MeasureTime = r.cfg.MeasureTime
	o.SustainMinSpeed = true
	return o
}

func (r *runner) downloadFromCandidates(ctx context.Context, results []ProbeResult, cands []Candidate, opts SelectOptions) error {
	tested, tooSlow, failed := 0, 0, 0
	var lastErr error
	for i, c := range cands {
		if why, isBad := r.bad[c.Addr]; isBad {
			r.log.Debugf("%s skipped: %s earlier in this run", c.Addr, why)
			continue
		}
		if r.cfg.MaxSpeedTests > 0 && tested >= r.cfg.MaxSpeedTests {
			r.log.Infof("Reached --max_speed_tests=%d", r.cfg.MaxSpeedTests)
			break
		}
		tested++
		incName := "-"
		if c.Inc != nil {
			incName = c.Inc.Name
		}
		r.log.Infof("%d/%d %s latency %dms slots_diff %d agreement %d | %s + %s",
			i+1, len(cands), c.Addr, c.Latency.Milliseconds(), c.SlotsDiff, c.Agreement, c.Full.Name, incName)

		full := c.Full
		res, err := r.dl.Fetch(ctx, c.Addr, full, r.cfg.SnapshotPath, r.fetchOptions(true))
		if errors.Is(err, ErrGone) {
			// The node rotated its full snapshot between probe and download.
			if nf, perr := r.prober.ProbeFull(ctx, c.Addr); perr == nil && nf.Slot > full.Slot && nf.Compressed() && !Blacklisted(*nf, r.cfg.Blacklist) {
				r.log.Infof("%s now serves %s — trying that", c.Addr, nf.Name)
				full = *nf
				res, err = r.dl.Fetch(ctx, c.Addr, full, r.cfg.SnapshotPath, r.fetchOptions(true))
			}
		}
		st := SpeedTest{Addr: c.Addr, MBps: res.MeasuredMBps, Result: "ok"}
		if err != nil {
			st.Result = err.Error()
		}
		r.sum.SpeedTests = append(r.sum.SpeedTests, st)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, ErrNoSpace):
				return &ExitError{ExitLocal, err}
			case errors.Is(err, ErrTooSlow):
				tooSlow++
				r.bad[c.Addr] = "too slow"
				r.log.Infof("%s: %v", c.Addr, err)
			default:
				failed++
				r.bad[c.Addr] = err.Error()
				r.log.Warnf("%s: %s failed: %v", c.Addr, full.Name, err)
			}
			lastErr = err
			continue
		}

		r.sum.Full = &res
		if r.cfg.DryRun {
			srcs := IncrementalSources(results, full, opts)
			if len(srcs) > 0 {
				r.sum.Incremental = &FetchResult{Addr: srcs[0].Addr, Archive: srcs[0].Inc, Kind: "incremental"}
				r.log.Infof("Dry run: would download %s from %s, then %s from %s", full.Name, c.Addr, srcs[0].Inc.Name, srcs[0].Addr)
			} else {
				r.log.Infof("Dry run: would download %s from %s (no matching incremental found)", full.Name, c.Addr)
			}
			return nil
		}
		r.sum.Files = append(r.sum.Files, res.Path)
		r.log.Infof("Downloaded %s (%s) in %.0fs, avg %.0f MB/s", full.Name, HumanBytes(res.Size), res.Seconds, res.AvgMBps)
		if r.cfg.Slot != 0 && full.Slot == r.cfg.Slot {
			return nil
		}
		return r.fetchIncremental(ctx, full, r.reprobeIncrementals(ctx, full, c.Addr, results), c.Addr)
	}
	if tested == 0 {
		return &ExitError{ExitNoSnapshot, errAllKnownBad}
	}
	if failed == 0 {
		return exitErr(ExitNoSnapshot, "none of the %d tested nodes reached --min_download_speed %.0f MB/s", tested, r.cfg.MinSpeedMB)
	}
	return &ExitError{ExitDownload, fmt.Errorf("download failed on %d node(s), %d too slow; last error: %w", failed, tooSlow, lastErr)}
}

// reprobeIncrementals asks the nodes again for their incremental snapshot:
// the full download takes minutes and the incrementals seen during discovery
// are stale (or deleted) by then. The result is ranked by rankIncrementalSources.
func (r *runner) reprobeIncrementals(ctx context.Context, full Archive, preferred string, results []ProbeResult) []IncSource {
	addrs := []string{preferred}
	type cand struct {
		addr string
		lat  time.Duration
	}
	var others []cand
	for _, res := range results {
		if res.Err == nil && res.Addr != preferred && res.Full != nil && res.Full.Slot == full.Slot && res.Full.Hash == full.Hash {
			others = append(others, cand{res.Addr, res.Latency})
		}
	}
	sort.Slice(others, func(i, j int) bool { return others[i].lat < others[j].lat })
	for i := 0; i < len(others) && i < 64; i++ {
		addrs = append(addrs, others[i].addr)
	}
	fresh := make([]ProbeResult, len(addrs))
	var wg sync.WaitGroup
	for i, a := range addrs {
		wg.Add(1)
		go func(i int, a string) {
			defer wg.Done()
			inc, lat, err := r.prober.ProbeIncremental(ctx, a)
			// Full: the full snapshot the node served at discovery (for the
			// preferred node, the one we just downloaded from it).
			f := full
			fresh[i] = ProbeResult{Target: Target{Addr: a}, Full: &f, Inc: inc, Latency: lat, Err: err}
		}(i, a)
	}
	wg.Wait()
	if slot, err := r.rpc.GetSlot(ctx); err == nil && r.cfg.Slot == 0 {
		r.currentSlot = slot
	}
	return rankIncrementalSources(IncrementalSources(fresh, full, r.selectOptions()), preferred, r.selectOptions(), r.bad)
}

// incFresh: the incremental is within --max_snapshot_age (or is the one
// asked for with --slot).
func incFresh(inc Archive, o SelectOptions) bool {
	return o.SpecificSlot != 0 || int64(o.CurrentSlot)-int64(inc.Slot) <= int64(o.MaxAge)
}

// rankIncrementalSources orders the sources of an incremental snapshot:
//  1. fresh (within --max_snapshot_age) before stale;
//  2. the node the full snapshot came from (preferred) first: it just proved
//     fast, while the newest incremental elsewhere may sit on a slow node —
//     a few hundred slots of freshness are not worth an hour of download;
//  3. nodes that already failed in this run last;
//  4. then --sort_order, as for full snapshots: latency (default), or the
//     newest incremental first (slots_diff); stale ones always newest first.
//
// The speed gate in the download then skips a slow node within seconds.
func rankIncrementalSources(srcs []IncSource, preferred string, o SelectOptions, bad map[string]string) []IncSource {
	out := append([]IncSource(nil), srcs...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if fa, fb := incFresh(a.Inc, o), incFresh(b.Inc, o); fa != fb {
			return fa
		}
		if pa, pb := a.Addr == preferred, b.Addr == preferred; pa != pb {
			return pa
		}
		_, ba := bad[a.Addr]
		_, bb := bad[b.Addr]
		if ba != bb {
			return bb
		}
		if (o.SortOrder == "slots_diff" || !incFresh(a.Inc, o)) && a.Inc.Slot != b.Inc.Slot {
			return a.Inc.Slot > b.Inc.Slot
		}
		if a.Latency != b.Latency {
			return a.Latency < b.Latency
		}
		return a.Inc.Slot > b.Inc.Slot
	})
	return out
}

// fetchIncremental downloads the incremental for full from the ranked
// sources, one after another; a source below --min_incremental_speed (or
// failing otherwise) is abandoned and its partial file deleted. When none
// works and the full snapshot alone is fresh enough, that is accepted.
func (r *runner) fetchIncremental(ctx context.Context, full Archive, srcs []IncSource, preferred string) error {
	age := int64(r.currentSlot) - int64(full.Slot)
	fullFresh := age <= int64(r.cfg.MaxAge)
	if len(srcs) == 0 {
		if fullFresh {
			r.log.Infof("No incremental snapshot based on slot %d, but the full snapshot is fresh enough (%d slots old)", full.Slot, age)
			return nil
		}
		return &ExitError{ExitDownload, fmt.Errorf("no node serves an incremental snapshot based on full slot %d (the full snapshot is kept; the next run only fetches the incremental)", full.Slot)}
	}
	limit := len(srcs)
	if r.cfg.MaxSpeedTests > 0 {
		limit = min(limit, r.cfg.MaxSpeedTests)
	}
	opts, minSpeed := r.incFetchOptions(), r.minIncSpeedMB()
	r.log.Infof("Incremental for full slot %d: %d source(s), trying up to %d, min speed %.0f MB/s", full.Slot, len(srcs), limit, minSpeed)
	var lastErr error
	for i, s := range srcs[:limit] {
		why := "next by " + r.cfg.SortOrder
		switch {
		case preferred != "" && s.Addr == preferred:
			why = "served the full snapshot"
		case preferred != "" && i == 0:
			why = "the full snapshot's node has no fresh incremental for this base; best by " + r.cfg.SortOrder
		}
		if !incFresh(s.Inc, r.selectOptions()) {
			why += fmt.Sprintf("; %d slots old (> --max_snapshot_age %d), using it anyway", int64(r.currentSlot)-int64(s.Inc.Slot), r.cfg.MaxAge)
		}
		r.log.Infof("Downloading incremental %s from %s (source %d/%d: %s; latency %dms)", s.Inc.Name, s.Addr, i+1, len(srcs), why, s.Latency.Milliseconds())
		inc := s.Inc
		res, err := r.dl.Fetch(ctx, s.Addr, inc, r.cfg.SnapshotPath, opts)
		if errors.Is(err, ErrGone) {
			// Incrementals rotate every few hundred slots; ask the node for its current one.
			if ni, _, perr := r.prober.ProbeIncremental(ctx, s.Addr); perr == nil && ni.BaseSlot == full.Slot && ni.Slot > inc.Slot && ni.Compressed() && !Blacklisted(*ni, r.cfg.Blacklist) {
				r.log.Infof("%s now serves %s — trying that", s.Addr, ni.Name)
				inc = *ni
				res, err = r.dl.Fetch(ctx, s.Addr, inc, r.cfg.SnapshotPath, opts)
			}
		}
		st := SpeedTest{Kind: "incremental", Addr: s.Addr, MBps: res.MeasuredMBps, Result: "ok"}
		if err != nil {
			st.Result = err.Error()
		}
		r.sum.SpeedTests = append(r.sum.SpeedTests, st)
		if err == nil {
			r.sum.Incremental = &res
			r.sum.Files = append(r.sum.Files, res.Path)
			r.log.Infof("Downloaded %s (%s) from %s in %.1fs, avg %.0f MB/s", inc.Name, HumanBytes(res.Size), s.Addr, res.Seconds, res.AvgMBps)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrNoSpace) {
			return &ExitError{ExitLocal, err}
		}
		if errors.Is(err, ErrTooSlow) || errors.Is(err, ErrStalled) {
			r.bad[s.Addr] = "incremental too slow"
		}
		r.log.Warnf("%s: incremental %s abandoned after %s (%s received): %v — partial file deleted, trying the next source",
			s.Addr, inc.Name, time.Duration(res.Seconds*float64(time.Second)).Round(100*time.Millisecond), HumanBytes(res.Transferred), err)
		lastErr = err
	}
	if fullFresh && r.cfg.Slot == 0 {
		r.log.Warnf("No incremental source worked (last error: %v), but the full snapshot is fresh enough (%d slots old) — using it alone", lastErr, age)
		return nil
	}
	return &ExitError{ExitDownload, fmt.Errorf("incremental snapshot for full slot %d could not be downloaded from %d source(s) (the full snapshot is kept): %w", full.Slot, limit, lastErr)}
}

// writeCandidates writes snapshot.json in upstream's format.
func (r *runner) writeCandidates(total int, cands []Candidate) error {
	type node struct {
		Addr      string   `json:"snapshot_address"`
		SlotsDiff int64    `json:"slots_diff"`
		Latency   float64  `json:"latency"`
		Files     []string `json:"files_to_download"`
	}
	out := struct {
		LastUpdateAt   float64 `json:"last_update_at"`
		LastUpdateSlot uint64  `json:"last_update_slot"`
		Total          int     `json:"total_rpc_nodes"`
		WithSnapshot   int     `json:"rpc_nodes_with_actual_snapshot"`
		Nodes          []node  `json:"rpc_nodes"`
	}{float64(time.Now().UnixNano()) / 1e9, r.currentSlot, total, len(cands), []node{}}
	for _, c := range cands {
		n := node{Addr: c.Addr, SlotsDiff: c.SlotsDiff, Latency: float64(c.Latency.Microseconds()) / 1000}
		if c.Inc != nil {
			n.Files = append(n.Files, "/"+c.Inc.Name)
		}
		n.Files = append(n.Files, "/"+c.Full.Name)
		out.Nodes = append(out.Nodes, n)
	}
	return WriteJSONAtomic(filepath.Join(r.cfg.SnapshotPath, "snapshot.json"), out)
}

// WriteJSONAtomic writes v as indented JSON via a temp file + rename.
func WriteJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func formatCounts(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	s := ""
	for i, k := range keys {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s=%d", k, m[k])
	}
	return s
}
