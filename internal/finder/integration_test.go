package finder_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	fc "github.com/iFrosta/snapshot-finder/internal/fakecluster"
	"github.com/iFrosta/snapshot-finder/internal/finder"
)

const slot = 1_000_000

func config(c *fc.Cluster, dir string) finder.Config {
	return finder.Config{
		RPC: c.RPC.URL, Threads: 16, MaxAge: 1300, MinSpeedMB: 5, MaxLatency: time.Second,
		MeasureTime: 300 * time.Millisecond, SnapshotPath: dir, Retries: 1, Sleep: 10 * time.Millisecond,
		SortOrder: "latency", IPBlacklist: finder.IPSet{}, ProbeTimeout: time.Second, RPCTimeout: 5 * time.Second,
		MaxSpeedTests: 15, StallTimeout: 2 * time.Second, MinAgreement: 1, SwitchOnSlowdown: true,
		ProgressEvery: time.Hour, WriteCandidates: true,
	}
}

type logBuf struct{ bytes.Buffer }

func run(t *testing.T, cfg finder.Config) (*finder.Summary, error, string) {
	t.Helper()
	var buf logBuf
	log := finder.NewLogger(&buf, true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sum, err := finder.Run(ctx, cfg, log, "test", nil)
	if testing.Verbose() {
		t.Log("\n" + buf.String())
	}
	return sum, err, buf.String()
}

// files lists the directory, ignoring the finder's own log/json files.
func files(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		n := e.Name()
		if n == "snapshot.json" || strings.HasPrefix(n, "snapshot-finder") {
			continue
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func mustVerify(t *testing.T, dir, name string, size int64) {
	t.Helper()
	if err := fc.Verify(filepath.Join(dir, name), name, size); err != nil {
		t.Fatal(err)
	}
}

func TestHappyPathPicksFreshestFastNode(t *testing.T) {
	c := fc.New(slot,
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 40, HeadDelay: 30 * time.Millisecond},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20},                                                        // newest incremental, fast
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, RateBps: 1e6},                                          // too slow
		fc.NodeSpec{FullSlot: slot - 30000, IncSlot: slot - 10, FullHash: fc.HashB, HeadDelay: 50 * time.Millisecond}, // older full; fine too, but slower to respond
		fc.NodeSpec{NoSnapshot: true, FullSlot: 1},                                                                    // RPC without snapshots
		fc.NodeSpec{Unreachable: true, FullSlot: 1},                                                                   // dead
	)
	defer c.Close()
	dir := t.TempDir()
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	full, inc := c.Nodes[1].FullName(), c.Nodes[1].IncName()
	got := files(t, dir)
	if len(got) != 2 || got[0] != inc || got[1] != full {
		t.Fatalf("files %v, want %s + %s", got, full, inc)
	}
	mustVerify(t, dir, full, 8<<20)
	mustVerify(t, dir, inc, 1<<20)
	if sum.Status != "ok" || sum.ExitCode != 0 || sum.Full == nil || sum.Incremental == nil || sum.Incremental.Archive.BaseSlot != sum.Full.Archive.Slot {
		t.Fatalf("summary: %+v", sum)
	}
	if sum.Discarded["unreachable"] != 1 || sum.Discarded["no_snapshot"] != 1 {
		t.Errorf("discarded: %v", sum.Discarded)
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshot.json")); err != nil {
		t.Error("snapshot.json not written")
	}
}

func TestAllTooSlowExitsNoSnapshotAndLeavesNothing(t *testing.T) {
	c := fc.New(slot,
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, RateBps: 1e6},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, RateBps: 2e6},
	)
	defer c.Close()
	dir := t.TempDir()
	cfg := config(c, dir)
	cfg.Retries = 5 // attempt 2 adds private RPCs; it finds nothing new, so attempts 3-5 are skipped
	sum, err, _ := run(t, cfg)
	if finder.CodeOf(err) != finder.ExitNoSnapshot {
		t.Fatalf("exit %d (%v), want %d", finder.CodeOf(err), err, finder.ExitNoSnapshot)
	}
	if got := files(t, dir); len(got) != 0 {
		t.Fatalf("left files behind: %v", got)
	}
	if len(sum.SpeedTests) != 2 || sum.Attempts != 2 {
		t.Errorf("speed tests %v attempts %d (slow nodes must not be re-tested in attempt 2)", sum.SpeedTests, sum.Attempts)
	}
}

func TestBrokenTransferFallsBackToNextNode(t *testing.T) {
	c := fc.New(slot,
		// Drops every connection after 1 MiB, more often than the per-node retries.
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, FailAfter: 1 << 20, FailTimes: 10},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, HeadDelay: 40 * time.Millisecond},
	)
	defer c.Close()
	dir := t.TempDir()
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Full.Addr != c.Nodes[1].Addr {
		t.Fatalf("full from %s, want fallback %s", sum.Full.Addr, c.Nodes[1].Addr)
	}
	mustVerify(t, dir, c.Nodes[1].FullName(), 8<<20)
	if got := files(t, dir); len(got) != 2 {
		t.Fatalf("files %v (partial file of the failed node left behind?)", got)
	}
}

func TestServerIgnoringRangeRestartsFromZero(t *testing.T) {
	// Agave answers a Range request with 200 + the whole file. Appending that
	// to the partial file would corrupt it.
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, FailAfter: 3 << 20, FailTimes: 1})
	defer c.Close()
	dir := t.TempDir()
	sum, err, log := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	mustVerify(t, dir, c.Nodes[0].FullName(), 8<<20)
	if sum.Full.Restarts != 1 || !strings.Contains(log, "ignored the Range request") {
		t.Fatalf("restarts %d; log:\n%s", sum.Full.Restarts, log)
	}
}

func TestRangeResume(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, Range: true, FailAfter: 3 << 20, FailTimes: 2})
	defer c.Close()
	dir := t.TempDir()
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	mustVerify(t, dir, c.Nodes[0].FullName(), 8<<20)
	if sum.Full.ResumedFrom != 6<<20 || sum.Full.Restarts != 0 || c.Nodes[0].RangeReq != 2 {
		t.Fatalf("resumed from %d, restarts %d, range requests %d", sum.Full.ResumedFrom, sum.Full.Restarts, c.Nodes[0].RangeReq)
	}
	if sum.Full.Transferred != 8<<20 {
		t.Errorf("transferred %d bytes, want exactly the file size (no byte fetched twice)", sum.Full.Transferred)
	}
}

func TestResumeAcrossRunsFromSameNode(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, Range: true})
	defer c.Close()
	dir := t.TempDir()
	name := c.Nodes[0].FullName()
	// A previous run was interrupted after 5 MiB.
	part := make([]byte, 5<<20)
	fc.Fill(name, 0, part)
	if err := os.WriteFile(filepath.Join(dir, "tmp-"+name), part, 0o644); err != nil {
		t.Fatal(err)
	}
	meta := c.Nodes[0].Addr + " " + name + " " + "8388608\n"
	if err := os.WriteFile(filepath.Join(dir, "tmp-"+name+".source"), []byte(meta), 0o644); err != nil {
		t.Fatal(err)
	}
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	mustVerify(t, dir, name, 8<<20)
	if sum.Full.ResumedFrom != 5<<20 {
		t.Fatalf("resumed from %d", sum.Full.ResumedFrom)
	}
}

func TestStaleIncrementalPairing(t *testing.T) {
	// Node 0 is fastest, but its incremental is based on an older full; the
	// incremental must come from a node whose incremental base == the full.
	c := fc.New(slot,
		fc.NodeSpec{FullSlot: slot - 5000, IncBase: slot - 9000, IncSlot: slot - 10},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 30, HeadDelay: 30 * time.Millisecond},
	)
	defer c.Close()
	dir := t.TempDir()
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Full.Addr != c.Nodes[0].Addr || sum.Incremental.Addr != c.Nodes[1].Addr || sum.Incremental.Archive.BaseSlot != slot-5000 {
		t.Fatalf("full %s inc %s %+v", sum.Full.Addr, sum.Incremental.Addr, sum.Incremental.Archive)
	}
}

func TestIncrementalRefreshedAfterFullDownload(t *testing.T) {
	// While the full downloads, the node produces a newer incremental; the
	// finder must fetch the newest one, not the one seen during discovery.
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 100, RateBps: 40e6})
	defer c.Close()
	n := c.Nodes[0]
	go func() {
		for n.Served.Load() < 4<<20 {
			time.Sleep(5 * time.Millisecond)
		}
		n.SetIncremental(slot-5000, slot+50, fc.HashB)
		c.Slot.Store(slot + 60)
	}()
	dir := t.TempDir()
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Incremental.Archive.Slot != slot+50 {
		t.Fatalf("incremental %s, want the refreshed one", sum.Incremental.Archive.Name)
	}
}

func TestFullRotatedBetweenProbeAndDownload(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20})
	defer c.Close()
	n := c.Nodes[0]
	var once sync.Once
	// Right after discovery saw the full snapshot, the node replaces it: the
	// archive GET gets a 404 and the finder must re-resolve.
	n.SetAfterRedirect(func(path string) {
		if path == "/snapshot.tar.bz2" {
			once.Do(func() {
				n.Rotate(slot-100, fc.HashB)
				n.SetIncremental(slot-100, slot-5, fc.HashC)
			})
		}
	})
	sum, err, _ := run(t, config(c, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Full.Archive.Slot != slot-100 || sum.Incremental.Archive.BaseSlot != slot-100 {
		t.Fatalf("full %+v inc %+v", sum.Full.Archive, sum.Incremental.Archive)
	}
}

func TestHashConflictMinorityIgnored(t *testing.T) {
	c := fc.New(slot,
		fc.NodeSpec{FullSlot: slot - 5000, FullHash: fc.HashB, IncSlot: slot - 1}, // fastest, but disagrees with the others
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, HeadDelay: 30 * time.Millisecond},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, HeadDelay: 30 * time.Millisecond},
	)
	defer c.Close()
	dir := t.TempDir()
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Full.Archive.Hash != fc.HashA || sum.Incremental.Archive.Slot != slot-20 || len(sum.Warnings) == 0 {
		t.Fatalf("full %+v inc %+v warnings %v", sum.Full.Archive, sum.Incremental.Archive, sum.Warnings)
	}
}

func TestReusesCompleteLocalFull(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20})
	defer c.Close()
	dir := t.TempDir()
	name := c.Nodes[0].FullName()
	buf := make([]byte, 8<<20)
	fc.Fill(name, 0, buf)
	if err := os.WriteFile(filepath.Join(dir, name), buf, 0o644); err != nil {
		t.Fatal(err)
	}
	sum, err, _ := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	if sum.ReusedLocalFull != name || c.Nodes[0].GetCount(name) != 0 || sum.Incremental == nil {
		t.Fatalf("reused %q, full GETs %d, inc %+v", sum.ReusedLocalFull, c.Nodes[0].GetCount(name), sum.Incremental)
	}
	if len(sum.Files) != 2 {
		t.Errorf("files %v", sum.Files)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, FullSize: 512 << 20, RateBps: 50e6})
	defer c.Close()
	dir := t.TempDir()
	cfg := config(c, dir)
	cfg.DryRun = true
	sum, err, _ := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := files(t, dir); len(got) != 0 {
		t.Fatalf("dry run wrote %v", got)
	}
	if sum.Full == nil || sum.Full.MeasuredMBps < 20 || sum.Incremental == nil || sum.Full.Transferred > 100<<20 {
		t.Fatalf("summary %+v", sum.Full)
	}
}

func TestRPCDownExitCode(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000})
	defer c.Close()
	c.RPCDown.Store(true)
	_, err, _ := run(t, config(c, t.TempDir()))
	if finder.CodeOf(err) != finder.ExitRPC {
		t.Fatalf("exit %d (%v)", finder.CodeOf(err), err)
	}
}

func TestStalledDownloadSwitchesNode(t *testing.T) {
	c := fc.New(slot,
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, StallAfter: 2 << 20},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, HeadDelay: 30 * time.Millisecond},
	)
	defer c.Close()
	dir := t.TempDir()
	sum, err, log := run(t, config(c, dir))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Full.Addr != c.Nodes[1].Addr || !strings.Contains(log, "stalled") {
		t.Fatalf("full from %s; log:\n%s", sum.Full.Addr, log)
	}
}

func TestNoSpace(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20})
	defer c.Close()
	dir := t.TempDir()
	dl := finder.NewDownloader(finder.NewLogger(&bytes.Buffer{}, false))
	dl.FreeSpace = func(string) (uint64, error) { return 1 << 20, nil }
	cfg := config(c, dir)
	_, err := finder.Run(context.Background(), cfg, finder.NewLogger(&bytes.Buffer{}, false), "test", dl)
	if finder.CodeOf(err) != finder.ExitLocal || !errors.Is(err, finder.ErrNoSpace) {
		t.Fatalf("exit %d (%v)", finder.CodeOf(err), err)
	}
	if got := files(t, dir); len(got) != 0 {
		t.Fatalf("files %v", got)
	}
}

func TestBadLocationAndBlacklists(t *testing.T) {
	c := fc.New(slot,
		fc.NodeSpec{FullSlot: slot - 5000, BadLocation: "/../../etc/passwd"},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20},
	)
	defer c.Close()
	cfg := config(c, t.TempDir())
	cfg.IPBlacklist = finder.ParseIPSet(c.Nodes[1].Addr)
	sum, err, _ := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Full.Addr != c.Nodes[2].Addr || sum.Discarded["bad_location"] != 1 || sum.Targets.IPBlacklist != 1 {
		t.Fatalf("full from %s, discarded %v, targets %+v", sum.Full.Addr, sum.Discarded, sum.Targets)
	}

	// Blacklisting the incremental (by slot) leaves only the full snapshot,
	// which is 5000 slots old -> nothing suitable.
	cfg = config(c, t.TempDir())
	cfg.Blacklist = []string{"999980"}
	_, err, _ = run(t, cfg)
	if finder.CodeOf(err) != finder.ExitNoSnapshot {
		t.Fatalf("blacklisted incremental: exit %d (%v)", finder.CodeOf(err), err)
	}
}

func TestPrivateRPCEnabledOnRetry(t *testing.T) {
	// The only snapshot node advertises no RPC; upstream retried with
	// --with_private_rpc, which probes <gossip ip>:8899. The fake cluster's
	// gossip port is not the node port, so this only checks the retry path.
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, PrivateOnly: true})
	defer c.Close()
	cfg := config(c, t.TempDir())
	cfg.Retries = 2
	sum, err, log := run(t, cfg)
	if finder.CodeOf(err) != finder.ExitNoSnapshot || !strings.Contains(log, "--with_private_rpc") || sum.Attempts != 2 {
		t.Fatalf("exit %d attempts %d\n%s", finder.CodeOf(err), sum.Attempts, log)
	}
}

func TestStaleTmpRemovedAndIgnoreLocal(t *testing.T) {
	c := fc.New(slot, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20})
	defer c.Close()
	dir := t.TempDir()
	stale := "tmp-" + fc.FullName(slot-90000, fc.HashB, "tar.zst")
	_ = os.WriteFile(filepath.Join(dir, stale), []byte("old partial"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, stale+".source"), []byte("x"), 0o644)
	// A complete-looking local full of the same slot: ignored with IgnoreLocal.
	name := c.Nodes[0].FullName()
	_ = os.WriteFile(filepath.Join(dir, name), []byte("truncated by some other tool"), 0o644)
	cfg := config(c, dir)
	cfg.IgnoreLocal = true
	sum, err, _ := run(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.ReusedLocalFull != "" || c.Nodes[0].GetCount(name) != 1 {
		t.Fatalf("local full reused: %q", sum.ReusedLocalFull)
	}
	mustVerify(t, dir, name, 8<<20)
	for _, f := range files(t, dir) {
		if strings.HasPrefix(f, "tmp-") {
			t.Errorf("stale %s left behind", f)
		}
	}
}
