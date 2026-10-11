package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	fc "github.com/iFrosta/snapshot-finder/internal/fakecluster"
	"github.com/iFrosta/snapshot-finder/internal/finder"
)

const slot = 2_000_000

func cluster(t *testing.T, specs ...fc.NodeSpec) *fc.Cluster {
	t.Helper()
	if len(specs) == 0 {
		specs = []fc.NodeSpec{{FullSlot: slot - 5000, IncSlot: slot - 20}}
	}
	c := fc.New(slot, specs...)
	t.Cleanup(c.Close)
	return c
}

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, &out, &errb)
	return code, out.String(), errb.String()
}

// The invocation used by typical validator bootstrap scripts (unchanged since
// the Python tool), with only the URL and path swapped for the fake cluster.
func TestBootstrapScriptInvocation(t *testing.T) {
	c := cluster(t)
	dir := filepath.Join(t.TempDir(), "ledger", "remote") // created by the tool
	code, out, _ := runCLI(t, "--snapshot_path", dir, "-r", c.RPC.URL, "--min_download_speed", "200")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	for _, name := range []string{c.Nodes[0].FullName(), c.Nodes[0].IncName()} {
		if err := fc.Verify(filepath.Join(dir, name), name, map[bool]int64{true: 8 << 20, false: 1 << 20}[strings.HasPrefix(name, "snapshot-")]); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"snapshot.json", "snapshot-finder.log", "snapshot-finder-summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
	var sum finder.Summary
	data, _ := os.ReadFile(filepath.Join(dir, "snapshot-finder-summary.json"))
	if err := json.Unmarshal(data, &sum); err != nil || sum.Status != "ok" || len(sum.Files) != 2 {
		t.Fatalf("summary %s (%v)", data, err)
	}
}

// Every flag of upstream 0.3.9 is still accepted, with one or two dashes.
func TestUpstreamFlagsAccepted(t *testing.T) {
	c := cluster(t)
	dir := t.TempDir()
	code, out, errOut := runCLI(t,
		"-t", "50", "-r", c.RPC.URL, "--slot", "0", "--version", "3.0.0", "--wildcard_version", "3.0",
		"--max_snapshot_age", "1300", "--min_download_speed", "1", "--max_download_speed", "500",
		"--max_latency", "500", "--measurement_time", "1", "--snapshot_path", dir,
		"--num_of_retries", "1", "--sleep", "1", "--sort_order", "slots_diff",
		"-ipb", "10.0.0.1:8899,10.0.0.2", "-b", "123,abc", "-v", "--threads-count=60", "--with_private_rpc",
		"--rpc_address="+c.RPC.URL, "--ip_blacklist", "10.0.0.3", "--blacklist", "456",
	)
	if code != 0 {
		t.Fatalf("exit %d\n%s\n%s", code, out, errOut)
	}
}

func TestExitCodes(t *testing.T) {
	if code, _, _ := runCLI(t, "--no-such-flag"); code != finder.ExitUsage {
		t.Errorf("unknown flag: %d", code)
	}
	if code, _, _ := runCLI(t, "--sort_order", "random"); code != finder.ExitUsage {
		t.Errorf("bad sort order: %d", code)
	}
	if code, out, _ := runCLI(t, "-V"); code != 0 || !strings.Contains(out, "snapshot-finder") {
		t.Errorf("-V: %d %q", code, out)
	}

	c := cluster(t)
	c.RPCDown.Store(true)
	if code, _, _ := runCLI(t, "--snapshot_path", t.TempDir(), "-r", c.RPC.URL, "--num_of_retries", "1"); code != finder.ExitRPC {
		t.Errorf("rpc down: %d", code)
	}

	slow := cluster(t, fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20, RateBps: 1e6, FullSize: 64 << 20})
	dir := t.TempDir()
	code, _, _ := runCLI(t, "--snapshot_path", dir, "-r", slow.RPC.URL, "--min_download_speed", "100",
		"--measurement_time", "0.5", "--num_of_retries", "1")
	if code != finder.ExitNoSnapshot {
		t.Errorf("too slow: %d", code)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.Contains(e.Name(), "tar") {
			t.Errorf("left %s behind", e.Name())
		}
	}

	ro := filepath.Join(t.TempDir(), "file")
	_ = os.WriteFile(ro, nil, 0o644)
	if code, _, _ := runCLI(t, "--snapshot_path", filepath.Join(ro, "sub")); code != finder.ExitLocal {
		t.Errorf("unusable path: %d", code)
	}
}

func TestJSONAndDryRun(t *testing.T) {
	c := cluster(t)
	dir := t.TempDir()
	code, out, _ := runCLI(t, "--snapshot_path", dir, "-r", c.RPC.URL, "--dry-run", "--json", "--summary_file", "", "--log_file", "")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	var sum finder.Summary
	if err := json.Unmarshal([]byte(out), &sum); err != nil {
		t.Fatalf("stdout is not the JSON summary: %v\n%s", err, out)
	}
	if !sum.DryRun || sum.Full == nil || sum.Incremental == nil {
		t.Fatalf("summary %+v", sum)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("dry run wrote %d entries", len(entries))
	}
}

func TestBlacklistFile(t *testing.T) {
	c := cluster(t,
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20},
		fc.NodeSpec{FullSlot: slot - 5000, IncSlot: slot - 20},
	)
	dir := t.TempDir()
	bl := filepath.Join(t.TempDir(), "blacklist")
	_ = os.WriteFile(bl, []byte("# bad node\n"+c.Nodes[0].Addr+"\n"), 0o644)
	code, out, _ := runCLI(t, "--snapshot_path", dir, "-r", c.RPC.URL, "--blacklist_file", bl, "--json", "--log_file", "")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	var sum finder.Summary
	_ = json.Unmarshal([]byte(out), &sum)
	if sum.Full == nil || sum.Full.Addr != c.Nodes[1].Addr || sum.Targets.IPBlacklist != 1 {
		t.Fatalf("summary %+v", sum)
	}
}
