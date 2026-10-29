package finder

import (
	"strings"
	"testing"
	"time"
)

const h1 = "7jMmeXZSNcWPrB2RsTdeXfXrsyW5c1BfPjqoLW2X5T7V"
const h2 = "RJVE6prxeCAjvaWffrmrUoeHe58jwX3QqmvWeKxA96q2"

func TestParseArchiveName(t *testing.T) {
	a, err := ParseArchiveName("snapshot-455562839-" + h1 + ".tar.zst")
	if err != nil || a.Kind != KindFull || a.Slot != 455562839 || a.Hash != h1 || a.Ext != "tar.zst" || !a.Compressed() {
		t.Fatalf("full: %+v %v", a, err)
	}
	i, err := ParseArchiveName("incremental-snapshot-455562839-455587439-" + h2 + ".tar.zst")
	if err != nil || i.Kind != KindIncremental || i.BaseSlot != 455562839 || i.Slot != 455587439 {
		t.Fatalf("inc: %+v %v", i, err)
	}
	u, err := ParseArchiveName("snapshot-1-" + h1 + ".tar")
	if err != nil || u.Compressed() {
		t.Fatalf("uncompressed: %+v %v", u, err)
	}
	for _, bad := range []string{
		"tmp-snapshot-1-" + h1 + ".tar.zst",
		"snapshot-1-" + h1 + ".tar.zst/../../etc/passwd",
		"../snapshot-1-" + h1 + ".tar.zst",
		"snapshot-1-short.tar.zst",
		"snapshot-1-" + h1 + "0OIl.tar.zst", // not base58
		"snapshot-x-" + h1 + ".tar.zst",
		"snapshot-1-" + h1 + ".zip",
		"incremental-snapshot-10-5-" + h1 + ".tar.zst",     // slot before base
		"snapshot-99999999999999999999-" + h1 + ".tar.zst", // overflows uint64
		"incremental-snapshot-1-" + h1 + ".tar.zst",        // missing slot
		"snapshot-1-" + h1 + ".tar.zst\n",
	} {
		if _, err := ParseArchiveName(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestParseLocation(t *testing.T) {
	name := "snapshot-100-" + h1 + ".tar.zst"
	if _, err := ParseLocation("/"+name, KindFull); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		name, "//evil.example/" + name, "http://evil.example/" + name, "/dir/" + name, "/" + name + "?x=1", "/../" + name,
	} {
		if _, err := ParseLocation(bad, KindFull); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if _, err := ParseLocation("/"+name, KindIncremental); err == nil {
		t.Error("full name accepted as incremental")
	}
}

func TestBlacklisted(t *testing.T) {
	a, _ := ParseArchiveName("incremental-snapshot-100-150-" + h1 + ".tar.zst")
	cases := map[string]bool{"100": true, "150": true, "15": false, h1: true, h1[:10]: true, "zzz": false, "": false}
	for e, want := range cases {
		if got := Blacklisted(a, []string{e}); got != want {
			t.Errorf("entry %q: got %v want %v", e, got, want)
		}
	}
}

func TestBuildTargets(t *testing.T) {
	s := func(v string) *string { return &v }
	nodes := []ClusterNode{
		{Pubkey: "a", RPC: s("1.1.1.1:8899"), Gossip: s("1.1.1.1:8001"), Version: s("2.3.1")},
		{Pubkey: "b", RPC: s("2.2.2.2:8899"), Gossip: s("2.2.2.2:8001"), Version: s("3.0.0")},
		{Pubkey: "c", RPC: nil, Gossip: s("3.3.3.3:8001"), Version: s("3.0.0")},
		{Pubkey: "d", RPC: s("4.4.4.4:8899"), Version: nil},
		{Pubkey: "e", RPC: s("5.5.5.5:9000"), Version: s("3.0.1")},
		{Pubkey: "dup", RPC: s("2.2.2.2:8899"), Version: s("3.0.0")},
	}
	got, st := BuildTargets(nodes, TargetFilter{})
	if len(got) != 4 || st.NoRPC != 1 {
		t.Fatalf("plain: %v %+v", got, st)
	}
	got, _ = BuildTargets(nodes, TargetFilter{WithPrivateRPC: true})
	if len(got) != 5 || !hasAddr(got, "3.3.3.3:8899") {
		t.Fatalf("private: %v", got)
	}
	got, st = BuildTargets(nodes, TargetFilter{WildcardVersion: "3.0"})
	if len(got) != 3 || st.ByVersion != 1 { // b, d (no version), e
		t.Fatalf("wildcard: %v %+v", got, st)
	}
	got, _ = BuildTargets(nodes, TargetFilter{WildcardVersion: "3.1"}) // "2.3.1" must not match
	if len(got) != 1 || got[0].Addr != "4.4.4.4:8899" {
		t.Fatalf("wildcard prefix: %v", got)
	}
	got, _ = BuildTargets(nodes, TargetFilter{Version: "3.0.0"})
	if len(got) != 2 { // b, d
		t.Fatalf("exact: %v", got)
	}
	got, st = BuildTargets(nodes, TargetFilter{IPBlacklist: ParseIPSet("1.1.1.1, 5.5.5.5:9000")})
	if len(got) != 2 || st.IPBlacklist != 2 {
		t.Fatalf("ip blacklist: %v %+v", got, st)
	}
}

func hasAddr(ts []Target, addr string) bool {
	for _, t := range ts {
		if t.Addr == addr {
			return true
		}
	}
	return false
}

func res(addr string, lat time.Duration, full, inc string) ProbeResult {
	r := ProbeResult{Target: Target{Addr: addr}, Latency: lat}
	if full != "" {
		a, err := ParseArchiveName(full)
		if err != nil {
			panic(err)
		}
		r.Full = &a
	}
	if inc != "" {
		a, err := ParseArchiveName(inc)
		if err != nil {
			panic(err)
		}
		r.Inc = &a
	}
	return r
}

func full(slot uint64, h string) string { return "snapshot-" + itoa(slot) + "-" + h + ".tar.zst" }
func inc(base, slot uint64, h string) string {
	return "incremental-snapshot-" + itoa(base) + "-" + itoa(slot) + "-" + h + ".tar.zst"
}
func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for ; v > 0; v /= 10 {
		b = append([]byte{byte('0' + v%10)}, b...)
	}
	return string(b)
}

func TestSelectCandidates(t *testing.T) {
	ms := time.Millisecond
	results := []ProbeResult{
		res("fast-stale:1", 5*ms, full(1000, h1), inc(1000, 1500, h2)),              // inc: slot 1500, but cluster best for 1000 is 9_950
		res("best:1", 10*ms, full(1000, h1), inc(1000, 9950, h2)),                   // fresh
		res("mismatch:1", 12*ms, full(1000, h1), inc(900, 9990, h2)),                // inc base != full: not paired
		res("far:1", 500*ms, full(1000, h1), inc(1000, 9950, h2)),                   // latency
		res("liar:1", 1*ms, full(1000, h2), inc(1000, 9960, h1)),                    // minority hash for slot 1000
		res("old:1", 2*ms, full(500, h1), ""),                                       // stale, no inc for 500
		res("raw:1", 3*ms, "snapshot-1000-"+h1+".tar", ""),                          // uncompressed
		{Target: Target{Addr: "dead:1"}, Err: errNoRedirect, Reason: "unreachable"}, // dead
	}
	sel := SelectCandidates(results, SelectOptions{CurrentSlot: 10_000, MaxAge: 1300, MaxLatency: 100 * ms})
	var addrs []string
	for _, c := range sel.Candidates {
		addrs = append(addrs, c.Addr)
	}
	// All three nodes serving the agreed full 1000 qualify: the freshest
	// incremental for base 1000 exists cluster-wide (9950), whichever node
	// serves the full. Sorted by latency.
	want := []string{"fast-stale:1", "best:1", "mismatch:1"}
	if len(addrs) != len(want) {
		t.Fatalf("candidates %v, want %v (discarded %v)", addrs, want, sel.Discarded)
	}
	for i := range want {
		if addrs[i] != want[i] {
			t.Fatalf("candidates %v, want %v", addrs, want)
		}
	}
	if c := sel.Candidates[2]; c.Inc != nil {
		t.Errorf("incremental with a different base was paired: %+v", c.Inc)
	}
	if c := sel.Candidates[0]; c.BestSlot != 9950 || c.SlotsDiff != 50 || c.Agreement != 5 {
		t.Errorf("best slot/agreement: %+v", c)
	}
	if sel.Discarded["hash_conflict"] != 1 || sel.Discarded["latency"] != 1 || sel.Discarded["stale"] != 1 ||
		sel.Discarded["uncompressed"] != 1 || sel.Discarded["unreachable"] != 1 {
		t.Errorf("discarded: %v", sel.Discarded)
	}
	if len(sel.Warnings) != 1 {
		t.Errorf("warnings: %v", sel.Warnings)
	}
	if sel.BestInc[1000].Slot != 9950 {
		t.Errorf("best inc: %+v", sel.BestInc)
	}

	// slots_diff ordering, and --min_agreement.
	sel = SelectCandidates(results, SelectOptions{CurrentSlot: 10_000, MaxAge: 1300, MaxLatency: time.Second, SortOrder: "slots_diff", MinAgreement: 6})
	if len(sel.Candidates) != 0 || sel.Discarded["low_agreement"] == 0 {
		t.Errorf("min agreement: %v %v", sel.Candidates, sel.Discarded)
	}
}

func TestSelectTieIsRejected(t *testing.T) {
	results := []ProbeResult{
		res("a:1", time.Millisecond, full(1000, h1), ""),
		res("b:1", time.Millisecond, full(1000, h2), ""),
	}
	sel := SelectCandidates(results, SelectOptions{CurrentSlot: 1001, MaxAge: 1300})
	if len(sel.Candidates) != 0 || sel.Discarded["hash_conflict"] != 2 {
		t.Fatalf("tie: %+v", sel)
	}
}

func TestSelectSpecificSlotAndFuture(t *testing.T) {
	results := []ProbeResult{
		res("exact-full:1", time.Millisecond, full(5000, h1), inc(5000, 5100, h2)),
		res("exact-inc:1", 2*time.Millisecond, full(4000, h1), inc(4000, 5000, h2)),
		res("other:1", time.Millisecond, full(4500, h1), ""),
	}
	sel := SelectCandidates(results, SelectOptions{CurrentSlot: 5000, SpecificSlot: 5000, MaxAge: 1300})
	if len(sel.Candidates) != 2 || sel.Candidates[0].Inc != nil || sel.Candidates[1].Inc == nil || sel.Discarded["slot"] != 1 {
		t.Fatalf("specific slot: %+v", sel)
	}
	future := []ProbeResult{res("future:1", time.Millisecond, full(9000, h1), "")}
	sel = SelectCandidates(future, SelectOptions{CurrentSlot: 5000, MaxAge: 1300})
	if sel.Discarded["future_slot"] != 1 {
		t.Fatalf("future: %+v", sel)
	}
}

func TestIncrementalSources(t *testing.T) {
	results := []ProbeResult{
		res("a:1", 30*time.Millisecond, full(1000, h1), inc(1000, 1200, h2)),
		res("b:1", 10*time.Millisecond, full(1000, h1), inc(1000, 1300, h2)),
		res("c:1", 5*time.Millisecond, full(1000, h1), inc(1000, 1300, h2)),
		res("d:1", 1*time.Millisecond, full(900, h1), inc(900, 1400, h2)),
	}
	f, _ := ParseArchiveName(full(1000, h1))
	srcs := IncrementalSources(results, f, SelectOptions{CurrentSlot: 1400})
	if len(srcs) != 3 || srcs[0].Addr != "c:1" || srcs[1].Addr != "b:1" || srcs[2].Inc.Slot != 1200 {
		t.Fatalf("sources: %+v", srcs)
	}
}

func TestRankIncrementalSources(t *testing.T) {
	ms := time.Millisecond
	src := func(addr string, slot uint64, lat time.Duration) IncSource {
		a, err := ParseArchiveName(inc(100, slot, h2))
		if err != nil {
			panic(err)
		}
		return IncSource{Addr: addr, Inc: a, Latency: lat}
	}
	srcs := []IncSource{
		src("newest-far:1", 1300, 50*ms), // the mainnet case: freshest incremental, slow node
		src("near:1", 1200, 5*ms),
		src("full:1", 1200, 20*ms), // the full snapshot came from here
		src("bad:1", 1300, 1*ms),   // failed earlier in this run
		src("stale:1", 300, 1*ms),  // older than --max_snapshot_age
	}
	bad := map[string]string{"bad:1": "too slow"}
	order := func(o SelectOptions) string {
		var out []string
		for _, s := range rankIncrementalSources(srcs, "full:1", o, bad) {
			out = append(out, s.Addr)
		}
		return strings.Join(out, " ")
	}
	o := SelectOptions{CurrentSlot: 1400, MaxAge: 1000, SortOrder: "latency"}
	if got, want := order(o), "full:1 near:1 newest-far:1 bad:1 stale:1"; got != want {
		t.Errorf("latency:\n got %s\nwant %s", got, want)
	}
	o.SortOrder = "slots_diff"
	if got, want := order(o), "full:1 newest-far:1 near:1 bad:1 stale:1"; got != want {
		t.Errorf("slots_diff:\n got %s\nwant %s", got, want)
	}
	// The full snapshot's node only goes first while its incremental is fresh.
	o = SelectOptions{CurrentSlot: 2250, MaxAge: 1000, SortOrder: "latency"}
	if got, want := order(o), "newest-far:1 bad:1 full:1 near:1 stale:1"; got != want {
		t.Errorf("stale preferred:\n got %s\nwant %s", got, want)
	}
	if srcs[0].Addr != "newest-far:1" {
		t.Error("input slice reordered")
	}
}

func TestParseContentRange(t *testing.T) {
	s, e, tot, err := parseContentRange("bytes 100-199/200")
	if err != nil || s != 100 || e != 199 || tot != 200 {
		t.Fatal(s, e, tot, err)
	}
	for _, bad := range []string{"", "bytes */200", "items 1-2/3", "bytes 1-2"} {
		if _, _, _, err := parseContentRange(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestCodeOfAndHumanBytes(t *testing.T) {
	if CodeOf(nil) != 0 || CodeOf(exitErr(ExitRPC, "x")) != ExitRPC || CodeOf(errNoRedirect) != ExitFailure {
		t.Fatal("CodeOf")
	}
	if HumanBytes(123458845273) != "123.5 GB" || HumanBytes(999) != "999 B" {
		t.Fatal(HumanBytes(123458845273))
	}
}
