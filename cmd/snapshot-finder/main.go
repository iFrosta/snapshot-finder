// Command snapshot-finder finds Solana RPC nodes serving a fresh full +
// incremental snapshot, picks the first one fast enough and downloads both
// archives into --snapshot_path.
//
// Command-line compatible with c29r3/solana-snapshot-finder 0.3.9 (same flag
// names and defaults); see README.md for the additions.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/iFrosta/snapshot-finder/internal/finder"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func init() {
	// `go install github.com/iFrosta/snapshot-finder/cmd/snapshot-finder@vX.Y.Z`
	// does not run ldflags; take the module version from the build info.
	if version == "dev" {
		if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			version = bi.Main.Version
		}
	}
}

const auto = "auto"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type options struct {
	threads          int
	rpc              string
	slot             uint64
	nodeVersion      string
	wildcardVersion  string
	maxAge           uint64
	minSpeed         float64
	maxSpeed         float64
	maxLatency       int
	withPrivateRPC   bool
	measureTime      float64
	snapshotPath     string
	retries          int
	sleep            float64
	sortOrder        string
	ipBlacklist      string
	blacklist        string
	blacklistFile    string
	verbose          bool
	dryRun           bool
	jsonOut          bool
	summaryFile      string
	logFile          string
	probeTimeout     float64
	rpcTimeout       float64
	maxSpeedTests    int
	stallTimeout     float64
	minAgreement     int
	noSlowdownSwitch bool
	reserveGB        float64
	progressInterval float64
	printVersion     bool
	ignoreLocal      bool
}

func newFlagSet(o *options, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("snapshot-finder", flag.ContinueOnError)
	fs.SetOutput(stderr)
	alias := func(names ...string) []string { return names }
	intVar := func(p *int, def int, usage string, names []string) {
		for _, n := range names {
			fs.IntVar(p, n, def, usage)
		}
	}
	uintVar := func(p *uint64, def uint64, usage string, names []string) {
		for _, n := range names {
			fs.Uint64Var(p, n, def, usage)
		}
	}
	strVar := func(p *string, def, usage string, names []string) {
		for _, n := range names {
			fs.StringVar(p, n, def, usage)
		}
	}
	floatVar := func(p *float64, def float64, usage string, names []string) {
		for _, n := range names {
			fs.Float64Var(p, n, def, usage)
		}
	}
	boolVar := func(p *bool, usage string, names []string) {
		for _, n := range names {
			fs.BoolVar(p, n, false, usage)
		}
	}

	// Upstream flags (same names and defaults).
	intVar(&o.threads, 1000, "number of nodes probed concurrently", alias("t", "threads-count"))
	strVar(&o.rpc, "https://api.mainnet-beta.solana.com", "cluster RPC URL for getSlot/getClusterNodes (comma-separated list = failover)", alias("r", "rpc_address"))
	uintVar(&o.slot, 0, "look for a snapshot of exactly this slot (network restarts)", alias("slot"))
	strVar(&o.nodeVersion, "", "only use nodes running exactly this version", alias("version"))
	strVar(&o.wildcardVersion, "", "only use nodes whose version starts with this, e.g. 3.0 (upstream: substring)", alias("wildcard_version"))
	uintVar(&o.maxAge, 1300, "maximum snapshot age in slots (full + newest incremental)", alias("max_snapshot_age", "max_snapshot_age_slots", "max-snapshot-age-slots"))
	floatVar(&o.minSpeed, 60, "minimum download speed in MB/s (10^6 bytes/s)", alias("min_download_speed"))
	floatVar(&o.maxSpeed, 0, "limit the download speed to this many MB/s (0 = unlimited)", alias("max_download_speed"))
	intVar(&o.maxLatency, 100, "skip nodes whose first response takes longer than this (ms)", alias("max_latency"))
	boolVar(&o.withPrivateRPC, "also probe <gossip ip>:8899 of nodes that do not advertise an RPC", alias("with_private_rpc"))
	floatVar(&o.measureTime, 7, "speed measurement window in seconds", alias("measurement_time"))
	strVar(&o.snapshotPath, ".", "directory to download into (e.g. /ledger/remote)", alias("snapshot_path"))
	intVar(&o.retries, 5, "number of discovery attempts", alias("num_of_retries"))
	floatVar(&o.sleep, 7, "seconds to wait between attempts", alias("sleep"))
	strVar(&o.sortOrder, "latency", "candidate order: latency or slots_diff", alias("sort_order"))
	strVar(&o.ipBlacklist, "", "comma-separated ip or ip:port to skip", alias("ipb", "ip_blacklist"))
	strVar(&o.blacklist, "", "comma-separated slots or archive hashes to skip", alias("b", "blacklist"))
	boolVar(&o.verbose, "debug logging", alias("v", "verbose"))

	// Additions.
	boolVar(&o.dryRun, "discover and speed-test only; write no archive", alias("dry-run", "dry_run"))
	boolVar(&o.jsonOut, "print the JSON summary on stdout at the end", alias("json"))
	strVar(&o.summaryFile, auto, "JSON summary file (auto = <snapshot_path>/snapshot-finder-summary.json, empty = none)", alias("summary_file"))
	strVar(&o.logFile, auto, "log file (auto = <snapshot_path>/snapshot-finder.log, empty = none)", alias("log_file"))
	strVar(&o.blacklistFile, "", "file with one blacklist entry per line: ip, ip:port, slot or hash (# comments)", alias("blacklist_file"))
	floatVar(&o.probeTimeout, 1.5, "per-request timeout when probing nodes, seconds", alias("probe_timeout"))
	floatVar(&o.rpcTimeout, 25, "timeout of cluster RPC calls, seconds", alias("rpc_timeout"))
	intVar(&o.maxSpeedTests, 15, "speed-test at most this many nodes per attempt (0 = all)", alias("max_speed_tests"))
	floatVar(&o.stallTimeout, 60, "abort a download that received nothing for this many seconds", alias("stall_timeout"))
	intVar(&o.minAgreement, 1, "require at least this many nodes serving the identical full snapshot (slot+hash)", alias("min_agreement"))
	boolVar(&o.noSlowdownSwitch, "never abandon a node that slows down mid-download", alias("no_slowdown_switch"))
	floatVar(&o.reserveGB, 5, "GB of free space to keep after the full snapshot (for the incremental)", alias("reserve_gb"))
	floatVar(&o.progressInterval, 10, "seconds between progress lines", alias("progress_interval"))
	boolVar(&o.ignoreLocal, "do not reuse a complete full snapshot already in --snapshot_path", alias("ignore_local"))
	boolVar(&o.printVersion, "print the version and exit", alias("V", "print_version"))

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: snapshot-finder [flags]\n\nFlags take one or two dashes (-r URL, --snapshot_path /ledger/remote).\n\n")
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nExit codes: 0 ok, 1 error, 2 usage, 3 cluster RPC unreachable, 4 no suitable node,\n5 download failed, 6 local disk/path problem, 130 interrupted.\n")
	}
	return fs
}

func dur(sec float64) time.Duration { return time.Duration(sec * float64(time.Second)) }

func run(args []string, stdout, stderr io.Writer) int {
	var o options
	fs := newFlagSet(&o, stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return finder.ExitOK
		}
		return finder.ExitUsage
	}
	if o.printVersion {
		fmt.Fprintln(stdout, "snapshot-finder", version)
		return finder.ExitOK
	}
	usage := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "snapshot-finder: "+format+"\n", a...)
		return finder.ExitUsage
	}
	if fs.NArg() > 0 {
		return usage("unexpected argument %q", fs.Arg(0))
	}
	if o.sortOrder != "latency" && o.sortOrder != "slots_diff" {
		return usage("--sort_order must be latency or slots_diff, got %q", o.sortOrder)
	}
	if o.minSpeed < 0 || o.maxSpeed < 0 || o.measureTime < 0 || o.threads < 1 || o.probeTimeout <= 0 {
		return usage("negative speed/time or zero --threads-count/--probe_timeout")
	}
	if o.maxSpeed > 0 && o.maxSpeed < o.minSpeed {
		return usage("--max_download_speed %.0f is below --min_download_speed %.0f: no node could pass", o.maxSpeed, o.minSpeed)
	}

	path, err := filepath.Abs(o.snapshotPath)
	if err != nil {
		return usage("bad --snapshot_path: %v", err)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		fmt.Fprintf(stderr, "snapshot-finder: cannot create %s: %v\n", path, err)
		return finder.ExitLocal
	}
	if probe, err := os.CreateTemp(path, ".write-test-*"); err != nil {
		fmt.Fprintf(stderr, "snapshot-finder: %s is not writable: %v\n", path, err)
		return finder.ExitLocal
	} else {
		probe.Close()
		os.Remove(probe.Name())
	}

	var logW io.Writer = stdout
	if o.jsonOut {
		logW = stderr // keep stdout for the JSON summary
	}
	if o.logFile == auto {
		o.logFile = filepath.Join(path, "snapshot-finder.log")
	}
	if o.logFile != "" {
		f, err := os.OpenFile(o.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(stderr, "snapshot-finder: cannot open log file: %v\n", err)
			return finder.ExitLocal
		}
		defer f.Close()
		logW = io.MultiWriter(logW, f)
	}
	log := finder.NewLogger(logW, o.verbose)

	ipBlacklist := finder.ParseIPSet(o.ipBlacklist)
	archiveBlacklist := splitList(o.blacklist)
	if o.blacklistFile != "" {
		data, err := os.ReadFile(o.blacklistFile)
		if err != nil {
			return usage("cannot read --blacklist_file: %v", err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if i := strings.IndexByte(line, '#'); i >= 0 {
				line = line[:i]
			}
			for _, e := range splitList(line) {
				if isIPEntry(e) {
					ipBlacklist[e] = true
				} else {
					archiveBlacklist = append(archiveBlacklist, e)
				}
			}
		}
	}

	cfg := finder.Config{
		RPC: o.rpc, Threads: o.threads, Slot: o.slot, Version: o.nodeVersion, WildcardVersion: o.wildcardVersion,
		MaxAge: o.maxAge, MinSpeedMB: o.minSpeed, MaxSpeedMB: o.maxSpeed,
		MaxLatency: time.Duration(o.maxLatency) * time.Millisecond, WithPrivateRPC: o.withPrivateRPC,
		MeasureTime: dur(o.measureTime), SnapshotPath: path, Retries: o.retries, Sleep: dur(o.sleep),
		SortOrder: o.sortOrder, IPBlacklist: ipBlacklist, Blacklist: archiveBlacklist, DryRun: o.dryRun,
		ProbeTimeout: dur(o.probeTimeout), RPCTimeout: dur(o.rpcTimeout), MaxSpeedTests: o.maxSpeedTests,
		StallTimeout: dur(o.stallTimeout), MinAgreement: o.minAgreement, SwitchOnSlowdown: !o.noSlowdownSwitch,
		ProgressEvery: dur(o.progressInterval), ReserveBytes: uint64(o.reserveGB * 1e9), WriteCandidates: true, IgnoreLocal: o.ignoreLocal,
	}
	log.Infof("snapshot-finder %s | rpc=%s path=%s min_speed=%.0fMB/s max_speed=%.0fMB/s max_age=%d max_latency=%dms private_rpc=%v sort=%s dry_run=%v",
		version, cfg.RPC, cfg.SnapshotPath, cfg.MinSpeedMB, cfg.MaxSpeedMB, cfg.MaxAge, o.maxLatency, cfg.WithPrivateRPC, cfg.SortOrder, cfg.DryRun)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	sum, err := finder.Run(ctx, cfg, log, version, nil)
	code := finder.CodeOf(err)
	if err != nil {
		log.Errorf("FAILED (exit %d): %v", code, err)
	} else if cfg.DryRun {
		log.Infof("Done (dry run, nothing downloaded)")
	} else {
		log.Infof("Done: %s", strings.Join(sum.Files, ", "))
	}

	if o.summaryFile == auto {
		o.summaryFile = filepath.Join(path, "snapshot-finder-summary.json")
	}
	if o.summaryFile != "" {
		if werr := finder.WriteJSONAtomic(o.summaryFile, sum); werr != nil {
			log.Warnf("cannot write summary: %v", werr)
		}
	}
	if o.jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(sum)
	}
	return code
}

func splitList(s string) []string {
	var out []string
	for _, f := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\r' }) {
		out = append(out, f)
	}
	return out
}

func isIPEntry(e string) bool {
	if net.ParseIP(e) != nil {
		return true
	}
	host, _, err := net.SplitHostPort(e)
	return err == nil && net.ParseIP(host) != nil
}
