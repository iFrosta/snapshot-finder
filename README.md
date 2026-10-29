# snapshot-finder

Find the fastest Solana RPC node serving a fresh full + incremental snapshot and
download both, as one static binary.

[![CI](https://github.com/iFrosta/snapshot-finder/actions/workflows/ci.yml/badge.svg)](https://github.com/iFrosta/snapshot-finder/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/iFrosta/snapshot-finder?sort=semver)](https://github.com/iFrosta/snapshot-finder/releases/latest)
[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)
[![Go version](https://img.shields.io/github/go-mod/go-version/iFrosta/snapshot-finder)](go.mod)

`snapshot-finder` is a drop-in replacement for the unmaintained Python
[solana-snapshot-finder] by c29r3. It takes the same command-line flags, adds
verification and clear exit codes, and needs no Python, pip, venv or wget on the
validator.

```sh
snapshot-finder --snapshot_path /ledger/remote -r https://api.mainnet-beta.solana.com --min_download_speed 200
```

## Contents

- [Features](#features)
- [Install](#install)
- [Usage](#usage)
- [Flags](#flags)
- [How it works](#how-it-works)
- [Exit codes and output files](#exit-codes-and-output-files)
- [What is verified where](#what-is-verified-where)
- [Compared with the Python tool](#compared-with-the-python-tool)
- [FAQ](#faq)
- [Contributing](#contributing)
- [License and acknowledgements](#license-and-acknowledgements)

## Features

- **One static binary** for linux/amd64 and linux/arm64. Pure Go standard
  library, no runtime dependencies.
- **Drop-in CLI.** Every flag of the Python tool 0.3.9 is accepted with the same
  name and default.
- **Fast discovery.** All RPC endpoints from `getClusterNodes` are probed
  concurrently with per-request timeouts: about 280 mainnet endpoints in under
  2 seconds.
- **Correct pairing.** The incremental snapshot always builds on the downloaded
  full one (base slot == full slot). It is re-resolved after the full download,
  so it is as fresh as possible.
- **Cross-node hash agreement.** For each slot, the snapshot hash served by most
  nodes wins. Nodes serving a different hash are dropped and reported, and
  `--min_agreement` raises the bar further.
- **Safe downloads.** Data goes to `tmp-<name>` and is renamed only after its
  size matches `Content-Length`; strict archive-name and redirect validation; a
  free-space check before writing.
- **Speed gate that is the download itself.** The first seconds of the real
  transfer are measured; slow nodes are abandoned and the next candidate is tried.
  The incremental has its own gate (`--min_incremental_speed`), enforced over
  the whole transfer.
- **Resilience:**
  - stall watchdog;
  - switches node when one slows down mid-transfer;
  - retries broken connections;
  - HTTP Range resume where the server supports it;
  - reuses a complete local full snapshot, so a failed incremental step costs a
    small download, not another 100+ GB.
- **Script-friendly.** Distinct exit codes, a JSON summary file, `--json` output
  and `--dry-run`.

## Install

### From GitHub Releases (recommended)

```sh
VERSION=v1.1.0
ARCH=amd64   # or arm64
BASE=https://github.com/iFrosta/snapshot-finder/releases/download/$VERSION

curl -fsSLO "$BASE/snapshot-finder-linux-$ARCH"
curl -fsSLO "$BASE/SHA256SUMS"
sha256sum -c --ignore-missing SHA256SUMS
chmod 755 "snapshot-finder-linux-$ARCH"
sudo mv "snapshot-finder-linux-$ARCH" /usr/local/bin/snapshot-finder
snapshot-finder -V
```

### With Go (1.24 or newer)

```sh
go install github.com/iFrosta/snapshot-finder/cmd/snapshot-finder@latest
```

### From source

```sh
git clone https://github.com/iFrosta/snapshot-finder && cd snapshot-finder
make build           # needs Go; or: make docker-build (uses the golang:1.25 image)
ls dist/             # snapshot-finder-linux-amd64, snapshot-finder-linux-arm64, SHA256SUMS
```

## Usage

The validator looks for downloaded snapshots in `<snapshots dir>/remote`. With
the default layout that is `<ledger>/remote`.

```sh
# Mainnet: the newest pair from the first node reaching 200 MB/s
snapshot-finder --snapshot_path /ledger/remote \
    -r https://api.mainnet-beta.solana.com --min_download_speed 200

# Testnet
snapshot-finder --snapshot_path /ledger/remote \
    -r https://api.testnet.solana.com --min_download_speed 50

# Look, don't download: discovery + a speed test, JSON summary on stdout
snapshot-finder -r https://api.mainnet-beta.solana.com --dry-run --json --log_file ""

# Network restart: the snapshot of exactly this slot
snapshot-finder --snapshot_path /ledger/remote --slot 123456789

# Step down the speed requirement only when no node is fast enough (exit code 4)
for speed in 200 150 100; do
    snapshot-finder --snapshot_path /ledger/remote --min_download_speed "$speed"
    rc=$?
    [ "$rc" -eq 4 ] || break
done
```

Flags take one or two dashes, and `--flag value` and `--flag=value` both work.

## Flags

### Compatible with the Python tool (same names and defaults)

| Flag | Default | Meaning |
|---|---|---|
| `-r`, `--rpc_address` | `https://api.mainnet-beta.solana.com` | cluster RPC for `getSlot` / `getClusterNodes`; a comma-separated list means failover |
| `--snapshot_path` | `.` | download directory, created if missing |
| `--min_download_speed` | `60` | MB/s (10^6 B/s) a node must reach during the measurement window |
| `--max_download_speed` | unlimited | rate limit, MB/s |
| `--measurement_time` | `7` | speed window in seconds; the first second (TCP warm-up) is not counted |
| `--max_snapshot_age` | `1300` | max `current slot − newest slot reachable (full + incremental)`; aliases `--max_snapshot_age_slots`, `--max-snapshot-age-slots` |
| `--max_latency` | `100` | ms until a node's first response, TCP connect included |
| `-t`, `--threads-count` | `1000` | concurrent probes |
| `--with_private_rpc` | off | also probe `<gossip ip>:8899` of nodes that advertise no RPC; switched on automatically after a failed attempt |
| `--num_of_retries` | `5` | discovery attempts; stops early when nothing new can turn up |
| `--sleep` | `7` | seconds between attempts |
| `--sort_order` | `latency` | or `slots_diff` |
| `--slot` | – | network restart: full snapshot at exactly this slot, or an incremental ending there |
| `--version` | – | only nodes running exactly this version |
| `--wildcard_version` | – | only nodes whose version starts with this, e.g. `3.0` (the Python tool matched substrings) |
| `-ipb`, `--ip_blacklist` | – | `ip` or `ip:port`, comma-separated |
| `-b`, `--blacklist` | – | slot numbers or archive hashes, comma-separated |
| `-v`, `--verbose` | off | debug log |

### Added

| Flag | Default | Meaning |
|---|---|---|
| `--dry-run` | off | discovery + speed test (reads `--measurement_time` seconds, discards them); writes no archive |
| `--json` | off | print the JSON summary on stdout; logs go to stderr |
| `--summary_file` | `<snapshot_path>/snapshot-finder-summary.json` | `""` disables |
| `--log_file` | `<snapshot_path>/snapshot-finder.log` | `""` disables |
| `--blacklist_file` | – | one entry per line: ip, ip:port, slot or hash; `#` starts a comment |
| `--min_agreement` | `1` | require at least N nodes serving the identical full snapshot (slot + hash) |
| `--max_speed_tests` | `15` | speed-test at most N nodes per attempt (`0` = all) |
| `--stall_timeout` | `60` | abandon a node that sends nothing for N seconds |
| `--no_slowdown_switch` | off | keep a node that slows down mid-download |
| `--min_incremental_speed` | `20` | MB/s an incremental source must keep over every `--measurement_time` window, from start to end; capped at `--min_download_speed` and `--max_download_speed`; `0` = off |
| `--reserve_gb` | `5` | free space that must remain after the full snapshot |
| `--ignore_local` | off | never reuse a full snapshot already in `--snapshot_path` |
| `--probe_timeout` | `1.5` | seconds per probe request |
| `--rpc_timeout` | `25` | seconds per cluster RPC call |
| `--progress_interval` | `10` | seconds between progress lines |
| `-V`, `--print_version` | – | print the version and exit |

## How it works

1. **Discover.** Call `getSlot` and `getClusterNodes`, then probe every RPC
   endpoint concurrently. A probe sends `HEAD /incremental-snapshot.tar.bz2` and
   `HEAD /snapshot.tar.bz2` on one keep-alive connection. Agave answers with
   `303` and `location: /<archive name>`. The redirect must be a same-host path
   holding a well-formed archive name, or the node is discarded.
2. **Select.**
   - **Hash agreement:** for each slot, the hash served by most nodes wins;
     on a tie, all of them are dropped.
   - **Pairing:** an incremental counts only if its base slot equals the full
     snapshot's slot *and* the node serving it serves that agreed full.
   - **Freshness:** each candidate's age is measured against the newest such
     incremental anywhere in the cluster.
   - Candidates are filtered by age, latency and blacklists, then sorted by
     latency (or `slots_diff`).
3. **Speed gate.** Download from the best candidate. If it averages less than
   `--min_download_speed` over `--measurement_time`, abort, delete the partial
   file and try the next candidate. Nodes that failed are not retried within the
   run.
4. **Download the full snapshot** into `tmp-<name>`. Rename it only when the
   size equals `Content-Length`, so a final name always means a complete file.
   - a stall watchdog abandons a node that sends nothing for `--stall_timeout`;
   - the slowdown switch abandons a node when finishing there would take longer
     than starting over at `--min_download_speed`;
   - broken connections are retried on the same node.
5. **Download the incremental snapshot.** The full download takes minutes, so
   the nodes are asked again afterwards. Sources serving an incremental for
   that full slot are ranked: fresh (within `--max_snapshot_age`) first; among
   those, the node the full came from first (it just proved fast); then the
   other nodes serving the agreed full by `--sort_order`. Each source must keep
   `--min_incremental_speed` over every `--measurement_time` window, and a
   watchdog checks this beside the transfer, so a node that trickles bytes or
   goes silent is abandoned within seconds. Its partial file is deleted and the
   next source is tried. If no source works and the full snapshot alone is
   within `--max_snapshot_age`, the run ends with the full snapshot only.
6. **Reuse.** If a complete full snapshot is already in the directory and the
   network still has a fresh incremental for it, only the incremental is fetched.

## Exit codes and output files

| Code | Meaning | Typical reaction |
|---|---|---|
| 0 | full (+ matching incremental) snapshot is on disk; in `--dry-run`, a suitable node was found | start the validator |
| 1 | unexpected error | look at the log |
| 2 | bad command line | fix the flags |
| 3 | cluster RPC unreachable (`getSlot` / `getClusterNodes`) | try another `-r` |
| 4 | no node matches the filters, or none reached `--min_download_speed` | lower the speed or relax the filters |
| 5 | a suitable node was found but the download failed | run again; a finished full snapshot is kept and reused |
| 6 | local problem: path not writable, not enough free space | free disk space |
| 130 | interrupted (SIGINT/SIGTERM) | a partial file is kept for a possible resume |

Written next to the archives (the validator ignores these names):

| File | Content |
|---|---|
| `snapshot-finder-summary.json` | status, exit code, error, chosen node, measured speed, size/time/average speed per file, discard reasons per category, hash-conflict warnings |
| `snapshot.json` | candidate list, in the Python tool's format |
| `snapshot-finder.log` | the log |

Deployment scripts should check the files as well as the exit code. Success
means: the newest `snapshot-<slot>-*.tar.*`, an `incremental-snapshot-<slot>-*`
with that slot as base, and no `tmp-*`.

## What is verified where

| Check | Python tool 0.3.9 | snapshot-finder | Validator (Agave) on load |
|---|---|---|---|
| archive name and redirect well-formed, same host | no | yes | parses the name |
| file size == Content-Length, atomic rename | no | yes | – |
| incremental base slot == full slot | no | yes | yes |
| several nodes serve the same (slot, hash) | no | majority wins, `--min_agreement` | – |
| archive content matches the hash in its name | no | no | yes: recomputes the accounts hash and compares |
| state is the canonical cluster state | no | no | only indirectly, by replaying and agreeing with the cluster's bank hashes |

## Compared with the Python tool

Measured on one host against mainnet (about 280 public RPC endpoints) and in
Docker against a local fake cluster:

| | Python solana-snapshot-finder 0.3.9 | snapshot-finder |
|---|---|---|
| Runtime | system Python + venv + pip at deploy time + wget | one ~6 MB static binary |
| Discovery | 17–18 s, 71–84 nodes found (`-t` is ignored; CPU-count threads, 1 s timeouts) | 1.6 s, 88 nodes found |
| Exit code on failure | 0 | 2–6, 130 |
| Truncated download | renamed to the final name (wget's status ignored) | rejected; size must match Content-Length |
| Full/incremental pairing | not checked | enforced |
| Python 3.13.16 / 3.14.0–3.14.5 | segfaults at exit after a successful download (CPython `_datetime` finalization bug, triggered by an unclosed tqdm bar) | n/a |
| Loopback throughput | wget | ~800 MB/s, on par with curl against the same server |

The original script, with a minimal hotfix, is kept in [`legacy/`](legacy/) for
reference. [docs/REVIEW.md](docs/REVIEW.md) has the full review, including the
segfault analysis.

## FAQ

**Why can't it resume an interrupted download?**
It can, but only from a server that honours HTTP Range. Agave's RPC HTTP service
ignores `Range` and answers `200` with the whole file: none of 89 mainnet nodes
honoured it when tested on 2026-10-11. The tool then truncates the partial file
and starts again from byte 0; it never appends a full response to a partial file.
Resuming from *another* node is impossible because archives of the same slot are
not byte-identical. One full snapshot measured 123.49–123.80 GB depending on the
node. Range resume does work behind a proxy or mirror that supports it, including
across runs: `tmp-<name>.source` records the node.

**Is a downloaded snapshot trustworthy?**
Any gossip node can serve any bytes under any name, over plain HTTP. On load,
the validator recomputes the accounts hash and compares it with the hash in the
archive name. A forger also chooses that name, so this proves the archive is
self-consistent, not that it is canonical. snapshot-finder reduces the risk:
- strict name and redirect validation;
- per-slot majority hash, so a forger has to outvote every honest node serving
  that slot (`--min_agreement 3` raises the bar);
- incrementals only from nodes serving the agreed full;
- HTTPS for the cluster RPC by default.

For stronger guarantees, compare the chosen archive's hash with what your own
trusted node serves for the same slot.

**Where should the files go?**
Into the directory the validator searches for downloaded snapshots: by default
`<ledger>/remote`, or `<--snapshots>/remote` when `--snapshots` is set. Start the
validator after the tool exits 0.

**Does `--with_private_rpc` scan the cluster?**
It probes `<gossip ip>:8899` of nodes that do not advertise an RPC port, as the
Python tool did. It is off by default and switched on only for a retry after a
failed attempt.

## Contributing

Issues and pull requests are welcome.

```sh
make test            # go vet + go test -race (needs Go >= 1.24)
make docker-test     # the same inside the golang:1.25 image
```

- Tests use `internal/fakecluster`: an in-process JSON-RPC endpoint plus nodes
  that behave like Agave (303 with a lowercase `location`, Content-Length,
  Range ignored). Knobs produce slow, broken, stalled, Range-capable, rotating,
  disagreeing and unreachable nodes. Archive bytes are position-dependent, so a
  mis-resumed file fails verification.
- To try the tool locally without mainnet:

  ```sh
  make build
  dist/fakecluster-linux-amd64 -nodes 3 -full-mb 2048 -inc-mb 64 -rate-mb 120,90,60 &
  dist/snapshot-finder-linux-amd64 -r http://127.0.0.1:18899 --snapshot_path /tmp/remote --min_download_speed 100
  dist/fakecluster-linux-amd64 -verify /tmp/remote/snapshot-995000-*.tar.zst
  ```

- Releases are built by GitHub Actions from `v*` tags (static binaries plus
  `SHA256SUMS`). See [CHANGELOG.md](CHANGELOG.md).

## License and acknowledgements

GPL-3.0, see [LICENSE](LICENSE).

This project builds on [solana-snapshot-finder] by **c29r3** (GPL-3.0), whose
GitHub repository is no longer available. The original script is preserved
verbatim in the first commit (upstream commit `0d233e1`) and lives in `legacy/`.
The Go code is a new implementation that reproduces the original's behaviour and
command-line interface, so it is distributed under the same license.

[solana-snapshot-finder]: https://github.com/c29r3/solana-snapshot-finder
