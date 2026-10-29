# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [1.1.0] - 2026-10-29

### Fixed
- The incremental snapshot had no speed check, only the stall watchdog, and
  was taken from whichever node had the newest one. On mainnet (2026-10-19 and
  2026-10-29) the full came at ~250 MB/s, then the incremental came from
  another node at ~0 MB/s with an ETA of an hour. The node trickled bytes, so
  the stall watchdog never fired.

### Added
- `--min_incremental_speed` (default 20 MB/s, capped at `--min_download_speed`
  and `--max_download_speed`). It is enforced over every `--measurement_time`
  window of the incremental download by a watchdog beside the transfer, so a
  blocked read is caught too. A slow source is abandoned, its partial file is
  deleted and the next source is tried. If every source fails and the full
  snapshot alone is fresh enough, the run ends with the full snapshot only.
- Incremental sources are ranked: fresh first, then the node the full came
  from, then the other nodes by `--sort_order`. Nodes that already failed in
  this run go last. The log names each source, says why it was chosen, and
  records every abort. Incremental attempts are in the summary's `speed_tests`
  with `"kind": "incremental"`.
- An incremental that rotated away between probe and download (404) is
  re-resolved on the same node.

### Changed
- `ErrTooSlow` reads "download speed below the minimum" (it is shared by both
  gates).

## [1.0.0] - 2026-10-11

First release of the Go rewrite of c29r3/solana-snapshot-finder 0.3.9.

### Added
- Single static binary (linux/amd64, linux/arm64); Go standard library only.
- Command-line compatibility with solana-snapshot-finder 0.3.9: same flag names
  and defaults.
- Concurrent discovery with per-request timeouts.
- Full/incremental pairing (incremental base slot == full slot); the incremental
  is re-resolved after the full download.
- Per-slot majority hash across nodes; `--min_agreement`.
- Size check against Content-Length, atomic rename, free-space check.
- Speed gate on the real download; stall watchdog; slowdown switch; retries.
- HTTP Range resume where supported. A server that ignores Range causes a
  restart from byte 0, never an append.
- Reuse of a complete local full snapshot (only the incremental is fetched);
  removal of stale partial downloads.
- Exit codes 0/1/2/3/4/5/6/130; JSON summary (`--summary_file`, `--json`);
  `--dry-run`; `--blacklist_file`; `--ignore_local`.
- `legacy/`: the original Python script, verbatim, plus a minimal hotfix. The
  hotfix covers the segfault at exit on CPython 3.13.16 / 3.14.0–3.14.5, exit
  codes, wget's exit status, header matching, pairing, speed-test robustness and
  pinned requirements.

### Changed
- `--wildcard_version` is a prefix match. Upstream used a substring test, so
  `1.18` matched `2.1.18`.

[1.1.0]: https://github.com/iFrosta/snapshot-finder/releases/tag/v1.1.0
[1.0.0]: https://github.com/iFrosta/snapshot-finder/releases/tag/v1.0.0
