# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

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

[1.0.0]: https://github.com/iFrosta/snapshot-finder/releases/tag/v1.0.0
