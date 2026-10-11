# Review of upstream solana-snapshot-finder 0.3.9

Subject: `legacy/snapshot-finder.py` as imported in the first commit (upstream
`0d233e1`, 2024-11-30, the last upstream version). Reproductions used a fake
cluster (RPC + snapshot node serving small archives) in Docker. Live measurements against
mainnet were read-only: HEAD requests and a few seconds of one download.

## Architecture

One 450-line script with no functions-level structure:
- `argparse` runs at import time, followed by about 25 module globals and the
  retry loop, all at module level.
- Discovery: `getClusterNodes` via `requests`, then a
  `multiprocessing.dummy.ThreadPool()` probes `HEAD /incremental-snapshot.tar.bz2`
  and `HEAD /snapshot.tar.bz2` per node, with a global `tqdm` bar.
- Speed test: sequential, 7 s per node, using `requests` with `stream=True`.
- Download: a `wget` subprocess writes to `tmp-<name>`, then `os.rename`.
- Dependencies: `requests` and `tqdm`, unpinned. `urllib3`, `idna`, `certifi`
  and `charset_normalizer` come in transitively; `charset_normalizer` ships a
  mypyc-compiled C extension. Also needs the system `python3`, `python3-venv` and
  `wget`.

## The Python 3.14 segfault

Symptom: "Done" is printed, both archives are on disk, then the process dies
with SIGSEGV (`segfault at 10 ... in python3.14`, exit 139). Any deployment
script that checks the exit code reads that as a failure.

Root cause, narrowed step by step:
1. Reproduced on `ubuntu:26.04` with the system `python3` (3.14.4-1ubuntu0.2) and
   on the official `python:3.14.4` image: 5/5 runs crash. `python:3.14.8`: 0/5.
2. `-X faulthandler`: the crash is in `tqdm/std.py format_meter` ← `__str__` ←
   `display` ← `close` ← `__del__`, during **"Garbage-collecting"** at
   interpreter shutdown.
3. Bisection: closing or joining the thread pool does not help, and removing the
   pool does not help. **Closing the tqdm bar fixes it** (0/3).
4. The line that faults is `datetime.now() + timedelta(...)` /
   `datetime.fromtimestamp(0, timezone.utc)`. A reproducer with no third-party
   code, an object in a reference cycle whose `__del__` calls those, crashes
   CPython **3.13.16 and 3.14.4–3.14.5**. It does not crash 3.12.15 or 3.14.6+.

So it is a **CPython regression**. Since `_datetime` was made an isolated module,
its static types can outlive its module state. Code that runs during the final GC
and touches `datetime` then dereferences NULL module state. Upstream tracks this
as python/cpython gh-151039 (fix PR #151044, June 2026); the version boundary
measured here (fixed in 3.14.6) is consistent with that. The script triggers it
because the module-global `pbar` is never closed, so its `__del__` runs in that
final GC. Python 3.12 predates the regression, which is why the script ran
fine for years.

Fix in `legacy/`: `pbar.close()` (and join the pool) right after probing. The Go
rewrite has no interpreter at all.

## Bugs

Most of these were reproduced; "(fake)" means seen against the fake cluster.

| # | Problem | Effect |
|---|---|---|
| 1 | **Failures exit 0.** `sys.exit()` without an argument after "Could not find a suitable snapshot", after a failed `getClusterNodes`, and on a missing wget. `main_worker`'s bare `except:` also swallows `SystemExit` (fake: exit=0 with nothing downloaded). | A wrapper that retries with a lower `--min_download_speed` on failure never steps down; "success" with no files. |
| 2 | **wget's exit status is ignored**, and the temp file is renamed to the final name regardless. | A truncated archive gets a final name. The validator fails late (on unpack) or the next run "reuses" it. |
| 3 | No size check against Content-Length, no free-space check. | Same as 2; a full disk is discovered at the end. |
| 4 | Full/incremental pairing is not verified. The full `HEAD` (`r2`) is never inspected (`r` is checked twice); the incremental's base slot is never compared with the full's slot, at probe time or at download time. | A node rotating its full between the two requests yields a mismatched pair, and the validator rejects it. |
| 5 | Header test is `'location' in str(r.headers)`, a case-sensitive substring of the dict's repr (fake: 0 nodes found with `Location:`). | Works only because Agave/hyper sends lowercase header names. |
| 6 | `-t/--threads-count` (default 1000) is never used; `ThreadPool()` gets `os.cpu_count()` threads. HEAD timeout is 1 s. | Mainnet discovery takes 17–18 s for 279 endpoints and loses nodes to timeouts (71–84 found vs 88). With `--with_private_rpc` (~3700 targets) it takes minutes. |
| 7 | Speed test: any exception (connection error, or under 1 s of data, which makes `statistics.median([])` raise) aborts the whole attempt, not just that node. The `num_of_rpc_to_check = 15` limit sits in an unreachable `elif`. | One bad node costs an attempt; with a high `--min_download_speed`, dozens of nodes are tested at 7 s each. |
| 8 | The log `FileHandler` opens `<snapshot_path>/snapshot-finder.log` *before* the directory is created; the later `mkdir` is dead code (live: crash in ~100 ms when the path does not exist). | Callers need `mkdir -p` first. |
| 9 | `json_data["rpc_nodes"]` is never reset between attempts. | Retries carry stale and duplicate candidates from earlier attempts. |
| 10 | The retry loop checks `NUM_OF_ATTEMPTS >= NUM_OF_MAX_ATTEMPTS` after incrementing. | `--num_of_retries 5` runs 4 attempts. |
| 11 | `FULL_LOCAL_SNAP_SLOT` is a `str`, compared with an `int` incremental base slot in the probe. | "Reuse the local full, fetch only the incremental" never matches during discovery. |
| 12 | `do_request`'s generic `except` formats `reqErr`, which is undefined there. | NameError, swallowed by the caller. |
| 13 | `--wildcard_version` is a substring test. | `1.18` also matches `2.1.18`. |
| 14 | `requirements.txt` is unpinned and `pip install` runs at deploy time. | Every new install gets whatever PyPI has that day, on whatever Python the OS ships. |
| 15 | No stall or slowdown handling of its own; it relies on wget defaults. tqdm progress and log lines share stdout. | A node that degrades mid-download is kept; logs are hard to parse. |

## Security notes

- The tool trusts any node in gossip. Any node can advertise an RPC address and
  serve any bytes under any archive name. Archives come over plain HTTP, the
  only thing Agave serves. Many deployments also query the cluster RPC over
  `http://`, so an on-path attacker could rewrite the node list too.
- Upstream verified nothing: name, size, pairing or agreement with other nodes.
- **What the validator verifies** when loading `/ledger/remote`:
  - it parses the archive names and requires the incremental's base slot to
    equal the full snapshot's slot;
  - it unpacks through a hardened unpacker (path and size limits);
  - it rebuilds the bank and compares the recomputed accounts hash with the hash
    in the archive *name*.

  That proves the archive is self-consistent. A forger also picks the name, so it
  does not prove the state is canonical. Canonicality is established only by
  replaying and agreeing with the bank hashes the supermajority votes on. When the
  validator downloads snapshots itself, `--known-validator` lets it check
  snapshot hashes against those validators' gossip; a download done outside the
  validator, like this tool's, bypasses that check.
- Rewrite mitigations:
  - strict parsing of `Location` and archive names (same-host path, base58 hash,
    numeric slots), so nothing a node sends can become a path outside the
    directory;
  - size == Content-Length before the atomic rename;
  - per-slot majority hash, with minority and tied hashes dropped and reported;
  - incrementals only from nodes serving the agreed full;
  - optional `--min_agreement N`;
  - HTTPS by default for the cluster RPC.
- Still open: a cross-check against a *trusted* source. A natural one is another
  node you run: compare the hash in the chosen archive name with the full snapshot
  name that node or a trusted RPC serves for the same slot.

## Decision: improve Python or rewrite?

**Rewrite in Go.** Measured reasons:
- The production failure described above was interpreter drift, not a logic bug. A venv
  pins packages, not the interpreter. A static binary removes the interpreter,
  pip/PyPI at deploy time, `python3-venv` and `wget`.
- Nearly every function needed changes: exit codes, download, verification,
  pairing, probing, retries. Fixing the script in place would have been a
  rewrite in Python anyway, still exposed to drift.
- Discovery is I/O-bound fan-out, which goroutines with per-request timeouts
  handle directly. 1.6 s versus 17–18 s on the same link, finding more nodes.
- Throughput is not a concern. On loopback the Go client keeps up with curl
  (~800 MB/s, limited by the fake server), above the 500–600 MB/s that
  well-connected validators typically get from the best nodes.
- Rust would bring the same deployment benefits with a heavier toolchain and more
  code, for no gain in a network-bound tool.

The Python script stays in `legacy/` with a minimal hotfix: the tqdm close,
exit codes, wget status, header test, pairing check, speed-test robustness and
pinned requirements. That covers installations that keep the Python
deployment for now.
