---
phase: 03-consensus-persistence-engine
reviewed: 2026-11-10T11:27:28Z
depth: standard
files_reviewed: 43
files_reviewed_list:
  - src/wal/mod.rs
  - src/wal/record.rs
  - src/wal/segment.rs
  - src/wal/io.rs
  - src/wal/writer.rs
  - src/wal/recovery.rs
  - src/wal/cache.rs
  - src/wal/store.rs
  - src/storage/mod.rs
  - src/storage/index.rs
  - src/raft/mod.rs
  - src/raft/types.rs
  - src/raft/config.rs
  - src/raft/node.rs
  - src/raft/state_machine.rs
  - src/snapshot/mod.rs
  - src/snapshot/format.rs
  - src/snapshot/reader.rs
  - src/snapshot/writer.rs
  - src/snapshot/store.rs
  - src/engine.rs
  - src/lib.rs
  - src/main.rs
  - Cargo.toml
  - src/net/mod.rs
  - src/net/protocol.rs
  - src/net/handler.rs
  - src/net/client.rs
  - src/net/server.rs
  - src/net/fake.rs
  - src/wire/mod.rs
  - src/wire/frame.rs
  - src/wire/hello.rs
  - tests/common/mod.rs
  - tests/cluster.rs
  - tests/transport.rs
  - tests/tcp_smoke.rs
  - tests/cli_smoke.rs
  - tests/openraft_api_gate.rs
  - tests/wal_test_util_reachable.rs
  - benches/wal.rs
  - scripts/check-warnings.sh
  - scripts/check-get-regression.sh
findings:
  critical: 3
  warning: 21
  info: 20
  total: 44
  necessity: 1
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-11-10T11:27:28Z
**Depth:** standard
**Files Reviewed:** 43
**Status:** issues_found

## Summary

The phase scope (43 files) was reviewed in four parallel slices, one reviewer per slice, and merged here by the orchestrator. Finding IDs were renumbered into one sequence; each finding names its slice and its original slice ID. Reviewers read source only; none ran builds or tests.

**WAL and storage index.** Reviewed the segmented group-commit WAL (record framing, segment layout, std/fake IO, writer thread,
recovery, offset index and entry cache, openraft store adapter) and the lock-free KV index. The core
fsync ordering (roll: sync old, create, sync dir, preamble, batch, sync new, ack; purge: marker
durable before any delete) is implemented as documented, and `storage/index.rs` is sound (the epoch
and CAS usage is correct, no finding). The problems are at the edges of the durability contract.

- Recovery treats any invalid record in the last segment as a torn tail, so one corrupted record
  silently discards every acknowledged record after it. This contradicts the module invariant "mid-log
  corruption is never treated as a torn tail".
- Oversized entries are not rejected up front. The writer can drop them silently, which leaves a
  durable index gap that makes the node unable to restart. One oversized client write is enough to
  trigger it.
- There are also a flushed-watermark over-report after an in-batch truncate, a purge/read race that
  surfaces a spurious I/O error, a config-change data-loss path in recovery, and an unvalidated log
  prefix.

Tests were not run (another process owns the test suite). Every finding was verified against the
source by tracing the call path.

(Finding IDs carry the `WAL-` prefix for the merge into 03-REVIEW.md.)

**Raft node, state machine, snapshot, engine, binary.** The state-machine apply path, the fuzzy-snapshot stamping scheme and the snapshot file format are sound. I traced the apply path (`apply_entry`, `InlineSink`) and the stamped skip semantics against the engine. I checked `scan_versioned` / `load_record` / replay, including the tombstone-drop rule. I read the full parse path of `format.rs` for panics on untrusted bytes. Every slice index is guarded by a prior length check, and the `expect("N bytes")` calls are all preceded by an exact-length split. The durability order is right:

- Build: write `.tmp`, `sync_all`, rename, fsync dir, delete older snapshots.
- Install: the transport `sync_all`s the received file, `install_received` re-validates every frame and CRC, and checks the header against the announced meta. It then renames, syncs the dir and deletes older snapshots. The engine swap happens last.

Snapshot names are numeric-only, so a peer-supplied string cannot reach a path. A `Skipped` CAS replay returns `Ok` instead of `CasFailed`. I traced where that response could reach a client: only entries proposed by the current leader carry responders, and those are never `Skipped`. So it is benign.

The problems are concentrated in three places:

- The binary advertises the wrong address for a wildcard bind.
- The snapshot store is not serialized against concurrent build, install and `get_current_snapshot`.
- Several start-up and operator-facing behaviours are footguns.

Static checks were not run (another process is running the test suite). No source file was modified.

**Peer network layer and wire framing.** The frame codec and handshake are in good shape. The length cap is enforced from the 4-byte prefix before the inner codec reserves any body capacity, and the `mid_frame` fix is correct. I traced it through the partial-read paths and found no resync hole. The pre-Hello cap of 4096 bytes and the handshake timeout are applied, `Hello::decode` is bounds-safe, and `Lane` is validated. Snapshot receive file names come from a process-local counter only, so no peer string reaches a path. Chunk offsets are strictly sequential and bounds-checked, and `TmpGuard` covers every in-process exit path. `clean_recv_dir` in `src/raft/node.rs` handles crash leftovers. I checked this: `recv-N.tmp` files are not leaked across restarts.

I confirmed the other protocol checks against the openraft 0.10.0-alpha.38 source:
- `Raft::install_full_snapshot` answers only after the install condition is met.
- A stale-vote rejection answers immediately, but then the file was not consumed, so `TmpGuard` removal is correct.

The problems are in connection lifecycle and trust:
- The client never checks which node answered.
- Nothing bounds the number of connections, so a peer can force large allocations and disk use.
- The replication, heartbeat and idle snapshot paths have no idle detection.
- The server can drop its final response frames when it tears a connection down.
- The server closes the connection after every non-accepted AppendEntries result, but the unary client path reuses it.
- The snapshot response wait is too short for large installs.

There are no Critical findings. The listener has no TLS or authentication by accepted decision D-08. I treat that as a given and flag only what a misconfiguration or a flood on a trusted network could trigger.

Context: each peer uses up to three TCP connections (replication, heartbeat, snapshot). That is by design (A4) and is not a finding.

**Integration tests, WAL bench, gate scripts.** The test suite is generally strong. The crash, partition, membership, snapshot catch-up and TCP wire tests each exercise the behavior they name, and most negative assertions are paired with a positive precondition. Nothing found would let a broken implementation ship green on a core path, so there are no Critical findings.

The warnings are:
- one assertion that is vacuous for half of what it claims (the leader-crash torn-tail check)
- one negative assertion with no evidence its precondition occurred
- timing-sensitive tests that will flake on loaded machines
- two benchmark methodology problems that undermine the stated purpose of the WAL bench (validating NFR-1 on a Linux host, and silently counting failed writes as latencies)

The gate scripts fail closed. The specific concerns are noted below as Info.

Verified as non-issues, so they are not reported:
- The `handshake_failures == 1` assertion in `handshake_timeout_closes_silent_connection` is not racy. `serve_connection` increments the counter before returning and dropping the socket.
- The shared `lightcycle-transport-test` default `recv_dir` is never written to, because no test uses the snapshot lane with the default config.
- `check-warnings.sh` handles the empty baseline correctly. `-s` takes the else branch, and `status` is checked independently.

## Critical Issues

### CR-01: Corruption in the last segment is treated as a torn tail and silently drops acknowledged records

**Slice:** WAL and storage index (slice ID `WAL-CR-01`)

**File:** `src/wal/recovery.rs:151-153`, `src/wal/recovery.rs:253-262` and `src/wal/recovery.rs:286-290`, `src/wal/recovery.rs:315-326`
**Issue:** In `Scanner::scan`, `Decoded::Invalid if is_last => break` stops the scan at the first bad
record of the last segment. `recover` then counts everything after `tail_offset` as torn bytes and
zeroes it (`set_len(tail)`, sync, `set_len(segment_size)`, sync). The same applies to `Decoded::End`
from a zero header. Torn writes only damage the unsynced suffix, but this code never checks whether
valid records or other non-zero data follow the bad record.

Concrete failure: the last segment holds acknowledged entries 100..200. A media bit flip, or a
misdirected or lost sector write, corrupts the record for entry 150. Recovery truncates at 150 and
permanently zeroes 151..200, which were fsynced and acknowledged. The node restarts believing its log
ends at 149 and reports `torn_bytes_discarded > 0` as if it were a normal crash. For a Raft follower
or leader, forgetting acknowledged (possibly committed) entries breaks Raft's safety assumptions. The
test `corrupt_middle_segment_is_hard_error` only covers non-final segments, and nothing tests a
bit flip in the last segment.

`head_is_unwritten` (lines 253-262) has the same flaw. `Decoded::Invalid => Ok(true)` means a
bit-flipped preamble in the last segment makes `recover` delete the entire segment, including all
acknowledged records after the preamble, and `sync_dir` the deletion. The comment "the preceding
segment holds every durable record" is false once the last segment was ever used.

**Fix:** Only classify a failure as a torn tail when nothing valid follows it. After an invalid record
at `off` in the last segment:
```rust
// Torn writes damage a suffix. If any later position decodes as a CRC-valid record,
// or non-zero data follows a *fully present* record whose CRC failed, this is
// corruption, not a tear.
fn valid_record_follows(image: &[u8], from: usize) -> bool {
    // scan 8-byte-aligned (or every) offsets for a record whose CRC verifies
    (from..image.len().saturating_sub(RECORD_HEADER_LEN))
        .any(|p| matches!(decode_record(&image[p..]), Decoded::Record { .. }))
}
// in scan(): on Invalid && is_last -> if valid_record_follows(image, off + 1) { return Err(corrupt(..)) }
```
Use the same check in `head_is_unwritten` before returning `Ok(true)` (require that the rest of the
image is zero or contains no valid record), and add a recovery test that flips a bit in a
mid-last-segment record that has synced records after it.

### CR-02: Oversized entries are not rejected up front; the writer can drop them silently and leave a durable index gap

**Slice:** WAL and storage index (slice ID `WAL-CR-02`)

**File:** `src/wal/writer.rs:465-469` and `src/wal/writer.rs:476-490`, `src/wal/store.rs:314-357`
**Issue:** `Writer::append` returns `Ok(false)` ("nothing appended") for any record with
`1 + payload > MAX_RECORD_LEN` (16 MiB) or one that cannot fit an empty segment. `apply_op` reports
this only through `acks.fail_last(...)` when `has_ack`. `WalLogStore::append` attaches the ack only to
the final entry of a batch (`store.rs:347-351`), so an oversized non-final entry is dropped with no
error at all. Nothing else bounds entry size: `src/raft` and `src/net` have no per-value limit, and the
network frame cap is 32 MiB, larger than the 16 MiB record cap. The writer also never checks that an
entry's index equals `next_index`.

Failure scenarios:
1. An `append` batch is `[A (>16 MiB, no ack), B (last, ack)]`. A is not written. B is written with
   index A+1 and acknowledged `Ok`. `LogIndex::push` sees a non-contiguous index. That is a
   `debug_assert` panic in debug builds (which turns into a writer fail-stop), or an error log and no
   index update in release builds. The WAL on disk now has a hole, and the next restart fails in
   recovery with `entry index N does not follow M`. The node cannot start without manual repair.
2. When the oversized entry is the last in the batch, the ack gets `InvalidInput`. openraft treats a
   storage write error as fatal, so a single large client write (or a large entry replicated from a
   leader) shuts the node down. `store.rs:337-339` has already inserted the entry into the cache and
   advanced `last_log_id` and `meta.next` before this point.

**Fix:** Validate in `WalLogStore::append` before touching the cache, `last_log_id` or `meta`:
reject the whole call (and complete the callback with `Err`) if any `postcard` encoded entry body
would exceed `MAX_RECORD_LEN - 9`. Better, enforce a per-request size limit at the client/network edge
that is lower than the record cap. In the writer, treat `Ok(false)` for an op without an ack as a
fail-stop (or at minimum log it), and reject an `Entry` whose index is not `next_index` (except after
truncate, purge or on a fresh log), so a gap can never become durable:
```rust
if !accepted && !has_ack {
    return Err(io::Error::new(io::ErrorKind::InvalidInput, "wal dropped an unacked record"));
}
```

### CR-03: Wildcard `--bind` publishes an unroutable address (`0.0.0.0:port`) into replicated cluster membership

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-CR-01`)

**File:** `src/main.rs:147-153` (also `src/net/server.rs:90-91`)
**Issue:** The bootstrap membership is built from `server.local_addr()`, which is `TcpListener::local_addr()` of the bound socket:

```rust
let local_addr = server.local_addr();
...
members.insert(config.node_id, BasicNode { addr: local_addr.to_string() });
```

With `--bind 0.0.0.0:7443` (or `[::]:7443`) this is the wildcard address, so the bootstrapping node writes `{1: "0.0.0.0:7443"}` into the first membership log entry. That entry is replicated to every peer and persisted in the WAL and snapshot meta.

Failure scenario: node 1 runs `--bind 0.0.0.0:7443 --peers 2=hostB:7443 --bootstrap`. Node 2 later needs to send a Vote, AppendEntries response path or snapshot request to node 1 and dials `0.0.0.0:7443`. On Linux that connects to node 2's own loopback, so node 1 is unreachable from every peer.

Node 1 can still lead (it dials the peers), but followers cannot reach it for elections. After node 1 restarts as a follower, or any leader change, the cluster cannot elect node 1 or route to it. The bad address cannot be corrected without a membership change.

There is no `--advertise` option, and `NodeConfig.bind` is never read. The README and `--bind` help text only say to bind to a private interface, so a wildcard bind is a plausible operator action. `local_addr` is correct only when the operator binds a concrete routable IP.

**Fix:** Add a separate advertised address and refuse to advertise a wildcard:
```rust
/// Address other nodes use to reach this one (defaults to --bind; must not be a wildcard).
#[arg(long)]
pub advertise: Option<String>,
...
let advertise = config.advertise.clone().unwrap_or_else(|| config.bind.clone());
let adv: std::net::SocketAddr = advertise.parse()
    .context("--advertise/--bind must be a literal ip:port")?;
anyhow::ensure!(!adv.ip().is_unspecified(),
    "cannot advertise wildcard {adv}; pass --advertise <routable ip:port>");
members.insert(config.node_id, BasicNode { addr: advertise });
```
Keep `local_addr` only for the port-0 (ephemeral) test case, substituting the real port into the advertised host.

## Warnings

### WR-01: Flushed watermark is raised past the truncation point when entries and a truncate marker share a batch

**Slice:** WAL and storage index (slice ID `WAL-WR-01`)

**File:** `src/wal/writer.rs:437-452` (with `src/wal/writer.rs:558-560` and `src/wal/writer.rs:604-626`)
**Issue:** In the `TruncateAfter` arm, `apply_truncate` clamps `self.unreported` before `write_run`.
Entries earlier in the same batch are still in `buf` at that moment, so they are not yet in
`unreported`. `write_run` then does `unreported = max(unreported, highest(locs))`, which includes
indexes above `keep_through`. After `observer.on_truncate` runs (the cache clamps `flushed` to `k`),
`finish_sync` calls `report_flushed`, and `on_flushed(10)` raises the cache watermark back above `k`.

Example: `Entry 6..10`, then `TruncateAfter(Some(5))` in one batch (openraft can issue a truncate
while an earlier append's flush callback is still pending). The cache watermark is now 10 although
the log ends at 5. Entries 6'..10' appended afterwards are treated as already durable, so
`EntryCache::evict` (`cache.rs:168-181`) may evict unsynced entries once the cache is over budget.
That breaks the documented invariant "never evict above the flushed watermark". If an eviction
happens before `on_entries_written` indexes the entry, `read_entry` returns `None` for a live entry
and `try_get_log_entries` returns a short result (a hole) to replication.

**Fix:** Clamp after `write_run`:
```rust
if ok {
    self.apply_truncate(keep_through);
    self.write_run()?;
    self.unreported = match (self.unreported, keep_through) {
        (Some(u), Some(k)) => Some(u.min(k)),
        _ => None,
    };
    self.observer.on_truncate(keep_through);
}
```

### WR-02: Purge deletes segment files before the index and cache drop the entries, so concurrent readers get a spurious I/O error

**Slice:** WAL and storage index (slice ID `WAL-WR-02`)

**File:** `src/wal/writer.rs:644-651` (with `src/wal/cache.rs:350-361`)
**Issue:** `run_purge` deletes the victim segment files and syncs the directory, and only then calls
`observer.on_purge(upto)`. A `WalLogReader` (replication task) that looked up an entry location just
before the deletion calls `read_at`, and `Handles::get` fails with `NotFound` because the file is gone
and the handle was removed in `delete_segment`. The error handler in `read_entry` only converts the
failure to "not found" when `index.get(index) != Some(loc)`. That check is still true until `on_purge`
runs, and the window includes the `sync_dir` call, which can last milliseconds. So `read_entry`
returns `Err`, `try_get_log_entries` returns `Err`, and openraft treats that as a storage error. The
module comment on `read_entry` says the opposite should happen. The race is on the live reader path only.

**Fix:** Call `self.observer.on_purge(upto)` after the marker is durable but before the delete loop.
The marker is already synced at that point, so dropping the in-memory index first is safe, and readers
will see `None`. Alternatively treat `io::ErrorKind::NotFound` from the segment read as `Ok(None)` in
`read_entry`.

### WR-03: Recovery caps reads at the configured segment size, then destroys anything beyond it

**Slice:** WAL and storage index (slice ID `WAL-WR-03`)

**File:** `src/wal/recovery.rs:61-63`, `src/wal/recovery.rs:285`, `src/wal/recovery.rs:312`, `src/wal/recovery.rs:318-325`
**Issue:** `read_image` reads at most `cfg.segment_size` bytes. If the node is restarted with a
smaller `segment_size` than the one the segments were created with (a plain config change), then in
the last segment a record that straddles the cap decodes as `Invalid`, which is treated as a torn
tail. `torn > 0` then runs `set_len(seq, tail_offset)` followed by `set_len(seq, cfg.segment_size)`,
permanently truncating the real file and destroying all acknowledged records past the cap. In
non-final segments a record boundary exactly at the cap makes the rest invisible (`End`), and the
failure shows up only later as a confusing `entry index ... does not follow` error.

**Fix:** Do not cap by config. Read the full `io.len(seq)` (or fail with a `WalError::Config` when any
existing segment is longer than `cfg.segment_size`), and only ever extend, never shrink, the file in
the torn-tail repair path. Also validate in `WalHandle::spawn` that `start.tail_offset` fits the
actual segment length.

### WR-04: Recovery never validates that the surviving log starts where the purge point says it should

**Slice:** WAL and storage index (slice ID `WAL-WR-04`)

**File:** `src/wal/recovery.rs:264-356`, `src/wal/store.rs:168-172`, `src/wal/segment.rs:59-60`
**Issue:** The sequence-gap check (`recovery.rs:276-280`) only compares neighbouring segments, so a
missing leading segment goes undetected. `Preamble.first_index` is written by the writer but never read
by recovery. Concrete case: segments 1..3 with no purge record, segment 1 lost (restore from a partial
copy, filesystem damage, an operator removing "old" files). Recovery starts at segment 2, and the
store sets `first = first_loc` (for example 100) while `get_log_state` reports `last_purged_log_id =
None`. The node then presents a log with a hole at the front to openraft as though it were complete.
Similarly, `Scanner::apply` accepts any first entry index after the locations become empty (after a
purge or truncate to nothing), so a hole at that boundary is not caught either.

**Fix:** After the scan, assert that the first recovered entry equals `purged.index + 1` (or the log's
initial index when nothing was purged), and that the first surviving segment's
`preamble.first_index` is consistent. Return `WalError::Corrupt` otherwise. In `Scanner::apply`,
require `index == purged_index + 1` (or the expected truncate point) when `locations` is empty and a
purge or truncate marker has been seen.

### WR-05: Snapshot store is not serialized against concurrent build / install / `get_current_snapshot`; a lost race becomes a spurious I/O error

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-WR-01`)

**File:** `src/snapshot/store.rs:156-163, 168-182, 219, 247`; `src/raft/state_machine.rs:300-330, 332-343`
**Issue:** `build_snapshot` runs on a blocking thread under `build_lock`, but `install_snapshot` and `get_current_snapshot` do not take that lock. Both are driven from the openraft state-machine worker while a build runs in its own task. Several store operations are listing-then-act sequences that error out when a concurrent operation deletes a file in between:

- `latest()` runs `snaps()`, then `read_header(&path)`, then `fs::metadata(&path)?`. If a concurrent `build` finishes between the listing and the read, it deletes the older file via `delete_snaps_where`. `read_header` then returns `SnapshotError::Io(NotFound)`, which `latest()` propagates as `Err(Io)` (line 174).
  - `get_current_snapshot` maps this to an `io::Error` returned from `RaftStateMachine`.
  - openraft treats storage errors from the state machine as fatal and stops the Raft core.
  - This is a transient condition that kills the node.
- `delete_snaps_where` uses `fs::remove_file(p)?`. A concurrent `build` and `install_received` (not serialized, since install does not take `build_lock`) can both list the same old file. The loser gets `NotFound` and its build or install returns an error after the new file was already renamed into place.
- A build that started before an install, on the pre-install engine, can finish afterwards. It publishes a lower-key `.snap` next to the installed one and returns a `Snapshot` whose meta is older than the installed state. `latest()` picks the highest key so on-disk state stays valid, but the stale file lingers until the next restart.

**Fix:** Make deletion and lookup tolerant of `NotFound`, and serialize install with builds.
```rust
fn delete_snaps_where(&self, pred: impl Fn((u64, u64)) -> bool) -> io::Result<()> {
    for (i, t, p) in self.snaps()? {
        if pred((i, t)) {
            match fs::remove_file(p) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
    }
    Ok(())
}
```
In `latest()`, loop: on `Io(NotFound)` from `read_header` or `metadata`, re-list and retry (bounded). In `install_snapshot`, acquire `self.build_lock` (`lock_owned().await`) before `install_received`, so a build cannot publish after the install.

### WR-06: A failed `rename` in `build_with_hook` leaks a full-size `.tmp`; the test named for this behaviour does not assert it

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-WR-02`)

**File:** `src/snapshot/store.rs:213-215` (test `src/snapshot/store.rs:460-467`)
**Issue:** The cleanup `let _ = fs::remove_file(&tmp)` runs only if `build_snapshot_file` fails. If `fs::rename(&tmp, &fin)?` fails (the test's directory-squatting case, `ENOSPC` on metadata, or a permission error), the fully written snapshot-sized `.tmp` stays on disk. It is removed only by the next successful build of the same `(index, term)` key (which differs on every attempt, since last_applied advances) or on process restart in `SnapshotStore::open`. A persistent rename failure with repeated snapshot attempts therefore leaks one snapshot-sized file per attempt on a node that is already struggling.

`failed_build_removes_partial_tmp` provokes exactly this case but only asserts `is_err()`. The tmp-removal claim in its name is never checked, and the test would still pass with the leak, so it gives false assurance.

**Fix:**
```rust
if let Err(e) = fs::rename(&tmp, &fin) {
    let _ = fs::remove_file(&tmp);
    return Err(e.into());
}
```
and in the test, add `assert!(!dir.path().join(file_name(3, 1, Ext::Tmp)).exists());`. (The hook-induced "crash" paths must keep the tmp, so scope the cleanup to the real-error branch only.)

### WR-07: A node started with no `--peers` silently initializes its own one-node cluster

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-WR-03`)

**File:** `src/main.rs:114-117`
**Issue:** `let bootstrap = config.bootstrap || peers.is_empty();` means that any fresh node whose peer list is empty (a typo, a templating bug, an env var that expanded to `""`, or an operator preparing a node to be added later with `add_learner`) runs `initialize` with `{self}` as the sole voter. It immediately elects itself leader of a brand-new, independent cluster at term 1.

That log (membership entry at index 1, term 1) conflicts with the real cluster's log, so the node can never be added as a learner afterwards. The only recovery is to wipe the data directory. The CLI has no join mode and no way to say "start uninitialized and wait", other than supplying a dummy non-empty `--peers`. The README documents only the static three-node flow, so the implicit single-node bootstrap is unexpected for the multi-node use case.

**Fix:** Make standalone bootstrap explicit, or log loudly when it is implicit:
```rust
let bootstrap = config.bootstrap;
if peers.is_empty() && !config.bootstrap && !config.standalone {
    anyhow::bail!("no --peers given: pass --standalone for a single-node cluster, or --peers ...");
}
```
At minimum, print a warning stating that an empty peer list initializes a standalone cluster.

### WR-08: Any unreadable-header error is reported as "wipe this node", including a snapshot written by a newer format version

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-WR-04`)

**File:** `src/snapshot/store.rs:172-176`, `src/snapshot/store.rs:115-117`, `src/raft/state_machine.rs:56-62, 143-145`
**Issue:** `latest()` maps every non-I/O header error to `CorruptNeedsWipe`. That includes `UnsupportedVersion(n)`, which is exactly what a binary downgrade produces after a newer build wrote a version-2 snapshot. The resulting message tells the operator to wipe the data directory and rejoin, which destroys a healthy node's state in a rollback scenario where reinstalling the newer binary would have worked.

Two related inconsistencies:
- `SnapshotStore::open` deletes every older snapshot (`delete_snaps_where(|k| k != newest)`) before the newest one is validated at all. If the newest is unreadable, the fallback is already gone. The no-fallback policy explains why this is tolerated, but it makes the wipe advice irreversible.
- Body corruption found by `load_snapshot` in `KvStateMachine::open` is returned as a raw `InvalidData` error such as "snapshot block 3 crc mismatch", with none of the wipe guidance that header corruption gets. The operator experience differs depending on which byte flipped.

**Fix:** Pass `UnsupportedVersion` through as its own error with a message such as "snapshot format vN is newer than this binary supports; run a newer build". Map body corruption in `KvStateMachine::open` to `CorruptNeedsWipe` consistently.

### WR-09: Client never checks that the node it connected to is the intended target

**Slice:** Peer network layer and wire framing (slice ID `NET-W01`)

**File:** `src/net/client.rs:231-232` (also `src/wire/hello.rs:235-243`)
**Issue:** `ensure_conn` discards the server's Hello (`Ok(_) => {}`). `client_handshake` checks only `cluster_id` on the ack, not `theirs.node_id`.
- `PeerClient` knows `self.target` and the Hello carries `node_id` for this purpose.
- If a membership address is stale or wrong (a re-provisioned host, a port reused after a restart, a copy-paste error), RPCs meant for node X go to node Y of the same cluster.
- Y's Vote response is then attributed to X by openraft, which keys responses by target id. One physical node can supply two votes, so a two-vote quorum can form from one node. That is a Raft safety hazard, not only a liveness one.
- The same applies to AppendEntries acknowledgements counted toward commit.

The `cluster_id` check cannot catch this because Y is in the same cluster.
**Fix:** Keep the server Hello from the handshake and reject a mismatch.
```rust
let theirs = match client_handshake(&mut framed, hello, self.cfg.handshake_timeout).await {
    Ok(h) => h,
    Err(/* existing mapping */) => ...
};
if theirs.node_id != self.target {
    return Err(unreachable_err(format!(
        "peer at {} identified as node {}, expected {}", self.addr, theirs.node_id, self.target)));
}
if theirs.lane != self.lane { /* also reject */ }
```
Better still, add a `HandshakeError::NodeMismatch` and an `expected_node_id: Option<u64>` parameter to `client_handshake` so the check lives with the other validations and has a unit test.

### WR-10: No bound on concurrent connections or concurrent snapshot receives; per-connection memory and disk are large

**Slice:** Peer network layer and wire framing (slice ID `NET-W02`)

**File:** `src/net/server.rs:145-154`, `:184`, `:655-683`, `:42`
**Issue:** `accept_loop` spawns a task for every accepted socket with no cap.
- After a (trivial, non-secret) Hello, the lane cap rises to `max_frame_len` (32 MiB). `LengthDelimitedCodec` reserves the declared frame size when it consumes the prefix, so one connection that declares a 32 MiB frame and then stalls pins about 32 MiB.
- Because the Hello is unauthenticated and `cluster_id` is a guessable u64, N such connections pin N x 32 MiB.
- On the snapshot lane, each connection may start its own receive of up to `max_snapshot_bytes` (8 GiB) in `recv_dir`. There is no global or per-peer limit on concurrent receives and no free-space check.
- A few connections that each stream data can fill the data volume. The WAL and snapshot store share that volume, so this can wedge the node.
- File descriptor exhaustion (EMFILE) is reachable the same way. The accept loop only sleeps 50 ms on error and keeps looping.

`Hello::cluster_id` is documented as not a security control and D-08 accepts no auth. Even so, unbounded resource use on an accepted-risk unauthenticated port should be capped.
**Fix:**
- Add a connection semaphore (for example `cfg.max_connections`, with `try_acquire_owned` in the accept loop that drops the socket on exhaustion).
- Add a global "active snapshot receives" limit, 1 per source IP or 1 per node, and reject a second `SNAP_BEGIN` with a non-fatal error.
- Check `fs2::available_space(recv_dir) >= total_len` before creating the file, or `set_len`/`fallocate` it so a lack of space fails at `SNAP_BEGIN` and not mid-transfer.
- Consider a lower frame cap until the first valid app frame.

### WR-11: No idle detection, TCP keepalive or write timeout on replication/heartbeat connections, or on an idle snapshot connection

**Slice:** Peer network layer and wire framing (slice ID `NET-W03`)

**File:** `src/net/server.rs:213-220`, `:345-350`, `:615-624`; `src/net/client.rs:228`
**Issue:** The `run_lane` reader waits on `stream.next()` forever.
- `run_snapshot_lane` also waits forever when no receive is active (`Some(framed.next().await)`).
- `writer_task` has no timeout on `sink.send`, and no socket sets `SO_KEEPALIVE`.
- If a peer host dies, loses power, or is behind a vanished NAT or firewall entry, the server never sees FIN or RST. The connection task, its `AppendSession`, the responder, the socket and the read buffer live until the OS gives up (hours with defaults, or never for a peer that is merely silent).
- Every reconnect by the client adds another leaked server connection. Over repeated peer crashes and reboots the leaked sessions accumulate.
- A peer that stops reading is not detected either. The writer blocks, the out queue (256 frames) fills, the responder blocks holding window permits, and the reader stops. The connection then stays parked forever.
- On the client, a half-open connection is only discovered on the next RPC (bounded by `soft_ttl`), so this side is acceptable.

**Fix:**
- Enable keepalive on both accepted and dialed sockets, for example with `socket2::SockRef::from(&socket).set_tcp_keepalive(&TcpKeepalive::new().with_time(30s).with_interval(10s))`.
- Add an idle bound on the server reader. A replication or heartbeat peer sends at least heartbeats, so `tokio::time::timeout(cfg.lane_idle_timeout, stream.next())` is safe on the heartbeat lane. For the replication lane choose a generous bound, or rely on keepalive alone.
- Wrap `sink.send` in `writer_task` with a timeout.
- Apply the idle timeout to the no-active-receive state of the snapshot lane too.

### WR-12: Writer task is aborted without draining, so the final response/error frame can be lost

**Slice:** Peer network layer and wire framing (slice ID `NET-W04`)

**File:** `src/net/server.rs:290-325`, `:339-341`, `:213-220`
**Issue:** `append_responder` enqueues the last frame with `out.send(...)`. That frame is the Fatal `RemoteFailure`, the encode-failure error, or the response that precedes the end of openraft's result stream. The responder then returns, and its `drop_guard` cancels `conn_cancel`.
- `run_lane` is selecting on `conn_cancel.cancelled()` (biased), breaks, and returns.
- `_writer: AbortOnDrop` aborts the writer task. Whether the writer has dequeued and flushed the final frame before the abort is a scheduling race.
- It can lose on a multi-thread runtime, or whenever the socket is not immediately writable.
- The code comments say a handler `Fatal` "is reported to the peer first". That is not guaranteed.
- A lost Fatal frame makes the peer see EOF (`Unreachable`) instead of the intended fatal-with-reason, so it does not get the "back off" signal the protocol defines.
- The lost frame can equally be the Conflict/HigherVote answer to the request, which the peer then never sees (see WR-13).

**Fix:** Do not abort the writer on a graceful end.
- Hold the `JoinHandle` in `run_lane` and, after the reader loop exits, drop the `out_tx` clones (the session, the vote tasks) and `tokio::time::timeout(ERROR_SEND_TIMEOUT, writer).await` so the queue drains.
- Keep abort only for the hard-cancel path (server shutdown).
- Alternatively have `append_responder` finish with a flush barrier (a oneshot that the writer acks after `sink.flush()`).

### WR-13: Server closes the connection after every Conflict/HigherVote AppendEntries result, but the unary client path reuses the connection

**Slice:** Peer network layer and wire framing (slice ID `NET-W05`)

**File:** `src/net/server.rs:300-325` (with `~/.cargo/registry/.../openraft-0.10.0-alpha.38/src/raft/stream_append.rs:93-96`); `src/net/client.rs:287-290`, `:336-340`
**Issue:** openraft's `Raft::stream_append` yields an `Ok(Err(_))` result (Conflict or HigherVote) and then terminates the stream (`Some((result, None))`).
- `append_responder` sends that response, then `results.next()` returns `None`.
- The responder exits and its guard cancels the connection, so the server closes the socket after every non-Accepted append result. Combined with WR-12, the response itself may also be lost.
- The pipelined client path tolerates this, because the stream ends by design after an API error.
- The unary `PeerClient::append_entries` path (used for heartbeats, or any non-pipelined caller) receives the Conflict/HigherVote response correctly, `unary()` puts `conn` back, and the next call writes to a connection the server has already closed.
- That call then fails with `Unreachable("connection closed by peer")` or a send error, and it reconnects only on the call after.
- Each log conflict or stale-leader heartbeat therefore costs one spurious Unreachable and an openraft back-off on that lane.

**Fix:**
- Client: treat an `APPEND_RESP` whose decoded result is Conflict/HigherVote as terminal for the connection on the append path, with `self.conn = None` after decoding in `append_entries`. Reconnect is then deliberate and not an error.
- Server: restart the `AppendSession` (`append = None`) instead of cancelling the whole connection when the result stream ends, so a Conflict does not tear the socket down.
- Add a test: unary append yields a Conflict, then a second unary append on the same `PeerClient` succeeds.

### WR-14: Snapshot response is awaited with the 30 s per-chunk idle timeout, but install includes full double validation of up to 8 GiB

**Slice:** Peer network layer and wire framing (slice ID `NET-W06`)

**File:** `src/net/client.rs:622-631` (`snap_recv`, `:528`); `src/net/server.rs:752-775`; `src/raft/state_machine.rs:314-323`
**Issue:** After `SNAP_END` the server runs `handler.install_snapshot`, which loads the whole file into a fresh engine (`load_snapshot`) and validates it a second time in `install_received`. The code comment says this is done twice on purpose.
- The sender waits for `SNAP_RESP` with the same `snapshot_idle_timeout` (30 s default) used for a single chunk gap. That timeout was designed as "per-frame", not for server-side work.
- For a multi-GiB snapshot the install can exceed 30 s. The leader then times out and drops the connection, and replication retries the entire snapshot (up to `max_snapshot_bytes`, 8 GiB).
- The server keeps installing the first copy regardless, because the server loop does not notice the disconnect during install.
- The next attempt is a full resend whose install is skipped as stale or duplicate. That can repeat indefinitely and it re-sends the whole snapshot each time.

**Fix:** Give the final response its own, larger bound. For example add `snapshot_install_timeout` (default several minutes, or scaled by `total_len`) used only for the post-`SNAP_END` `SNAP_RESP` wait. Optionally have the server send keep-alive `SNAP_ACK` frames while the install runs, so the idle bound stays meaningful.

### WR-15: Vote/pre-vote handler errors are always reported as non-fatal although they can only be Fatal

**Slice:** Peer network layer and wire framing (slice ID `NET-W07`)

**File:** `src/net/server.rs:395-409`; `src/net/handler.rs:39-48`; `src/net/client.rs:166-175`, `:292-297`
**Issue:** `RaftHandler::vote`/`pre_vote` return `RaftError<TypeConfig>`, which defaults `E = Infallible`. Its only inhabited variant is `Fatal` (the node has stopped). The server maps every `Err(e)` to `RemoteFailure::new(false, ..)`.
- The client sees `fatal == false`, keeps the connection, and returns `RPCError::Network` instead of the documented `Unreachable` ("when the remote node has stopped, Unreachable").
- Against a stopped remote the candidate gets repeated `Network` errors (retried by openraft) instead of the back-off intended for a stopped node. The module doc of `client.rs` promises the latter.
- The append path correctly sets `fatal = true`. This is inconsistent with it.

**Fix:**
```rust
Err(e) => error_frame(lane, req_id, &RemoteFailure::new(true, e.to_string())),
```
Match on `RaftError::Fatal(_)` explicitly for clarity so a future non-`Infallible` error type does not silently become fatal.

### WR-16: Second-crash torn-tail assertion is vacuous

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-WR-01`)

**File:** `tests/cluster.rs:347-352`
**Issue:** `kill_restart_keeps_committed_writes` is documented as crashing a follower and then the leader, each with a genuinely torn WAL tail. The follower crash is asserted at line 327: `first.torn_bytes_discarded > 0`. The leader crash is "asserted" at lines 348-352 as `first + second > 0`. Because `first > 0` was already proven, this can never fail, whatever `second` is. If the 301-byte leader tear silently degraded into a clean shutdown, because the writer had nothing in flight or the sync completed first, the test would still pass. The leader-recovery torn-tail path (a different code path from a follower: truncation of a log that was a leader, re-election) is therefore only covered by hope.
**Fix:** Assert on `second` directly, mirroring the follower check:
```rust
assert!(
    second.torn_bytes_discarded > 0,
    "leader restart must have discarded a torn tail: {second:?}"
);
```
If the leader tear is inherently less deterministic, make that explicit by retrying the crash, or by comparing `unsynced_bytes() > keep` immediately before `crash`. Do not sum it with the other result.

### WR-17: "Uncommitted write is discarded" has no evidence the write was ever appended

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-WR-02`)

**File:** `tests/cluster.rs:280-299`
**Issue:** The test claims "the old leader steps down, discards its uncommitted entry and converges". The only evidence is `engine().get(b"minority-only").is_none()` after healing. That passes equally if the write was rejected up front (for example, an immediate `ForwardToLeader` or `NotLeader` error, or a leader that already stepped down in isolation), so the entry never reached the old leader's log. In that case the discard/truncate path this test is meant to cover is never exercised. The preceding assertion `!matches!(res, Ok(Ok(_)))` also accepts any fast error.
**Fix:** Prove the precondition before healing. For example, capture the old leader's `metrics().last_log_index` before the write and assert it advanced afterwards (the entry is in the log but uncommitted). After healing, assert that `last_log_index` converges to the new leader's and that the entry's index now holds a different log id. Alternatively, require `res` to be `Err(Elapsed)` (timeout) rather than any non-Ok.

### WR-18: Exact-count assertions after a fixed 300 ms sleep (flaky on loaded hosts)

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-WR-03`)

**File:** `tests/transport.rs:443-445`, `tests/transport.rs:479-484`
**Issue:** `inflight_window_bounded` sleeps 300 ms and then asserts `append_req_frames == 4` and `max_inflight_observed == 4`. `byte_window_admits_oversized_request_alone` sleeps 300 ms and asserts `append_req_frames == 2`. Equality (not `<=`) on a count that only reaches its final value after the client has dialed, handshaked and pipelined frames means a slow or heavily loaded runner (a debug build, or many test threads) can observe 3 frames after 300 ms and fail. The same test is meaningful only as an upper bound plus an eventual lower bound.
**Fix:** Wait for the lower bound with the existing `eventually` helper, then hold the window open and check that the count does not exceed it. For example, `eventually("window filled", || frames == 4)`, then `sleep(100ms)`, then `assert_eq!(frames, 4)`. This separates "fills the window" (no fixed sleep) from "does not exceed the window" (a short, safe sleep).

### WR-19: `snapshot_does_not_starve_heartbeat` relies on absolute wall-clock bounds

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-WR-04`)

**File:** `tests/transport.rs:1705-1712`
**Issue:** The test asserts `median < 20 ms` and `max < 250 ms` for heartbeat round trips measured while a 32 MiB snapshot streams in a debug build. Each round trip is also separated by a 10 ms sleep. On a shared or throttled CI machine (or under `cargo test` parallelism with the other transport tests in this binary), a single scheduler stall above 250 ms, or a median above 20 ms, fails the test without any starvation bug. Much of the overlap window is the stub's 300 ms `install_delay` sleep, which does not contend for CPU or I/O. The median bound therefore also under-tests real chunk-transfer contention.
**Fix:** Loosen the bounds to a multiple of the install delay (the existing rationale), for example `max < 1 s`, and drop the median check or compare against a baseline RTT measured before the snapshot starts (`median < baseline * K`). Alternatively, count heartbeats that completed strictly before the snapshot's chunk phase ended, using the `snapshot_chunks` counter, instead of using absolute durations.

### WR-20: WAL bench "fsync" scenarios use `tempfile::tempdir()`, which is often tmpfs on Linux

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-WR-05`)

**File:** `benches/wal.rs:115-116` (and header comment `benches/wal.rs:3-7`)
**Issue:** The module doc states that "only a Linux NVMe host can validate NFR-1" and defers that manual verification to the user's Linux host. `std_scenario` creates its WAL directory with `tempfile::tempdir()`, which resolves to `$TMPDIR` or `/tmp`. On many Linux distributions `/tmp` is tmpfs (Fedora, Arch, some Ubuntu configurations, most containers), where `fsync` is effectively free. The `[tail]` and `[fsync]` figures from that host would then measure RAM, not NVMe, and would silently "validate" NFR-1. There is no way to point the bench at the real device, and the bench does not print which filesystem it is on.
**Fix:** Accept an override such as `LC_WAL_BENCH_DIR` (create the temp dir inside it with `tempfile::tempdir_in`). Print the resolved path in the `[info]` line. Optionally warn if `statfs` reports `tmpfs`.

### WR-21: Bench counts failed acks as latencies and cannot detect a failing WAL

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-WR-06`)

**File:** `benches/wal.rs:76-79`, `benches/wal.rs:99`, `benches/wal.rs:33-45`
**Issue:** An ack that returns `Err` is recorded as `u64::MAX`, and `drive` returns it in the latency vector without checking. `tail_report` then prints `max=18446744073709551615ns`, or, if more than 1% of acks fail, a `p99` of that value. The bench exits successfully either way, and a disk-full or fault-injected run is indistinguishable from a latency outlier unless someone reads the digits. A fast failing write path (acks return `Err` immediately) would also pull the p50 down and make the numbers look better rather than worse. The same is true of `fakeio_round_trip` (`benches/wal.rs:164`), which only `black_box`es the ack result and never checks `.is_ok()`.
**Fix:** Count failures in `drive` and panic (or at least print `[error] N acks failed` and exit non-zero) when any are present:
```rust
let failed = lat.iter().filter(|&&v| v == u64::MAX).count();
assert_eq!(failed, 0, "{failed} of {ops} appends failed; latencies are not meaningful");
```
Also `assert!(black_box(rx.recv().expect("ack")).is_ok())` inside the Criterion loop.

## Info

### IN-01: Dead error variants and a stale doc comment

**Slice:** WAL and storage index (slice ID `IN-01`)

**File:** `src/wal/segment.rs:223-233`, `src/wal/record.rs:75-77`
**Issue:** `WalError::Failed`, `WalError::Closed` and `WalError::RecordTooLarge` are never constructed
anywhere in `src/`. `encode_record`'s doc says the writer rejects oversize records "with
`WalError::RecordTooLarge`", but the writer fails the ack with `io::ErrorKind::InvalidInput`
(`writer.rs:466-469`). `encode_record` also does an unchecked `(1 + payload.len()) as u32`; it is only
safe because of the caller contract.
**Fix:** Use `RecordTooLarge` at the real rejection site (after CR-02) or delete the unused
variants, fix the doc, and add a `debug_assert!(payload.len() < MAX_RECORD_LEN as usize)` in
`encode_record`.

### IN-02: `WalLogStore::append` can return `Err` without completing the openraft callback

**Slice:** WAL and storage index (slice ID `IN-02`)

**File:** `src/wal/store.rs:330-357`
**Issue:** The doc and module header promise that each flush callback is completed exactly once. If
`self.submit` fails for a non-final entry (the writer has failed in the middle of the loop, because
`handle_mut` is re-checked on every `submit`), the function returns `Err` with the callback still in
`callback: Option<IOFlushed>`, which is dropped uncompleted. Earlier entries of the batch were already
submitted without acks. This is harmless today because openraft treats an `Err` from `append` as
fatal, but it relies on the caller's behaviour rather than the stated invariant.
**Fix:** On the error path, take the callback and call `fail_callback(cb, &e)` before returning.

### IN-03: Recovery hard-fails on a segment gap that a partially persisted purge can legitimately produce

**Slice:** WAL and storage index (slice ID `IN-03`)

**File:** `src/wal/recovery.rs:276-280`, `src/wal/writer.rs:644-651`
**Issue:** `run_purge` deletes victim segments one at a time and syncs the directory once at the end.
On a filesystem that does not persist unlinks in issue order, a power loss mid-purge can leave, for
example, segment 1 present but segment 2 gone. The marker is durable, so the state is recoverable, but
`recover` returns `segment sequence has a gap` and the node cannot start. Ordered-journal filesystems
make this unlikely, so this is low priority.
**Fix:** Tolerate a gap when every segment before it is wholly covered by the purge point carried in
the first segment after the gap, deleting the stragglers during recovery; or sync the directory after
each deletion.

### IN-04: `Display for KvRequest` dumps keys and values into any log line that formats an entry

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-IN-01`)

**File:** `src/raft/types.rs:31-35`
**Issue:** `write!(f, "{:?}", self)` prints the full `Bytes` of key and value. openraft formats entries and requests through `Display` in debug/trace logs and error messages. Values (potentially secrets, up to 16 MiB) would be copied into the log, with cost and disclosure proportional to value size. There is no concrete leak at the default log level, so this is Info.
**Fix:** Print only the variant, key length and value length (and a truncated key preview if wanted).

### IN-05: Declared minimum `bytes = "1.7"` is too low for `Bytes::from_owner`

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-IN-02`)

**File:** `Cargo.toml:17`, `src/engine.rs:83`
**Issue:** `Bytes::from_owner` was added in bytes 1.9.0. The lockfile pins 1.12.1, so builds work, but the manifest permits resolvers (or `-Z minimal-versions`, or a downstream crate consuming the lib) to select 1.7 or 1.8, which do not compile.
**Fix:** `bytes = { version = "1.9", features = ["serde"] }`.

### IN-06: Leader watcher re-announces leadership every hour

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-IN-03`)

**File:** `src/main.rs:166-190`
**Issue:** After a node becomes leader, the second `wait.metrics(.. != Leader ..)` times out after 3600 s while the node is still leader. The `Err(Timeout)` is treated as a non-event, so the loop restarts. `wait.state(Leader)` then returns immediately because the node is still leader, and it prints "node N is leader (term T)" again. A stable leader logs this line hourly, which suggests a leadership change that did not happen. Transitions that complete between two polls are also missed.
**Fix:** Use a metrics watch (`raft.metrics()` receiver `changed()`) tracking the previous state, and print only on a real transition.

### IN-07: `SnapshotStore::read_chunk` has no production caller

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-IN-04`)

**File:** `src/snapshot/store.rs:258-265`
**Issue:** The only callers are in this file's tests. The network client has its own `read_chunk` over an already-open `Arc<File>` (`src/net/client.rs:487`). Opening per chunk would be the wrong design for a long transfer anyway, since a concurrent build can delete the path mid-transfer. It is dead public API, and its existence invites that misuse.
**Fix:** Remove it, or mark it `#[cfg(test)]`.

### IN-08: Generality without instantiation, `NodeConfig.bind` is never read

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-IN-05`)

**File:** `src/raft/config.rs:89-90`, `src/main.rs:121`
**Category:** necessity
**Issue:** `NodeConfig.bind` is assigned in `main.rs:121`, but no code reads it (`grep` finds no `cfg.bind` or `.bind` read in `src/raft` or `src/main.rs`). `main` binds the server with `config.bind` directly (`main.rs:139`). `NodeConfig::validate` never checks it either, so it neither configures nor validates anything. The field implies the node config owns the listen address, which is also why the advertise-address problem (CR-03) went unnoticed.
**Fix:** Either wire it up (validate that it parses as a `SocketAddr` in `validate()` and have `main` read it from the config) or remove the field.

### IN-09: `RaftNode::start` creates the WAL directory before the config is validated

**Slice:** Raft node, state machine, snapshot, engine, binary (slice ID `RAFT-IN-06`)

**File:** `src/raft/node.rs:79-83, 96`
**Issue:** `StdFsIo::new(wal_dir)` creates `<data_dir>/wal` (mode 0700) before `start_with_io` calls `cfg.to_raft_config()?` (which runs `validate`). A config that is rejected (`node_id 0`, a self-referencing peer list, `--snapshot-every 0`) leaves directory side effects on disk. These are harmless in isolation but surprising.
**Fix:** Call `cfg.validate()?` at the top of `start` before constructing the WAL IO.

### IN-10: Server ignores the Hello `node_id`

**Slice:** Peer network layer and wire framing (slice ID `NET-I01`)

**File:** `src/net/server.rs:200-210`
**Issue:** `serve_connection` uses only `hello.lane`. The announced `node_id` is neither validated (for example, that it is not the server's own id, or that it is a known member) nor attached to per-connection logs or stats. It is useful for diagnostics and for rejecting self-dial or misrouted connections. Without it, debugging a mis-wired cluster has no handle on who connected.
**Fix:** Reject `hello.node_id == cfg.node_id`, and include `peer_node = hello.node_id` in the tracing spans for the connection. If a membership view is available, optionally reject non-members.

### IN-11: Settings that must match across peers are not negotiated, so mismatches fail late and opaquely

**Slice:** Peer network layer and wire framing (slice ID `NET-I02`)

**File:** `src/net/client.rs:60-80`, `:108-113`; `src/wire/hello.rs`
**Issue:** `snapshot_ack_window` (documented as "must match, otherwise stall until the idle timeout"), `snapshot_chunk_size` (sets the receiver's frame cap) and `max_frame_len` are per-node config that only work if compatible with the peer. A sender whose chunk size or `max_frame_len` exceeds the receiver's cap gets its connection closed with a decode error and no explanation. A window mismatch stalls for 30 s.
**Fix:** Either carry these values in the Hello and fail the handshake with a clear reason, or validate them in `TcpNetConfig` and document them as cluster-wide settings.

### IN-12: Fake snapshot delivery does not remove the received copy, unlike the real server

**Slice:** Peer network layer and wire framing (slice ID `NET-I03`)

**File:** `src/net/fake.rs:338-345`
**Issue:** The real server removes `recv-N.tmp` after the handler answers (`TmpGuard`, covers the stale-skip case). The fake copies the file into `recv_dir` and leaves it. Tests that exercise a skipped or stale snapshot cannot detect a leak through the fake, and the two transports diverge in observable file-system behaviour.
**Fix:** Mirror the guard: delete `dest` after `install_snapshot` returns (Ok or Err), or document that the fake leaves files for inspection.

### IN-13: Connection task panics/failures are discarded and `shutdown` does not wait for connection cleanup

**Slice:** Peer network layer and wire framing (slice ID `NET-I04`)

**File:** `src/net/server.rs:161`, `:119-123`
**Issue:** `Some(_) = conns.join_next()` drops the `JoinError`, so a panicking connection task (for example an `expect` firing) produces no log line. `shutdown()` awaits only the accept loop. Dropping the `JoinSet` aborts connection tasks asynchronously, so `TmpGuard` drops and in-flight `spawn_blocking` writes may still be running after `shutdown().await` returns. A test or restart that immediately inspects or re-creates `recv_dir` can observe leftover `recv-N.tmp` files.
**Fix:** Log `Err(e)` from `join_next()` at `warn`. In `accept_loop`, after the cancel break, call `while conns.join_next().await.is_some() {}` (with a timeout) before returning, so shutdown completes only after the connection tasks have unwound.

### IN-14: `check-get-regression.sh` no-match case exits silently; the diagnostic branch is dead

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-IN-01`)

**File:** `scripts/check-get-regression.sh:24-30, 38-44`
**Issue:** Under `set -euo pipefail`, `extract_p99` returns 1 when `grep -F` finds no `[tail] GET ...` line, because grep's status propagates through pipefail. The assignment `p99_1t="$(extract_p99 ...)"` then terminates the script immediately via `set -e`, before the `[[ -z "$p99_1t" ... ]]` check that would have printed the bench output and the "could not parse" message. The gate still fails closed (exit 1), but a changed bench label produces an exit code with no explanation. The `-z` branch is only reachable when grep matches but sed does not.
**Fix:** Make the function tolerate no-match: append `|| true` to the pipeline inside `extract_p99`, or use `p99_1t="$(extract_p99 ...)" || true`, so the existing parse-failure diagnostic runs.

### IN-15: `check-get-regression.sh` thresholds are a single-machine absolute baseline

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-IN-02`)

**File:** `scripts/check-get-regression.sh:7-10, 18-19`
**Issue:** The 334 ns and 667 ns limits are derived from one development machine, and the header notes a timer granularity of ~41.7 ns (Apple 24 MHz counter). On a Linux host with a ~1 ns clock, or a slower or throttled machine, the limits mean something different or fail spuriously. The best-of-3 policy hides regressions that are intermittently slow (the gate passes if any attempt is within limits). Both choices are documented, so this is a portability caveat rather than a bug.
**Fix:** Allow a recorded per-host baseline file (or env override) and report which attempt passed. Note the machine class in the script header. No code change is strictly required.

### IN-16: `check-warnings.sh` continues after a failed `cd`

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-IN-03`)

**File:** `scripts/check-warnings.sh:7, 9`
**Issue:** The script uses `set -u` without `-e` (intentionally, since `$status` is checked) but also does not guard `cd "$(dirname "$0")/.."`. If the `cd` fails, `cargo check` runs in the caller's directory. In an unrelated Cargo project, or with no manifest, the result is arbitrary (exit 0 for an unrelated clean crate). This is an extreme edge case, but it is a one-token fix.
**Fix:** `cd "$(dirname "$0")/.." || exit 2`.

### IN-17: WAL bench report runs unconditionally, ignoring Criterion filters and `--test`

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-IN-04`)

**File:** `benches/wal.rs:137-138`
**Issue:** `bench_wal` calls `append_ack_report()` before any Criterion filter or test-mode processing. `cargo bench --bench wal -- fakeio` and `cargo test --benches` / `--all-targets` (test mode) each run all three scenarios (200 000 FakeIo ops plus 5 000 and 300 real `fsync` ops; at the ~4 ms `F_FULLFSYNC` figure quoted in the header, that is more than 20 s on macOS). `LC_WAL_BENCH_OPS` exists as an override, but nothing steers `--test` runs toward it.
**Fix:** Skip the report when running in test mode (`std::env::args().any(|a| a == "--test")`) or gate it behind `LC_WAL_BENCH_TAIL=1`. Alternatively document the cost in the header.

### IN-18: Bench statistics are not trustworthy at the small op counts

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-IN-05`)

**File:** `benches/wal.rs:26-45, 133-134`
**Issue:** The 300-op single-in-flight scenario prints `p99.9`, which equals `max` at n=300, and `p99` is the third-largest sample. No warm-up samples are discarded (first segment creation, thread start and page faults land in the percentile data). The timestamp is taken before the payload `vec!` and the ack `Box` allocation, so allocator cost is counted as append-to-ack latency in the "pure CPU" FakeIo scenario. In `fakeio_round_trip` the `mpsc::channel()` and payload allocation sit inside `b.iter`. These are all small effects, but the numbers are being quoted in `[tail]` reports.
**Fix:** Drop the first ~1% of samples, suppress `p99.9` when `n < 1000`, and build the payload and ack channel outside the timed region (take `Instant::now()` immediately before `try_submit`).

### IN-19: Several tests make claims their assertions only partly support

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-IN-06`)

**File:** `tests/cluster.rs:548`, `tests/cluster.rs:606`, `tests/tcp_smoke.rs:267-272`, `tests/tcp_smoke.rs:374-378`, `tests/cluster.rs:453-458`, `tests/transport.rs:1513`
**Issue:** A group of small gaps:
- `kill_restart_snapshot_catchup` calls `cluster.kill()` (graceful shutdown) on one follower and partitions the other. No node is crash-killed, so the name overstates it. `stats.snapshot.delivered >= 1` is cluster-wide rather than for the lagging node. The engine-swap assertion is the real proof and is sufficient.
- In `three_nodes_over_tcp_elect_and_replicate`, "follower received AppendEntries over TCP" is satisfied by heartbeat-lane frames alone, since `run_lane` counts `append_req_frames` for both lanes. Replication itself is proven by `holds_all`, so this is redundant rather than wrong.
- The recv-dir watcher in `tcp_snapshot_lane_install` is explicitly "best effort"; with no sightings the loop asserts nothing. This is acknowledged in the comment.
- `membership_remove_voter` proves "the removed node sees no more writes" with a fixed 500 ms sleep followed by a negative check. A slow replication tail could slip past, but a false pass is the only risk.
- `snapshot_idle_timeout_is_per_chunk_not_total` says "four acknowledgements"; with 8 chunks and window 2 the final window needs no ack, so three delayed acks (~750 ms) occur. The assertion `elapsed > idle` is still sound.
**Fix:** Rename or reword the comments. Where cheap, tighten: for example, assert `stats.snapshot.delivered` per target if the fake network exposes it, and count only `Lane::Replication` frames for the replication claim.

### IN-20: Minor timing and diagnosability risks in test helpers

**Slice:** Integration tests, WAL bench, gate scripts (slice ID `TEST-IN-07`)

**File:** `tests/cluster.rs:332-341` (via `tests/common/mod.rs:332-341`), `tests/common/mod.rs:485-501`, `tests/cli_smoke.rs:32, 63`, `tests/transport.rs:299-312`
**Issue:** A group of small fragility points:
- `crash_with_unsynced_tail` detects `unsynced > keep` with a 10 ms poll, then calls `crash`. The writer's delayed fsync holds for 250 ms, so the window is fine in practice, but a >240 ms stall of the test task between the check and `crash` (a heavily loaded runner) would leave zero unsynced bytes, and then `torn_bytes_discarded > 0` (WR-16 and the follower check) fails spuriously.
- `MembershipLog::watch` samples a coalescing watch channel, so a positive `saw_joint` assertion (`membership_replace_voters_joint`) could miss a brief joint config if no task is scheduled between the joint and final entries. Observing it on any of five nodes makes this unlikely.
- `cli_smoke` sends the child's stderr to `/dev/null`, so a startup failure (port, permissions, panic) only produces "no leader line within 15 s" with no cause. If the child exits early, the closed channel gives the same misleading message.
- `connect_refused_is_unreachable` frees a port and assumes nothing rebinds it before the dial. This is vanishingly unlikely but non-deterministic.
**Fix:** Capture stderr to a file or pipe and include it in the panic message, and distinguish `Disconnected` from `Timeout` in `cli_smoke`. The other points are acceptable as is, or can be retried once on failure.

---

_Reviewed: 2026-11-10T11:27:28Z_
_Reviewer: Claude (gsd-code-reviewer, four slices merged by orchestrator)_
_Depth: standard_
