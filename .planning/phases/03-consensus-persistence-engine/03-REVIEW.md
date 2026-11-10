---
phase: 03-consensus-persistence-engine
reviewed: 2026-11-10T14:00:00Z
depth: standard
files_reviewed: 28
files_reviewed_list:
  - Cargo.toml
  - README.md
  - benches/wal.rs
  - scripts/check-get-regression.sh
  - scripts/check-warnings.sh
  - src/engine.rs
  - src/main.rs
  - src/net/client.rs
  - src/net/fake.rs
  - src/net/server.rs
  - src/raft/config.rs
  - src/raft/node.rs
  - src/raft/state_machine.rs
  - src/raft/types.rs
  - src/snapshot/format.rs
  - src/snapshot/store.rs
  - src/wal/record.rs
  - src/wal/recovery.rs
  - src/wal/segment.rs
  - src/wal/store.rs
  - src/wal/writer.rs
  - src/wire/hello.rs
  - src/wire/mod.rs
  - tests/cli_smoke.rs
  - tests/cluster.rs
  - tests/common/mod.rs
  - tests/tcp_smoke.rs
  - tests/transport.rs
findings:
  critical: 1
  warning: 4
  info: 26
  total: 31
  necessity: 1
status: issues_found
---

# Phase 3: Code Review Report (second pass)

**Reviewed:** 2026-11-10T14:00:00Z
**Depth:** standard
**Files Reviewed:** 28
**Status:** issues_found

## Summary

This is a re-review after 24 fix commits (`4641016..HEAD`). I read the fixed code as it is now, traced the logic-heavy fixes through their callers (including the openraft 0.10.0-alpha.38 sources for the replication batch, snapshot builder and install paths), and re-read the code behind every carried-forward Info finding. Nothing was run; no source file was modified. The structural-findings (fallow) and external-reviewer blocks were not supplied, so those sections are omitted.

**Fixes verified as resolved (no remaining finding):** CR-02 (oversize entries are now rejected whole before any state changes, and an unacked drop fail-stops the writer; the `index == next_index` check the fixer left out is no longer needed because the gap can no longer be produced), CR-03 (but see WR-02 for a regression), WR-01 (watermark re-clamped after `write_run`), WR-02 (`on_purge` before the unlinks), WR-03 (images read in full, never shrunk), WR-04 (`validate_log_start` on the final state), WR-05 (install takes `build_lock`; `get_snapshot_builder` is not used by openraft 0.10, so the lazy-lock builder path cannot publish a stale build after an install), WR-06, WR-08, WR-09, WR-10 (connection and receive caps; see IN-21 for what is left), WR-11 (keepalive, write timeout, heartbeat idle bound; keepalive covers the other lanes), WR-12 (writer drains, bounded), WR-13 (client redials), WR-14 (the install bound is effective: openraft does not wrap `full_snapshot` in its own timeout), WR-15, and the test and bench fixes WR-16..WR-21.

**New or remaining defects:**
- CR-01 below: nothing keeps an AppendEntries batch under the transport's 32 MiB frame cap, so a few large entries (or 300 moderately large ones) make replication to a follower fail forever. The CR-02 limit (about 16 MiB per entry) does not account for it.
- WR-01: the CR-01 recovery fix turns a legitimate crash (non-prefix tear, or a client value that embeds a WAL record) into a node that refuses to start.
- WR-02: the CR-03 fix runs its wildcard check on restarts and on standalone nodes, where nothing is published. This is a regression.
- WR-03: the WR-07 warning tells the operator to "stop now" while the process initializes the cluster immediately.
- WR-04: a corrupt or mismatched received snapshot is a fatal storage error for the receiving node.

**Prior Info findings IN-01..IN-20:** none were dropped. I re-read each against the current code and all still hold, with updated file and line references (carried as IN-01..IN-20 in the same order). IN-19 and IN-20 keep their sub-bullets; the line numbers moved. IN-21..IN-26 are new.

**Regressions introduced by fix commits:** WR-01 (commit dad0a11, CR-01 fix) and WR-02 (commit 48b2983, CR-03 fix). WR-03 is an incomplete fix (aff0012), not a regression.

## Critical Issues

### CR-01: AppendEntries batches are not bounded by the frame cap, so replication of large entries wedges permanently

**File:** `src/net/client.rs:751-760` (the `append_writer` check), `src/net/client.rs:115` (`max_frame_len` = 32 MiB), `src/raft/config.rs:144` (`max_payload_entries` = 300), `src/raft/node.rs:126,214-221`, `src/wal/store.rs:86-91`
**Issue:** The sender refuses any AppendEntries request whose encoded body exceeds `lane_frame_cap` (32 MiB):

```rust
if HEADER_LEN + body.len() > frame_cap {
    ... pending.send(Pending::Failed(network_err(too_large))); return;
}
```

The unary path fails the same way through the frame encoder. Nothing bounds the request below that cap. openraft batches by entry count only (`max_payload_entries` = 300, `max_append_entries` = 4096; there is no byte limit in `replication/`). The per-entry limit introduced by the CR-02 fix is `MAX_RECORD_LEN - 9`, about 16 MiB. The request the leader builds for a lagging or reconnecting follower is the next run of entries starting at that follower's match index. When that run holds more than 32 MiB, the send fails with `TooLarge`, the stream yields an error, openraft backs off and rebuilds the same request from the same match index, and it fails again. The follower can never pass that point.

Reachable with accepted inputs:
- Two 16 MiB values already in the log (the limit explicitly accepts each).
- Three 11 MiB values.
- About 300 pending entries of 110 KiB or more, which a follower that was down or partitioned for a short time accumulates.

With two followers wedged in a 3-node cluster, commits stop. `max_frame_len` has no CLI flag, so the operator cannot lift the cap without a code change. The CR-02 fix made 16 MiB entries "valid" without checking that the transport can carry them.

**Fix:** Make the sizes consistent and enforce it. Either lower the limits so a full batch always fits, or cap the batch by bytes:
```rust
// raft/config.rs, in NodeConfig::validate (and pass max_frame_len in):
// worst case: max_payload_entries entries of the largest accepted size, plus envelope
let worst = self.max_payload_entries.saturating_mul(max_entry_bytes(&self.wal) as u64 + 64);
if worst > max_frame_len as u64 { return Err(ConfigError::BatchExceedsFrameCap { worst, max_frame_len }); }
```
With the default 300-entry batch that forces a per-entry cap of roughly 100 KiB, so the practical choice is a smaller `max_payload_entries` (for example 16) together with a lower `RaftNode::write` limit, or raising `max_frame_len` to cover 300 x the entry limit. Also stop retrying forever: when `append_writer` sees `TooLarge`, log it at `error` with the offending size so the wedge is visible. Add a transport or cluster test that replicates three 12 MiB entries to a follower that starts behind.

## Warnings

### WR-01: The CR-01 recovery fix makes a legitimate crash unrecoverable (non-prefix tear, or a value that embeds a WAL record)

**File:** `src/wal/recovery.rs:87-90, 170-179, 282-289`
**Issue:** `valid_record_follows` treats any CRC-valid record anywhere after the first invalid byte as proof of media corruption and returns `WalError::Corrupt`; recovery then refuses to start and leaves the file untouched. The premise is that "a torn write damages only a suffix". That is not what an OS gives you on power loss: unsynced pages can reach the disk in any order. Two ways to hit it:

1. **Out-of-order persistence.** The group-commit writer issues one `write_at` for a whole batch (`writer.rs:566`). For a batch of many small records spanning several pages, a power loss can persist page 2 but not page 1. Page 1 reads back as zero, so the scan sees `Decoded::End` at `off`, finds the whole records in page 2 behind it, and returns `Corrupt`. None of those records were acknowledged, so before the fix recovery handled this correctly. The same applies to a freshly rolled segment (`head_is_unwritten`, line 284): preamble and first batch are separate writes, and a lost preamble page with a persisted batch now reports "damaged preamble with valid records behind it" instead of discarding the unacknowledged segment. `FakeIo::power_loss` (`wal/io.rs:262`) only models "keep the first N differing bytes", so no test covers this.
2. **Client-controlled bytes.** `valid_record_follows` tries every byte offset, including offsets inside the payload of the invalid (torn) record itself. A value that contains a CRC32C-valid encoded WAL record (trivial to craft: the CRC is a public algorithm, and the postcard encoding stores byte values raw) makes any prefix tear of that entry that includes the embedded record read as corruption. A client who can write values can therefore turn an ordinary crash into a node that will not restart. The same holds for a legitimate value that is itself a WAL segment image, such as a backup.

The fixer flagged the first case as a trade-off, but the result has no operator escape: there is no repair mode, and the error text does not say what to do.

**Fix:** Make "corruption" depend on something torn writes and client data cannot forge.
- Salt the record CRC with a per-segment random value stored in the preamble (not exposed to clients). Only records carrying the segment's salt count as "valid" in `valid_record_follows`, which removes case 2.
- For case 1, record a durable synced high-water offset (for example in the `Committed` or a new `Synced` record, written after each fsync) and treat an invalid region as corruption only when it lies below that offset. Anything above it is by definition unacknowledged and safe to discard even if later pages persisted.
- Add a `FakeIo` power-loss mode that keeps whole later pages and drops earlier ones, plus tests for both cases.
- Until that exists, give the `Corrupt` error from `scan` and `head_is_unwritten` actionable text and an explicit operator recovery option instead of an unrecoverable start.

### WR-02: The CR-03 wildcard check runs on restarts and on standalone nodes where no address is published (regression)

**File:** `src/main.rs:203-216`, `src/raft/node.rs:192-207`
**Issue:** `advertised_addr` is evaluated whenever `bootstrap` is true, but its result is only used when `bootstrap_if_needed` actually calls `initialize`. `bootstrap_if_needed` returns `Ok(false)` early when the cluster is already initialized. Consequences:
- A bootstrap node that was started earlier with `--bind 0.0.0.0:7443 --bootstrap` (the README says "restarting it later never re-initializes") now fails to start after an upgrade with "cannot advertise the wildcard address", even though nothing would be published.
- A standalone node (`bootstrap = config.bootstrap || peers.is_empty()`) with `--bind 0.0.0.0:PORT` and no peers also fails, although a one-member cluster has no peer that needs a routable address.
- The error occurs after `RaftNode::start` and `PeerServer::bind`, so the process has already opened the WAL and the port.

**Fix:** Check only on the initialization path. Move the check into the branch that runs `initialize` (for example pass a closure to `bootstrap_if_needed`, or test `node.raft().is_initialized()` first), and skip it for a standalone cluster whose only member is this node. State in the README that `--advertise` is needed only when a new multi-node cluster is bootstrapped from a wildcard bind.

### WR-03: The WR-07 warning tells the operator to "stop now" but the node initializes the cluster immediately

**File:** `src/main.rs:155-167, 217-222`
**Issue:** With an empty `--peers` and no `--bootstrap`, the binary prints "If this node is meant to join an existing cluster, stop now and pass --peers", then continues straight into `bootstrap_if_needed`, which writes a membership entry at index 1 and elects itself leader within one election timeout. The operator has no time to act, and the initialization is irreversible without wiping the data directory (the original WR-07 problem). The fix made the accident louder, not avoidable. `Ok(true) if config.peers.is_empty()` also tests the raw string, so a whitespace-only value that `parse_peers` accepts as empty prints "initialized a new cluster" instead of the standalone message.

**Fix:** Require the intent. Either add an explicit `--standalone` flag (and update `cli_smoke` and the README quickstart), or refuse an implicit standalone bootstrap when the data directory is non-empty or the environment looks like a join (for example `--bind` is not loopback). If the current CLI contract must stay, reword the message so it states what is already happening ("initializing a standalone cluster") and test `peers.is_empty()` instead of `config.peers.is_empty()`.

### WR-04: A corrupt or mismatched received snapshot is a fatal storage error for the receiving node

**File:** `src/raft/state_machine.rs:324-341`, `src/net/handler.rs:101-109`, `src/net/server.rs:866-877`
**Issue:** `install_snapshot` validates the received file (`load_snapshot`, `header != expected`, `install_received`) and maps any failure through `snapshot_io_error` into an `io::Error`. openraft treats a storage error from the state machine as fatal and stops the Raft core (the same premise as the prior WR-05). The listener is unauthenticated (D-08), and `cluster_id` defaults to 1. A peer that presents a vote at or above the follower's current vote and streams a garbage or mismatched snapshot therefore halts the follower. The T-03-31 comment ("a corrupt or mismatched file can never replace live state") is true for integrity but not for availability. A bad file from a faulty leader has the same effect.

**Fix:** Validate in the transport before handing the file to openraft. In `run_snapshot_lane`, after `SNAP_END` call a cheap `validate_file` plus a header/meta check on a blocking thread; on failure answer with a non-fatal `RemoteFailure` and drop the receive instead of calling `handler.install_snapshot`. Keep the full validation inside `install_snapshot` as defense in depth. Add a transport test that sends a corrupt file and asserts the node keeps running.

## Info

### IN-01: Dead error variants and a stale doc comment

**File:** `src/wal/segment.rs:224-236`, `src/wal/record.rs:73-78`
**Issue:** `WalError::Failed`, `WalError::Closed` and `WalError::RecordTooLarge` are never constructed in `src/`. `encode_record`'s doc says the writer rejects oversize records "with `WalError::RecordTooLarge`", but the writer fails the ack with `io::ErrorKind::InvalidInput` or, for an unacked record, fail-stops (`writer.rs:468-482`). `encode_record` still does an unchecked `(1 + payload.len()) as u32`; it is only safe because of the caller contract.
**Fix:** Delete the unused variants (or use `RecordTooLarge` at the real rejection site in `WalLogStore::append`), fix the doc, and add `debug_assert!(1 + payload.len() <= MAX_RECORD_LEN as usize)` in `encode_record`.

### IN-02: `WalLogStore::append` can return `Err` without completing the openraft callback

**File:** `src/wal/store.rs:387-397`
**Issue:** The module header promises each flush callback is completed exactly once. If `self.submit` fails for a non-final entry (the writer failed mid-loop; `handle_mut` is re-checked on every `submit`), the function returns `Err` with the callback still in `callback: Option<IOFlushed>`, which is dropped uncompleted. Earlier entries of the batch were already submitted without acks, and the cache, `last_log_id` and `meta.next` already include them. This is harmless today because openraft treats an `Err` from `append` as fatal, but it relies on the caller rather than the stated invariant.
**Fix:** On the error path take the callback and call `fail_callback(cb, &e)` before returning.

### IN-03: Recovery hard-fails on a segment gap that a partially persisted purge can legitimately produce

**File:** `src/wal/recovery.rs:335-339`, `src/wal/writer.rs:664-670`
**Issue:** `run_purge` deletes victim segments one at a time and syncs the directory once at the end. On a filesystem that does not persist unlinks in issue order, a power loss mid-purge can leave segment 1 present and segment 2 gone. The purge marker is durable, so the state is recoverable, but `recover` returns `segment sequence has a gap` and the node cannot start. Ordered-journal filesystems make this unlikely.
**Fix:** Tolerate a gap when every segment before it is wholly covered by the purge point carried by the first segment after it, and delete the stragglers during recovery; or `sync_dir` after each deletion.

### IN-04: `Display for KvRequest` dumps keys and values into any log line that formats an entry

**File:** `src/raft/types.rs:46-50`
**Issue:** `write!(f, "{:?}", self)` prints the full `Bytes` of key and value. openraft formats entries and requests through `Display` in debug and trace logs and in error messages. Values (potentially secrets, up to about 16 MiB) would be copied into the log, with cost and disclosure proportional to value size. No concrete leak at the default log level.
**Fix:** Print only the variant, key length and value length (and a truncated key preview if wanted).

### IN-05: Declared minimum `bytes = "1.7"` is too low for `Bytes::from_owner`

**File:** `Cargo.toml:17`, `src/engine.rs:83`
**Issue:** `Bytes::from_owner` was added in bytes 1.9.0. The lockfile pins a newer version, so builds work, but the manifest lets a resolver (`-Z minimal-versions`, or a downstream crate consuming the lib) select 1.7 or 1.8, which do not compile.
**Fix:** `bytes = { version = "1.9", features = ["serde"] }`.

### IN-06: Leader watcher re-announces leadership every hour

**File:** `src/main.rs:233-258`
**Issue:** After the node becomes leader, the second `wait.metrics(.. != Leader ..)` times out after 3600 s while the node is still leader. The `Err(Timeout)` is treated as a non-event, so the loop restarts. `wait.state(Leader)` then returns immediately and prints "node N is leader (term T)" again. A stable leader logs this hourly, suggesting a leadership change that did not happen. Transitions that complete between two polls are also missed.
**Fix:** Use the metrics watch (`raft.metrics()` receiver `changed()`), track the previous state and term, and print only on a real transition.

### IN-07: `SnapshotStore::read_chunk` has no production caller

**File:** `src/snapshot/store.rs:286-293`
**Issue:** The only callers are this file's tests (`store.rs:528, 537`). The network client has its own `read_chunk` over an already-open `Arc<File>` (`src/net/client.rs:546`). Opening per chunk would be the wrong design for a long transfer anyway, since a concurrent build can delete the path mid-transfer. It is dead public API, and its existence invites that misuse.
**Fix:** Remove it, or mark it `#[cfg(test)]`.

### IN-08: Generality without instantiation, `NodeConfig.bind` is never read

**File:** `src/raft/config.rs:89-90`, `src/main.rs:171`
**Category:** necessity
**Issue:** `NodeConfig.bind` is assigned in `main.rs:171`, but no code reads it (`grep` finds no read in `src/raft` or `src/main.rs`). `main` binds the server from `config.bind` directly (`main.rs:189`), and `NodeConfig::validate` never checks it, so the field neither configures nor validates anything. It implies the node config owns the listen address, which is also why the advertise-address problem went unnoticed. Evidence it cannot matter: the only writer is `main.rs:171` and `RaftNode` takes the address from `PeerServer`, not from the config.
**Fix:** Either wire it up (validate that it parses as a `SocketAddr` in `validate()` and have `main` read it from the config) or remove the field.

### IN-09: `RaftNode::start` creates the WAL directory before the config is validated

**File:** `src/raft/node.rs:93-97, 110`
**Issue:** `StdFsIo::new(wal_dir)` creates `<data_dir>/wal` (mode 0700) before `start_with_io` calls `cfg.to_raft_config()?` (which runs `validate`). A config that is rejected (`node_id 0`, a self-referencing peer list, `--snapshot-every 0`) leaves directory side effects on disk.
**Fix:** Call `cfg.validate()?` at the top of `start` before constructing the WAL IO.

### IN-10: Server ignores the Hello `node_id`

**File:** `src/net/server.rs:229-238`
**Issue:** `serve_connection` uses only `hello.lane`. The announced `node_id` is neither validated (for example, that it is not the server's own id) nor attached to per-connection logs or stats. It would help diagnose a mis-wired cluster and reject self-dial or misrouted connections.
**Fix:** Reject `hello.node_id == cfg.node_id`, and add `peer_node = hello.node_id` to the tracing spans for the connection. If a membership view is available, optionally reject non-members.

### IN-11: Settings that must match across peers are not negotiated, so mismatches fail late and opaquely

**File:** `src/net/client.rs:60-135`; `src/wire/hello.rs`
**Issue:** `snapshot_ack_window` (documented as "must match, otherwise stall until the idle timeout"), `snapshot_chunk_size` (sets the receiver's frame cap) and `max_frame_len` are per-node config that only work if compatible with the peer. A sender whose chunk size or `max_frame_len` exceeds the receiver's cap gets its connection closed with a decode error and no explanation; a window mismatch stalls for 30 s. The settings added by the fix commits (`max_connections`, `heartbeat_idle_timeout`, `snapshot_install_timeout`) are local and do not add to this, but CR-01 shows `max_frame_len` is also coupled to `max_payload_entries` and the entry limit.
**Fix:** Carry these values in the Hello and fail the handshake with a clear reason, or validate them in `TcpNetConfig` and document them as cluster-wide settings.

### IN-12: Fake snapshot delivery does not remove the received copy, unlike the real server

**File:** `src/net/fake.rs:336-345`
**Issue:** The real server removes `recv-N.tmp` after the handler answers (`TmpGuard`, covers the stale-skip case). The fake copies the file into `recv_dir` and leaves it. Tests that exercise a skipped or stale snapshot cannot detect a leak through the fake, and the two transports diverge in observable file-system behaviour.
**Fix:** Delete `dest` after `install_snapshot` returns (Ok or Err), or document that the fake leaves files for inspection.

### IN-13: Connection task panics/failures are discarded and `shutdown` does not wait for connection cleanup

**File:** `src/net/server.rs:187`, `src/net/server.rs:129-132`
**Issue:** `Some(_) = conns.join_next()` drops the `JoinError`, so a panicking connection task produces no log line. `shutdown()` awaits only the accept loop. Dropping the `JoinSet` aborts connection tasks asynchronously, so `TmpGuard` drops and in-flight `spawn_blocking` writes may still run after `shutdown().await` returns. A test or restart that immediately inspects or recreates `recv_dir` can see leftover `recv-N.tmp` files.
**Fix:** Log `Err(e)` from `join_next()` at `warn`. In `accept_loop`, after the cancel break, call `while conns.join_next().await.is_some() {}` (with a timeout) before returning.

### IN-14: `check-get-regression.sh` no-match case exits silently; the diagnostic branch is dead

**File:** `scripts/check-get-regression.sh:24-30, 38-44`
**Issue:** Under `set -euo pipefail`, `extract_p99` returns 1 when `grep -F` finds no `[tail] GET ...` line, because grep's status propagates through pipefail. The assignment `p99_1t="$(extract_p99 ...)"` then terminates the script via `set -e` before the `[[ -z ... ]]` check that would print the bench output and the "could not parse" message. The gate still fails closed, but a changed bench label gives an exit code with no explanation.
**Fix:** Append `|| true` to the pipeline inside `extract_p99`, or use `p99_1t="$(extract_p99 ...)" || true`, so the existing diagnostic runs.

### IN-15: `check-get-regression.sh` thresholds are a single-machine absolute baseline

**File:** `scripts/check-get-regression.sh:7-10, 18-20`
**Issue:** The 334 ns and 667 ns limits come from one development machine, and the header notes a timer granularity of about 41.7 ns (Apple 24 MHz counter). On a Linux host with a roughly 1 ns clock, or a slower or throttled machine, the limits mean something different or fail spuriously. The best-of-3 policy hides intermittently slow regressions. Both choices are documented, so this is a portability caveat.
**Fix:** Allow a recorded per-host baseline file (or env override), report which attempt passed, and note the machine class in the script header.

### IN-16: `check-warnings.sh` continues after a failed `cd`

**File:** `scripts/check-warnings.sh:7, 9`
**Issue:** The script uses `set -u` without `-e` (intentional, since `$status` is checked) but does not guard `cd "$(dirname "$0")/.."`. If the `cd` fails, `cargo check` runs in the caller's directory, and the result is arbitrary (exit 0 for an unrelated clean crate).
**Fix:** `cd "$(dirname "$0")/.." || exit 2`.

### IN-17: WAL bench report runs unconditionally, ignoring Criterion filters and `--test`

**File:** `benches/wal.rs:160-161`
**Issue:** `bench_wal` calls `append_ack_report()` before any Criterion filter or test-mode processing. `cargo bench --bench wal -- fakeio` and `cargo test --benches` / `--all-targets` each run all three scenarios (200 000 FakeIo ops plus 5 000 and 300 real `fsync` ops; at the roughly 4 ms `F_FULLFSYNC` figure quoted in the header, that is more than 20 s on macOS). `LC_WAL_BENCH_OPS` exists but nothing steers `--test` runs toward it.
**Fix:** Skip the report when running in test mode (`std::env::args().any(|a| a == "--test")`) or gate it behind `LC_WAL_BENCH_TAIL=1`; alternatively document the cost in the header.

### IN-18: Bench statistics are not trustworthy at the small op counts

**File:** `benches/wal.rs:26-45, 88-97, 168-176`
**Issue:** The 300-op single-in-flight scenario prints `p99.9`, which equals `max` at n=300, and `p99` is the third-largest sample. No warm-up samples are discarded (first segment creation, thread start and page faults land in the percentile data). The timestamp (`Instant::now()` at line 88) is taken before the payload `vec!` and the ack `Box` allocation, so allocator cost is counted as append-to-ack latency in the "pure CPU" FakeIo scenario. In `fakeio_round_trip` the `mpsc::channel()` and payload allocation sit inside `b.iter`.
**Fix:** Drop the first ~1% of samples, suppress `p99.9` when `n < 1000`, and build the payload and ack channel outside the timed region (take `Instant::now()` immediately before `try_submit`).

### IN-19: Several tests make claims their assertions only partly support

**File:** `tests/cluster.rs:596, 654, 501`; `tests/tcp_smoke.rs:266-272, 374-378`; `tests/transport.rs:1787-1793`
**Issue:**
- `kill_restart_snapshot_catchup` calls `cluster.kill()` (graceful shutdown) on one follower and partitions the other. No node is crash-killed, so the name overstates it. `stats.snapshot.delivered >= 1` is cluster-wide rather than for the lagging node. The engine-swap assertion is the real proof and is sufficient.
- In `three_nodes_over_tcp_elect_and_replicate`, "follower received AppendEntries over TCP" is satisfied by heartbeat-lane frames alone, since `run_lane` counts `append_req_frames` for both lanes. Replication itself is proven by `holds_all`, so this is redundant rather than wrong.
- The recv-dir watcher in `tcp_snapshot_lane_install` is explicitly "best effort"; with no sightings the loop asserts nothing.
- `membership_remove_voter` proves "the removed node sees no more writes" with a fixed 500 ms sleep and a negative check; only a false pass is possible.
- `snapshot_idle_timeout_is_per_chunk_not_total` says "four acknowledgements"; with 8 chunks and window 2 the fake receiver sends no ack for the final window (`transport.rs:1728-1731`), so three delayed acks (about 750 ms) occur. The assertion `elapsed > idle` is still sound.
**Fix:** Rename or reword the comments. Where cheap, tighten: assert `stats.snapshot.delivered` per target if the fake network exposes it, and count only `Lane::Replication` frames for the replication claim.

### IN-20: Minor timing and diagnosability risks in test helpers

**File:** `tests/common/mod.rs:332-341, 485-501`; `tests/cli_smoke.rs:32, 63`; `tests/transport.rs:492-504`
**Issue:**
- `crash_with_unsynced_tail` detects `unsynced > keep` with a poll, then calls `crash`. The writer's delayed fsync holds for 250 ms, so the window is fine in practice, but a stall above about 240 ms of the test task between the check and `crash` would leave zero unsynced bytes. The tests that assert `torn_bytes_discarded > 0` (the follower check and the leader retry loop in `kill_restart_keeps_committed_writes`) would then fail or burn their retries spuriously.
- `MembershipLog::watch` samples a coalescing watch channel, so a positive `saw_joint` assertion could miss a brief joint config if no task is scheduled between the joint and final entries. Observing it on any of five nodes makes this unlikely.
- `cli_smoke` sends the child's stderr to `/dev/null`, so a startup failure only produces "no leader line within 15 s" with no cause. If the child exits early, the closed channel gives the same misleading message.
- `connect_refused_is_unreachable` frees a port and assumes nothing rebinds it before the dial.
**Fix:** Capture stderr to a file or pipe and include it in the panic message, and distinguish `Disconnected` from `Timeout` in `cli_smoke`. The other points are acceptable as is, or can be retried once on failure.

### IN-21: Snapshot receive hardening is only partly done (no free-space check; two connections can hold both slots)

**File:** `src/net/server.rs:731-748`, `src/net/client.rs:84-95`
**Issue:** The WR-10 fix bounds concurrent receives at `max_snapshot_receives` (2), each up to `max_snapshot_bytes` (8 GiB), but still never checks free space, and the file is written before the handler sees the vote. An unauthenticated peer with the (default, guessable) cluster id can still claim up to 16 GiB of the volume shared with the WAL. The same two slots can be held by two trickling connections (a chunk every 29 s satisfies the idle bound), which starves the real leader's snapshot with a non-fatal "too many concurrent snapshot receives".
**Fix:** Check available space against `total_len` before creating the file (or `fallocate` it so ENOSPC fails at `SNAP_BEGIN`), and consider a per-source-IP slot so one address cannot hold both. Reject a `SNAP_BEGIN` whose vote is already stale before writing any bytes.

### IN-22: `estimate_size` duplicates `KvRequest::payload_len`

**File:** `src/wal/store.rs:68-80`, `src/raft/types.rs:31-43`
**Issue:** The CR-02 fix added `payload_len`, which computes the same key/value byte count that `estimate_size` already computes with a copy of the same `match`. The two will drift if a `KvRequest` variant is added.
**Fix:** Have `estimate_size` call `req.payload_len()` for `EntryPayload::Normal` and add `ENTRY_OVERHEAD_BYTES`.

### IN-23: A wrong lane in the HelloAck is reported as "unexpected frame", and only when a node id was expected

**File:** `src/wire/hello.rs:265-276`
**Issue:** `client_handshake_expecting` rejects a server Hello whose lane differs from the requested one with `HandshakeError::UnexpectedFrame`, which prints "unexpected frame during handshake" and hides the cause. The lane check is also nested under `expected_node_id.is_some()`, so the plain `client_handshake` entry point skips it. The handshake unit test covers only the node mismatch.
**Fix:** Add a `HandshakeError::LaneMismatch { theirs, ours }` variant, check the lane independently of the node-id expectation, and extend `client_rejects_a_server_that_is_not_the_expected_node` with a lane case.

### IN-24: `--advertise` accepts an unbracketed IPv6 address and publishes a malformed `host:port`

**File:** `src/main.rs:115-121`
**Issue:** For input that does not parse as a `SocketAddr`, `rsplit_once(':')` splits an unbracketed address such as `::1:7443` into host `::1` and port `7443`, so `advertise_addr` returns `::1:7443`. That string is ambiguous and not dialable, and it goes into the replicated membership. There is no test for it. The README line "required when binding a wildcard" is also broader than the code intends (see WR-02).
**Fix:** Reject a host that contains `:` unless it is bracketed (`host.contains(':')` is an error), and test `::1:7443`.

### IN-25: The entry-size limit is per node, so a follower with a smaller segment size goes fatal on an entry the leader accepted

**File:** `src/wal/store.rs:86-91, 348-367`, `src/raft/node.rs:126,214-221`
**Issue:** `max_entry_bytes` depends on the local `segment_size` and the limit is only enforced at `RaftNode::write` on the node that proposes and in `WalLogStore::append` on every node. A follower configured with a smaller `segment_size` than the leader (library config only today; there is no CLI flag) receives an entry its WAL rejects. `append` returns `Err`, which openraft treats as fatal for that follower, and it will fail on restart too, because the entry is replayed from the leader. This is the same class as CR-01: a cluster-wide limit has no cluster-wide definition.
**Fix:** Treat `segment_size` (or at least the derived entry limit) as a cluster-wide setting: carry it in the Hello and refuse peers that disagree, or document and validate it. Reject the entry at the leader against the minimum across members.

### IN-26: Test gaps left by the fix commits

**File:** `src/net/server.rs:245-262`, `tests/cluster.rs:315-318`, `tests/transport.rs` (snapshot starvation test)
**Issue:**
- `writer_task`'s `write_timeout` branch has no test (the fix report says so). A regression that dropped the `timeout` would still pass.
- In `partition_heals_and_minority_converges` the isolated write's outcome is asserted only `if isolated_write.is_finished()`; when the task is still running the assertion silently does not run. The follow-up assertions (key absent, logs converge) still hold the test together, but the "never returns Ok" claim is conditional.
- `snapshot_does_not_starve_heartbeat` now requires at least one heartbeat answered while `0 < snapshot_chunks < 32`. If the loopback transfer of the 32 MiB file completes between two 10 ms-spaced heartbeats on a fast machine, `during_chunks` stays 0 and the test fails without any starvation bug.
**Fix:** Add a test with a peer that handshakes and never reads while the server queues frames, asserting the connection closes within `write_timeout`. In the partition test, abort the task and assert on `JoinError::is_cancelled` or await it after healing. For the starvation test, slow the chunk phase (a small chunk size with a per-chunk delay in the stub) so the overlap window is wide.

---

_Reviewed: 2026-11-10T14:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
