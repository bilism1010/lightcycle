---
phase: 03-consensus-persistence-engine
fixed_at: 2026-11-10T00:00:00Z
review_path: .planning/phases/03-consensus-persistence-engine/03-REVIEW.md
iteration: 1
findings_in_scope: 24
fixed: 24
skipped: 0
status: all_fixed
---

# Phase 3: Code Review Fix Report

**Fixed at:** 2026-11-10
**Source review:** .planning/phases/03-consensus-persistence-engine/03-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 24 (CR-01..CR-03, WR-01..WR-21; Info findings are out of scope for `critical_warning`)
- Fixed: 24 (several only in part, see the "Partial" notes; none skipped)
- Skipped: 0

**Verification ran in:** an isolated git worktree (`.claude/worktrees/rf-03-...`), with a shared cargo target
directory outside the repo. Per fix: `cargo test --features test-util` for the touched area. At the end of the
run, in the same worktree: full `cargo test --offline --features test-util` (all suites green: 243 lib, 6 bin,
1 cli_smoke, 14 cluster, 2 openraft_api_gate, 2 tcp_smoke, 46 transport, 1 wal_test_util_reachable) and
`bash scripts/check-warnings.sh` (exit 0, no new diagnostics). This is a worktree-environment run; it is
reproducible from the main checkout once the commits are fast-forwarded onto `main`.

Status legend: **fixed** = fix applied and verified by tests; **fixed: requires human verification** = a logic
or protocol change whose semantics (not just syntax) should be confirmed by a person.

## Fixed Issues

### CR-01: Corruption in the last segment is treated as a torn tail
**Status:** fixed: requires human verification
**Files modified:** `src/wal/recovery.rs`
**Commit:** dad0a11
**Applied fix:** `valid_record_follows` scans the rest of the last segment for any CRC-valid record. An invalid
record or zero header with a valid record behind it is now `WalError::Corrupt`, not a torn tail; the same check
guards `head_is_unwritten` (a damaged preamble with records behind it no longer deletes the segment). Tests: bit
flip mid-last-segment with synced records behind it (hard error, bytes untouched), the same flip in the final
record (still repaired as a tear), damaged preamble with records behind it. Trade-off to confirm: a filesystem
that persists unsynced pages out of order could leave a valid record behind a torn one; that now needs operator
attention instead of silently discarding data.

### CR-02: Oversized entries dropped silently, leaving an index gap
**Status:** fixed: requires human verification
**Files modified:** `src/wal/store.rs`, `src/wal/writer.rs`, `src/raft/node.rs`, `src/raft/types.rs`, `tests/cluster.rs`
**Commit:** 6e657cc
**Applied fix:** `WalLogStore::append` measures every entry (exact postcard size) against `max_entry_bytes(cfg)`
(record cap and segment size) before touching the cache, `last_log_id` or `meta`, and rejects the whole call with
the callback completed with `Err`. The writer now fail-stops when a record without an ack is dropped, so a gap can
never become durable. `RaftNode::write` rejects a command whose key/value payload exceeds the entry limit with the
new `NodeError::RequestTooLarge` before proposing it (an `Err` from the log store is fatal for openraft). Partial:
the writer does not check `index == next_index`; that rule needs a decision on fresh-log and post-truncate start
indexes the review does not settle. The 1 KiB envelope reserve and the segment-size reserve are my choices.

### CR-03: Wildcard `--bind` published into membership
**Status:** fixed
**Files modified:** `src/main.rs`, `README.md`
**Commit:** 48b2983
**Applied fix:** New `--advertise <host:port>`; the bootstrap node publishes it (port 0 takes the bound port). With
no `--advertise`, a wildcard bound address is refused with a clear error. Unit tests cover the cases.

### WR-01: Flushed watermark raised past the truncation point
**Status:** fixed: requires human verification
**Files modified:** `src/wal/writer.rs`
**Commit:** 21c4f2f
**Applied fix:** The clamp is re-applied after `write_run` in the `TruncateAfter` arm (`clamp_unreported`). A
regression test fails without the fix (reproduced first: flushed 10 above log end 6).

### WR-02: Purge deleted files before the index dropped the entries
**Status:** fixed: requires human verification
**Files modified:** `src/wal/writer.rs`
**Commit:** 8bd6625
**Applied fix:** `observer.on_purge` now runs after the marker is durable and before the segment deletions. A
test observes that the segments still exist at `on_purge` time.

### WR-03: Recovery capped reads at the configured segment size
**Status:** fixed: requires human verification
**Files modified:** `src/wal/recovery.rs`
**Commit:** 4ae41f2
**Applied fix:** `read_image` reads the whole file; torn-tail repair restores `max(file length, segment_size)`
instead of shrinking. Test reopens 4096-byte segments with a 2048-byte config. `WalHandle::spawn` already rejects a
tail beyond the configured size with a `Config` error, so no extra check was added.

### WR-04: Recovery never validated the start of the surviving log
**Status:** fixed: requires human verification
**Files modified:** `src/wal/recovery.rs`
**Commit:** add3edd
**Applied fix:** `validate_log_start` runs on the final scan state: with no purge point the first segment must be
1; with one, the first surviving entry must be `purged + 1`. Partial vs. the review: the suggested per-entry check
inside `Scanner::apply` was tried and removed, because it rejects a valid log (a segment preamble can carry an
older purge point than a later marker in the same segment; real cluster tests failed on it). Only the final-state
check is correct.

### WR-05: Snapshot store not serialized against build/install
**Status:** fixed: requires human verification
**Files modified:** `src/snapshot/store.rs`, `src/raft/state_machine.rs`
**Commit:** 1151d7b
**Applied fix:** `delete_snaps_where` tolerates `NotFound`; `latest()` re-lists (bounded) when the newest file
vanishes; `install_snapshot` takes `build_lock` until the engine swap. A test shows install waits for an in-flight
build.

### WR-06: Failed rename leaked the snapshot `.tmp`
**Status:** fixed
**Files modified:** `src/snapshot/store.rs`
**Commit:** 43d8196
**Applied fix:** The `.tmp` is removed when the publishing rename fails (hook "crash" paths keep it). The test now
asserts the file is gone.

### WR-07: Empty `--peers` silently starts a one-node cluster
**Status:** fixed: requires human verification (partial)
**Files modified:** `src/main.rs`
**Commit:** aff0012
**Applied fix:** The review's minimum option: a loud startup warning when no peers and no `--bootstrap` are given,
and the init message says "standalone single-node cluster". The stricter option (a required `--standalone` flag)
was not taken: it changes the documented CLI contract and the existing `cli_smoke` test and README quickstart, which
is a product decision.

### WR-08: Unsupported snapshot version reported as "wipe this node"
**Status:** fixed
**Files modified:** `src/snapshot/format.rs`, `src/snapshot/store.rs`, `src/raft/state_machine.rs`
**Commit:** c7a4374
**Applied fix:** `latest()` passes `UnsupportedVersion` through with a message that says not to wipe; a corrupt
snapshot body found by `KvStateMachine::open` now gets the same wipe guidance as header corruption. Not changed:
`SnapshotStore::open` still deletes older snapshots before validating the newest (the review notes the no-fallback
policy explains it).

### WR-09: Client never checked which node answered
**Status:** fixed: requires human verification
**Files modified:** `src/wire/hello.rs`, `src/wire/mod.rs`, `src/net/client.rs`, `tests/transport.rs`
**Commit:** a3b5310
**Applied fix:** New `client_handshake_expecting` and `HandshakeError::NodeMismatch`; `PeerClient::ensure_conn`
passes its target node id and the lane is checked. `client_handshake` keeps its signature. Unit and TCP tests added.

### WR-10: No bound on connections or concurrent snapshot receives
**Status:** fixed: requires human verification (partial)
**Files modified:** `src/net/client.rs`, `src/net/server.rs`, `tests/transport.rs`
**Commit:** a9bd42b
**Applied fix:** `max_connections` (default 512) is enforced with a semaphore in the accept loop (over-cap sockets
are closed); `max_snapshot_receives` (default 2) refuses an extra `SNAP_BEGIN` with a non-fatal error. Both defaults
are my choice. Not done: the free-space check before creating the receive file (needs a new dependency and a
platform-specific call) and a lower pre-app-frame cap.

### WR-11: No idle detection, keepalive or write timeout
**Status:** fixed: requires human verification (partial)
**Files modified:** `Cargo.toml`, `Cargo.lock`, `src/net/client.rs`, `src/net/server.rs`, `tests/transport.rs`
**Commit:** 1504483
**Applied fix:** TCP keepalive on dialed and accepted sockets (`keepalive_time`, default 30 s; `socket2` added as a
direct dependency, already in the lockfile), a per-frame `write_timeout` (30 s) in the server writer, and a
`heartbeat_idle_timeout` (60 s) on the heartbeat-lane reader. Not done: idle bounds on the replication and the
idle snapshot lanes (the review leaves their values open; keepalive covers them). The write timeout has no test.

### WR-12: Writer task aborted before the final frame was flushed
**Status:** fixed: requires human verification
**Files modified:** `src/net/server.rs`, `tests/transport.rs`
**Commit:** 5819988
**Applied fix:** `run_lane` now cancels the producers, drops its queue handles and lets the writer drain (bounded by
`ERROR_SEND_TIMEOUT`) before it is aborted; server shutdown skips the courtesy. The new test failed on the old code
(the Fatal reason was lost) and now passes 40 rounds on a multi-thread runtime.

### WR-13: Unary client reused a connection the server closed after Conflict/HigherVote
**Status:** fixed: requires human verification (partial)
**Files modified:** `src/net/client.rs`, `tests/transport.rs`
**Commit:** 581a9bd
**Applied fix:** `PeerClient::append_entries` drops the connection after any non-Accepted answer so the next call
redials on purpose. The stub handler learned to end its session after a Conflict, as openraft does; the test failed
before the fix. Not done: the server-side option (restart the `AppendSession` instead of closing the socket).

### WR-14: Snapshot install response used the per-chunk idle timeout
**Status:** fixed: requires human verification
**Files modified:** `src/net/client.rs`, `tests/transport.rs`
**Commit:** 9a05929
**Applied fix:** New `snapshot_install_timeout` (default 600 s) bounds the wait for `SNAP_RESP` after `SNAP_END`.
Test: a 900 ms install against a 300 ms idle bound succeeds. The default is my choice; the optional server
keep-alive ack was not added.

### WR-15: Vote/pre-vote failures reported as non-fatal
**Status:** fixed
**Files modified:** `src/net/server.rs`, `tests/transport.rs`
**Commit:** 411c924
**Applied fix:** `vote_failure` matches `RaftError::Fatal` explicitly and reports it fatal. The existing test
`handler_error_is_network_and_keeps_connection` pinned the old behaviour (it injected `Fatal::Stopped` and expected
`Network` plus a reused connection). It is replaced by `stopped_remote_on_vote_is_unreachable_and_the_next_call_redials`,
and the non-fatal client path it used to cover is kept by `non_fatal_remote_failure_is_network_and_keeps_connection`
(a hand-rolled peer that sends a non-fatal error frame).

### WR-16: Second-crash torn-tail assertion was vacuous
**Status:** fixed
**Files modified:** `tests/cluster.rs`
**Commit:** 2adca58
**Applied fix:** The leader's own `recovery_stats().torn_bytes_discarded` is asserted. Because 301 bytes can in
principle end exactly on a record boundary, the leader crash is retried up to 5 times rather than weakening the
assertion.

### WR-17: "Uncommitted write is discarded" had no evidence the entry was appended
**Status:** fixed
**Files modified:** `tests/cluster.rs`
**Commit:** fed4a26
**Applied fix:** The review's suggestion (assert on the existing flow) failed: the isolated old leader had already
stepped down and refused the write with `NotLeader`, so the discard path was never exercised. The test now sends
the write to the old leader immediately after isolating it, waits until its `last_log_index` advances, and after
healing also checks the old leader's log index converges to the new leader's.

### WR-18: Exact-count assertions after a fixed 300 ms sleep
**Status:** fixed
**Files modified:** `tests/transport.rs`
**Commit:** 168df90
**Applied fix:** Wait for the window to fill, then hold it 100 ms and assert the count did not exceed it.

### WR-19: Absolute wall-clock bounds in the heartbeat starvation test
**Status:** fixed
**Files modified:** `tests/transport.rs`
**Commit:** 70f0c90
**Applied fix:** Measures an idle baseline RTT first; the median must be under `max(20 x baseline, 100 ms)`, max
under 2 s, and at least one heartbeat must complete while snapshot chunks are still arriving (structural overlap
evidence). Deliberately not the review's `max < 1 s` alone: that is above the 300 ms install delay and would stop
detecting starvation behind the install.

### WR-20: WAL bench used `tempfile::tempdir()` (often tmpfs)
**Status:** fixed
**Files modified:** `benches/wal.rs`
**Commit:** 974363e
**Applied fix:** `LC_WAL_BENCH_DIR` selects the parent directory; the `[info]` line prints it. The optional
`statfs` tmpfs warning was not added.

### WR-21: Bench counted failed acks as latencies
**Status:** fixed
**Files modified:** `benches/wal.rs`
**Commit:** b15a566
**Applied fix:** `drive` asserts no ack failed, and the Criterion loop asserts `result.is_ok()`.

## Skipped Issues

None.

---

_Fixed: 2026-11-10_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
