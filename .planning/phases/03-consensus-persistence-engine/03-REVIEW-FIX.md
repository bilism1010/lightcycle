---
phase: 03-consensus-persistence-engine
fixed_at: 2026-11-10T12:39:59Z
review_path: .planning/phases/03-consensus-persistence-engine/03-REVIEW.md
iteration: 1
findings_in_scope: 31
fixed: 30
skipped: 1
status: partial
---

# Phase 3: Code Review Fix Report (second pass)

**Fixed at:** 2026-11-10T12:39:59Z
**Source review:** .planning/phases/03-consensus-persistence-engine/03-REVIEW.md
**Iteration:** 1

This report replaces the one from the first fix pass. Its finding IDs referred to the earlier review and do
not correspond to the IDs below; nothing was treated as already fixed because an ID matched.

**Summary:**
- Findings in scope: 31 (CR-01, WR-01..WR-04, IN-01..IN-26; `fix_scope: all`)
- Fixed: 30, of which 4 are only partial (WR-01, WR-03, IN-11, IN-25: each labelled below with the remaining gap)
- Skipped: 1 (IN-21)

**Verification ran in:** the isolated git worktree `.claude/worktrees/rf-03-51922-1794312934` (temporary branch
`gsd-reviewfix/03-51922`), sharing the main checkout's cargo target directory. Per fix: `cargo check
--all-targets --features test-util` plus the tests of the touched area. At the end of the run, in that worktree:
full `cargo test --features test-util` (252 lib, 8 bin, 1 cli_smoke, 15 cluster, 2 openraft_api_gate, 3 tcp_smoke,
47 transport, 1 wal_test_util_reachable; all green) and `bash scripts/check-warnings.sh` (exit 0). After the
commits were fast-forwarded onto `main` and the worktree removed, the same two gates were run again in the main
checkout with the same result, so the numbers are reproducible from the tree a reader has.

Status legend: **fixed** = applied and verified by tests or a direct check; **fixed: requires human
verification** = a logic change whose correctness a syntax check and the tests above cannot fully prove;
**partial** = some of what the finding asks for is done, and the remaining gap is stated.

Commit hashes are the ones now on `main` (the first pass's commits are untouched; WR-01 and WR-02 were fixed
forward, dad0a11 and 48b2983 were not reverted).

## Fixed Issues

### CR-01: AppendEntries batches are not bounded by the frame cap, so replication of large entries wedges permanently

**Status:** fixed: requires human verification
**Files modified:** `src/wal/store.rs`, `src/net/client.rs`
**Commit:** d3cfba5
**Applied fix:** openraft batches by entry count only, but it reads entries through
`RaftLogReader::limited_get_log_entries`, which may return fewer than asked (always at least one). `WalLogReader`
now overrides it with a 16 MiB byte budget (`REPLICATION_BATCH_BYTES`): it stops before an entry that would pass
the budget, so a request is at most the budget or one entry (under 16 MiB), well below the 32 MiB frame cap. This
removes the wedge without changing the accepted entry size, the batch count or the frame cap, and `try_get_log_entries`
(apply path, truncation checks) still returns everything. `append_writer` now logs at `error`, with the request size
and the cap, when a request still exceeds the frame cap. A store-level test writes three 12 MiB entries plus small
ones and checks each read is bounded; a client test checks the default cap is at least twice the budget.
**Not done / residual:** no cluster or transport test replicates three 12 MiB entries to a lagging follower
end to end (the bound is proven at the log-reader level). A deployment that lowers `max_frame_len` below about
16 MiB plus slack can still refuse a batch of large entries; `TcpNetConfig::validate` (see IN-11) now refuses that
configuration at startup in the binary.

### WR-01: The CR-01 recovery fix makes a legitimate crash unrecoverable (non-prefix tear, or a value that embeds a WAL record)

**Status:** partial
**Files modified:** `src/wal/recovery.rs`
**Commit:** 0901105
**Applied fix:** the two "valid records follow damage" errors (in `scan` and `head_is_unwritten`) now say that the
segment files were left untouched, that media corruption and an out-of-order power-loss persist look identical, and
what to do (restore from backup, or move the data directory aside and rejoin an empty node so the leader catches it
up; do not delete segments by hand). This is the last bullet of the review's fix.
**Remaining gap:** the real fix is not done. The per-segment CRC salt (removes the client-embedded-record case), the
durable synced high-water mark (removes the out-of-order page case) and the `FakeIo` mode that keeps later pages
while dropping earlier ones all change the on-disk format or the writer's durability protocol, and the choice of
how to treat bytes above the high-water mark has more than one defensible answer. A tear that persists later pages
without earlier ones, and a client value embedding a valid WAL record, still make the node refuse to start (with a
better message and no operator flag; adding a repair mode would change the CLI surface).

### WR-02: The CR-03 wildcard check runs on restarts and on standalone nodes where no address is published (regression)

**Status:** fixed: requires human verification
**Files modified:** `src/main.rs`, `README.md`
**Commit:** f528805
**Applied fix:** `main` asks the node whether the cluster is already initialized and passes the answer to a new
`published_addr(will_initialize, standalone, advertise, local)`. The wildcard check runs only when this start will
initialize a multi-node cluster; a restart, or a standalone node with no `--advertise`, publishes the bound address
without checking. An explicit `--advertise` is still validated and honoured when initializing. The `--advertise`
help text and the README flag description say it is needed only when a new multi-node cluster is bootstrapped from
a wildcard bind. A unit test covers each combination.

### WR-03: The WR-07 warning tells the operator to "stop now" but the node initializes the cluster immediately

**Status:** partial
**Files modified:** `src/main.rs`
**Commit:** bc2a738
**Applied fix:** the review's fallback for "the current CLI contract must stay": the warning now states what is
already happening (the node is starting as a standalone one-node cluster and will initialize it now if the data
directory holds no cluster state, which cannot be undone without wiping it) and no longer says "stop now". The
standalone/initialized messages test the parsed peer list, so a whitespace-only `--peers` is reported as standalone.
**Remaining gap:** the irreversible implicit initialization is still reachable by an empty `--peers`; requiring the
operator's intent (an explicit `--standalone` flag, a refusal on a non-empty data directory or non-loopback bind)
changes the documented CLI and `cli_smoke`/README quickstart and was left as a design decision.

### WR-04: A corrupt or mismatched received snapshot is a fatal storage error for the receiving node

**Status:** fixed: requires human verification
**Files modified:** `src/net/handler.rs`, `src/net/server.rs`, `tests/tcp_smoke.rs`
**Commit:** f202e90, with a test-helper follow-up fe3c297
**Applied fix:** `RaftHandler` gains `validate_snapshot` (default accepts, so stubs are unaffected). The `Raft`
implementation runs `validate_file` and the header-vs-announced-meta check on a blocking thread. `run_snapshot_lane`
calls it after `SNAP_END` and before `install_snapshot`; a refusal is answered with a non-fatal failure and the
receive file is dropped. `install_snapshot` keeps its own validation as defence in depth. A tcp_smoke test streams a
garbage file with a high vote to a running node and checks the node keeps taking writes (it fails with the old code
and shows "remote node stopped"). The follow-up keeps the deferred test handler from refusing a file that the
node's own recv-dir cleanup deleted during startup, which had made `tcp_snapshot_lane_install` intermittently
count a decode error.
**Note:** a valid snapshot is now validated three times (transport, load, install). The review asked for the cheap
`validate_file`; the cost grows with snapshot size.

### IN-01: Dead error variants and a stale doc comment

**Status:** fixed
**Files modified:** `src/wal/segment.rs`, `src/wal/record.rs`
**Commit:** 04dcfbb
**Applied fix:** removed `WalError::Failed`, `Closed` and `RecordTooLarge` (no construction site or test used them);
rewrote the `encode_record` doc to the real contract (the store rejects oversize entries with `InvalidInput`, the
writer fail-stops on an unacked one) and added a `debug_assert!` on the record length.

### IN-02: `WalLogStore::append` can return `Err` without completing the openraft callback

**Status:** fixed: requires human verification
**Files modified:** `src/wal/store.rs`
**Commit:** d737896
**Applied fix:** `append` re-checks the writer before every entry, before the callback moves into the final op's
ack, and on failure completes the still-held callback with the error before returning `Err`.
**Residual:** a writer failure in the instant between that check and the publish still drops the final op's ack
(`submit` cannot hand the op back). Openraft treats the returned `Err` as fatal either way; no test injects a
mid-batch writer failure.

### IN-03: Recovery hard-fails on a segment gap that a partially persisted purge can legitimately produce

**Status:** fixed: requires human verification
**Files modified:** `src/wal/recovery.rs`
**Commit:** 63a2a0f
**Applied fix:** the review's first option (tolerate the gap). Segments before the last sequence gap are set aside as
purged stragglers; the rest of the log is read and its start validated against the durable purge point, and only then
are the stragglers deleted and the directory synced. A gap the purge point does not explain (a lost middle segment)
fails validation and is still reported as `segment sequence has a gap`, deleting nothing. Syncing the directory after
each unlink (the second option) was tried and reverted: it made the existing test
`crash_between_purge_marker_and_deletions_keeps_marker_durable` fail, since that test models no unlink as durable
without a final sync, and changing it would have weakened it. Two new recovery tests cover the straggler case and the
unexplained-gap case.

### IN-04: `Display for KvRequest` dumps keys and values into any log line that formats an entry

**Status:** fixed
**Files modified:** `src/raft/types.rs`
**Commit:** b094d6b
**Applied fix:** `Display` prints the variant and key/value byte lengths only; a test checks the content never appears.

### IN-05: Declared minimum `bytes = "1.7"` is too low for `Bytes::from_owner`

**Status:** fixed
**Files modified:** `Cargo.toml`
**Commit:** 5a6550d
**Applied fix:** `bytes = { version = "1.9", features = ["serde"] }`; `Cargo.lock` already pins 1.12.1 and is unchanged
(`cargo check --locked` passes).

### IN-06: Leader watcher re-announces leadership every hour

**Status:** fixed: requires human verification
**Files modified:** `src/main.rs`
**Commit:** 91bf29f
**Applied fix:** the watcher follows the metrics watch (`changed()`), tracks the last announced term and prints once per
term in which this node becomes leader; it ends when the metrics sender drops. `cli_smoke` still sees the leader line.

### IN-07: `SnapshotStore::read_chunk` has no production caller

**Status:** fixed
**Files modified:** `src/snapshot/store.rs`
**Commit:** f2a2fac
**Applied fix:** `#[cfg(test)]`, with a doc note on why the client must not reopen the path per chunk; the now
unused top-level imports moved into the function.

### IN-08: Generality without instantiation, `NodeConfig.bind` is never read

**Status:** fixed: requires human verification (removes a public field)
**Files modified:** `src/raft/config.rs`, `src/main.rs`, `tests/tcp_smoke.rs`
**Commit:** d53bec7
**Applied fix:** removed the field (the review's second option) and the only two assignments; nothing read it. Wiring it
up would have rejected hostnames that `PeerServer::bind` accepts.

### IN-09: `RaftNode::start` creates the WAL directory before the config is validated

**Status:** fixed
**Files modified:** `src/raft/node.rs`, `tests/cluster.rs`
**Commit:** e554770
**Applied fix:** `start` calls `cfg.validate()?` first. A cluster test starts a node with a rejected config and checks
the data directory was never created.

### IN-10: Server ignores the Hello `node_id`

**Status:** fixed
**Files modified:** `src/wire/hello.rs`, `src/net/server.rs`
**Commit:** 2616b4c
**Applied fix:** `server_handshake` rejects a Hello carrying the server's own node id (new `HandshakeError::SelfDial`,
reason sent to the peer), and every connection's log lines run inside a span carrying `peer_node` and `lane`. The
optional non-member rejection was not done (the server has no membership view at that point).

### IN-11: Settings that must match across peers are not negotiated, so mismatches fail late and opaquely

**Status:** partial
**Files modified:** `src/net/client.rs`, `src/main.rs`
**Commit:** 28af43d
**Applied fix:** the review's second option. `max_frame_len`, `snapshot_chunk_size` and `snapshot_ack_window` are
documented as cluster-wide, and a new `TcpNetConfig::validate` checks what one node can check (the frame cap holds a
full replication batch, chunk size and window non-zero); `main` runs it before starting the node.
**Remaining gap:** a mismatch between two nodes is still not detected. Carrying the values in the Hello needs a wire
version bump and handshake policy, which was left as a design decision.

### IN-12: Fake snapshot delivery does not remove the received copy, unlike the real server

**Status:** fixed
**Files modified:** `src/net/fake.rs`
**Commit:** b2a847e
**Applied fix:** `deliver_snapshot` removes the received copy after the handler returns, whatever it returned.

### IN-13: Connection task panics/failures are discarded and `shutdown` does not wait for connection cleanup

**Status:** fixed: requires human verification
**Files modified:** `src/net/server.rs`, `tests/transport.rs`
**Commit:** 451e8ab
**Applied fix:** a failed (non-cancelled) connection task is logged at `warn`; after the cancel break the accept loop
waits up to 5 s for the connection tasks so their guards and in-flight blocking writes finish before `shutdown`
returns.
**Note:** the added transport test passes without the drain as well (aborting a task is usually quick), so it guards
the behaviour but does not prove the race.

### IN-14: `check-get-regression.sh` no-match case exits silently; the diagnostic branch is dead

**Status:** fixed
**Files modified:** `scripts/check-get-regression.sh`
**Commit:** 23d9a0c
**Applied fix:** `extract_p99` ends in `|| true`. Checked directly: with the same shell options the old function exited 1
silently on a no-match input; the new one reaches the `could not parse` branch.

### IN-15: `check-get-regression.sh` thresholds are a single-machine absolute baseline

**Status:** fixed
**Files modified:** `scripts/check-get-regression.sh`
**Commit:** 9220da5
**Applied fix:** the header names the machine class behind the 334/667 ns defaults; a per-host baseline file
(`scripts/get-regression.local`, already matched by `.gitignore`, or `GET_BASELINE_FILE`) can set the two limits,
with the environment variables taking precedence (precedence checked in isolation); the PASS line names the attempt
that passed. The best-of-N policy is kept and its trade-off is documented. The script itself was not run end to end
(it needs a release bench build).

### IN-16: `check-warnings.sh` continues after a failed `cd`

**Status:** fixed
**Files modified:** `scripts/check-warnings.sh`
**Commit:** 7fb23d8
**Applied fix:** `cd "$(dirname "$0")/.." || exit 2`.

### IN-17: WAL bench report runs unconditionally, ignoring Criterion filters and `--test`

**Status:** fixed
**Files modified:** `benches/wal.rs`
**Commit:** 3c06ef9
**Applied fix:** the report runs only for a plain `cargo bench` (`--bench` present; no `--test`, `--list` or filter),
with `LC_WAL_BENCH_TAIL=1`/`0` to force or suppress it; documented in the header. Checked: `cargo test --bench wal`
prints no report; `-- --bench` prints three `[tail]` lines. A flag value such as `--save-baseline name` looks like a
filter and also skips the report (documented).

### IN-18: Bench statistics are not trustworthy at the small op counts

**Status:** fixed
**Files modified:** `benches/wal.rs`
**Commit:** 0330dad
**Applied fix:** the first 1% of samples is dropped as warm-up; `p99.9` is printed only from 1000 samples; the start
stamp is taken right before the first submit attempt, after the payload and ack box exist; `fakeio_round_trip`
builds its op and channel in an untimed `iter_batched` setup.

### IN-19: Several tests make claims their assertions only partly support

**Status:** fixed
**Files modified:** `src/net/server.rs`, `tests/cluster.rs`, `tests/tcp_smoke.rs`, `tests/transport.rs`
**Commit:** 20db3ec
**Applied fix:** reworded the claims (no node is crash-killed in `kill_restart_snapshot_catchup`, which keeps its name
because plans and validation filters use it; the lane counter is cluster-wide; the recv-dir watcher can pass with no
sightings; the fixed sleep is a negative check; three delayed acks, not four). The replication claim in the TCP test
now counts only replication-lane frames, through a new per-lane `ServerStats` counter.
**Not done:** a per-target `snapshot.delivered` assertion; the fake network has no per-target statistics.

### IN-20: Minor timing and diagnosability risks in test helpers

**Status:** fixed
**Files modified:** `tests/cli_smoke.rs`
**Commit:** edfdb1c
**Applied fix:** the child's stderr goes to a file whose contents are appended to the panic message, and a closed stdout
is reported separately from a 15 s timeout. The other three bullets are called acceptable by the review and are unchanged.

### IN-22: `estimate_size` duplicates `KvRequest::payload_len`

**Status:** fixed
**Files modified:** `src/wal/store.rs`
**Commit:** 4e83faa
**Applied fix:** `estimate_size` calls `payload_len` for normal entries.

### IN-23: A wrong lane in the HelloAck is reported as "unexpected frame", and only when a node id was expected

**Status:** fixed
**Files modified:** `src/wire/hello.rs`
**Commit:** 412454c
**Applied fix:** new `HandshakeError::LaneMismatch { theirs, ours }`, checked for every client handshake independently of
the node-id expectation; a unit test covers both entry points.

### IN-24: `--advertise` accepts an unbracketed IPv6 address and publishes a malformed `host:port`

**Status:** fixed
**Files modified:** `src/main.rs`
**Commit:** 323412f
**Applied fix:** a host containing `:` must be bracketed; tests cover `::1:7443`, `fe80::1:7443` and the bracketed forms.

### IN-25: The entry-size limit is per node, so a follower with a smaller segment size goes fatal on an entry the leader accepted

**Status:** partial
**Files modified:** `src/wal/segment.rs`, `src/wal/store.rs`
**Commit:** 3d6088c
**Applied fix:** the review's "document" alternative: `WalConfig::segment_size` and `max_entry_bytes` now state that the
value is effectively cluster-wide and what happens when members differ.
**Remaining gap:** nothing checks it. Carrying `segment_size` in the Hello to refuse disagreeing peers, or rejecting at
the leader against the minimum across members, needs a wire change and was left as a design decision. There is no
validation step either, since the value is library-only (no CLI flag).

### IN-26: Test gaps left by the fix commits

**Status:** fixed
**Files modified:** `src/net/server.rs`, `tests/cluster.rs`, `tests/transport.rs`
**Commit:** 44e41c5
**Applied fix:** (1) a `writer_task` test with a peer that never reads (it fails when the `write_timeout` is replaced by an
hour, checked by mutation); (2) the partition test now aborts the isolated write and asserts it was never `Ok`
unconditionally after the heal; (3) `snapshot_does_not_starve_heartbeat` uses 64 KiB chunks acknowledged one at a time
and a 2 ms heartbeat spacing, which widens the chunk phase (5 consecutive passes). The stub still has no per-chunk
delay; the overlap window is wider, not guaranteed.

## Skipped Issues

### IN-21: Snapshot receive hardening is only partly done (no free-space check; two connections can hold both slots)

**File:** `src/net/server.rs:731-748`, `src/net/client.rs:84-95`
**Reason:** each of the three suggested mitigations needs a decision the review does not settle, so none was applied.
(a) The free-space check or `fallocate` needs a statvfs/fallocate call, which means a new dependency (`libc` or similar)
and a policy for what "enough space" is on a volume shared with the WAL. (b) A per-source-IP slot would stop a legitimate
leader and an overlapping old leader from receiving at once whenever peers share an address (loopback test clusters,
NAT, a proxy), so the right limit is a policy choice. (c) Rejecting a `SNAP_BEGIN` whose vote is already stale before
writing needs a new handler method and changes the response the stale sender sees (a refusal instead of the
`SnapshotResponse` carrying the higher vote).
**Original issue:** the WR-10 fix bounds concurrent receives at 2 but never checks free space, and two trickling
connections can hold both slots and starve the real leader's snapshot.

---

_Fixed: 2026-11-10T12:39:59Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
