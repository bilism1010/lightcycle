---
phase: 03-consensus-persistence-engine
verified: 2026-11-10T12:00:00Z
status: human_needed
score: 6/7 must-haves verified
covered_files:
  - ".planning/phases/03-consensus-persistence-engine/03-01-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-01-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-02-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-02-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-03-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-03-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-04-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-04-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-05-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-05-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-06-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-06-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-07-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-07-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-08-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-08-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-09-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-09-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-10-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-10-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-11-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-11-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-12-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-12-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-13-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-13-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-14-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-14-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-15-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-15-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-16-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-16-SUMMARY.md"
  - ".planning/phases/03-consensus-persistence-engine/03-17-PLAN.md"
  - ".planning/phases/03-consensus-persistence-engine/03-17-SUMMARY.md"
  - "Cargo.lock"
  - "Cargo.toml"
  - "README.md"
  - "benches/local_engine.rs"
  - "benches/wal.rs"
  - "scripts/cargo-check-baseline.txt"
  - "scripts/check-get-regression.sh"
  - "scripts/check-warnings.sh"
  - "src/arena/mod.rs"
  - "src/arena/slab.rs"
  - "src/engine.rs"
  - "src/ipc/mod.rs"
  - "src/ipc/ring_buffer.rs"
  - "src/lib.rs"
  - "src/main.rs"
  - "src/net/client.rs"
  - "src/net/fake.rs"
  - "src/net/handler.rs"
  - "src/net/mod.rs"
  - "src/net/protocol.rs"
  - "src/net/server.rs"
  - "src/raft/config.rs"
  - "src/raft/mod.rs"
  - "src/raft/node.rs"
  - "src/raft/state_machine.rs"
  - "src/raft/types.rs"
  - "src/snapshot/format.rs"
  - "src/snapshot/mod.rs"
  - "src/snapshot/reader.rs"
  - "src/snapshot/store.rs"
  - "src/snapshot/writer.rs"
  - "src/storage/index.rs"
  - "src/storage/mod.rs"
  - "src/wal/cache.rs"
  - "src/wal/io.rs"
  - "src/wal/mod.rs"
  - "src/wal/record.rs"
  - "src/wal/recovery.rs"
  - "src/wal/segment.rs"
  - "src/wal/store.rs"
  - "src/wal/writer.rs"
  - "src/wire/frame.rs"
  - "src/wire/hello.rs"
  - "src/wire/mod.rs"
  - "tests/cli_smoke.rs"
  - "tests/cluster.rs"
  - "tests/common/mod.rs"
  - "tests/openraft_api_gate.rs"
  - "tests/tcp_smoke.rs"
  - "tests/transport.rs"
  - "tests/wal_test_util_reachable.rs"
covered_digest: "v4:sha256:bd421500a409f29cb1f27e44de15f8e09c3b23030e388b3c580b86afe651e6ed"
behavior_unverified: 0
overrides_applied: 0
re_verification: none
gaps: []
deferred:
  - truth: "NFR-1 (P99 read < 150us, P99 write < 2ms on 3 nodes) and NFR-2 (>= 500k ops/s) on real hardware"
    addressed_in: "Phase 6"
    evidence: "ROADMAP Phase 6 milestones 6.1 (high-load benchmark harness) and 6.2 (P99/P99.9 tail-latency profiling); 03-VALIDATION.md Manual-Only row says the replicated-write P99 claim needs an end-to-end measurement in Phase 6 on a Linux NVMe host. Not claimed by Phase 3 (absent from every plan's requirements frontmatter)."
human_verification:
  - test: "Decide whether up to three TCP connections per peer (replication, heartbeat, snapshot lanes) is acceptable against decision D-07 (one persistent connection per peer)"
    expected: "Either accept interpretation A4 (one persistent pipelined replication connection plus dedicated heartbeat and snapshot connections), recording it as a CONTEXT.md amendment, or require a single multiplexed connection (priority writer, head-of-line risk), which means rework of src/net/client.rs, src/net/server.rs and the lane tests"
    why_human: "D-07 is a locked user decision and D-05 marks the wire format as costly to reverse. Interpretation A4 was written into 03-RESEARCH.md as an assumption and never marked RESOLVED by the user (only A1, the openraft alpha, was confirmed). The code does what A4 says; only the user can say whether it satisfies D-07."
  - test: "Decide the status of FR-2 (atomic CAS plus WATCH/multi-key conditional updates)"
    expected: "Either accept FR-2 as split (single-key CAS done in Phases 2-3, WATCH/multi-key explicitly unscheduled) and add a roadmap item for it, or require it before Phase 3 closes. Plans 03-02 and 03-05 record requirements-completed: FR-2, which overstates this."
    why_human: "Scope and traceability decision. Multi-key conditionals (assumption A12) need a consistent-cut snapshot design; no ROADMAP milestone in Phases 3-6 schedules them (Phase 5 only lists a CAS command)."
  - test: "Decide whether the binary should warn on, or refuse, non-private --bind addresses"
    expected: "Either accept README-only guidance, or add a warning / refusal for 0.0.0.0 and non-loopback binds. Note the functional hazard below."
    why_human: "Security-posture decision for a plaintext, unauthenticated port (D-08). Observed in src/main.rs: any --bind is accepted silently, and the bootstrap membership advertises the listener's local_addr, so --bind 0.0.0.0:PORT would publish the unroutable address 0.0.0.0:PORT as this node's membership address."
  - test: "Run cargo bench --bench wal and an end-to-end 3-node write benchmark on a Linux NVMe host"
    expected: "WAL fsync percentiles on NVMe, and P99 write < 2ms (3 nodes), >= 500k ops/s, as NFR-1 and NFR-2 require"
    why_human: "Needs hardware the dev host (macOS, F_FULLFSYNC p50 about 4ms) does not have. 03-VALIDATION.md keeps this row open; it is not a Phase 3 claim and is carried to Phase 6."
---

# Phase 3: Consensus & Persistence Engine Verification Report

**Phase Goal (derived):** ROADMAP.md carries no explicit Goal line or success-criteria list for Phase 3, so the goal was derived from the four roadmap milestones plus 03-CONTEXT.md (Phase Boundary and decisions D-01 to D-16): "Deliver the distributed tier's durability and replication core: a Raft consensus node (openraft) with a segmented, group-committed on-disk WAL, a custom Tokio TCP peer transport, a snapshot/restore engine over the Phase 2 LocalEngine, and static-bootstrap plus dynamic membership." Must-haves were then taken from the milestones, the 17 PLAN `must_haves` blocks and the locked decisions.
**Verified:** 2026-11-10
**Status:** human_needed
**Re-verification:** No, initial verification

## Goal Achievement

The engineering goal is met in the codebase. Nothing is a stub or unwired, and every automated check I ran is green. Status is `human_needed` rather than `passed` because one locked decision (D-07) is implemented under an unconfirmed interpretation, and two scope/safety questions (FR-2 overclaim, `--bind`) need a user ruling. No gaps (BLOCKERs) were found.

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Milestone 3.1: a Raft state machine and log-store abstraction exist on openraft, applying stamped SET/DEL/CAS into LocalEngine | VERIFIED | `Cargo.toml` pins `openraft = "=0.10.0-alpha.38"`; `src/raft/state_machine.rs` (985 lines) implements `RaftStateMachine` with `ApplySink` seam (R-3); `src/wal/store.rs` implements the log storage traits; `raft::state_machine` 22/22 tests pass, including `cas_counterexample`, `fuzzy_replay_model` (proptest) and `naive_replay_diverges_*` (negative controls) which resolve R-1 |
| 2 | Milestone 3.2: segmented, CRC32C WAL on `std::fs` behind a trait, group commit on a dedicated thread draining the Phase 2 ring, ack only after fsync, no timer batching, torn tail discarded on recovery (D-01..D-04, D-16) | VERIFIED | `src/wal/writer.rs` (1537 lines) uses `Consumer::consume_batch` over `RingBuffer`, `SegmentIo` seam with `StdFsIo`/`FakeIo`, fsync errors are fail-stop; `wal::writer` 20/20 and `wal::recovery` 22/22 pass (power-loss ordering, torn tail at every offset, bad CRC in non-last segment is a hard error); default `--fsync always`, `periodic`/`never` warn UNSAFE (D-04). Performance on NVMe is NOT verified (see NFR-1/2 deferral) |
| 3 | Milestone 3.3: static bootstrap, dynamic add/remove via joint consensus, heartbeat and replication transport over TCP with a shared codec | VERIFIED (D-07 caveat in truth 7) | `src/net/client.rs`, `src/net/server.rs`, `src/wire/{frame,hello}.rs`; wire has no `openraft` import; `tests/cluster.rs` has `membership_add_voter`, `membership_remove_voter`, `membership_replace_voters_joint`, `membership_remove_leader_new_leader_elected` (assertions inspect real membership state and `saw_joint`); `tests/tcp_smoke.rs` elects and replicates over real localhost TCP; `pipelined_in_order` passes (real pipelined `stream_append`, request and byte windows) |
| 4 | Milestone 3.4: epoch-pinned fuzzy snapshot, framed CRC file via the shared codec, trigger every N applied entries with log purge, chunked transfer on its own connection with backpressure that cannot starve heartbeats (D-09..D-12) | VERIFIED | `src/snapshot/{format,writer,store,reader}.rs` use `FrameCodec`; `scan_versioned` in `src/storage/index.rs` pins per entry, never across the scan; `SnapshotPolicy` trigger in `src/raft/config.rs`; `tcp_snapshot_lane_install` and `kill_restart_snapshot_catchup` pass; `snapshot_does_not_starve_heartbeat` (32 MiB in flight) passes; release `snapshot::concurrent_scan::concurrent_scan_restore_replay_equals_live` passes (re-run by me) |
| 5 | Crash-recovery bar (D-16, FR-6): kill-and-restart loses no committed write; torn final WAL record is detected and discarded; non-vacuous | VERIFIED | `tests/cluster.rs kill_restart_keeps_committed_writes` asserts `torn_bytes_discarded > 0` and identical engines; `single_node_restart`, `single_node_snapshot_purge_restart`; orchestrator baseline 13/13 cluster tests green |
| 6 | Phase 2 is not regressed by the stamped-cell change (NFR-3, NFR-4) | VERIFIED | `bash scripts/check-get-regression.sh` re-run: GET p99 250ns at 1 thread (limit 334), 500ns at 8 threads (limit 667); Phase 2 tests (storage, engine, ipc, arena) green in baseline; tombstone rule (R-2) covered by `tombstone_*` tests |
| 7 | D-07: "One persistent connection per peer with pipelined AppendEntries" | UNCERTAIN (needs user decision) | `new_client`, `new_heartbeat_client` and `new_snapshot_client` in `src/net/client.rs:136-146` each dial their own TCP connection tagged by `Lane` in the Hello; README line 135 states "up to three TCP connections" per peer. Pipelining itself is real. The deviation is by design (assumption A4 in 03-RESEARCH.md, justified by D-12), but A4 was never confirmed by the user |

**Score:** 6/7 truths verified (1 uncertain, 0 failed, 0 behavior-unverified)

### Deferred Items

| # | Item | Addressed In | Evidence |
|---|------|-------------|----------|
| 1 | NFR-1 and NFR-2 hardware validation (P99 < 2ms writes on 3 nodes, 500k ops/s) | Phase 6 | ROADMAP milestones 6.1, 6.2; 03-VALIDATION.md Manual-Only row; absent from all plan `requirements` frontmatter; README says "no latency claim is made yet" |

### Required Artifacts

All artifacts named in the 17 plans' `must_haves.artifacts` exist, are substantive (no `TODO`/`FIXME`/`XXX`/`todo!`/`unimplemented!` in `src/`; the only `unimplemented!` hits are in `tests/openraft_api_gate.rs`, which is a deliberate compile-time skeleton, not production code) and are wired into `RaftNode::start` and `src/main.rs`.

| Artifact | Status | Details |
|----------|--------|---------|
| `src/wal/{record,io,segment,writer,recovery,cache,store}.rs` | VERIFIED | 243 to 1537 lines each, driven by `WalLogStore` in `RaftNode::start_with_io`; `StdFsIo` used in production start |
| `src/wire/{frame,hello}.rs` | VERIFIED | Used by net, snapshot format and (Phase 5 reuse) |
| `src/net/{handler,fake,protocol,client,server}.rs` | VERIFIED | `PeerServer::bind` and `TcpNetworkFactory` wired in `src/main.rs` |
| `src/raft/{types,config,state_machine,node}.rs` | VERIFIED | `RaftNode::engine()` reloads the shared `ArcSwap` handle on each call |
| `src/snapshot/{format,writer,store,reader}.rs` | VERIFIED | Used by `KvStateMachine` build and install |
| `src/storage/index.rs`, `src/engine.rs` | VERIFIED | `put_at`/`delete_at`/`cas_at`/`scan_versioned`/`load_versioned` present and used by `InlineSink` |
| `scripts/check-warnings.sh`, `scripts/cargo-check-baseline.txt`, `scripts/check-get-regression.sh` | VERIFIED | Baseline file is empty (0 bytes), GET guard re-run passes |
| `benches/wal.rs`, `tests/{cluster,tcp_smoke,transport,cli_smoke,openraft_api_gate,wal_test_util_reachable}.rs`, `tests/common/mod.rs` | VERIFIED | Present; no `#[ignore]` anywhere |

### Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| `InlineSink` | `LocalEngine::{set_at,delete_at,cas_at}` | log index as stamp | WIRED |
| WAL writer thread | `ipc::ring_buffer::Consumer::consume_batch` | drains ring, one fsync per batch | WIRED |
| `SharedObserver` | `WalObserver` callbacks | ordered `on_flushed`, `on_truncate`, `on_purge` | WIRED |
| `PeerServer` | `RaftHandler` (`Arc<dyn RaftHandler>`) | per-lane dispatch | WIRED |
| `PeerClient` | `wire::hello::client_handshake` | lane-tagged Hello on every connection | WIRED |
| `RaftNode::engine_handle` | `KvStateMachine::install_snapshot` | the same `Arc<ArcSwap<LocalEngine>>`; `engine_handle_tracks_install_snapshot` and `engine_accessor_is_not_captured_at_start` pass | WIRED |
| `main.rs` | `RaftNode::start`, `PeerServer::bind`, `bootstrap_if_needed` | CLI flags to `NodeConfig` | WIRED |

### Data-Flow Trace (Level 4)

Client write goes `RaftNode::write` to `raft.client_write`, then to the WAL append (group-commit fsync), then quorum commit, then `KvStateMachine::apply` to `InlineSink` to `LocalEngine::*_at`, and a read through `RaftNode::engine()` returns the value; `three_node_election_and_replication`, `tcp_smoke` and `single_node_restart` assert this against real engines on real WAL directories. Snapshot data flows engine, then `scan_versioned`, then framed file, then chunked snapshot lane, then `install_snapshot` into a fresh engine, then ArcSwap swap; `tcp_snapshot_lane_install` and `kill_restart_snapshot_catchup` assert contents through fresh `engine()` calls and `Arc::ptr_eq` identity change. Status: FLOWING.

### Behavioral Spot-Checks (run by me, in addition to the orchestrator's full-suite baseline)

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| State machine and R-1 proof | `cargo test --features test-util --lib raft::state_machine` | 22 passed | PASS |
| WAL writer durability ordering and group commit | `cargo test --features test-util --lib wal::writer` | 20 passed | PASS |
| WAL recovery and torn tails | `cargo test --features test-util --lib wal::recovery` | 22 passed | PASS |
| Pipelined AppendEntries | `cargo test --features test-util --test transport pipelined_in_order` | 1 passed | PASS |
| Heartbeat isolation under 32 MiB snapshot | `cargo test --features test-util --test transport snapshot_does_not_starve_heartbeat` | 1 passed | PASS |
| Release concurrent-scan stress (NFR-4) | `cargo test --release --features test-util --lib snapshot::concurrent_scan` | 1 passed | PASS |
| Phase 2 GET tail latency | `bash scripts/check-get-regression.sh` | p99 250ns / 500ns, within 334 / 667 | PASS |
| Full suite and zero-warning gate | orchestrator baseline: 227 lib, 1 cli_smoke, 13 cluster, 2 api gate, 2 tcp_smoke, 38 transport, 1 wal util; `check-warnings.sh` exit 0 | not re-run | PASS (taken from orchestrator) |

### Probe Execution

No `probe-*.sh` scripts exist and no plan declares one. Step 7c: SKIPPED (none declared).

### Requirements Coverage

Authoritative set from PLAN frontmatter (union of all 17 plans): **FR-2, FR-3, FR-4, FR-6, NFR-3, NFR-4**. All six are defined in `.planning/REQUIREMENTS.md` (which has no phase-mapping table, so no ORPHANED requirement can be derived from it). NFR-1 and NFR-2 appear in no plan and are correctly not claimed.

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|--------------|-------------|--------|----------|
| FR-2 | 03-02, 03-05 | Atomic CAS and transaction primitives (WATCH / multi-key conditional) | PARTIAL, NEEDS HUMAN | Single-key CAS is replicated through Raft and replay-safe under fuzzy snapshots (stamp gate plus R-1 proptest). WATCH and multi-key conditionals are not implemented and not scheduled in any roadmap milestone (assumption A12, restated in 03-16-SUMMARY). 03-02 and 03-05 SUMMARYs record `requirements-completed: FR-2`, which overstates this |
| FR-3 | 03-01, 04, 05, 06, 09, 12, 13, 14, 15, 16, 17 | 3-node cluster, automatic election, term progression, log replication | SATISFIED | `three_node_election_and_replication`, `leader_failover_new_leader_elected`, `partition_heals_and_minority_converges`, `three_nodes_over_tcp_elect_and_replicate` |
| FR-4 | 03-01, 02, 03, 05, 07, 08, 10, 11, 12, 13, 14, 16 | Disk WAL with group commit, snapshot create/restore to prune the log | SATISFIED on the macOS dev host | WAL writer/recovery tests, `single_node_snapshot_purge_restart`, snapshot tests. NVMe performance is NFR-1/2 territory, not claimed |
| FR-6 | 03-01, 03, 06, 08, 10, 11, 12, 13, 14, 15, 16 | Crash recovery with fast replay; dynamic membership changes | SATISFIED | `kill_restart_*`, `membership_*` tests. "Fast" replay is not measured |
| NFR-3 | 03-02, 07, 08, 10, 11 | Zero-copy hot paths, bounded memory, no allocator jitter | PARTIAL (WARNING) | Bounded memory is evidenced: byte-budgeted entry cache that never evicts above the flushed watermark, frame and snapshot size caps, GET p99 unchanged. Zero-copy is not achieved on the replication path: AppendEntries bodies are postcard-serialized (`src/net/protocol.rs`), a copy the research deferred as a later optimization. Reasonable for Phase 3 scope, but the broad "completed" claim is generous |
| NFR-4 | 03-02, 10, 16 | Lock-free reads and writes, bounded reclamation (no unbounded retired-node accumulation) | SATISFIED for Phase 3's contribution | Release `concurrent_scan` stress asserts slab live blocks stay bounded during a scan with a concurrent writer; scan holds no epoch pin across the scan. The live skiplist still leaves tombstones until the next snapshot restore (known Phase 2 warning, R-2 handled at snapshot time only) |

### Locked-Decision Audit (03-CONTEXT.md)

| Decision | Honored | Note |
|----------|---------|------|
| D-01 segmented WAL, CRC32C, purge by segment delete | Yes | |
| D-02 dedicated writer thread, Phase 2 ring, no batching timer | Yes | `PARK_BACKSTOP` is a lost-wakeup backstop, not a batching window |
| D-03 `std::fs` behind a trait | Yes | `SegmentIo` |
| D-04 fsync before ack by default, relaxation flagged unsafe | Yes | CLI and log warnings |
| D-05 custom length-prefixed binary over Tokio TCP | Yes | Hello adds magic, version and cluster id |
| D-06 separate port, shared codec | Yes | `src/wire` has no consensus import (static check) |
| **D-07 one persistent connection per peer, pipelined** | **Pipelined yes; "one connection" NO** | Up to three connections per peer (replication, heartbeat, snapshot lanes). Needs user ruling (human item 1) |
| D-08 plaintext, no TLS | Yes | README security section |
| D-09 epoch-pinned fuzzy snapshot, stamps | Yes | |
| D-10 snapshot every N applied entries | Yes | Default 1,000,000 (A3 raised it from the 100k example; "configurable, e.g. 100k" permits this) |
| D-11 framed snapshot file reusing codec | Yes | |
| D-12 chunked streaming, heartbeats isolated | Yes | Separate connections; test with 32 MiB in flight |
| D-13 static bootstrap then dynamic membership | Yes | |
| D-14 1-node cluster same code path | Yes | |
| D-15 in-process fake-network tests plus few real-TCP smoke tests | Yes | |
| D-16 kill/restart, torn record discarded | Yes | Non-vacuous assertion on `torn_bytes_discarded` |

### Anti-Patterns Found

None blocking. No debt markers in modified files, no `#[ignore]`d tests, no empty implementations on production paths.

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `src/main.rs` | bind and bootstrap block | `--bind` accepted without validation; membership advertises `server.local_addr()`, so `0.0.0.0:PORT` would be published as the node's address | WARNING | Security posture on a plaintext, unauthenticated port; also a functional hazard for multi-node bootstrap (peers cannot dial 0.0.0.0). README-only mitigation |
| `.planning/phases/03-.../03-VALIDATION.md` | row 03-02-03 | Status column still shows pending / "W0 (script)" although `scripts/check-get-regression.sh` exists and passes | INFO | Stale documentation only; the script exists and the gate paragraph records it passing. The sign-off box "Feedback latency < 30s" is also deliberately left unticked and "Approval: pending" |

### Human Verification Required

#### 1. D-07 connection model

**Test:** Review whether three TCP connections per peer (replication, heartbeat, snapshot) is acceptable against D-07 ("one persistent connection per peer").
**Expected:** A ruling. Option A: accept interpretation A4 and amend D-07 in CONTEXT.md. Option B: require one multiplexed connection (needs a priority writer to avoid head-of-line blocking; reworks the client, server and lane tests; the wire format is a costly-to-reverse contract per D-05, so deciding before Phase 4/5 builds on it is cheaper).
**Why human:** Locked user decision; A4 was an unconfirmed researcher assumption. This verifier reports that the code does deviate from the literal wording of D-07 and that it follows D-12's intent.

#### 2. FR-2 traceability

**Test:** Decide how FR-2 is tracked. Single-key CAS is done and replay-safe; WATCH and multi-key conditional updates are not done and not scheduled (A12 still stands).
**Expected:** Either split FR-2 and add a roadmap item for multi-key transactions (with the snapshot design reopened, since single-key stamping does not cover them), or correct the `requirements-completed: FR-2` entries in 03-02 and 03-05.
**Why human:** Scope decision; no later phase currently owns it.

#### 3. `--bind` hardening

**Test:** Start the binary with `--bind 0.0.0.0:7443` (accepted today with no warning) and decide whether to warn or refuse non-private binds, and whether to add an advertise-address flag.
**Expected:** A decision; today only the README advises binding privately.
**Why human:** Security and UX policy for a port the project itself documents as unauthenticated.

#### 4. NFR-1 / NFR-2 on real hardware

**Test:** On a Linux NVMe host run `cargo bench --bench wal`, and measure replicated 3-node write P99 and throughput.
**Expected:** P99 write < 2ms, throughput >= 500k ops/s.
**Why human:** Hardware not available on the macOS dev host. Open in 03-VALIDATION.md; deferred to Phase 6; not claimed by this phase.

### Gaps Summary

No gaps. Every functional must-have is backed by code that exists, is wired and is exercised by passing tests, including the ones I re-ran (state machine and R-1 proof, WAL writer and recovery, pipelining, heartbeat isolation, release concurrent-scan stress, GET regression guard). The phase is held at `human_needed` solely for the four items above, the most consequential being the D-07 connection-model ruling, which is cheapest to settle now while the wire contract has no other consumers.

---

_Verified: 2026-11-10_
_Verifier: Claude (gsd-verifier)_
