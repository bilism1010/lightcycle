---
phase: 03-consensus-persistence-engine
verified: 2026-11-12T04:30:00Z
status: passed
score: 7/7 must-haves verified
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
covered_digest: "v4:sha256:fc6a2279556222ab85fb412df4dd411c11fadf2a9bf20200f89eeb4ec2efc9a4"
behavior_unverified: 0
overrides_applied: 0
re_verification:
  previous_status: human_needed
  previous_score: 6/7
  gaps_closed:
    - "D-07 connection model: user accepted interpretation A4, recorded as an amendment in 03-CONTEXT.md (truth 7 now VERIFIED under the amendment)"
  gaps_remaining: []
  regressions: []
gaps: []
deferred:
  - truth: "NFR-1 (P99 read < 150us, P99 write < 2ms on 3 nodes) and NFR-2 (>= 500k ops/s) on real hardware"
    addressed_in: "Phase 6"
    evidence: "ROADMAP Phase 6 milestones 6.1 and 6.2; 03-UAT.md test 4 skipped by the user (will run on Linux NVMe); not claimed by any Phase 3 plan"
  - truth: "FR-2 remainder: WATCH / multi-key conditional updates"
    addressed_in: "Phase 4 Milestone 4.4"
    evidence: "ROADMAP.md Milestone 4.4 'FR-2 remainder - WATCH / multi-key conditional updates'; REQUIREMENTS.md FR-2 status note; accepted by the user in 03-UAT.md test 2"
---

# Phase 3: Consensus & Persistence Engine Verification Report

**Phase Goal (derived):** ROADMAP.md carries no explicit Goal line or success-criteria list for Phase 3, so the goal is derived from the four roadmap milestones (3.1 Raft state machine and log store, 3.2 group-committed WAL, 3.3 membership and heartbeat network layer, 3.4 snapshotting and state transfer) plus 03-CONTEXT.md decisions D-01 to D-16: "Deliver the distributed tier's durability and replication core: a Raft consensus node (openraft) with a segmented, group-committed on-disk WAL, a custom Tokio TCP peer transport, a snapshot/restore engine over the Phase 2 LocalEngine, and static-bootstrap plus dynamic membership."
**Verified:** 2026-11-12
**Status:** passed
**Re-verification:** Yes, after two code-review fix passes (about 55 fix commits, 8c871c6..44e41c5) and the user's UAT rulings (commits b3de4e1, 6bbf8e4)

## Goal Achievement

The goal is achieved in the current tree. Every must-have from the previous report was re-checked against the post-fix code and none regressed. The one previously uncertain truth (D-07) is resolved by the user's amendment. The suite and gates were re-run from scratch by this verifier (results below). The only remaining open items are the four the user has already ruled on, plus the documented review partials, none of which breaks a must-have.

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Milestone 3.1: a Raft state machine and log-store abstraction exist on openraft, applying stamped SET/DEL/CAS into LocalEngine | VERIFIED | `Cargo.toml:22` pins `openraft = "=0.10.0-alpha.38"` (default-features off, `tokio-rt`, `serde`); `src/raft/state_machine.rs` implements `RaftStateMachine` behind the `ApplySink` seam; `src/wal/store.rs` implements the log storage traits. Re-run: `cas_counterexample`, `fuzzy_replay_model`, `naive_replay_diverges_on_counterexample`, `naive_replay_diverges_in_generated_helper`, `tombstone_*` all ok. Post-fix change in `state_machine.rs` (build lock serializing `install_snapshot` against an in-flight build; `body_corruption` mapping) is covered by `install_waits_for_an_in_flight_build` and `open_reports_wipe_guidance_for_a_corrupt_snapshot_body`, both ok |
| 2 | Milestone 3.2: segmented CRC32C WAL on `std::fs` behind a trait, group commit on a dedicated thread draining the Phase 2 ring, ack only after fsync, no timer batching, torn tail discarded on recovery (D-01..D-04, D-16) | VERIFIED | `src/wal/writer.rs:295` still drains via `Consumer::consume_batch`. Post-fix writer changes re-read: `run_purge` still does `write_run`, `sync_active` (marker durable) before any `delete_segment`, then one `sync_dir`; the new `on_purge` observer call sits after the durable marker and before the unlinks (`purge_is_reported_to_the_observer_before_segments_are_deleted` ok). An unacked record that does not fit now fail-stops instead of leaving a durable index gap (`unacked_oversize_record_fail_stops_instead_of_leaving_a_gap` ok). Durability-ordering tests still pass: `tracer_acked_entry_survives_power_loss`, `rolled_segment_acked_entry_survives_power_loss`, `crash_between_purge_marker_and_deletions_keeps_marker_durable`, `fake_io_created_segment_lost_on_power_loss_without_sync_dir`. Recovery: `torn_tail_every_byte_offset`, `data_after_end_marker_in_non_last_segment_is_corrupt`, `entry_index_gap_is_corrupt`, `recovery_stats_report_torn_bytes` ok. The IN-03 change (tolerate a purge-straggler gap) did not weaken the hard error: `lost_middle_segment_is_still_a_gap_error_and_nothing_is_deleted` ok |
| 3 | Milestone 3.3: static bootstrap, dynamic add/remove via joint consensus, heartbeat and replication transport over TCP with a shared codec | VERIFIED | `src/net/{client,server,handler,fake,protocol}.rs`, `src/wire/{frame,hello}.rs`; no `openraft` reference under `src/wire` (grep empty). `tests/cluster.rs`: `membership_add_voter`, `membership_remove_voter`, `membership_replace_voters_joint`, `membership_remove_leader_new_leader_elected`, `three_node_election_and_replication`, `leader_failover_new_leader_elected`, `partition_heals_and_minority_converges` ok; `tests/tcp_smoke.rs` `three_nodes_over_tcp_elect_and_replicate` ok; `tests/transport.rs` `pipelined_in_order` ok. `src/main.rs` wires `TcpNetConfig::validate`, `RaftNode::start`, `PeerServer::bind` and `bootstrap_if_needed` |
| 4 | Milestone 3.4: epoch-pinned fuzzy snapshot, framed CRC file via the shared codec, trigger every N applied entries with purge, chunked transfer on its own connection with backpressure that cannot starve heartbeats (D-09..D-12) | VERIFIED | `src/snapshot/{format,writer,store,reader}.rs`; `tcp_snapshot_lane_install`, `kill_restart_snapshot_catchup`, `single_node_snapshot_purge_restart`, `snapshot_does_not_starve_heartbeat` (reworked by IN-26 to 64 KiB chunks and 2 ms heartbeat spacing) ok. Release-mode `snapshot::concurrent_scan::concurrent_scan_restore_replay_equals_live` re-run by this verifier: ok. New WR-04 path (`validate_snapshot` before `install_snapshot`, a corrupt received snapshot no longer fatal to the receiver) is covered by a tcp_smoke test, ok |
| 5 | Crash-recovery bar (D-16, FR-6): kill-and-restart loses no committed write; torn final WAL record detected and discarded; non-vacuous | VERIFIED | `kill_restart_keeps_committed_writes`, `single_node_restart`, `single_node_snapshot_purge_restart` ok; the review fixes WR-16/WR-17 hardened the previously vacuous torn-tail and uncommitted-write assertions rather than weakening them |
| 6 | Phase 2 is not regressed by the stamped-cell change (NFR-3, NFR-4) | VERIFIED | `bash scripts/check-get-regression.sh` re-run: GET p99 250 ns at 1 thread (limit 334), 542 ns at 8 threads (limit 667), PASS on attempt 1. Phase 2 storage/engine/ipc/arena tests are part of the 252 passing lib tests. `test_scan_never_sees_torn_value_stamp_pair`, `test_scan_drops_old_tombstones_keeps_newer`, `test_delete_at_leaves_stamped_tombstone` ok |
| 7 | D-07: one persistent pipelined connection per peer for replication | VERIFIED (under the user-accepted amendment) | `new_client`, `new_heartbeat_client`, `new_snapshot_client` at `src/net/client.rs:214/218/222` dial lane-tagged connections. 03-CONTEXT.md D-07 now carries the amendment (A4, accepted 2026-11-10): one pipelined replication connection plus dedicated lazily dialed heartbeat and snapshot connections, up to three per peer. Pipelining is real (`pipelined_in_order`). 03-UAT.md test 1: pass |

**Score:** 7/7 truths verified (0 failed, 0 behavior-unverified)

### Deferred Items

| # | Item | Addressed In | Evidence |
|---|------|-------------|----------|
| 1 | NFR-1 and NFR-2 hardware validation (P99 < 2 ms writes on 3 nodes, 500k ops/s) | Phase 6 (milestones 6.1, 6.2) | User skipped UAT test 4 with reason (will run on Linux NVMe); not in any Phase 3 plan `requirements` |
| 2 | FR-2 remainder: WATCH / multi-key conditional updates | Roadmap Milestone 4.4 | Added 2026-11-10 from UAT test 2; REQUIREMENTS.md FR-2 carries a status note |

### Required Artifacts

| Artifact | Status | Details |
|----------|--------|---------|
| `src/wal/{record,io,segment,writer,recovery,cache,store}.rs` | VERIFIED | Present, substantive, driven by `WalLogStore` in `RaftNode::start`; `StdFsIo` in production |
| `src/wire/{frame,hello}.rs` | VERIFIED | Hello now also rejects self-dial (IN-10) and reports lane mismatch (IN-23); unit tests ok |
| `src/net/{handler,fake,protocol,client,server}.rs` | VERIFIED | `PeerServer` and `TcpNetworkFactory` wired in `src/main.rs`; connection caps, idle/keepalive/write timeouts (WR-10/11) tested |
| `src/raft/{types,config,state_machine,node}.rs` | VERIFIED | `RaftNode::engine()` is `self.engine_handle.load_full()` (`node.rs:163`), not captured at start; `engine_accessor_is_not_captured_at_start` and `engine_handle_tracks_install_snapshot` ok. `start` now validates config before touching disk (IN-09) and rejects over-limit commands up front (CR-02, `oversized_write_is_rejected_without_stopping_the_node` ok) |
| `src/snapshot/{format,writer,store,reader}.rs` | VERIFIED | Used by `KvStateMachine` build and install |
| `src/storage/index.rs`, `src/engine.rs` | VERIFIED | `put_at`/`delete_at`/`cas_at`/`scan_versioned` present and exercised |
| `scripts/check-warnings.sh`, `scripts/cargo-check-baseline.txt`, `scripts/check-get-regression.sh` | VERIFIED | `check-warnings.sh` exit 0 (baseline file empty, 0 bytes); GET guard passes |
| `benches/{wal,local_engine}.rs`, `tests/*.rs`, `tests/common/mod.rs` | VERIFIED | No `#[ignore]` anywhere in src/tests/benches |

### Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| `InlineSink` | `LocalEngine::{set_at,delete_at,cas_at}` | log index as stamp | WIRED |
| WAL writer thread | `Consumer::consume_batch` | one fsync per drained batch | WIRED |
| WAL writer | `WalObserver` | ordered `on_flushed`, `on_truncate`, `on_purge` (purge reported after durable marker, before unlink) | WIRED |
| `PeerServer` | `RaftHandler` | per-lane dispatch, `validate_snapshot` before install | WIRED |
| `PeerClient` | `wire::hello` handshake | lane-tagged Hello on every connection | WIRED |
| `RaftNode::engine_handle` | `KvStateMachine::install_snapshot` | same `Arc<ArcSwap<LocalEngine>>` | WIRED |
| `main.rs` | `RaftNode::start`, `PeerServer::bind`, `bootstrap_if_needed` | CLI flags to `NodeConfig`, `--advertise` published address | WIRED |

### Data-Flow Trace (Level 4)

Client write goes `RaftNode::write` (size check) to `raft.client_write`, to WAL append and group-commit fsync, to quorum commit, to `KvStateMachine::apply` / `InlineSink` / `LocalEngine::*_at`; a read through `RaftNode::engine()` returns the value, asserted against real engines on real WAL directories by `three_node_election_and_replication`, `three_nodes_over_tcp_elect_and_replicate` and `single_node_restart`. Snapshot data flows engine, `scan_versioned`, framed file, snapshot lane, validate, `install_snapshot`, ArcSwap swap, asserted through fresh `engine()` calls by `tcp_snapshot_lane_install` and `kill_restart_snapshot_catchup`. Status: FLOWING.

### Behavioral Spot-Checks and Gates (run by this verifier)

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Full suite | `cargo test --features test-util` | 252 lib, 8 bin, 1 cli_smoke, 15 cluster, 2 openraft_api_gate, 3 tcp_smoke, 47 transport, 1 wal_test_util_reachable; 0 failed, 0 ignored; exit 0 | PASS |
| Zero-warning gate | `bash scripts/check-warnings.sh` | exit 0 | PASS |
| Phase 2 GET tail latency | `bash scripts/check-get-regression.sh` | p99 250 ns / 542 ns within 334 / 667 | PASS |
| NFR-4 concurrent scan stress | `cargo test --release --features test-util --lib snapshot::concurrent_scan` | 1 passed | PASS |

### Probe Execution

No `probe-*.sh` scripts exist and no plan declares one. Step 7c: SKIPPED.

### Requirements Coverage

Union of PLAN frontmatter `requirements` across the 17 plans: FR-2, FR-3, FR-4, FR-6, NFR-3, NFR-4. All six are defined in `.planning/REQUIREMENTS.md`, which has no phase-mapping table, so no ORPHANED requirement can be derived. NFR-1 and NFR-2 appear in no plan and are correctly not claimed.

| Requirement | Source Plans | Status | Evidence |
|-------------|--------------|--------|----------|
| FR-2 | 03-02, 03-05 | SATISFIED for Phase 3 scope (single-key CAS); remainder scheduled | Stamp-gated `cas_at`, `cas_counterexample`, R-1 proptest. WATCH/multi-key accepted as split by the user; ROADMAP Milestone 4.4; REQUIREMENTS.md FR-2 status note |
| FR-3 | 03-01, 04, 05, 06, 09, 12-17 | SATISFIED | election, failover, partition-heal, TCP election tests ok |
| FR-4 | 03-01, 02, 03, 05, 07, 08, 10-14, 16 | SATISFIED | WAL writer/recovery tests, snapshot and purge-restart tests ok. NVMe performance belongs to NFR-1/2 (deferred) |
| FR-6 | 03-01, 03, 06, 08, 10-16 | SATISFIED | `kill_restart_*`, `membership_*` ok. "Fast" replay is not benchmarked here |
| NFR-3 | 03-02, 07, 08, 10, 11 | PARTIAL (WARNING, unchanged) | Bounded memory evidenced (byte-budgeted entry cache, frame and snapshot caps, GET p99 unchanged). Zero-copy not achieved on the replication path (postcard-encoded AppendEntries); deliberate Phase 3 trade-off, a later optimization |
| NFR-4 | 03-02, 10, 16 | SATISFIED for Phase 3's contribution | Release concurrent-scan stress bounds slab live blocks; scan holds no epoch pin across the scan |

### Locked-Decision Audit (03-CONTEXT.md)

D-01 through D-16 honored. D-07 is honored under the user-accepted A4 amendment in 03-CONTEXT.md. D-04 default `--fsync always` retained. D-06 shared codec with no consensus import in `src/wire`. D-08 plaintext, no TLS (README states the peer port must be private or firewalled).

### Anti-Patterns Found

No `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` markers in src, tests, benches or scripts. The only `unimplemented!` hits are in `tests/openraft_api_gate.rs`, a deliberate compile-time skeleton. No `#[ignore]`.

| Item | Severity | Impact |
|------|----------|--------|
| Review partial WR-01: a tear persisting later pages without earlier ones, or a client value embedding a valid WAL record, still makes the node refuse to start (clearer message, segments untouched) | WARNING (documented limitation) | Fail-closed, never silent data loss; fix needs an on-disk format change. Does not break a must-have: torn-tail and non-prefix cases covered by tests still recover or fail loudly |
| Review partial WR-03: an empty `--peers` still initializes a one-node cluster implicitly; warning text corrected | WARNING (documented limitation) | CLI design decision left open; `cli_smoke` and README quickstart rely on it |
| Review partial IN-11: cluster-wide settings (`max_frame_len`, chunk size, ack window) are validated per node but not negotiated across peers | INFO | Mismatch fails late, not silently |
| Review partial IN-25: `segment_size`/`max_entry_bytes` must match across members; documented, not checked (library-only setting, no CLI flag) | INFO | Operator-visible only on heterogeneous configs |
| Review skipped IN-21: no free-space check on snapshot receive; two connections can hold both receive slots | INFO | Hardening deferred; receive is bounded by WR-10 |
| IN-02 residual: a writer failure between the pre-publish check and the publish drops the final ack | INFO | openraft treats the returned error as fatal either way |

### Human Verification Required

None new. The four items from the previous report were ruled on by the user (03-UAT.md status complete, commits b3de4e1 and 6bbf8e4):

1. D-07 connection model: accepted (interpretation A4); amendment recorded in 03-CONTEXT.md.
2. FR-2: accepted as split; WATCH/multi-key is ROADMAP Milestone 4.4; REQUIREMENTS.md FR-2 status note.
3. `--bind`: user accepted the current behaviour (`--advertise` flag; wildcard refused only when initializing a multi-node cluster). Confirmed in `src/main.rs` tests `wildcard_bind_without_advertise_is_refused`, `explicit_advertise_replaces_a_wildcard_bind`, `wildcard_check_runs_only_when_a_multi_node_cluster_is_initialized`.
4. NFR-1/NFR-2 on a Linux NVMe host: user deferred (UAT row skipped with reason), carried to Phase 6; not claimed by this phase.

### Gaps Summary

No gaps. All seven must-haves hold against the current code after the review fix passes; no regression was found in the WAL durability ordering, recovery strictness, engine-handle tracking, snapshot lane isolation or Phase 2 GET latency. The full suite (252/8/1/15/2/3/47/1) and both gates are green.

---

_Verified: 2026-11-12_
_Verifier: Claude (gsd-verifier)_
