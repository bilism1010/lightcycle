---
phase: "3"
slug: consensus-persistence-engine
status: planned
nyquist_compliant: true
wave_0_complete: false
created: "2026-11-09"
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: `03-RESEARCH.md` § Validation Architecture. Task IDs are `{plan}-{task}`; commands are the exact `<automated>` commands in the plan files.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `cargo test` (17 Phase 2 tests green at plan time) + Criterion 0.5 benches; dev-deps `proptest` 1.12, `tempfile` 3 (added in 03-01) |
| **Config file** | none (Cargo.toml only); `[[bench]] name = "wal"` with `harness = false` is added in 03-11; the `test-util` feature is enabled for tests through a self dev-dependency (03-01) |
| **Quick run command** | `cargo test --lib -- wal:: wire:: snapshot:: raft:: net::` (module-scoped) |
| **Full suite command** | `cargo test` (the stress test needs `cargo test --release --lib snapshot::concurrent_scan`) |
| **Zero-warning gate** | `bash scripts/check-warnings.sh` (created in 03-01; exits 1 on any cargo diagnostic line absent from `scripts/cargo-check-baseline.txt`, which 03-01 records right after the dependency add and which must name no file under src/, tests/ or benches/; baseline at HEAD before the add: empty output; plan 03-02 runs in wave 1 against HEAD's dependency set and uses the literal empty-output check) |
| **Parallel execution** | Plans in one wave share a single crate: run each in its own git worktree or with an isolated `CARGO_TARGET_DIR`; a failed check caused only by a sibling's half-written module (diagnostic in a path the plan does not own) is not the plan's failure and is re-run after the sibling merges; the gate is never weakened |
| **Estimated runtime** | ~30 seconds quick, a few minutes full |

---

## Sampling Rate

- **After every task commit:** Run the task's `<automated>` command (module-scoped, see map below)
- **After every plan wave:** Run `cargo test` plus the zero-warning gate
- **Before `/gsd-verify-work`:** Full suite green (including the 17 Phase 2 tests), `bash scripts/check-get-regression.sh` exits 0 (GET p99 <= 334ns single thread, <= 667ns at 8 threads; Phase 2 baseline 250ns / 542ns), `cargo test --release --lib snapshot::concurrent_scan` green
- **Max feedback latency:** 30 seconds for unit tasks; the `--test cluster`, `--test tcp_smoke` and release stress commands are integration-weight and may take longer

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 1 | FR-3, FR-4, FR-6 (A1, A2) | alpha.38 trait hooks exist or execution stops at a checkpoint; Entry round-trips via postcard | compile + unit | `cargo test --test openraft_api_gate && cargo test --lib raft::types` | ❌ W0 | ⬜ pending |
| 03-01-02 | 01 | 1 | FR-4 | test-util seam reachable from integration tests; unapproved crates absent; zero-warning gate script and recorded baseline installed (baseline names no src/tests/benches path) | unit + static | `cargo test --lib feature_gate_tests && cargo test --test openraft_api_gate && bash scripts/check-warnings.sh && ! grep -E '(src|tests|benches)/' scripts/cargo-check-baseline.txt && ! grep -E '^(bincode|openraft-memstore|openraft-rt-sim)' Cargo.toml` | ❌ W0 | ⬜ pending |
| 03-02-01 | 02 | 1 | FR-4, FR-2 | Stamped cell: put_at skips stale index; 17 Phase 2 tests unchanged | unit | `cargo test --lib storage:: && cargo test --lib engine::` | ✅ (modify) | ⬜ pending |
| 03-02-02 | 02 | 1 | FR-4, NFR-4 | Tombstones carry stamps; scan keeps newer tombstones, drops older, holds no epoch pin | unit | `cargo test --lib storage::index` | ✅ (modify) | ⬜ pending |
| 03-02-03 | 02 | 1 | FR-4, NFR-3 | Engine apply-at/scan/restore; GET p99 not regressed vs 250ns / 542ns | unit + bench guard | `cargo test && bash scripts/check-get-regression.sh` | ❌ W0 (script) | ⬜ pending |
| 03-03-01 | 03 | 2 | FR-4 / D-01, D-03 | CRC detects every single-bit flip; dir 0700 / files 0600; FakeIo importable from tests/; FakeIo models dir-entry durability (create/delete durable only after sync_dir), event log, unsynced_bytes, crash_before_nth_delete, crash during a delayed sync | unit + integration | `cargo test --lib -- wal::record wal::io && cargo test --test wal_test_util_reachable` | ❌ W0 | ⬜ pending |
| 03-03-02 | 03 | 2 | FR-4, FR-6 | Numeric-only strict segment names; preamble carries vote/purged/committed | unit | `cargo test --lib -- wal:: && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-04-01 | 04 | 2 | FR-3 / D-05 | Oversize declared length rejected from the 4-byte prefix | unit | `cargo test --lib -- wire::frame` | ❌ W0 | ⬜ pending |
| 03-04-02 | 04 | 2 | FR-3 / D-05, D-06 | Handshake rejects bad magic/version/cluster; idle connection times out; wire has no consensus import | unit + static | `cargo test --lib -- wire:: && ! grep -rn "openraft" src/wire/` | ❌ W0 | ⬜ pending |
| 03-05-01 | 05 | 2 | FR-3, FR-4 / R-3 | Entries apply through the swappable ApplySink | unit | `cargo test --lib raft::state_machine` | ❌ W0 | ⬜ pending |
| 03-05-02 | 05 | 2 | FR-4, FR-2 / R-1, D-09 | Stamped fuzzy-snapshot replay equals sequential apply; CAS counter-example; negative control diverges | proptest | `cargo test --lib raft::state_machine` | ❌ W0 | ⬜ pending |
| 03-06-01 | 06 | 2 | FR-3 / D-15 | RPC routed through FakeNetwork to a stub handler | unit | `cargo test --lib net::fake` | ❌ W0 | ⬜ pending |
| 03-06-02 | 06 | 2 | FR-3, FR-6 / D-15 | Deterministic partition/delay/drop; stream_append order preserved; snapshot file copied | unit | `cargo test --lib net::fake && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-07-01 | 07 | 3 | FR-4 / D-02 | Entry acked only after sync; bytes decode on disk; acked entry in the lazily created first segment survives power loss (sync_dir after create) | unit | `cargo test --lib wal::writer::tests::tracer` | ❌ W0 | ⬜ pending |
| 03-07-02 | 07 | 3 | FR-4 | Roll re-emits preamble with current vote; acked entry in a rolled segment survives power loss; every create is dir-synced before its first sync; purge marker durable before any deletion and crash between marker and deletions keeps it; purge never deletes active segment; callbacks in op order | unit (FakeIo power loss + event order) | `cargo test --lib wal::writer` | ❌ W0 | ⬜ pending |
| 03-07-03 | 07 | 3 | FR-4 / D-02, D-04 | Group commit far fewer syncs than appends, no early ack; fsync and dir-sync errors fail all acks; panic completes callbacks | unit (FakeIo) | `cargo test --lib wal::writer && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-08-01 | 08 | 3 | FR-4, FR-6 | Clean multi-segment log recovers exact locations and state | unit | `cargo test --lib wal::recovery::tests::recovers_clean_log` | ❌ W0 | ⬜ pending |
| 03-08-02 | 08 | 3 | FR-4, FR-6 / D-16 | Torn tail discarded at every byte offset and reported in RecoveryStats.torn_bytes_discarded; bad CRC in non-last segment is a hard error | unit / property | `cargo test --lib wal::recovery` | ❌ W0 | ⬜ pending |
| 03-08-03 | 08 | 3 | NFR-3 | Cache never evicts above the flushed watermark; disk fallback read | unit | `cargo test --lib wal::cache && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-09-01 | 09 | 3 | FR-3 / D-05 | Vote over real TCP; bad cluster id and silent connection rejected | integration | `cargo test --test transport vote_roundtrip_over_tcp && cargo test --test transport hello_ && cargo test --test transport handshake_timeout` | ❌ W0 | ⬜ pending |
| 03-09-02 | 09 | 3 | FR-3 / D-07 | More than one AppendEntries in flight, responses in order; window bounded; oversize closes connection | integration | `cargo test --test transport pipelined_in_order && cargo test --test transport inflight_window_bounded && cargo test --test transport oversize_frame_closes_connection` | ❌ W0 | ⬜ pending |
| 03-10-01 | 10 | 3 | FR-4 / D-09, D-11 | Engine to file to fresh engine; tombstone rule (keep > a, drop <= a) | unit | `cargo test --lib snapshot::writer::tests::roundtrip && cargo test --lib snapshot::format` | ❌ W0 | ⬜ pending |
| 03-10-02 | 10 | 3 | FR-4 | Any bit flip/truncation detected; exactly one snapshot kept; durable-before-delete ordering | unit | `cargo test --lib -- snapshot::format snapshot::store` | ❌ W0 | ⬜ pending |
| 03-10-03 | 10 | 3 | FR-4, NFR-4 | Concurrent writers: restore + replay equals live; live blocks bounded during scan | stress | `cargo test --release --lib snapshot::concurrent_scan` | ❌ W0 | ⬜ pending |
| 03-11-01 | 11 | 4 | FR-4, FR-6 | Log store survives reopen with vote and entries | unit | `cargo test --lib wal::store::tests::append_vote_restart` | ❌ W0 | ⬜ pending |
| 03-11-02 | 11 | 4 | FR-4 | Truncate marker; vote/purged/committed survive purge + restart; runtime not blocked by full ring; failure completes callbacks once; acked append in fresh and rolled segments survives crash(0) + reopen; crash between durable purge marker and segment deletions reopens cleanly; purge marker synced before first delete | unit (FakeIo crash + reopen + event order) | `cargo test --lib wal::store && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-11-03 | 11 | 4 | none (NFR-1, NFR-2 deferred; NOT claimed) | Bench builds and prints append-to-ack and fsync percentiles as informational output; no latency assertion; not in any plan's requirements frontmatter | bench | `cargo bench --bench wal -- --test` | ❌ W0 | ⬜ pending |
| 03-12-01 | 12 | 4 | FR-4, FR-6 | Apply stream then snapshot build then reopen reports same applied state | unit | `cargo test --lib raft::state_machine::tests::apply_stream_and_build_snapshot_roundtrip` | ❌ W0 | ⬜ pending |
| 03-12-02 | 12 | 4 | FR-4, FR-6 / D-09 | install validates before swap; no overlapping builds; restart + replay converges; apply yields; an EngineHandle cloned before install_snapshot returns the new engine afterwards | unit | `cargo test --lib raft::state_machine && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-17-01 | 17 | 4 | FR-3 / D-11, D-12 | A 3 MiB snapshot crosses a dedicated snapshot-lane connection in chunks, lands in a numeric 0600 tmp file, and is handed to the handler after the final chunk | integration | `cargo test --test transport snapshot_transfer_chunked_and_installed` | ❌ W0 | ⬜ pending |
| 03-17-02 | 17 | 4 | FR-3 / D-12 | Bad offset, size lies, over-max Begin, double Begin, early close, cancel and idle timeouts leave no tmp file and never install; ack window bounds outstanding chunks | integration | `cargo test --test transport snapshot_` | ❌ W0 | ⬜ pending |
| 03-17-03 | 17 | 4 | FR-3 / D-12 | Heartbeat lane on its own connection with replication framing; a 32 MiB snapshot in flight does not starve heartbeats | integration | `cargo test --test transport snapshot_does_not_starve_heartbeat && cargo test --test transport heartbeat_ && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-13-01 | 13 | 5 | FR-6 / D-14 | Single-node write, restart via WAL, no re-initialize; RaftNode::engine() reloads the shared handle on every call and recovery_stats() is exposed | integration | `cargo test --test cluster single_node_restart && cargo test --test cluster engine_accessor_is_not_captured_at_start` | ❌ W0 | ⬜ pending |
| 03-13-02 | 13 | 5 | FR-3 / D-10, D-12 | Config defaults and validation; strict peer parsing; unsafe fsync flagged | unit | `cargo test --lib raft::config` | ❌ W0 | ⬜ pending |
| 03-13-03 | 13 | 5 | FR-4, FR-6 / D-10 | Bootstrap idempotent; snapshot trigger purges WAL; restart from snapshot + log | integration | `cargo test --test cluster bootstrap_if_needed_is_idempotent && cargo test --test cluster single_node_snapshot_purge_restart` | ❌ W0 | ⬜ pending |
| 03-14-01 | 14 | 6 | FR-3 / D-15 | 3-node election and replication over FakeNetwork on distinct lanes | integration | `cargo test --test cluster three_node_election_and_replication` | ❌ W0 | ⬜ pending |
| 03-14-02 | 14 | 6 | FR-3 | Failover keeps committed writes; isolated leader cannot commit; heal converges | integration | `cargo test --test cluster leader_failover_new_leader_elected && cargo test --test cluster partition_heals_and_minority_converges` | ❌ W0 | ⬜ pending |
| 03-14-03 | 14 | 6 | FR-6 / D-16 | Crash with delayed fsync and writes in flight leaves a torn tail; restart reports torn_bytes_discarded > 0 (not vacuous); no acknowledged write lost; nodes identical; node rejoins | integration | `cargo test --test cluster kill_restart_keeps_committed_writes && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |
| 03-15-01 | 15 | 6 | FR-3 / D-15 | 3 nodes over real localhost TCP elect and replicate | integration | `cargo test --test tcp_smoke three_nodes_over_tcp_elect_and_replicate` | ❌ W0 | ⬜ pending |
| 03-15-02 | 15 | 6 | FR-3, FR-4 / D-12 | Lagging follower installs a snapshot over the TCP snapshot lane | integration | `cargo test --test tcp_smoke tcp_snapshot_lane_install` | ❌ W0 | ⬜ pending |
| 03-15-03 | 15 | 6 | FR-6 / D-13, D-08 | Binary starts a node from CLI flags; README states plaintext/no-auth and bootstrap usage | integration + static | `cargo test --test cli_smoke && grep -c "Raft port" README.md && grep -c -- "--bootstrap" README.md` | ❌ W0 | ⬜ pending |
| 03-16-01 | 16 | 7 | FR-6 / D-13 | Add a voter via learner then membership change (single-step path) | integration | `cargo test --test cluster membership_add_voter` | ❌ W0 | ⬜ pending |
| 03-16-02 | 16 | 7 | FR-6 / D-13 | Remove voter; joint-consensus replace; remove leader | integration | `cargo test --test cluster membership_` | ❌ W0 | ⬜ pending |
| 03-16-03 | 16 | 7 | FR-6 / D-16, phase gate | Snapshot catch-up after purge with kill/restart; caught-up follower's engine() identity changes (install_snapshot swap visible through RaftNode::engine()); full phase gate | integration | `cargo test --test cluster kill_restart_snapshot_catchup && cargo test --lib -- storage:: engine:: ipc:: arena:: && bash scripts/check-warnings.sh` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Resolved by plan:

- [ ] `tests/cluster.rs` + `tests/common/mod.rs` harness (03-13, extended in 03-14 and 03-16); `FakeNetwork` / `NetController` (03-06); `tests/transport.rs` (03-09, extended by the snapshot and heartbeat lanes in 03-17); `tests/tcp_smoke.rs` and `tests/cli_smoke.rs` (03-15)
- [ ] `src/wal/io.rs` `FakeIo` — counts syncs, injects delay and errors (segment sync and dir sync), simulates power loss/crash with a directory-durability model, records an event log, reports unsynced bytes, crashes before the nth delete (03-03)
- [ ] `scripts/check-warnings.sh` and `scripts/cargo-check-baseline.txt` — the shared zero-warning gate and its recorded baseline (03-01 task 2)
- [ ] Rust port of the Python model check as a proptest — `raft::state_machine::tests::fuzzy_replay_model` (03-05, before any snapshot code in 03-10 and 03-12)
- [ ] Dev-deps `tempfile`, `proptest` (03-01); bench `benches/wal.rs` and its `[[bench]]` entry (03-11); `scripts/check-get-regression.sh` (03-02)
- [ ] openraft API gate — skeletons of the log, state machine and network traits against `=0.10.0-alpha.38` (03-01 task 1; A1 accepted by user, stop at a checkpoint if a hook is missing)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| WAL fsync latency and throughput on real NVMe | NFR-1 / NFR-2 | Dev Mac cannot measure it: `sync_data` maps to `F_FULLFSYNC` (p50 4.0ms). User's Linux host not yet connected. **DEFERRED: NOT validated and NOT claimed as met in Phase 3; these IDs are intentionally absent from every plan's `requirements` frontmatter so execution cannot mark them complete.** | Run `cargo bench --bench wal` on the Linux NVMe host once access is provided and record the `[tail]` and `[fsync]` lines; the P99 < 2ms (3 nodes) claim needs an end-to-end replicated-write measurement in Phase 6 |
| Multi-key conditional operations (FR-2 WATCH, multi-key CAS) replay safety | FR-2 (deferred, assumption A12) | Single-key stamping does not cover them; a consistent-cut snapshot design is needed before such an operation is added | Reopen the snapshot design before implementing any multi-key conditional |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [ ] Feedback latency < 30s (unit tasks yes; integration and release-stress tasks are heavier, accepted)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
