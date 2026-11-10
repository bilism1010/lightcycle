---
phase: 03-consensus-persistence-engine
plan: 13
subsystem: raft
tags: [raft, openraft, wal, snapshot, config, bootstrap, membership]
requires:
  - phase: 03-05
    provides: KvStateMachine, EngineHandle
  - phase: 03-06
    provides: FakeNetwork / NetController
  - phase: 03-11
    provides: WalLogStore (recovery_stats, wal_stats)
  - phase: 03-12
    provides: snapshot-aware state machine (open, install_snapshot)
provides:
  - NodeConfig, ConfigError, parse_peers, to_raft_config
  - RaftNode (start, start_with_io, bootstrap_if_needed, write, add_learner, change_membership, remove_voters, metrics, shutdown)
  - tests/common harness (fast_config, eventually, wait_for_leader)
  - first end-to-end run of WAL + state machine + snapshot store under a real Raft core
affects: [03-14, 03-15, 03-16]
tech-stack:
  added: []
  patterns:
    - "capture recovery/WAL stats and the engine handle before moving the store and state machine into Raft::new"
    - "engine() reloads the shared ArcSwap handle on every call"
key-files:
  created:
    - src/raft/config.rs
    - src/raft/node.rs
    - tests/cluster.rs
    - tests/common/mod.rs
  modified:
    - src/raft/mod.rs
    - src/main.rs
key-decisions:
  - "RaftNode::metrics() returns a cloned RaftMetrics snapshot (borrow_watched().clone()) rather than the watch receiver, so callers and tests need no openraft async_runtime trait imports"
  - "NodeConfig::validate rejects election max <= min (openraft also rejects equality), so the typed error fires before openraft's string error"
  - "NodeConfig gained recv_dir() (data_dir/recv) and RaftNode startup creates it and removes leftover recv-<n>.tmp files (carry-over from 03-17)"
requirements-completed: [FR-3, FR-4, FR-6]
status: complete
actuals:
  tokens: 10500
  tasks: 3
  commits: 3
plan_head_before: 07217bd8b2df791a088611766b57ad43f6578ed7
plan_head_after: aa4338c73992149a8aa991dd7063ac6e385d3c01
---

# Phase 3 Plan 13: Node assembly, config and single-node end-to-end Summary

**A one-node cluster now runs the WAL, the stamped state machine and the snapshot store under a real openraft core, with correct restart, snapshot-triggered purge and idempotent bootstrap, plus the NodeConfig surface the multi-node plans build on.**

## Accomplishments

- **Task 1 (tracer):** `RaftNode::start`/`start_with_io` opens the snapshot-backed state machine and the WAL, reads `recovery_stats()`/`wal_stats()` and clones the `EngineHandle` before the store and machine move into `Raft::new`. `engine()` is exactly `engine_handle.load_full()` on every call; no field caches an `Arc<LocalEngine>`. `single_node_restart` proves initialize, write, read, shutdown, restart without re-initialize, WAL replay (`entries_recovered > 0`, `torn_bytes_discarded == 0`) and a further write. `engine_accessor_is_not_captured_at_start` swaps the shared handle and asserts pointer identity changes.
- **Task 2:** `NodeConfig::validate` (node id, 3x-heartbeat election floor, election range, snapshot interval and chunk, 4096-byte segment floor, self in peer list), strict `parse_peers`, `is_unsafe`, and the full openraft mapping with documented rationale (pre-vote, 150-300 ms / 50 ms, 1M snapshot interval, 1 MiB chunks, 30 s install timeout, lag threshold = 2x interval). 8 unit tests including defaults and a test-sized config passing openraft's own validate. `--peers` help text now documents the `id=addr` format (flag not renamed).
- **Task 3:** `add_learner`, `change_membership`, `remove_voters` thin wrappers. `bootstrap_if_needed_is_idempotent` and `single_node_snapshot_purge_restart` (snapshot every 50, keep 10, purge batch 10, 4096-byte segments, 400 keys): the lowest WAL segment advances past 1, openraft reports a purged log id, and after restart all 400 keys read back and a new write succeeds.

## Task Commits

1. Task 1: `7388957` feat(03-13): single-node cluster end to end (config, node, restart tracer)
2. Task 2: `7de03c9` feat(03-13): NodeConfig validation, defaults rationale and strict peer parsing
3. Task 3: `aa4338c` feat(03-13): membership API and snapshot-triggered purge path on one node

## Verification

- `cargo test --test cluster`: 4 passed; run 6 times with no flake (~3.7 s each).
- `cargo test --lib raft::config`: 8 passed. `cargo test --lib raft::node`: 1 passed.
- `bash scripts/check-warnings.sh`: exit 0.

## Deviations from Plan

### Auto-fixed / additive

**1. [Rule 2 - carry-over] recv_dir cleanup at startup**
- **Found during:** Task 1 (03-17 summary asked 03-13 to clean the receive directory at startup)
- **Fix:** `NodeConfig::recv_dir()` plus `clean_recv_dir` run at `start_with_io` (creates the directory 0700, removes `recv-*.tmp` regular files); one unit test in `node.rs`.
- **Files:** src/raft/config.rs, src/raft/node.rs

**2. [Rule 3 - API shape] `metrics()` returns a cloned `RaftMetrics`**
- The watch receiver type needs the openraft `WatchReceiver` trait in scope to read, which the integration tests would otherwise have to import; a value snapshot is simpler and sufficient.

**3. [Rule 2] Extra validation:** `heartbeat_interval_ms == 0` is rejected (typed `ZeroHeartbeat`), since the 3x floor alone would permit it. `ConfigError` also gained `PartialEq, Eq` for test assertions.

No bugs found in prior plans' files.

## Notes for downstream plans

- `shutdown(self)` calls `Raft::shutdown`; the core then drops the `WalLogStore`, whose `WalHandle` Drop joins the writer thread. Tests use `multi_thread` runtimes and restart on the same directory immediately after, which works reliably (6 consecutive runs).
- `ApplyResponder` send path is now exercised end to end: `client_write` returns the `KvResponse` produced by `KvStateMachine::apply`.
- `bootstrap_if_needed` returns `Ok(false)` for a non-bootstrap node without touching Raft; it is never called implicitly by `start`.
- The recv directory convention (`<data_dir>/recv`) is defined here; the TCP server wiring in 03-15 should pass `cfg.recv_dir()`, and the install handler must stage received files inside the snapshot dir (`SnapshotStore::staging_path`) per 03-12.
- Membership wrappers are exercised only via compilation here; multi-node behavior is tested through the cluster harness in 03-14 and 03-16.

## Known Stubs

None.

## Threat Flags

None. T-03-32 (re-initialize on restart) is mitigated and tested; T-03-33 mitigated by pre-vote default, the 3x-heartbeat validation and the UNSAFE warning for relaxed fsync modes.

## Self-Check: PASSED

- src/raft/config.rs, src/raft/node.rs, tests/cluster.rs, tests/common/mod.rs present
- Commits 7388957, 7de03c9, aa4338c are ancestors of HEAD
