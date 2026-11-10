---
phase: 03-consensus-persistence-engine
plan: 05
subsystem: raft
tags: [raft, state-machine, openraft, snapshot, proptest, cas]
requires:
  - phase: 03-01
    provides: KvRequest, KvResponse, TypeConfig
  - phase: 03-02
    provides: stamped LocalEngine set_at/delete_at/cas_at, scan_versioned, load_record
provides:
  - KvStateMachine, ApplySink, InlineSink, EngineHandle in src/raft/state_machine.rs
  - Executable R-1 proof that stamped fuzzy-snapshot replay equals sequential apply
affects: [03-10, 03-12, 03-13]
tech-stack:
  added: []
  patterns: ["ApplySink seam for Phase 4 core-pinned apply", "ArcSwap engine handle for snapshot-restore swap"]
key-files:
  created: [src/raft/state_machine.rs]
  modified: [src/raft/mod.rs]
key-decisions:
  - "Entry log index passed as the engine stamp; replay safety rests on stamp-skip (R-1)"
  - "Apply is synchronous and guard-free; Phase 4 swaps in a worker via a different ApplySink (R-3)"
patterns-established:
  - "Snapshot-replay test helper models writes racing a fuzzy scan"
requirements-completed: [FR-3, FR-4, FR-2]
status: complete
duration: ~20min
completed: 2026-11-09
commits: 2
plan_head_before: 60abfd47818790e2917e1eebb2873ac016805efa
plan_head_after: 4966ba93d393d3575ba3212aae996b8c76862a17
actuals:
  tokens: 9000
  tasks: 2
  commits: 2
---

# Phase 3 Plan 05: KvStateMachine core and R-1 proof Summary

**KvStateMachine applies Set/Delete/Cas entries into the stamped LocalEngine through a swappable ApplySink, with a 3000-case property test plus deterministic counter-examples proving fuzzy-snapshot replay equals sequential apply.**

## Accomplishments
- `KvStateMachine` (engine in `Arc<ArcSwap<LocalEngine>>`, sink, last applied log id, last membership) with `apply_entry`, `replace_engine`, `engine_handle`, `engine`, `applied_state`. Blank advances applied only; Membership updates `last_membership`; Normal goes through the sink with `log_id.index()` as the stamp.
- `InlineSink` maps `Applied`/`Skipped` to `KvResponse::Ok` and `CasFailed` to `KvResponse::CasFailed`.
- R-1 tests: `cas_counterexample`, `tombstone_newer_than_snapshot_is_kept`, `tombstone_at_or_below_start_is_dropped`, `naive_replay_diverges_on_counterexample`, `naive_replay_diverges_in_generated_helper`, and proptest `fuzzy_replay_model` (3000 cases, history 1..60, 4 keys, 3 values, random start index and racing writes between scanned keys, `failure_persistence: None` so no regression file is written).
- Negative control verified beyond the unit test: temporarily restoring with stamps forced to 0 made `fuzzy_replay_model` fail; reverted. The property test has teeth.

## Task Commits
1. Task 1 (tracer): KvStateMachine + ApplySink - `bda4049`
2. Task 2: R-1 proof tests - `4966ba9`

## Verification
- `cargo test --lib raft::state_machine`: 12 passed.
- `bash scripts/check-warnings.sh`: exit 0.

## Deviations from Plan
None - plan executed as written. Minor: `entry.log_id.index()` (inherent accessor) used rather than the field; an extra test `naive_replay_diverges_in_generated_helper` added to show the property helper's unstamped mode also discriminates.

## Known Stubs
None.

## Threat Flags
None. T-03-11 mitigated by the tests above; T-03-12 mitigated (no guard, nothing async in the module).

## Notes for downstream plans
- Constraint recorded in the test module: only single-key operations are covered; multi-key conditionals (FR-2 WATCH) need a consistent-cut snapshot and would reopen the design (A12, deferred).
- openraft trait impl, snapshot build/install remain for 03-12.

## Self-Check: PASSED
- src/raft/state_machine.rs present; commits bda4049 and 4966ba9 on the branch.
