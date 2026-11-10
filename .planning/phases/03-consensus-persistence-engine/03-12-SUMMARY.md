---
phase: 03-consensus-persistence-engine
plan: 12
subsystem: consensus
tags: [raft, state-machine, snapshot, install-snapshot, arc-swap, spawn-blocking, crash-safety]
requires: ["03-05", "03-10"]
provides:
  - "raft::state_machine::KvStateMachine: impl openraft RaftStateMachine<TypeConfig> (SnapshotData = SnapshotFile, SnapshotBuilder = KvSnapshotBuilder)"
  - "raft::state_machine::KvSnapshotBuilder: impl RaftSnapshotBuilder, build runs on spawn_blocking"
  - "KvStateMachine::open(snapshot_dir) -> io::Result<KvStateMachine>: reloads the latest snapshot"
affects: [03-13, 03-15]
tech-stack:
  added: []
  patterns: [owned tokio Mutex guard as no-overlap build token, capture-at-builder-creation serialized with apply, validate-load-persist-swap install order, cooperative yield every 256 applied entries]
key-files:
  created: []
  modified:
    - src/raft/state_machine.rs
key-decisions:
  - "try_create_snapshot_builder takes the build lock with try_lock_owned (None while a build is outstanding); get_snapshot_builder takes it lazily inside build_snapshot, so builds are serialized but never concurrent"
  - "The builder captures Arc<LocalEngine>, last_applied and last_membership at creation (serialized with apply because the method takes &mut self); the scan start index is therefore the applied index at that instant"
  - "install_snapshot order is fixed: load_snapshot (full validation) into a fresh engine, header meta must equal the meta argument, install_received (durable single snapshot), then replace_engine through the shared ArcSwap"
  - "Without a snapshot store (KvStateMachine::new) a build returns io::Error(Unsupported, 'snapshots are not configured') and get_current_snapshot returns None"
  - "SnapshotError::CorruptNeedsWipe surfaces from open() as an InvalidData io::Error telling the operator to wipe and rejoin"
metrics:
  tasks: 2
  completed: 2026-11-09
status: complete
actuals:
  tokens: 14000
  tasks: 2
  commits: 2
plan_head_before: cc4c96f5a9df3c66a4a81ee80a15e75fbfdae695
plan_head_after: 80e91d2
---

# Phase 3 Plan 12: RaftStateMachine over the snapshot store Summary

`KvStateMachine` is now a complete openraft `RaftStateMachine`: chunked apply, non-overlapping fuzzy snapshot builds that run off the Tokio runtime and are durable before returning, crash-ordered `install_snapshot` that swaps the shared `EngineHandle`, and `open()` that restarts from the latest snapshot.

## What was built

- **Apply:** consumes the `(entry, Option<ApplyResponder>)` stream, calls `apply_entry`, answers each responder with the `KvResponse`, and `yield_now().await` every 256 entries (A8).
- **Builders:** `try_create_snapshot_builder` / `get_snapshot_builder` capture engine, `last_applied` and `last_membership` together. `build_snapshot` acquires the build lock if the builder does not already hold it, runs `SnapshotStore::build` in `spawn_blocking` (file sync, rename and directory sync complete before it returns, so the later log purge is safe, T-03-30), then releases the lock.
- **`install_snapshot`:** validate + load on a blocking thread, compare header meta, `install_received`, then `replace_engine`. Any earlier failure changes nothing (T-03-31). The double validation (load, then `install_received`) is deliberate and commented.
- **`open`:** `SnapshotStore::open`, `latest()`, `load_snapshot` on `spawn_blocking`, then `replace_engine` with the snapshot's log id and membership.
- **`get_current_snapshot`:** maps `SnapshotStore::latest()` to `Snapshot`.

## Tests (all in src/raft/state_machine.rs)

`apply_stream_and_build_snapshot_roundtrip` (tracer), `no_overlapping_snapshot_builds`, `build_without_store_reports_not_configured`, `install_snapshot_replaces_state_and_reports_applied`, `engine_handle_tracks_install_snapshot`, `install_rejects_corrupt_file_and_keeps_old_engine`, `install_rejects_meta_mismatch`, `install_keeps_exactly_one_snapshot_on_disk`, `restart_loads_latest_snapshot_then_replays`, `apply_yields_every_256_entries`. `cargo test --lib raft::` passes (24 tests including the 03-05 ones); `scripts/check-warnings.sh` exits 0.

## Deviations from Plan

None - plan executed as written. Two small additions beyond the listed behavior tests: `build_without_store_reports_not_configured` and `install_rejects_meta_mismatch`, covering the two explicit error paths the action text describes.

## Notes for downstream plans

- `ApplyResponder` has a crate-private constructor in openraft, so responders cannot be exercised in unit tests here; apply tests use `None` responders. The send path is exercised end to end in plan 03-13.
- A builder keeps scanning the engine `Arc` it captured. If `install_snapshot` swaps the engine while a build is outstanding, that build publishes an older snapshot than the installed one; `SnapshotStore::open` trims to the highest on restart and `latest()` already picks the highest. openraft does not run both concurrently in normal operation.
- `install_snapshot` expects the received file to be staged (e.g. at `SnapshotStore::staging_path`) inside the store directory, because `install_received` renames it into place. A failed install leaves the staged `.tmp` for `SnapshotStore::open` to clean.

## Known Stubs

None.

## Threat Flags

None. No new network or trust-boundary surface beyond the plan's threat model.

## Self-Check: PASSED

- src/raft/state_machine.rs modified and present
- Commits 44f6e5d (Task 1) and 80e91d2 (Task 2) are ancestors of HEAD
