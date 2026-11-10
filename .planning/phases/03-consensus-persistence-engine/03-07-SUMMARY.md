---
phase: 03-consensus-persistence-engine
plan: 07
subsystem: storage
tags: [wal, group-commit, fsync, writer-thread, spsc-ring, fail-stop, durability-ordering]
requires: ["03-03"]
provides:
  - "src/wal/writer.rs: WalHandle<P>, WalOp<P>, WalPayload, Ack, WalStats, WAL_RING_CAPACITY"
  - "Group-commit writer thread over SegmentIo: segment roll with preamble re-emission, truncate/purge/vote/committed markers, ordered observer callbacks, fail-stop, fsync modes"
affects: [03-11, 03-14]
actuals:
  tokens: 14100
  tasks: 3
  commits: 3
plan_head_before: ad1a45623822cf0783e856f47c8d2e93a973086b
plan_head_after: bea901fb6e823628761f025b5b36f9896f4fa1fe
tech-stack:
  added: []
  patterns:
    - "Dedicated std::thread drains the Phase 2 SPSC ring; one fsync per batch; acks only after the covering sync (D-02), no timer window"
    - "Spin then park with a SeqCst-fence handshake and a 100 ms park_timeout used only as a lost-wakeup backstop"
    - "Ack ownership: acks move into an AckBatch guard before anything that can panic; Drop and catch_unwind complete every ack exactly once"
    - "Durability ordering encoded as call order and verified by FakeIo power-loss images plus IoEvent order"
key-files:
  created: []
  modified:
    - src/wal/writer.rs
key-decisions:
  - "Purge deletes only a contiguous prefix of non-active segments (stops at the first segment holding an entry above upto). A truncation marker lives in a later segment than the entries it cancels, so deleting an entry-less later segment while an earlier one survives would resurrect truncated entries."
  - "WalStats::fsync_percentile_us takes p as a fraction (0.99), matching benches/local_engine.rs, and returns the upper edge of the log2 microsecond bucket."
  - "try_submit keeps accepting ops after fail-stop; the writer completes their acks with Err. Returning the op instead would make a retry loop spin forever."
  - "Fsync::Never skips every segment sync and every directory sync; Fsync::Periodic syncs when the interval elapsed, at roll, before purge deletion, on idle timeout and once on clean shutdown."
  - "Records that can never fit (larger than segment_size minus the preamble, or over MAX_RECORD_LEN) fail only their own ack with InvalidInput; no segment is created for them and state is unchanged."
requirements-completed: [FR-4, NFR-3]
duration: ~45 min
completed: 2026-11-09
status: complete
---

# Phase 3 Plan 07: Group-Commit WAL Writer Summary

**Single writer thread over the SegmentIo seam: batches ops from the SPSC ring, fsyncs once per batch, acks only after the covering sync, rolls segments with vote/purged/committed re-emitted in every preamble, and goes fail-stop on any I/O error or panic without orphaning a callback.**

## Performance

- **Duration:** ~45 min
- **Tasks:** 3 of 3
- **Files modified:** 1 (`src/wal/writer.rs`, about 700 lines of implementation and 800 of tests)

## Accomplishments

- **Tracer (Task 1):** one entry flows ring, writer thread, StdFsIo, fsync, ack, and the on-disk bytes decode to Preamble then Entry. The lazily created first segment follows create, `sync_dir`, preamble, batch, sync. `tracer_acked_entry_survives_power_loss` fails if `sync_dir` is dropped (confirmed by mutation).
- **Rolling, markers, purge (Task 2):** `append` rolls when `segment::fits` is false, with sync(old), create(new), sync_dir, preamble carrying the live `PreambleState`, then the batch. TruncateAfter is an appended marker (prefix bytes proven unchanged). Purge writes the marker, syncs the active segment, deletes the oldest wholly purged non-active segments, then one `sync_dir`, then `on_purge`. Observer callbacks arrive in op order.
- **Fail-stop and modes (Task 3):** any write, sync, create, delete or dir-sync error, or a panic in `encode_into`, sets the failed flag, completes every pending and later ack with `Err`, never retries a sync, and keeps the thread draining until shutdown. `Fsync::Periodic` and `Fsync::Never` ack after write and log a `tracing::warn!` containing "UNSAFE" at spawn.
- **Group commit measured on FakeIo with a 2 ms sync delay:** 1000 rapid appends cause between 1 and 50 syncs, and every ack closure verifies its entry is already in the durable image. The 5000-cycle submit/wait stress leaves `timeout_wakeups_with_work` at 0 (6 repeated full-module runs, no flake).

## Task Commits

1. Task 1 (tracer): `ab23817` - writer implementation plus the two tracer tests
2. Task 2: `a2b68b5` - roll/marker/purge/ordering tests (10 tests)
3. Task 3: `bea901f` - group commit, fail-stop, panic, modes, stress, ring-full, dir-sync-failure tests (8 tests)

## Verification

- `cargo test --lib wal::writer`: 20 passed.
- `bash scripts/check-warnings.sh`: exit 0.
- Mutation checks (temporary, reverted): removing `sync_dir` after create fails `tracer_acked_entry_survives_power_loss`; removing the pre-delete `sync_segment` fails `purge_marker_durable_before_any_deletion` and `crash_between_purge_marker_and_deletions_keeps_marker_durable`; removing the `sync_dir` after deletions fails `purge_marker_durable_before_any_deletion` and `purge_deletes_only_old_non_active_segments`.

## Deviations from Plan

### Auto-fixed / judgement calls

**1. [Process] Implementation committed with Task 1, tests layered per task**
- The loop, roll, truncate, purge, fail-stop and fsync-mode code is one tightly interlinked state machine, so the full implementation landed in the Task 1 commit (`ab23817`) with only the tracer tests. Tasks 2 and 3 commits add their behavior tests. The code and all tests passed before the corresponding commits; the Task 2 and 3 tests were not red-first against a stub.

**2. [Rule 1 - Bug, found during design] Purge restricted to a contiguous prefix**
- **Issue:** a naive "delete every non-active segment with last index at or below upto, entry-less counts as eligible" could delete an entry-less segment holding only a TruncateAfter marker while an earlier segment still held the entries it cancelled, resurrecting truncated entries on recovery.
- **Fix:** `run_purge` uses `take_while` over segments in ascending order. This still satisfies the plan's tests (every segment holding an entry above upto stays).

No other deviations. `src/wal/mod.rs`, `io.rs` and `segment.rs` were not touched.

## Known Stubs

None.

## Threat Flags

None. No new network surface; the writer only touches the segment directory through `SegmentIo`. Threats T-03-14 (orphaned ack), T-03-15 (failed fsync treated as success) and T-03-16 (silent relaxed fsync) are mitigated and covered by `panicking_payload_completes_every_ack_with_err`, `fail_stop_after_sync_error` and the two fsync-mode tests.

## Notes for downstream plans

- 03-11 should pass `Vec<u8>` or its own `WalPayload`; `Purge` record payload is `upto u64 LE` followed by the blob; `Committed` and `Vote` carry their blob as the whole payload; TruncateAfter payload is flag byte then optional u64 LE.
- The preamble state emitted at a roll excludes the op that triggered the roll (that op's record follows in the new segment), so recovery must apply preamble state first, then the segment's records in order.
- Fresh-start preamble `first_index` is the triggering entry's index, or 0 for a non-entry first op. `next_index` for resumed starts is the highest `last_index` in `WalStart.segments` plus one, else purged upto plus one, else 0.
- macOS `File::sync_data` is `F_FULLFSYNC` (about 4 ms p50), so `WalStats::fsync_percentile_us` values taken here are correctness-only.

## Self-Check: PASSED

- `src/wal/writer.rs` exists and contains `WalHandle`, `WalOp`, `WalPayload`, `Ack`, `WalStats`.
- Commits `ab23817`, `a2b68b5`, `bea901f` are ancestors of HEAD.
