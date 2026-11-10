---
phase: 03-consensus-persistence-engine
plan: 08
subsystem: persistence
tags: [wal, recovery, crash-safety, entry-cache, offset-index]
requires: ["03-03"]
provides:
  - "wal::recovery::{recover, RecoveredLog, RecoveryStats}: scans segments in order, rebuilds locations plus vote/purged/committed, repairs a torn tail"
  - "wal::recovery::test_support::LogBuilder (cfg test): declarative segment/record builder over FakeIo, reused by cache tests"
  - "wal::cache::{LogIndex, EntryCache, LogShared, SharedObserver, read_entry}: offset index, byte-budgeted cache that never evicts unflushed entries, pread fallback"
affects: [03-11, 03-13, 03-14]
tech-stack:
  added: []
  patterns: [last-segment-only torn tail discard, zero-extend-and-sync repair, flushed-watermark eviction guard, racing-purge reads return None]
key-files:
  created: []
  modified:
    - src/wal/recovery.rs
    - src/wal/cache.rs
key-decisions:
  - "An Invalid record is a torn tail only in the last segment; in any earlier segment, and any non-zero data after the end marker of an earlier segment, it is WalError::Corrupt (T-03-17)"
  - "Repair is set_len(T), sync, set_len(segment_size), sync, sync_dir, so the tail is durably zero before any append (T-03-18)"
  - "A last segment whose head is all zero or a torn preamble (crash right after creation) is deleted and the directory synced; a zero header followed by data, or a valid non-preamble first record, is Corrupt rather than silently deleted"
  - "Entries physically present at or below the purge point (purge marker lived in a deleted segment) are skipped, not indexed; later preambles also re-apply their purge point"
  - "EntryCache clamps the flushed watermark on truncate, so entries rewritten after a truncation are not treated as durable"
  - "read_entry does not populate the cache on a miss (keeps the deque ascending); a purge/truncate racing with a disk read yields Ok(None), not Corrupt"
  - "LogIndex::push on a non-contiguous batch is a debug_assert in debug builds and Err(WalError) in release; SharedObserver logs the release error and leaves the index unchanged"
metrics:
  tasks: 3
  completed: 2026-11-10
status: complete
actuals:
  tokens: 21000
  tasks: 3
  commits: 3
plan_head_before: ad1a45623822cf0783e856f47c8d2e93a973086b
plan_head_after: 23198e7c616c696f6cf55e4fe7ccd0b431419c2a
---

# Phase 3 Plan 08: Recovery and entry cache Summary

Crash recovery rebuilds the logical log from segments with torn-tail repair and hard errors on mid-log corruption, and a byte-budgeted entry cache with an on-disk offset index bounds memory without ever losing track of where an entry lives.

## What was built

- **`src/wal/recovery.rs`**
  - `recover(io, cfg)`: lists segments (gap is Corrupt), checks the last segment head first, then walks each segment applying Entry / Vote / Committed / TruncateAfter / Purge in order. The first record must be a Preamble naming the file's sequence; later preambles replace the running state wholesale.
  - Builds `SegmentSummary` per segment from surviving locations; `WalStart.active_seq` / `tail_offset` set from the last segment.
  - `RecoveryStats { torn_bytes_discarded, segments_scanned, entries_recovered }` via `RecoveredLog::stats()`. Restart tests in 03-11/03-13/03-14 can assert `torn_bytes_discarded > 0` to prove a torn tail was really discarded.
  - `test_support::LogBuilder`: `segment`, `segment_at`, `entry`, `vote`, `committed`, `truncate_after`, `purge`, `raw`, `sync`; exposes `io` (FakeIo), `entries` (EntryLocation per entry written) and `records`.
- **`src/wal/cache.rs`**
  - `LogIndex` (first index plus `VecDeque<EntryLocation>`, O(1) `get`), `EntryCache<E>` (ascending deque, byte accounting, flushed watermark, oldest-first eviction only at or below the watermark), `LogShared<E>`, `SharedObserver<E>` (implements `WalObserver`; the store, not the observer, inserts into the cache), and `read_entry` (cache hit, else one positional read through the index, decode and verify type/index/length, then the caller's decode closure).

## Verification

- `cargo test --lib -- wal::recovery wal::cache`: 33 passed (22 recovery, 11 cache), including the exhaustive power-loss sweep (every unsynced byte count), corruption flips at every record position of a non-last segment, and the 500-case proptest.
- `bash scripts/check-warnings.sh` exits 0.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] Non-zero data after the end marker of a non-last segment**
- **Found during:** Task 2
- **Issue:** The plan only calls an Invalid record in an earlier segment corrupt. A zeroed record header followed by live bytes decodes as `End` and would silently end the scan, dropping later committed records (T-03-17).
- **Fix:** Non-last segments are checked for zero bytes after the end marker; any non-zero byte is `WalError::Corrupt` at that offset. Test: `data_after_end_marker_in_non_last_segment_is_corrupt`.
- **Files modified:** src/wal/recovery.rs
- **Commit:** 10c4799

**2. [Rule 2 - Missing critical functionality] Entries at or below the purge point in surviving segments**
- **Found during:** Task 2
- **Issue:** If the Purge marker lived in a deleted segment, entries physically left in the first surviving segment at or below the purged id would have been indexed and could resurrect purged entries or trip the continuity check.
- **Fix:** Skipped on scan, and each preamble re-applies its purge point. Test: `entries_at_or_below_the_purge_point_in_surviving_segments_are_ignored`.
- **Files modified:** src/wal/recovery.rs
- **Commit:** 10c4799

**3. [Rule 2 - Missing critical functionality] Flushed watermark clamp on truncate; racing reads**
- **Found during:** Task 3
- **Issue:** Without clamping, entries rewritten after a truncation inherit the old watermark and could be evicted before being durable, violating the "never evict unflushed" guarantee.
- **Fix:** `EntryCache::truncate` clamps the watermark (`None` resets it). `read_entry` returns `Ok(None)` if the location vanished from the index while the disk read was in flight. Tests: `truncate_clamps_the_flushed_watermark`, `disk_read_detects_wrong_record_and_tolerates_concurrent_purge`.
- **Files modified:** src/wal/cache.rs
- **Commit:** 23198e7

### Test adjustments

- `recovery_stats_report_torn_bytes` uses 100-byte entry bodies. With 40-byte bodies the whole unsynced batch is under 301 differing bytes, so `power_loss(301)` kept everything and left no torn bytes.
- `log_index_push_requires_contiguous_indices` is split by `cfg(debug_assertions)`: it expects a panic in debug builds and an `Err` in release builds, matching the plan's "debug assertion and a returned WalError in release".

### Process note

An unscoped `cargo fmt` reformatted unrelated files; those were reverted file by file with `git checkout -- <path>` before any commit. Only `src/wal/recovery.rs` and `src/wal/cache.rs` are changed by this plan.

## Known Stubs

None.

## Threat Flags

None. Mitigations T-03-17, T-03-18 and T-03-19 are implemented and tested as planned.

## Self-Check: PASSED

- src/wal/recovery.rs and src/wal/cache.rs exist.
- Commits abc8505, 10c4799, 23198e7 are ancestors of HEAD.
