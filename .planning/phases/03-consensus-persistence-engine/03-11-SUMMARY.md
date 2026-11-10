---
phase: 03-consensus-persistence-engine
plan: 11
subsystem: persistence
tags: [wal, openraft, log-storage, group-commit, durability-ordering, bench]
requires: ["03-01", "03-07", "03-08"]
provides:
  - "wal::store::WalLogStore: open(dir, cfg), open_with_io(Box<dyn SegmentIo>, cfg), impl RaftLogStorage<TypeConfig>, async shutdown(self), wal_stats(), recovery_stats()"
  - "wal::store::WalLogReader: Clone, impl RaftLogReader<TypeConfig> (try_get_log_entries, read_vote), read-only over shared cache, offset index and SegmentRead"
  - "wal::store::EntryBody: WalPayload adapter that postcard-encodes Entry<TypeConfig> on the writer thread"
  - "benches/wal.rs: informational wal/append_ack Criterion group plus [tail] and [fsync] reports"
affects: [03-13, 03-14]
tech-stack:
  added: []
  patterns: [single ring producer owned by the &mut self store, store-side cache insert before submit, ack-awaited truncate/purge/vote, yield-then-short-sleep submit loop, vote and live bounds shared with readers through one RwLock<Meta>]
key-files:
  created:
    - benches/wal.rs
  modified:
    - src/wal/store.rs
    - Cargo.toml
key-decisions:
  - "Readers are bounded by a store-maintained Meta { vote, first, next } rather than by the offset index, because the index is populated by the writer thread slightly after append returns while the cache (filled by the store) is already readable"
  - "A rejected append (writer already failed) completes its IOFlushed with Err and also returns Err, so no callback is ever left uncompleted"
  - "save_committed(None) is persisted as an empty blob and decoded back to None on recovery"
  - "Submit loop yields 64 times, then sleeps 50us per retry: a full ring never blocks a worker, and a long fsync stall does not burn a core"
  - "last_log_id after recovery is read by decoding the entry at the last indexed location (None -> purged id), matching openraft's rule that last_log_id falls back to last_purged_log_id"
metrics:
  tasks: 3
  completed: 2026-11-09
status: complete
actuals:
  tokens: 26000
  tasks: 3
  commits: 3
commits: 3
plan_head_before: cc4c96f5a9df3c66a4a81ee80a15e75fbfdae695
plan_head_after: f8a15a072a423c02457b26de422bd8b19eeef5ad
---

# Phase 3 Plan 11: openraft WAL log store and informational bench Summary

`WalLogStore` implements openraft `RaftLogStorage` and `RaftLogReader` on top of the group-commit writer, recovery and entry cache: appends are readable immediately and acknowledged exactly once after the covering fsync, vote, truncate and purge return only when durable, and a full ring yields instead of blocking a Tokio worker.

## What was built

- **`src/wal/store.rs`**
  - `WalLogStore::open_with_io`: runs `recover`, keeps `RecoveredLog::stats()` for `recovery_stats()`, decodes the recovered vote, purged id and committed id, builds `LogShared` with a cold cache, finds `last_log_id` by decoding the last indexed entry from disk (purged id when the log is empty), then spawns the writer with a `SharedObserver`.
  - `append`: inserts each entry into the cache itself (size estimate key + value + 32), updates `last_log_id` and bounds, submits one `WalOp::Entry` per entry with an `EntryBody` payload (encoding happens on the writer thread); only the last op carries the `Ack`, a closure that calls `IOFlushed::io_completed` once. An empty append completes the callback immediately.
  - `save_vote`, `truncate_after`, `purge` submit with a oneshot ack and await it; `save_committed` is fire-and-forget; `read_committed` is served from memory.
  - `shutdown(self)` joins the writer in `spawn_blocking`; plain `Drop` falls back to `WalHandle::drop`, which joins synchronously.
  - `WalLogReader`: cache hit else one positional read via `cache::read_entry`; stops at the first hole instead of skipping it.
- **`benches/wal.rs`** and the `[[bench]] name = "wal"` entry: three informational scenarios plus a Criterion `wal/append_ack/fakeio_round_trip` function. No latency assertions.

## Verification

- `cargo test --lib wal::` : 97 passed (14 are `wal::store`).
- `bash scripts/check-warnings.sh` exits 0 (also compiles the new bench via `--all-targets`).
- `cargo bench --bench wal -- --test` runs and prints `[tail]` and `[fsync]` lines.
- Mutation checks (temporary, reverted, writer.rs untouched in the final tree):
  - removing the `sync_dir` after `create_segment` in `roll` makes `acked_append_in_fresh_and_rolled_segments_survives_crash` fail;
  - removing the `sync_active` after the purge marker in `run_purge` makes both `purge_marker_synced_before_first_delete_through_store` and `purge_crash_between_marker_and_deletions_reopens_cleanly` fail.
  The durability-ordering tests therefore have teeth; no writer or recovery bug was found, so neither `writer.rs` nor `recovery.rs` was modified.

### Tests in `wal::store`

`append_vote_restart` (tracer), `truncate_marker`, `truncate_everything_then_reappend`, `preamble_survives_purge`, `purge_everything_keeps_last_log_id`, `append_visible_before_flush`, `append_empty_completes_callback`, `read_committed_roundtrip`, `log_reader_serves_evicted_entries_from_disk`, `ring_full_backpressure_yields`, `failed_store_rejects_ops_and_completes_callbacks_once`, `acked_append_in_fresh_and_rolled_segments_survives_crash`, `purge_crash_between_marker_and_deletions_reopens_cleanly` (n = 1 and 2), `purge_marker_synced_before_first_delete_through_store`.

## Bench output (INFORMATIONAL, Apple silicon macOS, `--test` run)

NFR-1 and NFR-2 are NOT claimed and NOT validated by these numbers. On this host `File::sync_data` is `F_FULLFSYNC`; the manual verification on a Linux NVMe host remains open in 03-VALIDATION.md.

```
[tail] fakeio zero delay, window 256   n=200000 p50=1082000ns p99=2328708ns p99.9=14806416ns max=20507375ns
[tail] stdfs always, 64 in flight      n=5000   p50=7954583ns p99=13481708ns p99.9=13482791ns max=13483375ns
[fsync] stdfs always, 64 in flight     p50=4096us p99=8192us max=8192us batches=128 records=5001 syncs=128
[tail] stdfs always, 1 in flight       n=300    p50=4002417ns p99=6058417ns p99.9=9751291ns max=9751291ns
[fsync] stdfs always, 1 in flight      p50=4096us p99=8192us max=16384us batches=300 records=301 syncs=300
```

Group-commit amortization is visible: 5001 records were covered by 128 syncs with 64 in flight, versus one sync per record with a single op in flight. fsync percentiles are upper edges of log2 microsecond buckets. The FakeIo scenario's window-256 latency is dominated by queueing behind the writer, not by a single op's service time.

## Deviations from Plan

None - plan executed as written. Two small test-side adjustments: the truncate-marker test compares only the written (non-zero) prefix of the preallocated segment, because the zero-filled tail legitimately changes when later records land; and the backpressure test uses 12000 single-entry appends to guarantee the 8192-slot ring plus the writer's pending queue overflows during the 300 ms stall.

## Auth gates

None.

## Known Stubs

None.

## Threat Flags

None. T-03-27 (vote lost with a deleted segment) is covered end to end by `preamble_survives_purge`; T-03-28 (full ring blocks a worker) by `ring_full_backpressure_yields`; T-03-29 is handled by `StdFsIo` (plan 03-03).

## Notes for downstream plans

- `WalLogStore` is `Send + Sync` (the ring producer is only touched through `&mut self`), so it can be moved into `Raft::new`. Read `recovery_stats()` and `wal_stats()` before moving it.
- After `store.shutdown().await` or a plain drop the writer thread has been joined, so a `FakeIo` handle can be reopened immediately.
- A dropped store inside a Tokio runtime blocks that thread for the join; prefer `shutdown().await`.
- `NFR-1` and `NFR-2` are deliberately absent from this plan's requirements; do not mark them complete from the bench output.

## Self-Check: PASSED

- src/wal/store.rs, benches/wal.rs, Cargo.toml bench entry: present.
- Commits aef407a, 1fa61ad, f8a15a0: ancestors of HEAD.
