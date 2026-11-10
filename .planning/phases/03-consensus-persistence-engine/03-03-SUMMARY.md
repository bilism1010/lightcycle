---
phase: 03-consensus-persistence-engine
plan: 03
subsystem: storage
tags: [wal, crc32c, segment-io, fake-io, durability, postcard]
requires: ["03-01"]
provides:
  - "src/wal/record.rs: encode_record / decode_record (CRC32C framing), RecordType, Decoded"
  - "src/wal/io.rs: SegmentIo, SegmentRead, StdFsIo, and test-util FakeIo/FakeStats/IoEvent"
  - "src/wal/segment.rs: segment naming, Preamble/PreambleState, WalStart, SegmentSummary, EntryLocation, WalObserver/NoopObserver, Fsync, WalConfig, WalError, fits"
  - "wal/mod.rs declaring all seven submodules plus doc-only stubs writer/recovery/cache/store"
affects: [03-07, 03-08, 03-11, 03-14]
tech-stack:
  added: []
  patterns:
    - "SegmentIo seam: plain std::fs on the caller's thread, no tokio::fs"
    - "FakeIo with a volatile/durable image pair and separate volatile/durable directory listings"
    - "numeric-only segment names; O_EXCL create with mode 0600 in a 0700 directory"
key-files:
  created:
    - src/wal/io.rs
    - src/wal/record.rs
    - src/wal/segment.rs
    - src/wal/writer.rs
    - src/wal/recovery.rs
    - src/wal/cache.rs
    - src/wal/store.rs
    - tests/wal_test_util_reachable.rs
  modified:
    - src/wal/mod.rs
key-decisions:
  - "Record layout [len u32 LE][crc32c u32 LE][type u8][payload] is the WAL file format; FORMAT_VERSION=1 in every preamble allows later migration"
  - "StdFsIo uses Unix-only std APIs (FileExt, DirBuilderExt, OpenOptionsExt); no cfg(not(unix)) fallback"
  - "FakeIo power_loss makes the surviving (possibly torn) image the new durable state"
requirements-completed: [FR-4, FR-6]
duration: ~30 min
completed: 2026-11-09
status: complete
actuals:
  tokens: 12500
  tasks: 2
  commits: 2
plan_head_before: 60abfd47818790e2917e1eebb2873ac016805efa
plan_head_after: 6b2072e5285c4e4162c041efd108edfe19399eb0
commits: 2
---

# Phase 3 Plan 03: WAL primitives Summary

**CRC32C record framing, the `SegmentIo` seam with a std::fs backend, and a deterministic `FakeIo` that models directory-entry durability, torn writes, crash/revive and an ordered event log, plus the shared WAL types so plans 03-07 and 03-08 can build in parallel.**

## Tasks

| Task | Name | Commit |
| ---- | ---- | ------ |
| 1 (tracer) | One record through SegmentIo on disk: record.rs, io.rs, FakeIo reachable from integration tests | 70c776e |
| 2 | Segment layout, shared WAL types and module reservations | 6b2072e |

## What was built

- `src/wal/record.rs`: `encode_record` / `decode_record` with `Decoded::{Record, End, Invalid}`. CRC32C covers the len bytes, the type byte and the payload. A fully zero header, or fewer than 9 bytes remaining, decodes as `End`; bad length, oversize length, short body, unknown type and CRC mismatch decode as `Invalid`. A test flips every bit of an encoded record and asserts none is accepted.
- `src/wal/io.rs`: `SegmentIo` / `SegmentRead` traits; `StdFsIo` (directory 0700, segments created with `create_new` and mode 0600 then `set_len`, `write_all_at`/`read_at`, `sync_data`, directory `sync_all`, shared open-handle cache so `reader()` is `Send + Sync`). The doc comment records that `sync_data` is `F_FULLFSYNC` on Apple targets (about 4 ms p50), so local latency numbers are correctness-only.
- `FakeIo` (gated `cfg(any(test, feature = "test-util"))`): volatile and durable images per segment; explicit durable vs volatile directory listing (create/delete take effect durably only through a successful `sync_dir`); `power_loss(keep)` removes never-dir-synced segments, resurrects deleted-but-not-dir-synced ones with their durable image, and keeps the first `keep` differing bytes as a torn write; `crash`/`revive`; `crash_before_nth_delete`; `fail_next_sync/dir_sync/write`; `set_sync_delay` slept without holding the state lock, with a crash check after the sleep so an interrupted sync neither persists nor returns Ok; `unsynced_bytes`, `durable_bytes`, `segment_bytes`, `durable_len`; optional ordered `IoEvent` log (off by default).
- `src/wal/segment.rs`: strict `segment_file_name`/`parse_segment_name` (16 lowercase hex plus `.wal`), postcard `Preamble`/`PreambleState` with `decode_preamble` rejecting any `format_version != 1`, `SegmentSummary`, `WalStart::fresh()`, `EntryLocation`, `WalObserver` + `NoopObserver`, `Fsync` (`is_unsafe`), `WalConfig` defaults (64 MiB segments, `Always`, 4 MiB batch, 256 MiB cache), `WalError` (thiserror), `fits`.
- `src/wal/mod.rs` declares `cache, io, record, recovery, segment, store, writer` and re-exports the shared types; `writer.rs`, `recovery.rs`, `cache.rs`, `store.rs` are doc-only reservations naming plans 03-07, 03-08, 03-08, 03-11.
- `tests/wal_test_util_reachable.rs`: `fake_io_reachable_from_integration_tests`.

## Verification

- `cargo test --lib -- wal::`: 30 passed, 0 failed.
- `cargo test --test wal_test_util_reachable`: 1 passed.
- `bash scripts/check-warnings.sh`: exit 0. `cargo check --lib` (without `test-util`) also clean.
- Tracer gate: the Task 1 `<verify>` command passed before and after the tracer commit, so expansion proceeded.

## Deviations from Plan

### Auto-fixed Issues

None for correctness. Additions beyond the listed tests (all within the files this plan owns):

**1. [Rule 2 - Missing test coverage] Extra tests**
- Added `two_records_back_to_back_then_end`, `zero_len_with_nonzero_header_is_invalid`, `unknown_type_is_invalid` (record), `std_fs_list_ignores_foreign_files_and_sorts`, `std_fs_rejects_non_directory_path`, `fake_io_reader_reads_volatile_image` (io), `wal_start_fresh_is_empty` (segment). These cover threat T-03-05/T-03-07 edges (non-segment files ignored, unknown type rejected) at negligible cost.

### Interface notes for downstream plans (no signature changes forced)

- `RecordType` gained a `from_u8` helper; `Decoded` derives `Debug, Clone, PartialEq, Eq`.
- `IoEvent::Write { len }` is a `usize`; `FakeStats` counters are `u64`; `durable_len`, `segment_bytes` and `durable_bytes` return `0` / empty for an absent segment rather than `Option`.
- `FakeIo::new()` exists (also `Default`); `FakeIo` implements `SegmentIo`, and `reader()` returns a separate `FakeReader` wrapper so `SegmentIo::read_at` and `SegmentRead::read_at` never collide on one type.
- `WalError::Corrupt.reason` is a `String`; `WalError::Io` has `#[from] io::Error`.
- `FakeIo::create_segment` initialises the durable image to zeros of the full size, so a freshly created, dir-synced segment shows zero `unsynced_bytes`; writes beyond the image length extend it (as a real file would).
- `crash_before_nth_delete` counts every `delete_segment` call made after arming, including calls for missing segments.
- After `power_loss`, the surviving torn bytes become the durable image (what is actually on disk now).

## Known Stubs

`src/wal/writer.rs`, `recovery.rs`, `cache.rs`, `store.rs` are intentionally doc-only module reservations; they are filled by plans 03-07, 03-08 and 03-11 and do not affect this plan's goal.

## Threat Flags

None. Mitigations in the plan's threat model are implemented and tested: T-03-05 (CRC32C, single-bit-flip test), T-03-06 (0700/0600 modes asserted), T-03-07 (strict numeric-only names, `create_new`).

## Self-Check: PASSED

- Files exist: src/wal/{io,record,segment,writer,recovery,cache,store,mod}.rs, tests/wal_test_util_reachable.rs.
- Commits 70c776e and 6b2072e are ancestors of HEAD.
