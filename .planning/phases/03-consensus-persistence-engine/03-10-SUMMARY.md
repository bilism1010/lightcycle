---
phase: 03-consensus-persistence-engine
plan: 10
subsystem: persistence
tags: [snapshot, fuzzy-scan, crc32c, frame-codec, epoch-reclamation, crash-safety]
requires: ["03-01", "03-02", "03-04", "03-05"]
provides:
  - "snapshot::format: SnapshotFileWriter / SnapshotFileReader / SnapshotItem / SnapshotError, frame kinds SNAP_HEADER 32, SNAP_BLOCK 33, SNAP_FOOTER 34"
  - "snapshot::writer::build_snapshot_file(engine, meta, path) -> Result<u64, SnapshotError>"
  - "snapshot::reader::{load_snapshot, validate_file, read_header}"
  - "snapshot::store::{SnapshotStore, Stage}: open, latest, build, build_with_hook, install_received, staging_path, read_chunk"
affects: [03-12, 03-13, 03-15, 03-09]
tech-stack:
  added: []
  patterns: [snapshot file is plain shared-codec frames so a byte range is an InstallSnapshot chunk, stage-hook crash injection, tmp-sync-rename-dirsync-then-delete publish order, per-key epoch pin scan]
key-files:
  created:
    - src/snapshot/format.rs
    - src/snapshot/writer.rs
    - src/snapshot/reader.rs
    - src/snapshot/store.rs
  modified:
    - src/snapshot/mod.rs
    - src/wire/frame.rs
key-decisions:
  - "Block bodies are exactly [count][records][crc]; the footer's rolling CRC is crc32c_append over whole block bodies (CRC trailer included) and total_payload_bytes is the sum of key and value lengths"
  - "Reader checks magic, then version, then header CRC, so a future format version is reported as UnsupportedVersion rather than HeaderCrc"
  - "latest() validates only the header frame and its agreement with the file name (cheap, called often); full block validation happens in load_snapshot and install_received"
  - "install_received deletes snapshots strictly older than the installed one; open() deletes everything but the highest (index, term)"
  - "Two SnapshotError variants added beyond the interface list: RecordTooLarge (writer, record cannot fit a 16 MiB frame) and MetaMismatch (install_received)"
metrics:
  tasks: 3
  completed: 2026-11-09
status: complete
actuals:
  tokens: 36000
  tasks: 3
  commits: 4
plan_head_before: ad1a45623822cf0783e856f47c8d2e93a973086b
plan_head_after: cf2d9143c77ed47f4ba9ee8e0496da955c16e2c7
---

# Phase 3 Plan 10: Fuzzy snapshot file layer Summary

A live `LocalEngine` is scanned without stopping writers into a CRC-protected frame file, restored into a fresh engine, and kept on disk as exactly one crash-safe snapshot, with restore plus replay proven equal to the live state under a 1M-key concurrent mixed-write stress test.

## What was built

- **`format.rs`:** header, block and footer frames on the shared `FrameCodec` (lane 0, flags 0, `req_id` = frame sequence number, all validated on read). CRC32C on the header, on every block and on the footer; footer carries total records, total payload bytes and a rolling CRC. `SnapshotFileWriter<W: Write>` packs records into 64 KiB blocks (a record larger than that gets its own block). `SnapshotFileReader<R: Read>` reads 256 KiB at a time, yields one block at a time (bounded memory) and rejects wrong kind/flags/lane/sequence, bad CRCs, footer count or rolling-CRC mismatch, truncation (including at frame boundaries) and trailing bytes.
- **`writer.rs`:** `build_snapshot_file` creates the file with `create_new` and mode 0600, scans with `threshold = meta.last_log_id index or 0` (live keys any stamp, tombstones above the threshold kept, tombstones at or below dropped: R-2), `sync_all`, returns the length.
- **`reader.rs`:** `load_snapshot` streams records into a fresh engine with `load_record`; `validate_file` is the same pass without an engine; `read_header` checks only the header frame.
- **`store.rs`:** `SnapshotStore` with `snap-<index:020>-<term:020>.snap/.tmp` names parsed strictly (non-matching names and symlinks are ignored), directory created 0700. Publish order: write `.tmp` + `sync_all`, rename, fsync directory, delete older, fsync directory. `build_with_hook` takes a `Stage` hook whose error aborts at that stage like a crash. `open()` removes leftover strict-named `.tmp` files and all but the highest `.snap`. `latest()` returns `CorruptNeedsWipe` for an unreadable header or a header that disagrees with the file name, with no fallback to an older file. `install_received` fully validates the received file, compares its header meta with the expected meta, then renames, syncs and trims. `read_chunk(path, offset, len)` serves byte ranges.

## Measured stress-test numbers

`cargo test --release --lib snapshot::concurrent_scan -- --nocapture` (1,000,000 preloaded keys, 64-byte values, one writer thread unthrottled, 70/15/15 set/delete/cas, key space 1.1M), repeated runs:

| Run | scan time | writer ops during scan | max slab live_blocks | live keys start/end | bound used |
|-----|-----------|------------------------|----------------------|---------------------|------------|
| 1 | 91.7 ms | 94,002 | 1,000,072 | 1,000,000 / 994,355 | 1,023,500 |
| 2 | 98.7 ms | 93,102 | 1,000,070 | 1,000,000 / 994,426 | 1,023,275 |
| 3 | 88.2 ms | 79,755 | 1,000,069 | 1,000,000 / 995,213 | 1,019,938 |

Across about ten runs the scan took 80 to 127 ms and the writer did 80k to 125k ops during it. The measured excess of live blocks over the live key count is about 40 to 70 blocks, so per-key pinning keeps retired memory essentially flat. Negative control (temporarily holding one `epoch::pin()` across the whole `build_snapshot_file` call): peak live blocks 1,104,170 against a bound of 1,031,345, test failed as intended, then the control was removed. After the scan, `live_blocks` drains to within 1,000 of the live key count (polled with `epoch::pin().flush()`).

Choice of key count: at 300k keys the scan took 41 ms and the writer managed only 53k ops (about 45k retirements, under the 50k bound), which would make the bound vacuous; 1M keys makes the scan long enough that the writer retires well past the noise floor. Peak RSS of the test is about 590 MB (live engine plus restored engine).

Bound as implemented: `max_live_blocks <= max(start_live, end_live) + min(50_000, ops_during_scan / 4)`. This never exceeds the plan's live-plus-50,000, and tightens automatically when the writer is slower, so the check keeps its teeth. Release runs additionally assert at least 20,000 writer ops during the scan. The debug profile uses 20,000 keys and skips that guard.

## Verification

- `cargo test --lib -- snapshot::`: 26 passed (format 9, writer 3, store 13 incl. exhaustive single-bit-flip and truncation at every offset, crash-stage tests, corrupt-latest, numeric names, chunked copy, install rejection; 1 concurrent_scan).
- `cargo test --release --lib snapshot::concurrent_scan`: passes, repeatedly.
- Full `cargo test --lib`: 133 passed. `bash scripts/check-warnings.sh` exits 0.
- Tracer gate: Task 1 `<verify>` (roundtrip + format tests) passed before Tasks 2 and 3 expanded the slice.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] FrameCodec cap check misread body bytes as a length prefix on partial reads**
- **Found during:** Task 1 (a 5,000-key snapshot with about 10 blocks failed with `FrameCorrupt` on read)
- **Issue:** `tokio_util`'s `LengthDelimitedCodec` consumes the 4-byte prefix as soon as it sees it and then waits for the body. `FrameCodec::decode` (plan 03-04) re-ran its own cap check on `src[0..4]` on every call, so a retry with a partially received body treated body bytes as a declared length, producing a spurious `TooLarge` or silently skipping the cap. Any frame split across reads was affected, including the TCP transport of plan 03-09.
- **Fix:** added a `mid_frame` flag so the cap check runs only at the start of a frame; regression test `frame_split_across_partial_reads_decodes` feeds four frames in 1, 3, 7 and 100 byte steps.
- **Files modified:** `src/wire/frame.rs`
- **Commit:** 2a134b5

**2. [Plan interface additions] Two extra `SnapshotError` variants and a `staging_path` helper**
- `RecordTooLarge(usize)` for a record that cannot fit a 16 MiB frame (reporting it as `FrameCorrupt` would hide a write-side limit) and `MetaMismatch` for `install_received`; `SnapshotStore::staging_path(meta)` gives the transport a deterministic strict-named `.tmp` path that `open()` will clean up. No removal or change of anything in the interface block.

**3. [Test tuning] Stress-test sizing and bound**
- Key count raised from the plan's "multi-hundred-thousand" to 1,000,000 and the slack tightened adaptively, as described above, so the bound is meaningful on this machine.

## Known Limitations

- A single record larger than about 16 MiB (key plus value) cannot be snapshotted; `build_snapshot_file` fails with `RecordTooLarge`. The engine currently imposes no value size limit; a later plan should either cap values at the API boundary or raise `MAX_FRAME_LEN`.
- `latest()` does not read the block data, so mid-file corruption surfaces at `load_snapshot` / `install_received` time rather than at open.

## Known Stubs

None.

## Threat Flags

None beyond the plan's threat model. T-03-24, T-03-25 and T-03-26 are mitigated as specified (CRC and sequence validation with exhaustive bit-flip and truncation tests; per-key pin with a bounded-memory stress test; tmp, sync, rename, dir sync, delete order with stage-hook crash tests and a hard error on a corrupt latest snapshot).

## Self-Check: PASSED

- Files found: `src/snapshot/format.rs`, `writer.rs`, `reader.rs`, `store.rs`, `mod.rs`, `src/wire/frame.rs`.
- Commits found: 2a134b5 (frame fix), b92c606 (format/writer/reader), 8845a24 (store), cf2d914 (stress test).
