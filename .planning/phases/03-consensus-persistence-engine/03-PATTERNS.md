# Phase 3: Consensus & Persistence Engine - Pattern Map

**Mapped:** 2026-11-09
**Files analyzed:** 36 (2 modified, 34 new incl. Cargo/lib wiring)
**Analogs found:** 14 strong / 36. The repo is small (1211 LOC, 3 modules + 1 bench, no `tests/` dir). Most Phase 3 code is greenfield (I/O, network, Raft). Analogs are conventions-level, not structural. All analog paths are git-tracked (verified via `git ls-files`).

## Repo conventions (apply to ALL new files)

- Module doc comment `//!` at top of every file explaining design and invariants (see `src/storage/index.rs:1-13`, `src/ipc/ring_buffer.rs:1-13`, `src/engine.rs:1-8`).
- Dir modules: `mod.rs` is ~5 lines: `//!` doc, `pub mod x;`, `pub use x::{...};` (`src/storage/mod.rs`).
- Errors: library code uses small hand-written `#[derive(Debug, Clone, PartialEq, Eq)] pub enum` error types (`CasError`, `src/storage/index.rs:17-22`); `anyhow` only in `main.rs`. No `thiserror` dep exists; add it only if RESEARCH mandates, otherwise hand-write enums + `impl Display`.
- Every `unsafe` has a `// SAFETY:` comment immediately above/inside; `unsafe impl Send/Sync` carries a comment justifying (`src/ring_buffer.rs:33-36`, `src/arena/slab.rs:34-36`).
- Tests: in-module `#[cfg(test)] mod tests { use super::*; ... }`, names `test_<thing>_<behavior>`; deterministic xorshift for pseudo-random mixes (no rand dep); threads via `std::thread::spawn` + `Arc` + `join().unwrap()`.
- Zero compiler warnings. Phase 2 baseline: 17 tests pass.
- Edition 2021, deps in `Cargo.toml` flat list; `[[bench]] name=... harness=false` per bench.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match |
|---|---|---|---|---|
| `Cargo.toml` (modify) | config | n/a | itself (`[[bench]] local_engine`) | exact |
| `src/lib.rs` (modify) | config | n/a | itself (4 `pub mod`) | exact |
| `src/storage/index.rs` (modify) | storage / lock-free | CRUD, epoch | itself | exact |
| `src/engine.rs` (modify) | service | request-response | itself | exact |
| `src/wal/mod.rs`, `wire/mod.rs`, `raft/mod.rs`, `snapshot/mod.rs`, `net/mod.rs` | module root | n/a | `src/storage/mod.rs` | exact |
| `src/wal/io.rs` (SegmentIo, StdFsIo, FakeIo) | utility / trait | file-I/O | none (test-double style from `engine.rs` tests) | none |
| `src/wal/record.rs` | utility | transform (encode/decode+CRC) | `src/ipc/ring_buffer.rs` (small-primitive + inline tests) | partial |
| `src/wal/segment.rs` | utility | file-I/O | none | none |
| `src/wal/writer.rs` | service (thread) | event-driven, ring-drain, batch | `src/ipc/ring_buffer.rs` Consumer + `engine.rs::apply_batch` | role-match |
| `src/wal/recovery.rs` | service | file-I/O, batch | none (test style: `index.rs` tests) | none |
| `src/wal/cache.rs` | utility | CRUD | `src/arena/slab.rs` (budgeted pool) | partial |
| `src/wal/store.rs` (WalLogStore) | service | event-driven | `engine.rs` (assembles parts + holds Producer) | partial |
| `src/wire/frame.rs`, `hello.rs` | utility | transform / request-response | none (ring_buffer inline test style) | none |
| `src/raft/types.rs` | model | n/a | `engine.rs::Command` (`engine.rs:41-45`) | role-match |
| `src/raft/state_machine.rs` | service | CRUD apply | `engine.rs::LocalEngine` | role-match |
| `src/raft/config.rs`, `node.rs` | config / service | request-response | `src/main.rs` `Config` (clap) | partial |
| `src/net/client.rs`, `server.rs`, `fake.rs` | service | request-response, streaming | none | none |
| `src/snapshot/format.rs`, `writer.rs`, `reader.rs`, `store.rs` | utility/service | file-I/O, streaming scan | `index.rs::len` (iteration) | partial |
| `tests/cluster.rs`, `transport.rs`, `tcp_smoke.rs` | test | integration | none (no `tests/` yet); style from `engine.rs` concurrent tests | none |
| `benches/wal.rs` | bench | batch | `benches/local_engine.rs` | exact |

## Pattern Assignments

### `src/storage/index.rs` (MODIFY: stamped cell, scan API, `*_at`)

**Current shape to replace** (`index.rs:26-28`, `Slot { value: Atomic<Bytes> }`). New: `Slot { cell: Atomic<Versioned> }` with `struct Versioned { index: u64, value: Option<Bytes> }` so (value, stamp) swap atomically; tombstones are `Versioned{value:None, index}` not null.

**Imports to keep** (`index.rs:15-18`):
```rust
use bytes::Bytes;
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
```

**Epoch-pin / retire convention** (copy exactly; `index.rs:70-79`):
```rust
let guard = epoch::pin();
let old = entry.value().value.swap(Owned::new(value), AcqRel, &guard);
if !old.is_null() {
    // SAFETY: `old` was unlinked by the swap; no new reader can reach it.
    unsafe { guard.defer_destroy(old) };
}
```
**Read**: `load(Acquire,&guard)` then `unsafe { p.as_ref() }.cloned()` with `// SAFETY: pointer is protected by guard` (`index.rs:60-64`).
**Drop of Slot** uses `epoch::unprotected()` + `into_owned()` under `&mut self` (`index.rs:41-50`); keep, but now the pointer may be non-null for tombstones.

**CAS retry loop** (`index.rs:103-137`): reclaim own allocation on lost race (`Err(back) => new = back`). The `*_at(index, ...)` variants (put_at/delete_at/cas_at) must add skip-if-already-applied: load cell, `if cell.index >= index { return skipped }` before the CAS; stamp comparison must be inside the same CAS loop (re-check each iteration).

**Scan API rules** (RESEARCH line 362, 376): do NOT copy `len()` pattern (`index.rs:145-152`, one pin for whole iteration). Instead per key: `let guard = epoch::pin();` load, clone `Bytes` + stamp, drop guard, then advance iterator (`SkipMap::iter().next()` pins itself). Scan yields live keys, tombstones with `stamp > a`, drops tombstones with `stamp <= a`.

**`len()`/`get()`** must treat `value: None` as absent (previously `is_null()` filter, `index.rs:147-151`).

**Tests**: keep all existing tests unchanged as regression (`test_basic_put_get_delete`, `test_cas_semantics`, `test_ordered_len`, `test_lock_free_index_concurrent_ops`, `test_concurrent_cas_counter_is_exact`); add `test_*_at_skips_stale`, `test_tombstone_stamp_survives`, scan tests in same `mod tests`. Reuse the xorshift mix (`index.rs:~236-240`).

---

### `src/engine.rs` (MODIFY: `apply_at`, restore/iter)

**Analog:** itself. Keep `materialize` (`engine.rs:60-73`) as the only slab-copy path; `apply_at(index, &KvRequest)` must route Set/Delete/Cas through `materialize` then `index.*_at`.
Existing `Command` enum is Set/Delete only (`engine.rs:41-45`); do not widen it for Raft; put CAS-bearing `KvRequest` in `src/raft/types.rs`. Keep `get/set/delete/cas/apply_batch/slab()/index()` signatures stable (bench depends on `get`/`set`).
Test style to mirror (`engine.rs:~100-205`): `test_engine_*` names; slab recycling test polls `crossbeam_epoch::pin().flush()` until `live_blocks()` drains (use same polling approach for `snapshot::concurrent_scan` boundedness check; never assert immediately, epoch advancement is delayed by other tests). Re-run `benches/local_engine.rs` after change; Phase 2 GET p99 baseline 250ns/542ns must not regress.

---

### `src/wal/writer.rs` (service thread; event-driven, group commit)

**Analog:** `src/ipc/ring_buffer.rs` (Producer/Consumer) and `engine.rs::apply_batch`.
**Ring conventions:** SPSC enforced by non-Clone handles from `RingBuffer::<T, N>::channel()` (`ring_buffer.rs:47`); N must be power of two (compile-time assert, `ring_buffer.rs:38-45`). `Producer` is owned by exactly one task (`WalLogStore`, since `RaftLogStorage` methods take `&mut self`).
**Drain pattern** (`engine.rs:~96-104`):
```rust
rx.consume_batch(max, |cmd| match cmd { ... })   // returns count; tail published once
```
Writer loop: `consume_batch` -> write all -> one `sync` -> fire acks; if 0 consumed, park/unpark (RESEARCH). Backpressure: `try_publish` returns `Err(item)` when full (`ring_buffer.rs:101-106`); `publish_spin` exists (`:119`) but avoid spinning in async context.
**Error handling:** on fsync error fail all pending acks and stop (fail-stop test). Use plain enum error, not panic. Tests via `FakeIo` (counts syncs, injects delay/errors).
Note: macOS `sync_data` is F_FULLFSYNC (~4ms p50); tests/benches must use FakeIo or accept it.

### `src/wal/io.rs` (trait SegmentIo + StdFsIo + FakeIo)
No analog. Define trait `SegmentIo: Send + 'static`; `FakeIo` lives in `#[cfg(test)]` or a `test-util` feature (integration tests in `tests/` cannot see `cfg(test)` items, so `tests/*.rs` that need FakeIo require the feature or use `tempfile` + StdFsIo). Decide once and apply to `net/fake.rs` too. Share state with `Arc<parking_lot::Mutex<..>>` (parking_lot already a dep).

### `src/wal/record.rs`, `segment.rs`, `recovery.rs`, `cache.rs`, `store.rs`
No structural analog. Follow repo conventions only: `//!` design doc, hand-written error enum, inline `#[cfg(test)]`, SAFETY comments. Test names per VALIDATION map: `wal::record`, `wal::recovery::corrupt_middle`, `wal::store::preamble_survives_purge`, `wal::store::truncate_marker` -- test fn module paths must match so `cargo test --lib wal::recovery` filters work (put tests in the file's own `mod tests` or nested `mod corrupt_middle`-style names as specified; the filter is a path prefix).
`cache.rs`: byte-budgeted, loosely modeled on `SlabPool` accounting (`src/arena/slab.rs`, `live_blocks()` stat accessor style).
`store.rs`: assembles parts like `LocalEngine` struct (`engine.rs:47-51`: owns components, exposes accessors).

### `src/wire/frame.rs`, `hello.rs`
No analog. Constraint: NO openraft imports (Phase 5 reuse). Unit tests inline (`cargo test --lib wire::`): round-trip, oversize reject, handshake mismatch. Uses `bytes::{Bytes, BytesMut}` (dep exists); tokio-util `LengthDelimitedCodec` is a new dep per RESEARCH.

### `src/raft/types.rs` (model)
**Analog:** `engine.rs:41-45`
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command { Set { key: Bytes, value: Bytes }, Delete { key: Bytes } }
```
Mirror derive set + `Bytes` fields; `KvRequest` adds `Cas { key, expected: Option<Bytes>, desired: Option<Bytes> }`. Serde derives are already available (`serde` with `derive`).

### `src/raft/state_machine.rs`
**Analog:** `engine.rs::LocalEngine` (thin wrapper delegating to index). `apply` -> `engine.apply_at(log_index, &req)`; responses from `CasError` (`index.rs:17-22`) map to `KvResponse`. Required tests: `fuzzy_replay_model` (proptest, deterministic xorshift fallback fine), `cas_counterexample`.

### `src/raft/config.rs` / `node.rs`
**Analog:** `src/main.rs:3-14` clap `Config` (`node_id: u64`, `bind: String`, `peers: String` comma list). Derive `NodeConfig` from it; do not change existing flag names. Bootstrap: `if !raft.is_initialized().await? { raft.initialize(members) }` only on designated node. `main.rs` currently only prints (`main.rs:16-27`); wiring there is optional.

### `src/net/*`, `src/snapshot/*`
No analog for sockets. Snapshot scan: per-key short pin (see index.rs section). Format tests: truncation and bit-flip detection, `snapshot::format`, `snapshot::concurrent_scan` run with `--release` (writers via `std::thread::spawn` + `Arc<LocalEngine>`, same shape as `engine.rs::test_engine_concurrent_mixed`).

### `tests/cluster.rs`, `tests/transport.rs`, `tests/tcp_smoke.rs`
No `tests/` dir exists. Use `#[tokio::test]` (tokio `full` already). Import from crate: `use lightcycle::engine::LocalEngine;` (same as bench, `local_engine.rs:8`). Add dev-deps `tempfile`, `proptest`. Test fn names must match VALIDATION filters (`pipelined_in_order`, `snapshot_does_not_starve_heartbeat`, `membership_*`, `single_node_restart`, `kill_restart_snapshot_catchup`).

### `benches/wal.rs`
**Analog:** `benches/local_engine.rs` (exact style).
**Structure to copy:**
- Header `//!` stating what is measured; imports (`local_engine.rs:7-11`):
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lightcycle::...;
use std::time::Instant;
```
- Helpers `percentile` (`:31-34`) and `tail_report` (`:51-62`) printing `[tail] label n= p50= p99= p99.9= max=`; copy verbatim (no shared bench util module exists).
- Explicit pre-Criterion report fn called first in the bench fn (`tail_latency_report();` `:93`).
- Multi-thread measurement via `iter_custom` with `Barrier` (`:~117-146`).
- Group naming `"local_engine/single_thread"` -> use `"wal/append_ack"`.
- Config block (`:151-157`):
```rust
criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20)
        .warm_up_time(Duration::from_millis(500)).measurement_time(Duration::from_secs(2));
    targets = bench_wal
}
criterion_main!(benches);
```
- Register in `Cargo.toml` (mirror existing):
```toml
[[bench]]
name = "wal"
harness = false
```
Bench is informational (NFR-1/2); do not assert pass/fail hard on fsync latency (macOS ~4ms).

### `Cargo.toml` / `src/lib.rs`
`lib.rs` currently: `pub mod arena; pub mod engine; pub mod ipc; pub mod storage;` (alphabetical). Add `net, raft, snapshot, wal, wire` alphabetically. Add deps (openraft alpha.38 per RESEARCH Wave 0 API gate, tokio-util, crc32c) in the flat `[dependencies]` list; dev-deps `tempfile`, `proptest` beside `criterion = "0.5"`.

## Shared Patterns

### Epoch pin / unsafe discipline
Source: `src/storage/index.rs:60-79, 41-50`. Apply to index.rs changes, snapshot scan. Short pins; `defer_destroy` after unlink; `// SAFETY:` on every unsafe.

### Ring-buffer handoff
Source: `src/ipc/ring_buffer.rs:47, 101, 153`; `engine.rs::apply_batch`. Apply to wal writer/store.

### Error types
Source: `src/storage/index.rs:17-22`. Small derive-heavy enums, carry observed state in the variant.

### Testing under shared epoch collector
Source: `engine.rs::test_engine_slab_blocks_are_recycled`. Poll with deadline + `pin().flush()`; never assert immediate reclamation.

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `src/wal/{io,segment,recovery,record,cache,store}.rs` | storage | file-I/O | No file I/O in repo |
| `src/wire/*`, `src/net/*` | transport | streaming | No networking in repo |
| `src/raft/node.rs` | service | async | No openraft use yet; use RESEARCH |
| `tests/*.rs` | integration | n/a | No `tests/` dir yet |

## Metadata

**Analog search scope:** all tracked files under `src/`, `benches/`, `Cargo.toml` (11 Rust files, 1211 LOC).
**Note:** RESEARCH layout includes `src/net/` (client/server/fake) beyond the scope list; mapped above.
**Pattern extraction date:** 2026-11-09
