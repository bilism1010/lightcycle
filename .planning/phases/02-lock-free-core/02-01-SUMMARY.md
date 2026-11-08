---
phase: 02-lock-free-core
plan: 01
subsystem: core-engine
tags: [rust, lock-free, slab-allocator, epoch-reclamation, skiplist, disruptor, criterion]
requires: []
provides:
  - "arena::SlabPool: 64B-64KB cacheline-aligned slab allocator with lock-free free lists"
  - "storage::KvIndex: lock-free ordered KV index (get/put/delete/cas) with EBR"
  - "ipc::RingBuffer: Disruptor-style SPSC ring with cacheline-padded cursors"
  - "engine::LocalEngine: assembly of the above with slab-backed zero-copy values"
  - "benches/local_engine.rs: Criterion suite plus explicit P50/P99/P99.9 report"
affects: [03-consensus-persistence, 04-state-machine-integration, 06-benchmarking]
tech-stack:
  added: [crossbeam-epoch 0.9, crossbeam-skiplist 0.1, criterion 0.5 (dev)]
  patterns: [epoch-based reclamation, per-class lock-free free lists, SPSC type-enforced handles, cached opposing cursor]
key-files:
  created:
    - src/lib.rs
    - src/arena/mod.rs
    - src/arena/slab.rs
    - src/storage/mod.rs
    - src/storage/index.rs
    - src/ipc/mod.rs
    - src/ipc/ring_buffer.rs
    - src/engine.rs
    - benches/local_engine.rs
  modified:
    - Cargo.toml
    - Cargo.lock
key-decisions:
  - "Per-key value held in an epoch-managed Atomic<Bytes> slot inside the SkipMap, so put/delete/cas are single atomic pointer operations (linearizable CAS, which SkipMap alone cannot provide)"
  - "delete leaves a tombstone (empty slot) in the skiplist; physical compaction deferred"
  - "Ring buffer exposed as non-Clone Producer/Consumer handles so SPSC is enforced by the type system"
  - "Slab values wrapped via Bytes::from_owner; block returns to pool when the last Bytes reference (after epoch retirement) drops"
requirements-completed: []
duration: ~35 min
completed: 2026-11-08
status: complete
commits: 4
plan_head_before: 9e1ba7e535cd860a575d176100f8ef1d47d02538
plan_head_after: 5bb9716e4723f736af1e0fdbb27e5a2b2ebf7a04
actuals:
  tasks: 4
  commits: 4
---

# Phase 2 Plan 01: Lock-Free Memory Arena & Local KV Engine Summary

Cacheline-aligned slab arena, EBR-managed lock-free skiplist KV index with linearizable CAS, a Disruptor-style SPSC ring buffer, and a `LocalEngine` that ties them together, with Criterion benchmarks showing read P99 of 250ns (1 thread) / 542ns (8 threads) against the 150us target.

## Tasks

| Task | Name | Commit |
|------|------|--------|
| 1 | Cacheline-aligned arena and slab (tracer) | 6c80f07 |
| 2 | Lock-free KV index with epoch-based reclamation | c92d102 |
| 3 | Disruptor-style SPSC ring buffer | d4ffc52 |
| 4 | LocalEngine assembly and Criterion suite | 5bb9716 |

### Task 1: SlabPool
Eleven power-of-two size classes (64B to 64KB). Each class has a `SegQueue` free list; fresh chunks come from `std::alloc` with `Layout` align 64. `test_slab_alignment_and_lifecycle` asserts `ptr % 64 == 0` for all sizes and that 1,200 alloc/free cycles after the first round cause zero new system allocations. A concurrent test (8 threads x 5000 ops) bounds fresh allocations by peak concurrency.

### Task 2: KvIndex
`SkipMap<Bytes, Slot>` where `Slot` wraps `crossbeam_epoch::Atomic<Bytes>`. Replaced values go through `defer_destroy`. `test_lock_free_index_concurrent_ops` runs 16 threads, 100,000 total mixed put/get/delete ops with value-integrity assertions. A second test runs 16 threads x 2000 CAS increments and asserts the exact total (32,000).

### Task 3: RingBuffer
Const-generic power-of-two capacity (checked at compile time), `#[repr(align(64))]` head and tail cursors (test asserts size/alignment 64 and distinct lines), each side caching the opposing cursor, batched `consume_batch` that publishes the tail once and commits progress in a drop guard so a panicking callback cannot double-drop items. `test_ring_buffer_spsc_throughput`: 2M ordered messages, observed ~13.5 Mmsg/s in a debug build.

### Task 4: LocalEngine and benches
`LocalEngine` copies values into slab blocks exposed as zero-copy `Bytes`; oversize (>64KB) values fall back to heap. `apply_batch` drains `Command::{Set,Delete}` from a ring `Consumer`. `benches/local_engine.rs` (harness=false) has Criterion groups plus an explicit tail-latency report.

## Verification

- `cargo check --all-targets`: 0 warnings. `cargo test`: 17 passed, 0 warnings (run 5 times in succession, all green). `cargo check --benches`: passes.
- Slab alignment, 16-thread/100k-op test: pass.
- Benchmark run (`cargo bench --bench local_engine`, release profile, macOS, 128B values, 10k keys):

| Op | p50 | p99 | p99.9 |
|----|-----|-----|-------|
| GET 1 thread | 166ns | 250ns | 292ns |
| SET 1 thread | 208ns | 1.5us | 2.0us |
| GET 8 threads | 291ns | 542ns | 834ns |
| SET 8 threads | 708ns | 33.5us | 129us |

Criterion means: get_hit 87.7ns, set_overwrite 145.7ns single-threaded.

## P99 target status

The "P99 < 150us for memory reads" target is met for the local in-memory read path (542ns worst measured P99, ~275x margin). Caveats, stated plainly: this is a single-node, no-network, no-Raft measurement on one developer machine with `Instant` timing (itself ~tens of ns of overhead); it says nothing about the end-to-end NFR-1 cluster figure. One 8-thread GET sample hit a 1.2ms max (scheduler noise); that is outside P99.9. The P99 check in the bench prints PASS/FAIL but does not fail the build, so it is not a CI regression gate.

## Deviations from Plan

### Auto-fixed / added

**1. [Rule 3 - Blocking] Added `src/lib.rs`**
- **Issue:** The crate was binary-only; benches under `benches/` cannot import modules from a binary crate, and the plan's `src/arena`, `src/storage`, `src/ipc` modules need a root.
- **Fix:** Added a library target (`src/lib.rs`) exposing `arena`, `engine`, `ipc`, `storage`. `src/main.rs` is unchanged.
- **Commit:** 6c80f07 (extended in later tasks)

**2. [Rule 1 - Bug, test-only] Ring buffer test asserted on a stale cached cursor**
- **Found during:** Task 3. `consume_batch` correctly uses the cached head; my test expected a fresh view. The test was wrong, not the implementation; changed it to drain until empty.

**3. [Rule 1 - Bug, test-only] Slab-recycling test was flaky under parallel test execution**
- **Found during:** Task 4. Other tests' pinned threads delay global epoch advancement, so a fixed "fresh < 10k" assertion failed when run in parallel. Replaced with a bounded poll (10s) asserting retired blocks drain back to the pool (`live_blocks() == 1`).

### Design additions beyond the plan text
- `CasError::Mismatch { current }`, `Producer`/`Consumer` handle split, `Command` enum, and `LocalEngine::apply_batch` were my interpretation of the sparse contracts.

## Known limitations (not stubs)

- **Tombstones:** deleted keys stay in the skiplist as empty slots; key memory is not reclaimed. This slightly weakens NFR-4's "no unbounded retired-node accumulation" for churn-heavy keyspaces. Needs a compaction pass in a later phase.
- **Leaked slab blocks:** a `SlabBlock` dropped without `SlabPool::deallocate` leaks its chunk (documented). `LocalEngine` always routes through `SlabValue`, which deallocates on drop.
- **No per-thread slab caches:** RESEARCH.md suggested thread-local freelists; this uses one shared lock-free queue per class. Not measured as a bottleneck here.
- **Slab memory is never returned to the OS** until the pool is dropped, and there is no upper bound on pool size (NFR-3 "bound memory growth" is not yet enforced).
- **Verification limits:** "zero deadlocks / zero data races" is evidenced by passing stress tests only. No Miri, loom, or ThreadSanitizer run was performed. The ring buffer and slab contain `unsafe` that would benefit from a Miri pass.
- CPU pinning / NUMA affinity from RESEARCH.md was not implemented (out of the four plan tasks).

## Threat Flags

None. No network, auth, or file-access surface was added.

## Known Stubs

None.

## Self-Check: PASSED

- Files verified present: src/lib.rs, src/arena/{mod,slab}.rs, src/storage/{mod,index}.rs, src/ipc/{mod,ring_buffer}.rs, src/engine.rs, benches/local_engine.rs.
- Commits verified as ancestors of HEAD: 6c80f07, c92d102, d4ffc52, 5bb9716.
- `cargo test` 17/17 passing, 0 warnings; `cargo check --benches` passing.
