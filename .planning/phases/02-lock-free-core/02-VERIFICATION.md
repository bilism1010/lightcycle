---
phase: 02-lock-free-core
verified: 2026-11-08T00:00:00Z
status: human_needed
score: 6/7 must-haves verified
covered_files:
  - ".planning/phases/02-lock-free-core/02-01-SUMMARY.md"
  - ".planning/phases/02-lock-free-core/PLAN.md"
  - "benches/local_engine.rs"
  - "src/arena/slab.rs"
  - "src/engine.rs"
  - "src/ipc/ring_buffer.rs"
  - "src/storage/index.rs"
covered_digest: "v4:sha256:fc5b67703a7ec5b4fe1562fe53b7a45731578c2aac101fb029d87417ecf3961d"
behavior_unverified: 1
overrides_applied: 0
gaps: []
behavior_unverified_items:
  - truth: "High-concurrency read/write test (16 threads, 100k ops) completes with zero deadlocks and zero data races"
    test: "Run the unsafe-heavy modules (src/arena, src/ipc, src/storage + engine) under Miri (or loom for the ring buffer cursors and slab free list) and/or ThreadSanitizer on Linux."
    expected: "No UB, no data-race reports, no uninitialized-memory reads."
    why_human: "Tests prove no deadlock and no observable corruption, but absence of data races cannot be shown by passing stress tests. Miri is not installed here (nightly lacks the miri component), and the TSan build compiled but its runtime SIGSEGVs at process start on this macOS host (even for `--list`), so no sanitizer evidence could be obtained."
human_verification:
  - test: "Miri / loom / ThreadSanitizer pass over unsafe code (slab.rs, ring_buffer.rs, index.rs Slot Drop/Atomic usage)"
    expected: "Clean run; in particular Miri should confirm or flag the as_slice/as_mut_slice-over-uninitialized-memory pattern noted under Anti-Patterns."
    why_human: "Required tooling is not runnable in this environment."
---

# Phase 2: Lock-Free Memory Arena & Local KV Engine Verification Report

**Phase Goal:** Implement the core local execution tier: cacheline-aligned slab allocators, a lock-free concurrent KV index governed by epoch-based reclamation, and a Disruptor-style ring buffer; assemble into `LocalEngine` with Criterion microbenchmarks (P99 < 150us for memory reads).
**Verified:** 2026-11-08
**Status:** human_needed
**Re-verification:** No, initial verification (the pre-existing `VERIFICATION.md` is the plan-check file and was left untouched)

Source was not modified. All claims below were checked against code and by running commands myself.

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Slab allocator: 64B-64KB size classes, every pointer `% 64 == 0`, no leak/zero fresh allocation under reuse | VERIFIED | `src/arena/slab.rs`: 11 classes, `Layout` align 64, `SegQueue` free lists, `class_for` rejects > 64KB. `test_slab_alignment_and_lifecycle` (asserts alignment for 12 sizes, 1,200 reuse cycles with `fresh_allocations` unchanged, `live_blocks()==0`) and `test_slab_concurrent_reuse` pass. |
| 2 | Lock-free ordered KV index with get/put/delete/cas on crossbeam-epoch + crossbeam-skiplist; CAS is linearizable | VERIFIED | `src/storage/index.rs`: `SkipMap<Bytes, Slot>`, `Slot{Atomic<Bytes>}`. put/delete are single `swap`; cas is load, content compare, `compare_exchange` on the pointer, retry on loss. Old pointers go through `defer_destroy`. No ABA on the pointer because `cur` is protected by the pin guard. `test_cas_semantics` and `test_concurrent_cas_counter_is_exact` (16 threads x 2000 increments, exact 32,000) pass. |
| 3 | Disruptor-style ring buffer: cacheline-padded cursors, non-blocking `try_publish`, batch consumption | VERIFIED | `src/ipc/ring_buffer.rs`: `#[repr(align(64))] Cursor` for head and tail; const-checked power-of-two capacity; cached opposing cursors; `consume_batch` with a drop guard; Release/Acquire pairing on head/tail is correct for SPSC; Drop drains `[tail, head)`. Non-Clone `Producer`/`Consumer` enforce SPSC by type. `test_cursor_cacheline_padding`, `test_full_and_empty`, `test_drops_unconsumed_items`, `test_ring_buffer_spsc_throughput` (2M ordered messages) pass. |
| 4 | `LocalEngine` assembles slab + index + ring, with Criterion benchmarks in `benches/local_engine.rs` | VERIFIED | `src/engine.rs` wires `SlabPool` -> `Bytes::from_owner(SlabValue)` -> `KvIndex`, `apply_batch` drains a ring `Consumer`; oversize values fall back to heap. 5 engine tests pass, including EBR-driven slab recycling. Bench compiles and runs. |
| 5 | Memory-read P99 < 150us | VERIFIED | I re-ran `cargo bench --bench local_engine` (release, scratch target dir): GET 1 thread p99=250ns, GET 8 threads p99=500ns (summary claimed 250/542ns; consistent). Criterion get_hit ~88ns. Caveat: in-process, no network/Raft; the bench prints PASS/FAIL but does not gate CI. |
| 6 | `cargo check` / `cargo test` pass with 0 warnings; alignment criterion holds | VERIFIED | `cargo test`: 17 passed, 0 failed, 0 warnings. `cargo check --all-targets` and `cargo check --benches`: clean. `cargo clippy --all-targets`: clean. `cargo test --release --lib` x3: 17/17 each time. |
| 7 | 16-thread / 100k-op read/write test completes with zero deadlocks and zero data races | PRESENT_BEHAVIOR_UNVERIFIED | The test exists and passes (`test_lock_free_index_concurrent_ops`: 16 threads, 100,000 ops, per-read integrity assertions; no hangs in repeated runs). Zero deadlocks is evidenced. Zero data races is not provable by this test; no Miri/loom/TSan evidence exists. TSan build attempted by me: compiled but the sanitizer runtime crashes on startup on this host (exit 139 even for `--list`). See Human Verification. |

**Score:** 6/7 truths verified (1 present, behavior-unverified)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/arena/{mod,slab}.rs` | Slab pool | VERIFIED | 223 lines, substantive, used by `engine.rs` |
| `src/storage/{mod,index}.rs` | KV index | VERIFIED | 295 lines, used by `engine.rs` |
| `src/ipc/{mod,ring_buffer}.rs` | Ring buffer | VERIFIED | 280 lines; used by `engine::apply_batch` (and tests) |
| `src/engine.rs` | LocalEngine | VERIFIED | 205 lines, all three components wired |
| `src/lib.rs` | Library root (deviation: crate was bin-only) | VERIFIED | exports arena, engine, ipc, storage; `main.rs` untouched |
| `benches/local_engine.rs` | Criterion suite | VERIFIED | `harness = false` bench registered in `Cargo.toml`, runs |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `engine.rs` | `arena::SlabPool` | `materialize` -> `allocate`, `SlabValue::drop` -> `deallocate` | WIRED | Recycling proven by `test_engine_slab_blocks_are_recycled` |
| `engine.rs` | `storage::KvIndex` | `set/get/delete/cas` | WIRED | |
| `engine.rs` | `ipc::Consumer` | `apply_batch` -> `consume_batch` | WIRED | `test_engine_ring_dispatch` |
| `benches` | `lightcycle::engine` | lib target | WIRED | |

### Data-Flow Trace (Level 4)

Values flow `LocalEngine::set` -> slab block copy -> `Bytes::from_owner` -> `Owned<Bytes>` in `Slot` -> `get` returns a cloned `Bytes` backed by the same slab block (zero-copy). Real data, no static fallbacks. FLOWING.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Unit/stress tests | `cargo test` | 17 passed | PASS |
| All targets compile | `cargo check --all-targets` | no warnings | PASS |
| Benches compile | `cargo check --benches` | ok | PASS |
| Lint | `cargo clippy --all-targets` | no warnings | PASS |
| Tail latency | `cargo bench --bench local_engine` | GET p99 250ns / 500ns | PASS |
| ThreadSanitizer | nightly `-Zsanitizer=thread -Zbuild-std` | binary SIGSEGV at startup | SKIP (env) |
| Miri | `cargo +nightly miri` | component not installed | SKIP (env) |

### Probe Execution

None declared; `SKIPPED (no probe scripts)`.

### Requirements Coverage

PLAN declares no requirement IDs; SUMMARY `requirements-completed: []`. No orphaned requirements identified.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `src/arena/slab.rs` | 52, 58; `engine.rs` 26, 62 | `as_slice` / `as_mut_slice` build `&[u8]`/`&mut [u8]` over the whole block, but memory comes from `alloc` (not `alloc_zeroed`); the SAFETY comment ("allocated zeroed-or-written") is inaccurate. `SlabValue::as_ref` and `materialize` therefore form references over uninitialized bytes. | Warning | Technically UB-adjacent per Rust's reference rules for uninit; not observable in tests. Fix: use `alloc_zeroed`, or expose `*mut u8`/`MaybeUninit` and slice only the written `len`. Miri would be the arbiter. |
| `src/storage/index.rs` | 10-12, 75, 90 | Deleted keys remain as tombstones (empty `Slot`) in the skiplist; key memory never reclaimed. `put` also eagerly allocates `Bytes::copy_from_slice(key)` on every call even when the key exists. | Warning | Unbounded key-space growth under key churn (weakens NFR-4 "no unbounded retired-node accumulation"); documented by the executor as a known limitation and not in this phase's success criteria. Needs a compaction pass in a later phase. Hot-path allocation on `put` is a perf nit, not a correctness issue. |
| `src/arena/slab.rs` | 26-27 | A `SlabBlock` dropped without `deallocate` leaks its chunk; pool has no upper bound and never returns memory to the OS | Warning | Documented; NFR-3 memory bounding not enforced. `LocalEngine` always routes through `SlabValue`, so no leak on the engine path. |
| `src/arena/slab.rs` | 63 | Free list is `crossbeam SegQueue`, which allocates segments internally | Info | "Steady-state off the global allocator" is approximately, not strictly, true. Test only counts slab chunk allocations. |
| `src/ipc/ring_buffer.rs` | 24-30 | Struct is not `repr(C)`; padding correctness relies on both cursors having align 64 | Info | Sound as written (test asserts distinct lines). |

No TODO/FIXME/XXX debt markers in files changed by this phase.

### CAS correctness review

- Linearization point is the successful pointer `compare_exchange` (AcqRel/Acquire). The content comparison happens against a pointer protected by the pin guard, so the compared value cannot be freed or recycled to a new allocation before the exchange: no pointer ABA.
- Equal-content replacement by another writer between load and CAS is harmless (CAS fails on pointer and re-validates, or succeeds against an equal value).
- Lost race returns the owned allocation (`e.new`) and retries, so no leak. `desired = None` path uses `Shared::null()` correctly. Absent-key paths (`expected=None` insert, `expected=Some` mismatch, `None/None` no-op) are correct and covered by tests.
- Reclamation: replaced values use `defer_destroy`; `Slot::drop` frees the live pointer using `unprotected()` only when the skiplist node is destroyed (exclusive access). Reader `Entry` handles keep the node alive. Sound by inspection.

### Unsafe code review summary

- `slab.rs`: `Send`/`Sync` for `SlabBlock` justified by unique ownership; layout in `Inner::drop` matches allocation class; uninit-slice issue noted above. Free-list handoff through `SegQueue` gives the needed happens-before between a previous owner's writes and the next owner.
- `ring_buffer.rs`: SPSC discipline enforced by non-Clone handles; slot write is published by Release store of head, read after Acquire load of head; producer reuses a slot only after Acquire load of tail; `Commit` guard avoids double drop on panic; `Drop` drops exactly `[tail, head)`. No defects found by inspection.

### Human Verification Required

#### 1. Race/UB tooling pass on unsafe code

**Test:** Run `cargo +nightly miri test --lib` (install the miri component), and/or a loom model for the ring buffer and slab free list, and/or ThreadSanitizer on a Linux host.
**Expected:** No data races, no UB; particularly check the uninitialized-slice pattern in `SlabBlock::as_slice` / `as_mut_slice`.
**Why human:** Tooling unavailable in this environment (no miri component; TSan runtime crashes at startup here). The phase criterion "zero data races" is only supported by stress-test evidence.

### Gaps Summary

No blocking gaps: every functional deliverable exists, is substantive, is wired, and passes; tests and benchmark claims reproduced independently. The single open item is that the "zero data races" half of Verification Criterion 3 rests on stress tests alone, so it is routed to human verification rather than counted as verified. Warnings (uninit slice references, tombstone growth, unbounded pool) are real but documented or outside this phase's stated criteria; the tombstone/compaction and pool-bounding items should be scheduled in a later phase (3 or 6) so NFR-3/NFR-4 are not forgotten.

---

_Verified: 2026-11-08_
_Verifier: Claude (gsd-verifier)_
