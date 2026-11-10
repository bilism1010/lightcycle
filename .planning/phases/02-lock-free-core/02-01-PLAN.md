# Phase 2 Plan: Lock-Free Memory Arena & Local KV Engine

## Overview
Implement the core local execution tier of Lightcycle: cacheline-aligned slab allocators, a lock-free concurrent KV index governed by Epoch-Based Reclamation (EBR), and a zero-allocation Disruptor ring buffer for inter-thread message dispatch.

---

## Tasks

### Task 1 (Tracer Slice): Cacheline-Aligned Arena & Memory Slab
- **Objective**: Create `src/arena/mod.rs` and `src/arena/slab.rs` implementing cacheline-aligned (64-byte) slab pools for variable payload sizes.
- **Contract / Interface**:
  - `struct SlabPool { ... }` with size classes (64B to 64KB).
  - `fn allocate(&self, size: usize) -> Option<SlabBlock>`
  - `fn deallocate(&self, block: SlabBlock)`
  - Unit tests verifying 64-byte pointer alignment and zero allocator leak under reuse.
- **Verification**: `cargo test test_slab_alignment_and_lifecycle`

### Task 2: Lock-Free KV Index with Epoch-Based Reclamation
- **Objective**: Create `src/storage/mod.rs` and `src/storage/index.rs` implementing an ordered, lock-free key-value index using `crossbeam-epoch` and `crossbeam-skiplist`.
- **Contract / Interface**:
  - `pub struct KvIndex { ... }`
  - `fn get(&self, key: &[u8]) -> Option<Bytes>`
  - `fn put(&self, key: &[u8], value: Bytes)`
  - `fn delete(&self, key: &[u8]) -> bool`
  - `fn cas(&self, key: &[u8], expected: Option<&[u8]>, desired: Option<Bytes>) -> Result<(), CasError>`
- **Verification**: `cargo test test_lock_free_index_concurrent_ops`

### Task 3: Disruptor-Style Lock-Free Ring Buffer IPC
- **Objective**: Create `src/ipc/mod.rs` and `src/ipc/ring_buffer.rs` implementing a high-throughput, low-latency ring buffer with cacheline-padded atomic sequence cursors.
- **Contract / Interface**:
  - `pub struct RingBuffer<T, const CAPACITY: usize> { ... }`
  - Zero false sharing between publisher and subscriber cursors (`#[repr(align(64))]`).
  - Non-blocking `try_publish` and batch consumption support.
- **Verification**: `cargo test test_ring_buffer_spsc_throughput`

### Task 4: Local KV Engine Assembly & Microbenchmark Suite
- **Objective**: Assemble components into `LocalEngine` and add Criterion microbenchmarks under `benches/local_engine.rs`.
- **Contract / Interface**:
  - Measure single-threaded and multi-threaded GET/SET tail latency and verify P99 < 150µs for memory reads.
- **Verification**: `cargo check --benches` & `cargo test`

---

## Verification Criteria
1. `cargo check` and `cargo test` pass with 0 warnings.
2. Memory pointers from the slab allocator satisfy `ptr as usize % 64 == 0`.
3. High-concurrency read/write test (16 threads, 100k operations) completes with zero deadlocks and zero data races.
