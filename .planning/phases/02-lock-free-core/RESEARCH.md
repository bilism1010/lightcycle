# Phase 2: Lock-Free Memory Arena & Local KV Engine - Research

## 1. Domain Problem & Constraints
To hit sub-millisecond P99 latency (target: <150µs reads, <2ms writes), standard heap allocators (`malloc`, default `jemalloc`) introduce variable latency spikes under high concurrent thread contention. Project Lightcycle needs:
- Cacheline-aligned memory layout (64-byte boundary to avoid false sharing).
- Fixed-size chunk or slab allocation for cache values and keys.
- Safe concurrent pointer reclamation using Epoch-Based Reclamation (`crossbeam-epoch`).
- High-performance inter-thread messaging via Disruptor pattern ring buffers.

## 2. Component Design

### 2.1 Cacheline-Aligned Arena / Slab Allocator
- **Slab Classes**: Size classes (e.g. 64B, 128B, 256B, 512B, 1KB, 2KB, 4KB, 8KB, 16KB).
- **Chunk Layout**: Contiguous chunks aligned to 64 bytes (`repr(align(64))`).
- **Free-List**: Per-thread local freelists + fallback concurrent freelist to minimize cross-core CAS contention.

### 2.2 Lock-Free Concurrent Index
- **Option 1: Concurrent SkipList** (`crossbeam-skiplist`)
  - Wait-free reads, lock-free writes.
  - Natural ordered traversal (useful for range scans and snapshots).
- **Option 2: Concurrent Hash Map**
  - O(1) expected lookup, but resizes require complex incremental migration.
- **Selection**: `crossbeam-skiplist` for Phase 2 provides ordered keys, safe EBR pointer management, and predictable latency bounds without stop-the-world resizing pauses.

### 2.3 Disruptor Ring Buffer IPC
- Ring buffer sized to powers of two for bitwise mask wrapping (`index & (CAPACITY - 1)`).
- Single-Producer / Single-Consumer (SPSC) or Multi-Producer / Single-Consumer (MPSC) slots padded to 64 bytes to eliminate false sharing between sequence cursors.
- Busy-spin / yield backoff strategy for worker threads pinned to CPU cores.

## 3. Risk Analysis & Mitigations
- **Risk**: Memory bloat if a thread stalls in an active epoch guard.
  - *Mitigation*: Bounded batch pin durations; periodically release epoch pin in worker loops.
- **Risk**: Contention on slab allocation under heavy write skew.
  - *Mitigation*: Thread-local caching of small slab allocations.
