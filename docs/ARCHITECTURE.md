# Project Lightcycle — Architecture & Design Specification

> Sub-millisecond in-memory cache engine with Raft-based consensus replication and custom lock-free memory buffers.

---

## 1. Problem Statement & System Motivation

Modern distributed microservice architectures demand low tail-latency caching layers without sacrificing strong consistency across failure boundaries.

### 1.1 The Operational Gap
* **Traditional in-memory key-value systems (Redis, Memcached)** deliver single-digit millisecond or sub-millisecond local reads, but rely on asynchronous leader-follower replication. Network partitions or primary failure can lead to silent writes disappearing or split-brain inconsistencies.
* **Distributed consensus systems (Etcd, Consul, Raft KV)** guarantee linearizability and fault tolerance across partitions, but incur high consensus overhead (3-15ms P99) and lock contention, rendering them unsuitable for high-throughput operational caches.
* **Memory thrash and lock contention**: Conventional locks (`std::sync::Mutex`, `RwLock`) trigger OS context switches and cache-invalidation storms across multi-socket CPUs. Standard allocators (`glibc malloc`) fragment rapidly under millions of small allocations per second.

---

## 2. The Solution: Lightcycle Architecture

Lightcycle bridges the gap between raw hardware memory throughput and distributed linearizability by combining a lock-free cache arena with a pipelined Raft state machine in Rust.

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        LIGHTCYCLE RUNTIME ENGINE                       │
└────────────────────────────────────────────────────────────────────────┘

              [ Ingress Network: TCP / gRPC / RESP ]
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │ Zero-Copy Frame Ring  │
                     └───────────┬───────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 │                               │
        [ Read Operations ]             [ Write Operations ]
                 │                               │
                 ▼                               ▼
    ┌─────────────────────────┐     ┌─────────────────────────┐
    │  Leader Lease / Read-Idx│     │   Pipelined Raft Log    │
    │  Validation Gate        │     │   (Batching Ring)       │
    └────────────┬────────────┘     └────────────┬────────────┘
                 │                               │
                 ▼                               ▼
    ┌─────────────────────────┐     ┌─────────────────────────┐
    │ Lock-Free Key-Value     │     │ Quorum Commit & NVMe    │
    │ SkipList / Hash Index   │     │ WAL Vectorized Fsync    │
    └────────────┬────────────┘     └────────────┬────────────┘
                 │                               │
                 │              Commit Event     │
                 └───────────────────────────────┘
```

### 2.1 Core Architectural Subsystems

1. **Lock-Free Cache Arena (Local Engine)**:
   * **Cacheline-Aligned Storage**: Node structures and hash buckets aligned to 64-byte boundaries to eliminate false sharing.
   * **Memory Reclamation**: Epoch-Based Reclamation (EBR) guarantees safe garbage reclamation of retired nodes without global synchronization locks.
   * **Custom Slab / Arena Pool**: Zero-allocator-churn runtime pre-allocating slab blocks for key-value payloads.

2. **Pipelined Consensus Layer (Distributed Engine)**:
   * **Pipelined Raft**: Leader log proposals are pipelined across network channels without waiting for round-trip RPC acknowledgments for previous entries.
   * **Leader Leases & Read Index**: Stated consistency guarantees allow local in-memory queries on the leader node without committing read entries to the Raft log, maintaining P99 < 150µs reads.
   * **Log Batching**: High-frequency writes are batched and committed to disk via vectorized `io_uring` / `pwritev`.

3. **Multi-Protocol Surface**:
   * RESP (Redis Serialization Protocol) compatibility enables immediate drop-in replacement in existing stacks.
   * Low-overhead binary framing protocol for high-performance cluster-internal replication.

---

## 3. Latency & Throughput Targets

* **P99 Read Latency**: `< 150 microseconds`
* **P99 Write Latency**: `< 2.0 milliseconds` (3-node Raft consensus cluster)
* **Sustained Throughput**: `> 500,000 ops/second / node`
* **Linearizability Guarantee**: Sequential consistency with bounded leader leases.
