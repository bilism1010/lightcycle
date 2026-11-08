# Lightcycle - Project Context & Specification

## Executive Summary
Lightcycle is a predictable, sub-millisecond, distributed in-memory key-value and cache store written in Rust. It pairs lock-free shared-memory ring buffers and zero-copy hazard pointer / epoch reclamation structures with a pipelined Raft consensus engine.

## Core Capabilities & Technical Pillars

### 1. Lock-Free Cache Engine (Local Tier)
- **Memory Architecture**: Cacheline-aligned memory blocks (64-byte alignment) with custom slab/arena chunk allocation to eliminate OS allocator thrash and memory fragmentation.
- **Concurrent Index**: Lock-free concurrent hash table or skiplist supporting wait-free reads and non-blocking multi-producer writes.
- **Memory Reclamation**: Safe memory reclamation for concurrent pointers (evaluating Hazard Pointers vs. Epoch-Based Reclamation vs. QSBR).
- **IPC & Inter-thread Queuing**: Disruptor-pattern lock-free ring buffers for low-latency thread-to-thread communication.

### 2. Consensus & Replication (Distributed Tier)
- **Raft Engine**: Pipelined Raft consensus engine (evaluating custom low-overhead Raft vs. production integrations like `openraft`).
- **Disk & Persistence WAL**: Asynchronous batching and group commit for NVMe/disk WAL syncing.
- **Read Scalability**: Read-Index and Leader Lease queries enabling linearizable local reads without consensus quorum latency.
- **Snapshot & Compaction**: Zero-copy snapshotting of in-memory state machines without stalling mutation pipelines.

### 3. Client Protocol & Wire Interface
- **Redis Protocol (RESP2/RESP3)**: Native subset (GET, SET, MGET, DEL, WATCH, CAS) for immediate compatibility with existing client drivers.
- **High-Performance Binary RPC**: Custom zero-copy binary RPC over raw TCP / gRPC with length-prefixed framing.

### 4. Performance Targets
- **P99 Read Latency**: < 150 microseconds (local / leader lease read).
- **P99 Write Latency**: < 2 milliseconds (3-node Raft cluster with fsync batching).
- **Throughput**: > 500,000 ops/sec per node.
