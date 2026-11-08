# Project Lightcycle

> Sub-millisecond in-memory cache engine with Raft-based consensus replication and custom lock-free memory buffers.

Named after the high-speed, razor-sharp lightcycles traversing the Grid with zero latency and relentless coordination.

---

## 1. Goal

Build a predictable, sub-millisecond, distributed in-memory key-value and cache store in Rust. Lightcycle combines lock-free shared-memory ring buffers (such as Disruptor/hazard pointer patterns) for single-digit microsecond local reads/writes with a pipelined Raft consensus engine for low-latency linearizable distributed mutations.

---

## 2. Specification

### 2.1 Core Architectural Pillars
- **Lock-Free Cache Engine (Local Tier)**:
  - Cache storage backed by cacheline-aligned memory blocks and lock-free concurrent hash maps (or lock-free skiplists).
  - Allocation strategy minimizing allocator thrash (custom arena/slab allocators with chunk pooling).
  - Lock-free ring buffer IPC / inter-thread message queues (Disruptor pattern).
- **Consensus & Replication (Distributed Tier)**:
  - Pipelined Raft consensus engine (or open-source Raft integration like `openraft` / `async-raft`).
  - Batching and group commit for log compaction and disk/NVMe WAL syncing.
  - Read-Index / Lease-Read optimization to achieve sub-millisecond local reads without consensus round-trip overhead on leader nodes.
- **Client Protocol**:
  - High-performance RESP (Redis-compatible) wire protocol subset (GET, SET, MGET, DEL, WATCH, CAS).
  - Custom zero-copy binary RPC protocol over gRPC / raw TCP with framing.

### 2.2 Performance Targets
- **P99 Read Latency**: < 150 microseconds (local / leader lease read).
- **P99 Write Latency**: < 2 milliseconds (3-node Raft consensus cluster with fsync batches).
- **Throughput**: > 500,000 ops/sec per node.

---

## 3. Recommended Prompt for Next Refinement Phase

```text
Act as a Principal Distributed Systems and Low-Latency Engineer. We are refining 'Project Lightcycle', a sub-millisecond distributed cache with Raft consensus and lock-free memory buffers written in Rust.
Please provide:
1. Architecture diagrams and state machine specifications for the integration between the Raft log and the lock-free memory arena.
2. Comparative analysis of lock-free memory reclamation strategies (Hazard Pointers vs. Epoch-Based Reclamation vs. QSBR) for our key-value index.
3. Design for Read Index / Leader Lease queries to prevent stale reads without full quorum round-trips.
4. Benchmark harness architecture to accurately measure tail latency (P99/P99.9) under coordinated omission.
```

---

## 4. Scaffolding & Getting Started

```bash
# Verify Rust toolchain
cargo check

# Run tests
cargo test

# Launch standalone node
cargo run -- --node-id 1 --bind 127.0.0.1:7443
```
