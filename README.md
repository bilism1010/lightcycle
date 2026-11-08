<div align="center">

```text
██╗     ██╗ ██████╗ ██╗  ██╗████████╗ ██████╗██╗   ██╗ ██████╗██╗     ███████╗
██║     ██║██╔════╝ ██║  ██║╚══██╔══╝██╔════╝╚██╗ ██╔╝██╔════╝██║     ██╔════╝
██║     ██║██║  ███╗███████║   ██║   ██║      ╚████╔╝ ██║     ██║     █████╗  
██║     ██║██║   ██║██╔══██║   ██║   ██║       ╚██╔╝  ██║     ██║     ██╔══╝  
███████╗██║╚██████╔╝██║  ██║   ██║   ╚██████╗   ██║   ╚██████╗███████╗███████╗
╚══════╝╚═╝ ╚═════╝ ╚═╝  ╚═╝   ╚═╝    ╚═════╝   ╚═╝    ╚═════╝╚══════╝╚══════╝
```

### **GRID-SPEED IN-MEMORY CACHE & RAFT CONSENSUS ENGINE**

[![Rust](https://img.shields.io/badge/RUST-1.75%2B-00e5ff?style=for-the-badge&logo=rust&logoColor=white&labelColor=03060f)](https://www.rust-lang.org/)
[![Status](https://img.shields.io/badge/STATUS-ACTIVE%20RESEARCH-18a0ff?style=for-the-badge&labelColor=03060f)](https://github.com/bsommers)
[![Latency](https://img.shields.io/badge/TAIL%20LATENCY-P99%20%3C%20150%C2%B5s-00e5ff?style=for-the-badge&labelColor=03060f)](#performance-targets)
[![Replication](https://img.shields.io/badge/CONSENSUS-RAFT%20PIPELINED-00e5ff?style=for-the-badge&labelColor=03060f)](#the-solution)

<p align="center">
  <b>Sub-millisecond in-memory cache engine with Raft-based consensus replication and custom lock-free memory buffers.</b><br>
  <i>Traversing the Grid with zero latency, lock-free ring buffers, and linearizable distributed state.</i>
</p>

---

</div>

## ⚡ The Problem Statement

Distributed caching and transactional storage systems currently force architectures to accept painful compromises across latency, concurrency, and consistency:

```text
┌───────────────────────────────┐     ┌────────────────────────────────┐
│   Standard In-Memory Stores   │ vs  │  Traditional Distributed Raft  │
│ (Redis, Memcached, Local L1)  │     │    (Etcd, Consul, ZooKeeper)   │
├───────────────────────────────┤     ├────────────────────────────────┤
│ ❌ Single-node failure domain  │     │ ❌ High tail latency (> 5-10ms)│
│ ❌ Asynchronous replica loss  │     │ ❌ Synchronous quorum bottlenecks│
│ ❌ Global mutexes & GC pauses │     │ ❌ Allocator churn under load  │
└───────────────────────────────┘     └────────────────────────────────┘
```

1. **The Consistency vs. Latency Paradox**: Fast distributed caches traditionally rely on asynchronous replication, leaving systems vulnerable to silent data loss during node partitions. Conversely, strongly consistent systems (like Etcd or Consul) penalize reads and writes with multi-millisecond quorum negotiations, making them unusable as high-throughput application caches.
2. **Lock Contention & Memory Fragmentation**: Modern multi-core servers choke on shared-mutex memory structures (B-Trees, locked hash tables) and standard heap allocators. Thread preemption and allocator lock contention cause severe tail-latency spikes (P99.9 > 15ms) when processing hundreds of thousands of operations per second.
3. **Consensus Round-Trip Overhead on Reads**: Traditional Raft engines require an explicit consensus round-trip or serialization through a single leader state-machine loop for every read query, eliminating cache read efficiency.

---

## 💡 The Solution: Project Lightcycle

**Lightcycle** decouples local hardware-speed memory execution from distributed linearizable consensus, synthesizing lock-free memory topology with an optimized pipelined Raft engine.

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                      PROJECT LIGHTCYCLE ARCHITECTURE                   │
 └────────────────────────────────────────────────────────────────────────┘

    Client Operations (RESP / Custom Binary RPC)
           │
           ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │  INGRESS & PROTOCOL DECODER (Zero-Copy Framing)                  │
    └──────────────┬───────────────────────────────────┬───────────────┘
                   │ [Read Path: Lease-Read / Index]   │ [Write Path: Propose]
                   ▼                                   ▼
    ┌──────────────────────────────┐    ┌──────────────────────────────┐
    │ LOCK-FREE CACHE ARENA        │    │ PIPELINED RAFT CONSENSUS     │
    │                              │    │                              │
    │ • Cacheline-aligned blocks   │    │ • Non-blocking ring buffer   │
    │ • Disruptor message ring     │◀───│ • Vectorized log batching    │
    │ • Epoch-based reclamation    │Commit • Group commit to NVMe WAL  │
    │ • P99 Read: < 150 µs         │    │ • P99 Write: < 2 ms (3-node) │
    └──────────────────────────────┘    └──────────────────────────────┘
                   ▲                                   │
                   └───────────────────────────────────┘
```

### Architectural Pillars

* **Lock-Free Cache Engine (Local Tier)**:
  * Cache storage organized into cacheline-aligned 64-byte blocks to prevent false sharing across CPU cores.
  * Multi-producer, single-consumer (MPSC) and Disruptor-style ring buffers replace kernel mutexes with atomic CAS primitives.
  * Epoch-Based Reclamation (EBR) safely cleans stale pointers without stopping the world or suffering JVM-style GC pauses.
* **Consensus & Replication (Distributed Tier)**:
  * Pipelined Raft consensus engine decoupling log replication across network sockets from disk syncing.
  * Leader Lease & Read-Index protocols enable **lock-free linearizable local reads** directly from the local memory arena without consensus network round-trips.
  * Group commit and zero-copy append log for NVMe writes maximize I/O batching efficiency.
* **Dual Wire Protocol**:
  * Redis RESP-compatible subset (`GET`, `SET`, `MGET`, `DEL`, `CAS`) for drop-in application adoption.
  * Ultra-low-overhead binary framing protocol for intra-cluster peer gossip and microsecond RPCs.

---

## 📊 Performance Targets

| Metric | Target | Description |
| :--- | :--- | :--- |
| **P99 Read Latency** | `< 150 µs` | Local node / leader lease reads via lock-free memory arena |
| **P99 Write Latency** | `< 2.0 ms` | 3-node distributed Raft consensus with persistent fsync batching |
| **Throughput** | `> 500,000 ops/sec` | Sustained read/write operations per node on commodity modern hardware |
| **Jitter / Max Pause** | `< 500 µs` | Zero stop-the-world garbage collection; deterministic epoch reclamation |

---

## 🛠️ Scaffolding & Quickstart

```bash
# 1. Verify Rust toolchain & dependencies
cargo check

# 2. Run local unit and integration tests
cargo test

# 3. Launch a standalone local Lightcycle node
cargo run -- --node-id 1 --bind 127.0.0.1:7443

# 4. Launch a 3-node local Raft cluster
cargo run -- --node-id 1 --bind 127.0.0.1:7441 --peers "2=127.0.0.1:7442,3=127.0.0.1:7443" &
cargo run -- --node-id 2 --bind 127.0.0.1:7442 --peers "1=127.0.0.1:7441,3=127.0.0.1:7443" &
cargo run -- --node-id 3 --bind 127.0.0.1:7443 --peers "1=127.0.0.1:7441,2=127.0.0.1:7442"
```

---

## 🔮 Next Refinement Phase Prompt

```text
Act as a Principal Distributed Systems and Low-Latency Engineer. We are refining 'Project Lightcycle', a sub-millisecond distributed cache with Raft consensus and lock-free memory buffers written in Rust.
Please provide:
1. Architecture diagrams and state machine specifications for the integration between the Raft log and the lock-free memory arena.
2. Comparative analysis of lock-free memory reclamation strategies (Hazard Pointers vs. Epoch-Based Reclamation vs. QSBR) for our key-value index.
3. Design for Read Index / Leader Lease queries to prevent stale reads without full quorum round-trips.
4. Benchmark harness architecture to accurately measure tail latency (P99/P99.9) under coordinated omission.
```

---

<div align="center">
<sub>SYSTEM NOTICE: GRID COMMUNICATIONS ENCRYPTED &bull; LINEARIZABLE BY SPECIFICATION</sub>
</div>
