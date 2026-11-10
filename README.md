<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="assets/logo-light.svg">
  <img src="assets/logo.svg" alt="Lightcycle - grid-speed in-memory cache and Raft consensus engine" width="640">
</picture>

### **GRID-SPEED IN-MEMORY CACHE & RAFT CONSENSUS ENGINE**

[![Rust](https://img.shields.io/badge/RUST-1.75%2B-00e5ff?style=for-the-badge&logo=rust&logoColor=white&labelColor=03060f)](https://www.rust-lang.org/)
[![Status](https://img.shields.io/badge/STATUS-ACTIVE%20RESEARCH-18a0ff?style=for-the-badge&labelColor=03060f)](https://github.com/bsommers)
[![License](https://img.shields.io/badge/LICENSE-MIT%20OR%20APACHE--2.0-00e5ff?style=for-the-badge&labelColor=03060f)](#-license)
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

# 3. Launch a standalone local Lightcycle node (a one-member cluster; it initializes itself)
#    State lives in ./lightcycle-data/node-<id> unless --data-dir is given.
cargo run -- --node-id 1 --bind 127.0.0.1:7443

# 4. Launch a 3-node local Raft cluster (use fresh data directories, not the one from step 3)
#    Node 1 is the designated bootstrap node: --bootstrap makes it run initialize, once, with
#    the full membership. Restarting it later never re-initializes. Nodes 2 and 3 do not bootstrap.
cargo run -- --node-id 1 --bind 127.0.0.1:7441 --peers "2=127.0.0.1:7442,3=127.0.0.1:7443" --bootstrap &
cargo run -- --node-id 2 --bind 127.0.0.1:7442 --peers "1=127.0.0.1:7441,3=127.0.0.1:7443" &
cargo run -- --node-id 3 --bind 127.0.0.1:7443 --peers "1=127.0.0.1:7441,2=127.0.0.1:7442"
```

Other flags: `--advertise <host:port>` (the address published in the membership by the bootstrap
node; defaults to `--bind`, and is required when binding a wildcard such as `0.0.0.0`),
`--data-dir <path>`, `--cluster-id <n>` (default 1; nodes with different values
refuse each other), `--fsync always|periodic|never` (default `always`) and
`--snapshot-every <entries>` (default 1000000). The binary prints `[Lightcycle] node <id> is leader
(term <t>)` when the node wins an election and shuts down cleanly on Ctrl-C or SIGTERM.

### Raft port security (Phase 3)

The Raft peer port (`--bind`) is **plaintext and unauthenticated**. This is a deliberate Phase 3
decision (D-08): TLS/mTLS and peer authentication are deferred to a later hardening phase. Any host
that can reach the port can inject Raft RPCs and take over the cluster, so bind it to a private
interface (or firewall it to the cluster members) and never expose it to an untrusted network.
Each peer relationship uses up to three TCP connections to that port (replication, heartbeat and
snapshot lanes), so allow all of them.

* The CRC32C checksums on frames and WAL records detect corruption. They are **not** authentication.
* WAL segments and snapshot files are plaintext on disk (directories are created `0700`, files `0600`).
* `--fsync periodic` and `--fsync never` can lose acknowledged writes on power failure. They are
  **unsafe** and exist for benchmarking only; the node logs a warning when either is used.
* Hardware validation of NFR-1 and NFR-2 (WAL fsync latency and throughput on Linux NVMe) is still
  **pending**. The performance targets above are targets, and no latency claim is made yet for the
  replicated write path.

---

## 📄 License & Commercial Terms

Project Lightcycle is dual-licensed under either:

* **Apache License, Version 2.0** ([`LICENSE-APACHE`](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))
* **MIT License** ([`LICENSE-MIT`](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.

### Commercial & Enterprise Inquiries
As the creator and copyright holder, Bill Sommers retains ownership of the codebase. Enterprise extensions, bespoke integration, commercial distribution rights, and managed hosting solutions are available under customized commercial licensing agreements. For inquiries, contact the author.

---

## 🎨 Brand Assets

Original light-trail artwork in the Grid palette (`#00e5ff`, `#18a0ff`, `#03060f`). All SVGs are hand-written, self-contained, and use no external fonts.

| File | Purpose |
| :--- | :--- |
| [`assets/logo.svg`](assets/logo.svg) | Horizontal logo, transparent background |
| [`assets/logo-dark.svg`](assets/logo-dark.svg) / [`assets/logo-light.svg`](assets/logo-light.svg) | Variants for dark and light themes |
| [`assets/icon.svg`](assets/icon.svg) | Square app icon (PNG: [512](assets/icon-512.png), [256](assets/icon-256.png), [64](assets/icon-64.png), [32](assets/icon-32.png)) |
| [`assets/favicon.svg`](assets/favicon.svg) | Simplified mark for 16-32px use |
| [`assets/banner.svg`](assets/banner.svg) | 1280x320 hero banner ([PNG](assets/banner.png)) |

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
