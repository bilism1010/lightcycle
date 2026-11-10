# Lightcycle - High-Level Implementation Roadmap

```
Phase 1: Architecture Spikes & Deep Research ──► Phase 2: Lock-Free Core & Memory Tier ──► Phase 3: Raft Consensus Engine
        ▲                                                                                              │
        │                                                                                              ▼
        └── Phase 6: Benchmark & Optimization ◄── Phase 5: Wire Protocol & Client ◄── Phase 4: Integration
```

---

## Phase 1: Architectural Foundations & Deep Research [ACTIVE / QUESTIONING]
- [ ] Research Spike 1.1: Memory Reclamation Strategy (Hazard Pointers vs Epoch-based / Crossbeam-epoch vs QSBR).
- [ ] Research Spike 1.2: Raft Subsystem Selection (`openraft` vs custom async Raft engine).
- [ ] Research Spike 1.3: Disruptor Ring Buffer IPC & Thread Topology (NUMA affinity, worker threads, I/O poller).
- [ ] Research Spike 1.4: Linearizable Read Paths (Read-Index vs Leader Leases vs Quorum Reads).
- [ ] Decision Gate: Align with user on key architectural trade-offs via targeted Q&A.

## Phase 2: Lock-Free Memory Arena & Local KV Engine [COMPLETE - 2026-11-09]
- [x] Milestone 2.1: Cacheline-aligned slab/arena memory allocator.
- [x] Milestone 2.2: Lock-free index (concurrent skiplist / hash map) with safe reclamation.
- [x] Milestone 2.3: Disruptor-style ring buffer for internal message queues.
- [x] Milestone 2.4: Local micro-benchmarks & stress tests (evaluating cacheline contention & P99 latency).

## Phase 3: Consensus & Persistence Engine
- [ ] Milestone 3.1: Raft protocol state machine & log store abstractions.
- [ ] Milestone 3.2: High-throughput NVMe/disk WAL with group commit and pipelined batching.
- [ ] Milestone 3.3: Cluster membership, peer discovery, and heartbeat network layer.
- [ ] Milestone 3.4: Snapshotting engine and state transfer.

## Phase 4: State Machine Integration & Read-Path Optimizations
- [ ] Milestone 4.1: Bridge Raft committed entries to lock-free memory arena.
- [ ] Milestone 4.2: Implement Read-Index / Leader Lease read paths to bypass quorum consensus for reads.
- [ ] Milestone 4.3: Linearizable validation and Jepsen-style fault injection tests.

## Phase 5: Wire Protocols & Client API
- [ ] Milestone 5.1: Redis RESP protocol parser & command executor (`GET`, `SET`, `MGET`, `DEL`, `CAS`).
- [ ] Milestone 5.2: Custom zero-copy binary RPC protocol over TCP.
- [ ] Milestone 5.3: Connection multiplexing, keep-alive, and backpressure handling.

## Phase 6: Benchmarking, Tail-Latency Profiling & Hardening
- [ ] Milestone 6.1: High-load benchmark harness accounting for coordinated omission (e.g. using HDRHistogram).
- [ ] Milestone 6.2: Tail latency profiling (tracing P99/P99.9 latency under memory pressure and churn).
- [ ] Milestone 6.3: Production packaging, configuration validation, and documentation.
