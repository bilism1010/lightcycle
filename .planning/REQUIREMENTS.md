# Lightcycle - System Requirements

## Functional Requirements
- **FR-1 [KV Operations]**: Support basic CRUD operations (`GET`, `SET`, `DEL`, `MGET`) with strict linearizability guarantees across a Raft cluster.
- **FR-2 [Atomic Operations]**: Support atomic Compare-And-Swap (`CAS`) and transaction primitives (`WATCH`/multi-key conditional updates). *Status (2026-11-10): split - single-key `CAS` done in Phases 2-3; `WATCH`/multi-key is roadmap Milestone 4.4. Plans 03-02 and 03-05 list FR-2 as completed; that covers the single-key part only.*
- **FR-3 [Replication]**: Cluster topology support (minimum 3 nodes) with automatic leader election, term progression, and log replication.
- **FR-4 [Persistence & Snapshotting]**: Disk/NVMe-backed Write-Ahead Log (WAL) with group commit and snapshot creation/restoration to prune raft logs.
- **FR-5 [Wire Protocol]**: Dual-protocol support: RESP (Redis wire protocol) and custom zero-copy binary RPC over TCP.
- **FR-6 [High Availability & Fault Recovery]**: Crash-recovery with fast state replay and dynamic cluster membership configuration changes.

## Non-Functional Requirements
- **NFR-1 [Latency]**: P99 read latency < 150μs; P99 write latency < 2ms (3 nodes).
- **NFR-2 [Throughput]**: Minimum 500,000 operations/sec on standard server hardware.
- **NFR-3 [Allocation Safety]**: Zero-copy pipelines on hot paths; custom arena/slab memory management with bound memory growth and zero allocator jitter.
- **NFR-4 [Concurrency]**: Lock-free index reads and writes with bounded reclamation overhead (no unbounded retired-node accumulation).
