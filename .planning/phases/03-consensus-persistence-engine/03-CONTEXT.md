# Phase 3: Consensus & Persistence Engine - Context

**Gathered:** 2026-11-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Deliver the distributed tier's durability and replication core: a Raft consensus node (built on `openraft`) with a segmented, group-committed on-disk WAL, a custom Tokio TCP peer transport, a snapshot/restore engine over the Phase 2 `LocalEngine`, and static-bootstrap plus dynamic membership. Roadmap milestones 3.1-3.4.

Out of scope here (other phases): bridging committed entries into the arena and Read-Index/Leader Lease read paths (Phase 4), Jepsen-style linearizability testing (Phase 4), client-facing RESP and binary RPC (Phase 5), benchmark harness (Phase 6).

</domain>

<decisions>
## Implementation Decisions

### WAL design & fsync
- **D-01:** WAL is segmented append-only files (fixed-size segments, e.g. 64MB) with per-record CRC32C. Purge after snapshot = delete old segments; replay is sequential. Implements openraft's log storage trait.
- **D-02:** Group commit via a dedicated WAL writer thread: appends are queued (use the Phase 2 Disruptor-style ring), writer drains everything pending, writes once, fsyncs once, then acks all. No timer-based batching window (no added latency floor when idle).
- **D-03:** I/O backend is plain `std::fs` on the dedicated blocking writer thread (keeps fsync off the Tokio runtime; works on macOS dev machine). Put it behind a trait so `io_uring` can be added later on Linux. Not `tokio::fs`.
- **D-04:** Default durability: a write is acked only after fsync on a quorum. A config flag may relax fsync (periodic) for benchmarking, clearly labelled unsafe. — **Reversibility:** reversible — config flag only.

### Raft network transport
- **D-05:** Raft RPCs (AppendEntries, Vote, InstallSnapshot) use a custom length-prefixed binary protocol over Tokio TCP, zero-copy friendly with `Bytes`. Not gRPC/tonic, not QUIC. — **Reversibility:** costly — wire format becomes a cluster-compat contract once nodes interoperate.
- **D-06:** Separate listener/port from Phase 5's client RPC; the length-prefixed frame codec is written once as a shared module/crate and reused by Phase 5.
- **D-07:** One persistent connection per peer with pipelined AppendEntries (multiple in flight per follower), per the roadmap's "pipelined Raft".
  - **Amendment (2026-11-10, accepted by the user in Phase 3 UAT, interpretation A4):** "one persistent connection" means one persistent pipelined *replication* connection per peer. Heartbeats and snapshot transfers each use their own dedicated, lazily dialed connection (D-12), so a peer pair has up to three TCP connections. Chosen so a bulk lane cannot head-of-line block heartbeats; a single multiplexed connection is not required.
- **D-08:** No TLS/auth in this phase (plaintext on trusted network). TLS/mTLS is a deferred idea.

### Snapshot format & trigger
- **D-09:** Snapshot = epoch-pinned iteration of the live skiplist while writes continue (no stop-the-world). Entries carry their Raft apply index; replay from the snapshot's last-applied index reconciles the fuzzy view. — **Reversibility:** costly — snapshot semantics couple to the state machine and the Phase 4 apply path. See risk R-1 below.
- **D-10:** Snapshot trigger: every N applied log entries since the last snapshot (configurable, e.g. 100k); triggers log purge. Not WAL-size based.
- **D-11:** Snapshot file format: simple framed file - header + length-prefixed key/value records + CRC - sequential and streamable, reusing the shared frame codec so bytes chunk directly into InstallSnapshot.
- **D-12:** Snapshot transfer to lagging followers is chunked streaming (bounded chunks, e.g. 1MB) over the peer connection with backpressure; heartbeats/AppendEntries use a separate logical channel so a snapshot cannot cause election timeouts.

### Membership & test harness
- **D-13:** Membership: static peer list at bootstrap (config file/CLI), then dynamic add/remove via openraft's membership-change (joint consensus) API. No gossip/auto-discovery. Covers FR-6.
- **D-14:** A 1-node cluster is a valid config using the same code path (including WAL), for dev and WAL benchmarking.
- **D-15:** Consensus tests run an in-process multi-node cluster behind the transport trait with an injectable fake network (drop/delay/partition, deterministic). A small number of real-TCP localhost smoke tests exercise the actual codec.
- **D-16:** Crash-recovery bar: kill-and-restart tests - node replays WAL + snapshot and rejoins with no lost committed writes; a torn/truncated final WAL record is detected by CRC and discarded. Jepsen/Elle-style checking is Phase 4.

### Claude's Discretion
- Exact segment size, record header layout, chunk size, snapshot-N default, and Raft timing parameters (election timeout/heartbeat) - choose sensible defaults, make them configurable.
- Crate/module layout for the WAL, transport, codec, and snapshot code.

### Risks flagged for research (not decided)
- **R-1 (fuzzy snapshots vs CAS):** D-09's replay-reconcile is only sound if re-applying entries after the snapshot's index is idempotent. `SET`/`DEL` are; `CAS` is not naturally idempotent. Researcher must determine whether per-entry apply-index stamping (with skip-if-already-applied) makes this correct, or whether a different snapshot approach is needed. This must be resolved before planning the snapshot milestone.
- **R-2 (tombstones):** Phase 2 leaves tombstones in the skiplist; a snapshot must decide whether to include or drop them (also relates to NFR-4).
- **R-3 (threading):** Raft apply must hand off to the core-pinned arena worker threads (Phase 1 decision) without blocking the Tokio runtime or the WAL writer.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project requirements and decisions
- `.planning/PROJECT.md` - Pillar 2 (Consensus & Replication), performance targets (P99 write < 2ms, 3 nodes)
- `.planning/REQUIREMENTS.md` - FR-3 (replication), FR-4 (WAL + snapshotting), FR-6 (fault recovery, dynamic membership), NFR-1, NFR-3, NFR-4
- `.planning/STATE.md` - Decisions Record: `openraft` selected; hybrid Tokio + core-pinned workers; EBR; `crossbeam-skiplist`
- `.planning/ROADMAP.md` - Phase 3 milestones 3.1-3.4 and Phase 4/5 boundaries
- `.planning/research/ARCHITECTURE_SPIKES.md` - Section 2 (Raft framework selection), Section 3 (threading), Section 4 (read strategies, Phase 4)

### Phase 2 outputs this phase builds on
- `.planning/phases/02-lock-free-core/02-01-SUMMARY.md` - what LocalEngine exposes
- `.planning/phases/02-lock-free-core/02-VERIFICATION.md` - known warnings (tombstones, unbounded slab pool, uninitialized-memory SAFETY comment)
- `docs/ARCHITECTURE.md` - system design overview

No external ADRs - decisions above are the specification.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/ipc/ring_buffer.rs` (SPSC Disruptor-style ring, `try_publish` / `consume_batch`): queue between Raft append path and the WAL writer thread.
- `src/engine.rs` `LocalEngine` (`set/get/delete/cas`, `apply_batch`): the state machine that Raft will apply into and that snapshots iterate.
- `src/storage/index.rs` `KvIndex` (`SkipMap` + `crossbeam-epoch`): the structure snapshotted via epoch-pinned iteration.
- `Cargo.toml` already has `tokio`, `bytes`, `serde`, `tracing`, `crossbeam`; no `openraft` yet.

### Established Patterns
- Non-Clone `Producer`/`Consumer` enforce SPSC by type (one ring per producer/consumer pair).
- Zero-copy values via `Bytes::from_owner(SlabValue)`.
- Tests are in-module unit tests plus Criterion benches; library crate in `src/lib.rs`, `main.rs` untouched so far.

### Integration Points
- New modules (WAL, transport/codec, raft node, snapshot) hang off `src/lib.rs`; `LocalEngine` is wrapped as openraft's state machine.
- Shared frame codec is consumed later by Phase 5.

</code_context>

<specifics>
## Specific Ideas

- Primary targets driving choices: P99 write < 2ms on a 3-node cluster, > 500k ops/s per node.
- Developing on macOS: Linux-only I/O (`io_uring`) cannot be tested locally, hence `std::fs` first behind a trait.

</specifics>

<deferred>
## Deferred Ideas

- TLS / mTLS and peer authentication - hardening phase.
- `io_uring` WAL backend (Linux) - after the trait seam exists.
- Gossip-based auto-discovery of peers.
- Dynamic Raft timing auto-tuning.

</deferred>

---

*Phase: 3-Consensus & Persistence Engine*
*Context gathered: 2026-11-09*
