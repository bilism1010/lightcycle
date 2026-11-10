# Phase 3: Consensus & Persistence Engine - Research

**Researched:** 2026-11-09
**Domain:** Raft consensus (openraft) + segmented group-commit WAL + custom Tokio TCP transport + fuzzy-snapshot state machine, in Rust
**Confidence:** MEDIUM-HIGH (openraft API and replication behaviour verified from source at the tagged release; R-1 resolution verified by a randomized model check, not yet by Rust code; hardware-dependent latency claims are unverifiable on this machine)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**WAL design & fsync**
- **D-01:** WAL is segmented append-only files (fixed-size segments, e.g. 64MB) with per-record CRC32C. Purge after snapshot = delete old segments; replay is sequential. Implements openraft's log storage trait.
- **D-02:** Group commit via a dedicated WAL writer thread: appends are queued (use the Phase 2 Disruptor-style ring), writer drains everything pending, writes once, fsyncs once, then acks all. No timer-based batching window (no added latency floor when idle).
- **D-03:** I/O backend is plain `std::fs` on the dedicated blocking writer thread (keeps fsync off the Tokio runtime; works on macOS dev machine). Put it behind a trait so `io_uring` can be added later on Linux. Not `tokio::fs`.
- **D-04:** Default durability: a write is acked only after fsync on a quorum. A config flag may relax fsync (periodic) for benchmarking, clearly labelled unsafe. — **Reversibility:** reversible — config flag only.

**Raft network transport**
- **D-05:** Raft RPCs (AppendEntries, Vote, InstallSnapshot) use a custom length-prefixed binary protocol over Tokio TCP, zero-copy friendly with `Bytes`. Not gRPC/tonic, not QUIC. — **Reversibility:** costly — wire format becomes a cluster-compat contract once nodes interoperate.
- **D-06:** Separate listener/port from Phase 5's client RPC; the length-prefixed frame codec is written once as a shared module/crate and reused by Phase 5.
- **D-07:** One persistent connection per peer with pipelined AppendEntries (multiple in flight per follower), per the roadmap's "pipelined Raft".
- **D-08:** No TLS/auth in this phase (plaintext on trusted network). TLS/mTLS is a deferred idea.

**Snapshot format & trigger**
- **D-09:** Snapshot = epoch-pinned iteration of the live skiplist while writes continue (no stop-the-world). Entries carry their Raft apply index; replay from the snapshot's last-applied index reconciles the fuzzy view. — **Reversibility:** costly — snapshot semantics couple to the state machine and the Phase 4 apply path. See risk R-1 below.
- **D-10:** Snapshot trigger: every N applied log entries since the last snapshot (configurable, e.g. 100k); triggers log purge. Not WAL-size based.
- **D-11:** Snapshot file format: simple framed file - header + length-prefixed key/value records + CRC - sequential and streamable, reusing the shared frame codec so bytes chunk directly into InstallSnapshot.
- **D-12:** Snapshot transfer to lagging followers is chunked streaming (bounded chunks, e.g. 1MB) over the peer connection with backpressure; heartbeats/AppendEntries use a separate logical channel so a snapshot cannot cause election timeouts.

**Membership & test harness**
- **D-13:** Membership: static peer list at bootstrap (config file/CLI), then dynamic add/remove via openraft's membership-change (joint consensus) API. No gossip/auto-discovery. Covers FR-6.
- **D-14:** A 1-node cluster is a valid config using the same code path (including WAL), for dev and WAL benchmarking.
- **D-15:** Consensus tests run an in-process multi-node cluster behind the transport trait with an injectable fake network (drop/delay/partition, deterministic). A small number of real-TCP localhost smoke tests exercise the actual codec.
- **D-16:** Crash-recovery bar: kill-and-restart tests - node replays WAL + snapshot and rejoins with no lost committed writes; a torn/truncated final WAL record is detected by CRC and discarded. Jepsen/Elle-style checking is Phase 4.

### Claude's Discretion
- Exact segment size, record header layout, chunk size, snapshot-N default, and Raft timing parameters (election timeout/heartbeat) - choose sensible defaults, make them configurable.
- Crate/module layout for the WAL, transport, codec, and snapshot code.

### Deferred Ideas (OUT OF SCOPE)
- TLS / mTLS and peer authentication - hardening phase.
- `io_uring` WAL backend (Linux) - after the trait seam exists.
- Gossip-based auto-discovery of peers.
- Dynamic Raft timing auto-tuning.

### Risks flagged for research (resolved in "Resolution of Flagged Risks" below)
- **R-1 (fuzzy snapshots vs CAS):** D-09's replay-reconcile is only sound if re-applying entries after the snapshot's index is idempotent. `SET`/`DEL` are; `CAS` is not naturally idempotent. Researcher must determine whether per-entry apply-index stamping (with skip-if-already-applied) makes this correct, or whether a different snapshot approach is needed.
- **R-2 (tombstones):** Phase 2 leaves tombstones in the skiplist; a snapshot must decide whether to include or drop them (also relates to NFR-4).
- **R-3 (threading):** Raft apply must hand off to the core-pinned arena worker threads (Phase 1 decision) without blocking the Tokio runtime or the WAL writer.

Out of scope for Phase 3 (other phases): bridging committed entries into the arena and Read-Index/Leader Lease read paths (Phase 4), Jepsen-style linearizability testing (Phase 4), client-facing RESP and binary RPC (Phase 5), benchmark harness (Phase 6).
</user_constraints>

## Project Constraints (from CLAUDE.md)

- No project-level `CLAUDE.md` or `.claude/` / `.agents/` skills directory exists in the repo [VERIFIED: `ls` of repo root, this session].
- Global `~/.claude/CLAUDE.md`: new repos get Apache-2.0 unless the repo already specifies a license. This repo already declares `license = "MIT OR Apache-2.0"` with `LICENSE-APACHE` and `LICENSE-MIT` [VERIFIED: Cargo.toml, directory listing]. Any new crate/workspace member must declare the same dual license.
- Project conventions to follow (from Phase 2 artifacts): in-module `#[cfg(test)]` unit tests, Criterion benches with `harness = false`, library code in `src/lib.rs`, `main.rs` untouched so far, zero compiler warnings (17 tests pass, baseline re-run this session) [VERIFIED: `cargo test --offline` -> `17 passed`].

<phase_requirements>
## Phase Requirements

ROADMAP maps no requirement IDs to Phase 3. Coverage below is derived from REQUIREMENTS.md and CONTEXT.md canonical refs.

| ID | Description | Research Support |
|----|-------------|------------------|
| FR-3 | Replication: >=3 nodes, automatic leader election, term progression, log replication | openraft 0.10 alpha: `stream_append` pipelining, pre-vote, heartbeat client; custom TCP transport (Standard Stack, Pattern 3) |
| FR-4 | Disk-backed WAL with group commit; snapshot creation/restoration to prune raft logs | Segmented WAL + writer-thread group commit (Pattern 1); stamped fuzzy snapshot + purge (Pattern 4) |
| FR-6 | Crash-recovery with fast state replay; dynamic membership | WAL recovery algorithm, snapshot+replay, `initialize`/`add_learner`/`change_membership` (Pattern 5) |
| FR-2 (partial) | Atomic CAS must replicate and replay correctly | `KvRequest::Cas` in the log; R-1 resolution (stamps) makes CAS replay-safe for single-key ops |
| NFR-1 | P99 write < 2ms on 3 nodes | Dominated by fsync latency; measured 4.0ms p50 `sync_data` on this Mac (F_FULLFSYNC) -> cannot be validated on the dev machine (Pitfall 7, Environment) |
| NFR-2 | >= 500k ops/s | Pipelined AppendEntries + group commit + batched apply; sizing math in Pitfall 11 |
| NFR-3 | Bounded memory, zero-copy hot paths | Byte-budgeted log cache, `Bytes` slicing in codec, bounded ring/frames (Patterns 1-3) |
| NFR-4 | No unbounded retired-node accumulation | Snapshot iteration must not hold one epoch pin for the whole scan (Pitfall 1); tombstone rule (R-2) |
</phase_requirements>

## Summary

Phase 3 is buildable with the decisions in CONTEXT.md, but three findings change how it must be planned. **First, the openraft version matters for D-07 and D-12.** The stable 0.9.26 replication task has a single in-flight AppendEntries per follower (its own source says "Currently there is at most 1 in flight data") and its storage traits are sealed behind an "unstable" `storage-v2` feature. Only the `0.10.0-alpha` line exposes a transport-level `RaftNetworkV2::stream_append` (a stream of requests in, a stream of responses out, i.e. real pipelining) and `RaftNetworkFactory::new_heartbeat_client` / `new_snapshot_client` (dedicated clients for heartbeats and snapshots, i.e. exactly D-12). The recommendation is to pin `openraft = "=0.10.0-alpha.38"` and treat that as a user-confirmable assumption (A1), with 0.9.26 as the documented fallback that forces D-07 to be relaxed.

**Second, R-1 is resolvable without abandoning D-09, but only with a Phase 2 change.** Naive replay of a fuzzy snapshot is provably wrong for CAS (counter-example below; 310-396 failures per 100-200k random histories in a model check). Per-key apply-index stamping with skip-if-already-applied is sound for single-key operations, provided the (value, stamp) pair is read and written atomically, tombstones carry stamps, and the snapshot drops only tombstones whose stamp is <= the snapshot's start index. The model check shows 0 failures in 300k histories for exactly that rule, and failures for each weaker variant. This requires changing `Slot` in `src/storage/index.rs` from a bare `Atomic<Bytes>` to an atomically-swapped versioned cell. It is NOT sound for multi-key conditional operations (FR-2 transactions); that limit must be recorded as a constraint on future work.

**Third, the dev machine cannot validate NFR-1.** Rust's `File::sync_data` and `sync_all` both issue `fcntl(F_FULLFSYNC)` on Apple targets; measured here: 4.0ms p50 / 7.5ms p99 per 4KB write+`sync_data`. Group commit amortizes this, but a 2ms P99 durable write needs Linux on NVMe with fast fsync. Plans must treat latency numbers from the Mac as correctness-only and must build the fsync-latency instrumentation early.

**Primary recommendation:** Pin `openraft =0.10.0-alpha.38` (default-features off, `tokio-rt` + `serde`), implement a hand-written segmented WAL as an `append-only, marker-based-truncate` log behind a `SegmentIo` trait, a three-lane (replication / heartbeat / snapshot) TCP transport over `tokio_util::codec::LengthDelimitedCodec`, and stamp every key with its Raft apply index (Phase 2 `KvIndex` change) before writing any snapshot code.

## Architectural Responsibility Map

This is a single-binary node, not a web app; "tiers" here are execution contexts.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Raft protocol state machine (elections, replication, membership) | Tokio runtime (openraft `RaftCore` tasks) | - | openraft owns it; we only supply storage/network/state-machine traits |
| Log durability (append, vote, truncate, purge, fsync) | Dedicated blocking WAL writer thread | Tokio (async facade `RaftLogStorage` + oneshot callbacks) | D-03: fsync must stay off the Tokio runtime |
| Log reads for replication/apply | In-memory byte-budgeted cache (any task) | Disk `pread` fallback via offset index | Reads must not queue behind the writer thread |
| Peer transport (frames, pipelining, lanes) | Tokio runtime (TCP tasks) | Shared frame codec module | D-05/D-06 |
| Applying committed entries to `LocalEngine` | openraft SM worker task (inline, chunked) | Phase 4: core-pinned arena workers via ring | R-3; inline is ~100s of ns/op, handoff seam kept for Phase 4 |
| Snapshot build | `spawn_blocking` thread (reads live skiplist) | Tokio task awaiting it | Multi-second scans must not occupy a Tokio worker |
| Snapshot send/receive | Tokio (snapshot lane connection) | `tokio::fs` or blocking file reads for chunks | Bulk, off the hot path; isolated from heartbeats (D-12) |
| Snapshot install (state replace) | SM worker task + blocking load thread | `ArcSwap<LocalEngine>` swap | Readers keep old `Arc` until swap |
| Membership change / bootstrap | openraft `Raft` API (leader) | Config/CLI static peer list | D-13 |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `openraft` | `=0.10.0-alpha.38` (features: `tokio-rt`, `serde`; `default-features = false`) | Raft consensus | Locked by STATE.md decision; only line with `stream_append` pipelining + dedicated heartbeat/snapshot clients. Published 2026-11-08 [VERIFIED: crates.io API]. Compiles on rustc 1.99.0 with these features, and `Entry` round-trips through postcard [VERIFIED: scratch spike compiled and test passed this session] |
| `crc32c` | `0.6` (0.6.9) | CRC32C per WAL record / snapshot block | Hardware-accelerated (aarch64 module present, 30.3 GB/s measured on M4 Pro); `crc32c_append` supports chaining; check value of `"123456789"` is `0xe3069283` (the standard CRC-32C vector) [VERIFIED: ran benchmark this session] |
| `postcard` | `1.1` (1.1.3) | serde binary encoding of openraft messages/entries/vote/meta | Maintained, compact varints, `use-std` feature; `bincode` is unmaintained (RUSTSEC-2025-0141) [VERIFIED: rustsec.org page title "Bincode is unmaintained"; crates.io shows bincode 3.0.0 2025-12-16] |
| `tokio-util` | `0.7` (0.7.20), feature `codec` | `LengthDelimitedCodec` framing (default max frame 8MB, `max_frame_length`, `length_field_length`) | Standard length-prefixed framing; do not hand-roll [CITED: docs.rs/tokio-util 0.7.20 length_delimited::Builder] |
| `futures` | `0.3` | `Stream`/`Sink` combinators for `stream_append` plumbing | Required by openraft stream APIs |
| `arc-swap` | `1.9` | Atomic swap of the live `LocalEngine` on snapshot install | Lock-free reader path; Phase 4 readers keep working |
| `bytes` (existing) | `1.7` + feature `serde` | Zero-copy payloads | Already a dependency; `serde` feature needed for `KvRequest` (spike compiled) |
| `serde` (existing) | `1.0` | derive for `KvRequest`/`KvResponse` | Already a dependency |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `thiserror` | `2` | WAL/codec error enums | Error types at module boundaries |
| `tempfile` (dev) | `3` | Temp dirs for WAL/snapshot tests | All durability tests |
| `proptest` (dev) | `1.12` | Randomized stamped-replay model test, torn-write fuzz | R-1 property test; WAL truncation sweep |
| `criterion` (existing dev) | `0.5` | WAL append->ack latency/throughput bench | Early fsync/group-commit measurement |
| `openraft-memstore` / `openraft-rt-sim` (dev, optional) | `=0.10.0-alpha.38` | In-memory log store; deterministic simulated runtime | Optional: deterministic consensus tests (see Open Question 4). Both flagged SUS by the legitimacy seam (low downloads) but are published from the openraft repo |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `openraft =0.10.0-alpha.38` | `openraft 0.9.26` (stable, released 2026-11-09) | Stable, but: one in-flight AppendEntries per follower, heartbeat and replication share the `network` while snapshots get a second instance, storage traits need the `storage-v2` feature ("unstable and may change"). Forces D-07 to be relaxed to "batched, one in flight". Use only if the alpha is rejected |
| Hand-written WAL (D-01..D-03) | `raft-log` 0.4.6 (openraft author's WAL crate, used by openraft's `examples/log-wal`) | Faster to ship, but has its own worker thread, file format, and codec; contradicts D-02 (our ring) and D-01 (CRC32C format). Keep as reference code and as a schedule escape hatch |
| `postcard` | `rmp-serde` (openraft examples use it) | Both fine; postcard is smaller/faster, no extra decisions |
| `LengthDelimitedCodec` | custom decoder | Only needed if the header must be validated before the body is buffered; a thin wrapper over the codec is enough |

**Installation (Cargo.toml additions):**
```toml
openraft   = { version = "=0.10.0-alpha.38", default-features = false, features = ["tokio-rt", "serde"] }
bytes      = { version = "1.7", features = ["serde"] }
crc32c     = "0.6"
postcard   = { version = "1.1", features = ["use-std"] }
tokio-util = { version = "0.7", features = ["codec"] }
futures    = "0.3"
arc-swap   = "1.9"
thiserror  = "2"

[dev-dependencies]
tempfile = "3"
proptest = "1.12"
```
Disabling openraft default features drops `clap`/`parse-size`, so `Config` must be built with a struct literal plus `..Default::default()` and then `.validate()` [VERIFIED: spike compiled with `default-features = false`; examples/raft-kv-memstore uses the same literal form].

**Version verification:** crates.io API this session: openraft max 0.10.0-alpha.38 (2026-11-08) / max_stable 0.9.26 (2026-11-09); crc32c 0.6.9 (2026-11-02); postcard 1.1.3 (2025-07-24); tokio-util 0.7.20 (2026-10-09); futures 0.3.34; arc-swap 1.9.5 (2026-11-09); thiserror 2.0.21; tempfile 3.27.0; proptest 1.12.0 [VERIFIED: crates.io API].

## Package Legitimacy Audit

Run via `gsd-tools query package-legitimacy check --ecosystem crates ...` this session.

| Package | Registry | Age | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|-----|-----------|-------------|---------|-------------|
| openraft | crates.io | 2022-01 (first publish) | ~78k/wk | github.com/datafuselabs/openraft (databendlabs) | OK | Approved (already a locked project decision) |
| crc32c | crates.io | 2017-09 | ~1.8M/wk | github.com/zowens/crc32c | OK | Approved |
| postcard | crates.io | 2019-04 | ~3.0M/wk | github.com/jamesmunns/postcard | OK | Approved |
| tokio-util | crates.io | 2018-02 | ~17.8M/wk | github.com/tokio-rs/tokio | OK | Approved |
| futures | crates.io | 2016-07 | ~16.6M/wk | github.com/rust-lang/futures-rs | OK | Approved |
| arc-swap | crates.io | 2018-04 | ~8.7M/wk | github.com/vorner/arc-swap | OK | Approved |
| thiserror | crates.io | 2019-10 | ~39.5M/wk | github.com/dtolnay/thiserror | OK | Approved |
| tempfile | crates.io | 2015-04 | ~19.1M/wk | github.com/Stebalien/tempfile | OK | Approved (dev) |
| proptest | crates.io | - | ~5.7M/wk | - | OK | Approved (dev) |
| raft-log | crates.io | 2024-10 | ~1.7k/wk | github.com/drmingdrmer/raft-log | OK | Reference only, not a dependency |
| openraft-rt-sim | crates.io | 2026-10 | ~40/wk | github.com/databendlabs/openraft | SUS | `openraft-rt-sim` [WARNING: flagged as suspicious (low downloads) — verify before using.] Planner must add `checkpoint:human-verify` before adding it, or skip it (it is optional) |
| openraft-memstore | crates.io | 2022-07 | ~840/wk | github.com/datafuselabs/openraft | SUS | `openraft-memstore` [WARNING: flagged as suspicious (low downloads) — verify before using.] Optional dev-dep; same checkpoint rule. Same-repo crate as openraft, so risk is low, but the gate still applies |

**Packages removed due to [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** `openraft-rt-sim`, `openraft-memstore` (both optional dev-dependencies; planner inserts `checkpoint:human-verify` before each install, or avoids them)

Package names above were discovered from authoritative sources (openraft's own repo and Cargo manifests, crates.io API, docs.rs) and returned `OK` from the seam, except as noted. `unsafe`/postinstall scripts: Rust crates have no postinstall; not applicable.

## Architecture Patterns

### System Architecture Diagram

```
                    client_write / membership API (Phase 5 later; tests call Raft directly)
                                    |
                                    v
  +--------------------------- openraft RaftCore (Tokio tasks) ----------------------------+
  |  elections / pre-vote / membership / commit index / replication streams per follower   |
  +--+------------------+------------------------+-------------------+---------------------+
     | RaftLogStorage   | RaftStateMachine       | RaftNetworkFactory|
     | (&mut self,      | (apply stream,         | new_client        |  replication lane
     |  single owner)   |  snapshot builder)     | new_heartbeat_...  |  heartbeat lane
     v                  v                        | new_snapshot_...   |  snapshot lane
 +-----------+   +--------------------+          v                    
 | WalLog    |   | KvStateMachine     |     +------------------+  frames (LengthDelimited + header)
 | Store     |   |  apply_at(idx,cmd) |     | PeerClient x3    |========= TCP =========> peer node
 |  Producer |   |  ArcSwap<Local     |     | (per lane, own   |                        |
 +-----+-----+   |   Engine>          |     |  connection)     |                        v
       | SPSC    +----+----------+----+     +------------------+               +-----------------+
       | ring         |          |                                              | PeerServer      |
       v              |          | try_create_snapshot_builder                  | (accept loop,   |
 +------------+       |          v                                              |  per-lane       |
 | WAL writer |       |   +-------------------+  spawn_blocking                 |  handlers) -->  |
 | thread     |       |   | SnapshotBuilder   |-- iterate KvIndex (short pins)  |  Raft::stream_  |
 | drain->    |       |   | (a = applied idx) |-- frames+CRC --> snap-<a>.tmp   |  append / vote /|
 | write->    |       |   +-------------------+   fsync, rename, fsync dir      |  install_full_  |
 | fsync->ack |       v                                                         |  snapshot       |
 +-----+------+   LocalEngine (Phase 2: SlabPool + KvIndex[Slot{value+stamp}])  +-----------------+
       |
       v   segment files 0000...01.wal, 0000...02.wal  (preamble: vote, purged, first_index)
```

Primary use case trace: a write enters `RaftCore` -> `append()` on `WalLogStore` caches the entry, publishes a `WalOp::Append` to the ring, returns -> the replication stream pulls the entry from the cache and the `PeerClient` pipelines it to followers while the writer thread batches, writes, fsyncs and fires `IOFlushed::io_completed` -> followers do the same through `Raft::stream_append` -> quorum of flushed copies commits -> SM worker `apply`s into `LocalEngine` and answers the responder.

### Recommended Project Structure
```
src/
|- lib.rs                  # add: pub mod raft; pub mod wal; pub mod wire; pub mod net; pub mod snapshot;
|- engine.rs               # CHANGE: apply_at(index, &KvRequest), restore/iter support
|- storage/index.rs        # CHANGE: Slot holds Atomic<Versioned{index, value: Option<Bytes>}>; *_at methods; scan API
|- wire/                   # shared by Phase 5: NO openraft dependency
|  |- frame.rs             # LengthDelimitedCodec wrapper + FrameHeader {kind, flags, lane, req_id}
|  `- hello.rs             # connection handshake (magic, version, cluster_id, node_id, lane)
|- wal/
|  |- io.rs                # trait SegmentIo (create/append/sync/read_at/delete/list); StdFsIo; test FakeIo
|  |- record.rs            # record framing + CRC32C, RecordType
|  |- segment.rs           # segment header/preamble, rolling, naming
|  |- writer.rs            # WAL writer thread: ring drain, group commit, ack, park/unpark
|  |- recovery.rs          # scan segments, torn-tail truncate, rebuild offset index/state
|  |- cache.rs             # byte-budgeted entry cache + offset index + disk fallback reads
|  `- store.rs             # WalLogStore: RaftLogStorage + RaftLogReader
|- raft/
|  |- types.rs             # TypeConfig, KvRequest, KvResponse
|  |- state_machine.rs     # KvStateMachine: RaftStateMachine (+ builder, install)
|  |- config.rs            # NodeConfig -> openraft Config + WalConfig + SnapshotConfig
|  `- node.rs              # RaftNode::start(...): bootstrap/initialize, add_learner, change_membership
|- net/
|  |- client.rs            # RaftNetworkFactory + PeerClient (RaftNetworkV2), three lanes
|  |- server.rs            # accept loop; dispatch per lane to Raft::{stream_append,vote,pre_vote,install_full_snapshot}
|  `- fake.rs              # FakeNetwork + NetController (drop/delay/partition) - cfg(test) or `test-util` feature
`- snapshot/
   |- format.rs            # header, record blocks, footer, CRC
   |- writer.rs            # fuzzy iterate -> file (blocking)
   |- reader.rs            # streaming load into a fresh LocalEngine
   `- store.rs             # directory scan, tmp->final rename, retention, chunk read/write
```
Keep `wire/` free of Raft types so Phase 5 can reuse it unchanged (D-06). Extract to a workspace crate later if wanted; a module avoids restructuring `Cargo.toml` now (discretion).

### Verified openraft 0.10.0-alpha.38 API surface (quoted from source opened this session)

`RaftLogStorage` (openraft/src/storage/v2/raft_log_storage.rs), comments elided:
```rust
pub trait RaftLogStorage<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    type LogReader: RaftLogReader<C>;
    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error>;
    async fn get_log_reader(&mut self) -> Self::LogReader;
    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error>;
    async fn save_committed(&mut self, _committed: Option<LogIdOf<C>>) -> Result<(), io::Error> { Ok(()) }
    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> { Ok(None) }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where I: IntoIterator<Item = C::Entry> + OptionalSend, I::IntoIter: OptionalSend;
    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error>;
    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error>;
}
```
No `Sealed` supertrait in 0.10 (0.9.26 has `Sealed` and only lets third parties implement it with feature `storage-v2`: `#[cfg(feature = "storage-v2")] impl<T> Sealed for T {}`).

`IOFlushed` (storage/callback.rs): `pub enum IOFlushed<C> { Noop, Notify(IOFlushedNotify<C>), Signal(OneshotSenderOf<C, Result<(), io::Error>>) }` with `pub fn io_completed(self, result: Result<(), io::Error>)`; the doc notes completions "may complete out of order" and the watermark only moves forward; once an error is recorded it is never reverted.

`RaftStateMachine` (storage/v2/raft_state_machine.rs):
```rust
type SnapshotData: OptionalSend + 'static;
type SnapshotBuilder: RaftSnapshotBuilder<C, SnapshotData = Self::SnapshotData>;
async fn applied_state(&mut self) -> Result<(Option<LogIdOf<C>>, StoredMembershipOf<C>), io::Error>;
async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<C>, io::Error>> + Unpin + OptionalSend;
async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder>
async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder;
async fn install_snapshot(&mut self, meta: &SnapshotMetaOf<C>, snapshot: Self::SnapshotData) -> Result<(), io::Error>;
async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<C, Self::SnapshotData>>, io::Error>;
```
`RaftSnapshotBuilder`: `async fn build_snapshot(&mut self) -> Result<SnapshotOf<C, Self::SnapshotData>, io::Error>;`. `SnapshotMeta` has exactly two fields: `pub last_log_id: Option<LogId<CLID>>` and `pub last_membership: StoredMembership<CLID, Mem>` (no `snapshot_id` in 0.10). `EntryResponder<C> = (EntryOf<C>, Option<ApplyResponder<C>>)`; `ApplyResponder::send(self, response: C::R)`.

`RaftNetworkV2` (network/v2/network.rs): `type SnapshotData`; `append_entries(&mut self, rpc: AppendEntriesRequest<C>, option: RPCOption) -> Result<AppendResult<C>, RPCError<C>>`; `stream_append<'s, S>(&'s mut self, input: S, option: RPCOption) -> BoxFuture<'s, Result<BoxStream<'s, Result<AppendResult<C>, RPCError<C>>>, RPCError<C>>> where S: Stream<Item = AppendEntriesRequest<C>> + OptionalSend + Unpin + 'static` (default = sequential); `vote`, `pre_vote`, `full_snapshot(&mut self, vote: VoteOf<C>, snapshot: SnapshotOf<C, Self::SnapshotData>, cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static, option: RPCOption) -> Result<SnapshotResponse<C>, StreamingError<C>>`; `transfer_leader`; `backoff`.

`RaftNetworkFactory` (network/factory.rs): `type Network: NetBackoff<C> + NetStreamAppend<C> + NetVote<C> + NetSnapshot<C> + NetTransferLeader<C>;` `async fn new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network;` plus provided `new_heartbeat_client` and `new_snapshot_client` (default: delegate to `new_client`; docs say to override "to isolate these probes on an independent connection"). Used at `core/heartbeat/handle.rs:97` (heartbeat) and `core/raft_core.rs:2670` (snapshot).

Server-side entry points on `Raft`: `stream_append(stream) -> impl Stream<Item = Result<AppendResult<C>, Fatal<C>>>`, `vote`, `pre_vote`, `install_full_snapshot(vote, snapshot)`, `initialize(members)`, `is_initialized()`, `add_learner(id, node, blocking)`, `change_membership(members, retain)`, `wait(timeout)`, `trigger()` (has `snapshot()`, `purge_log(upto)`, `elect`, `heartbeat`).

`Config` defaults (config/config.rs DEFAULTS): `election_timeout_min: 150`, `election_timeout_max: 300`, `heartbeat_interval: 50`, `install_snapshot_timeout: 200`, `max_payload_entries: 300`, `max_append_entries: 4096`, `replication_lag_threshold: 5000`, `snapshot_policy: SnapshotPolicy::LogsSinceLast(5000)`, `snapshot_max_chunk_size: 3 * 1024 * 1024`, `max_in_snapshot_log_to_keep: 1000`, `purge_batch_size: 1`, `api_batch_linger_ms: 0`, `enable_pre_vote: None`.

`declare_raft_types!` defaults (raft/mod.rs docs): `LeaderId = leader_id_adv::LeaderId<Self::Term, Self::NodeId>` (multiple leaders per term allowed), `Responder<T> = ProgressResponder`, `AsyncRuntime = TokioRuntime`, `Node = BasicNode`. `leader_id_std` is "standard Raft mode, enforcing single leader per term".

### Pattern 1: WAL as an append-only, marker-truncated, group-committed log (milestone 3.2)

**What:** Segment files `<seq:016x>.wal`, fixed size (default 64 MiB, configurable), never modified in place. All mutations (entries, vote, truncate, purge, committed) are *records appended through the one SPSC ring* to the one writer thread, which satisfies openraft's rule "all write IO must be serialized".

**Record framing (recommended; discretion item):**
```
[len: u32 LE][crc32c: u32 LE][type: u8][payload: len-1 bytes]   crc32c covers type || payload || len
types: 1=SegmentPreamble 2=Entry 3=Vote 4=TruncateAfter 5=Purge 6=Committed
Entry payload = index: u64 LE || postcard(C::Entry)     (index outside the blob so recovery can index without decoding)
```
- **Truncation is a marker, never an in-place edit.** `truncate_after(last_log_id)` appends a `TruncateAfter(index)` record; recovery applies markers in order while rebuilding the offset index. In-place `set_len` truncation followed by new appends risks resurrecting a stale tail (old valid records beyond the new data) if the truncate is not durable but the appended data is. Append-only removes that class entirely. (This is the same idea etcd's WAL uses with "later record with a lower index overrides" [ASSUMED from training knowledge].)
- **Segment preamble** (first record in every segment): `{format_version, segment_seq, first_index, last_purged_log_id, latest vote, latest committed}`. The writer re-emits the current vote/purged/committed at every segment roll. Purge deletes only segments entirely at or below the purge point *and older than the newest preamble*, so the vote can never disappear with a deleted segment (forgetting a vote allows double-voting in a term).
- **Writer loop (group commit, D-02):** `drain ring (up to byte cap) -> serialize into one reusable buffer -> write_all -> sync (once) -> complete all callbacks`. Self-clocking batching: while one fsync runs, the next batch accumulates. No timer. Records never span segments; when the next record does not fit, `sync` the old segment, create the next (write preamble), continue.
- **Idle wake-up without a timer window:** the writer spins briefly, then sets `parked = true`, re-checks the ring, then `thread::park()`; the producer publishes, issues `fence(SeqCst)`, and `unpark()`s if `parked` is set (mirror fence on the writer side). Add a long `park_timeout` (e.g. 100ms) purely as a lost-wakeup backstop, never as a batching window.
- **Single producer is natural:** `RaftLogStorage` methods take `&mut self` and the store is moved into `Raft::new`, so exactly one task owns the non-`Clone` `Producer` [VERIFIED: signatures above; `Producer` is non-Clone per src/ipc/ring_buffer.rs:92-93]. The `LogReader` returned by `get_log_reader` is a separate read-only handle sharing the cache.
- **Ring item:** `WalOp { kind, entries: Vec<C::Entry>/pre-encoded bytes, ack: IOFlushed<C> | oneshot }`. Serialize on the writer thread to keep `RaftCore` fast, but cache the `Entry` objects (cheap `Bytes` clones) before publishing so the "entries readable when `append` returns" rule holds.
- **Durability modes (D-04):** `Fsync::Always` (default), `Periodic{ms}` and `Never` (acks sent after `write`, labelled UNSAFE in config docs and a startup warning log).
- **Fail-stop on I/O error:** on any write/fsync error, complete the pending callbacks with `Err`, mark the store failed, and refuse all further ops. Never retry an fsync and assume success (after a failed fsync the kernel may have dropped the dirty pages) [ASSUMED from training knowledge: "fsyncgate"].
- **Directory fsync:** fsync the WAL directory after creating a segment, and after deleting segments on purge (file creation/unlink are not durable until the directory is synced) [ASSUMED from POSIX semantics].

**Read path / memory (NFR-3):** `Arc<RwLock<LogShared>>` holding a `VecDeque` of recent entries under a *byte budget* (default 256 MiB), an offset index `index -> (segment_seq, offset, len)` (about 20 bytes/entry), plus `last_log_id`, `last_purged_log_id`, `vote`, `committed`. Invariant: the cache never evicts an entry above the writer's `flushed_index` watermark (so the offset index always knows where an evicted entry lives). Cache misses `pread` (`FileExt::read_at`) from the segment and decode. Do not keep "everything since last snapshot" in memory (Pitfall 11).

**Recovery (D-16):** list segments sorted by seq; for each, verify preamble, scan records; apply Entry/Truncate/Purge/Vote/Committed to rebuild `LogShared`. In the **last** segment, stop at the first record with bad length/CRC/short read, `set_len` the file to the last good offset and fsync it before accepting appends (this is the torn-tail discard). In a **non-last** segment, a bad record is corruption: return a hard error, never silently skip. Everything fsynced was acked as one batch, so the only invalid bytes a crash can leave are in the final unacked batch.

### Pattern 2: Stamped state machine (milestone 3.1 + R-1)

**What:** every key stores `(value|tombstone, stamp)` in one atomically-swapped cell; `apply_at(index, cmd)` skips when `stamp >= index`.

Current Phase 2 shape [VERIFIED: src/storage/index.rs:26-28]:
```rust
struct Slot {
    value: Atomic<Bytes>,
}
```
Required change (design sketch, not compiled):
```rust
struct Versioned { index: u64, value: Option<Bytes> }   // None = tombstone, still carries its stamp
struct Slot { cell: Atomic<Versioned> }                  // one pointer swap = one atomic (value, stamp) pair
// apply_at(index, op): pin; cur = cell.load; if cur.index >= index { return Skipped }
//   Set/Delete -> swap in Versioned{index, ..}; Cas -> evaluate on cur.value, compare_exchange only on success
//   failed CAS does NOT stamp; defer_destroy(old) as today
```
Keep the existing unversioned `put/get/delete/cas` entry points (index 0) so Phase 2 tests and benches keep passing; add `*_at` variants and a scan API (`for_each_versioned`) since `KvIndex.map` is private.

The Raft-facing command type must include CAS; Phase 2's ring `Command` only has Set/Delete [VERIFIED: src/engine.rs:41-44]:
```rust
pub enum Command {
    Set { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}
```
so define a separate serde `KvRequest { Set, Delete, Cas { key, expected: Option<Bytes>, desired: Option<Bytes> } }` and `KvResponse { Ok, Value(Option<Bytes>), CasFailed { current: Option<Bytes> } }` in `raft/types.rs` (this exact shape compiled with `declare_raft_types!` in the spike).

**Apply path (R-3):** inline in the SM worker task, in chunks (yield to the scheduler every ~256 entries). Phase 2 measured 1-thread SET p50 208 ns / p99 1.5 us [VERIFIED: 02-01-SUMMARY.md table], so a max `api_batch`/`max_append_entries` batch of 4096 is on the order of 1 ms worst case and no `.await` happens while an epoch guard is live (`crossbeam_epoch::Guard` is `!Send`, so the compiler enforces it inside async fns). Put applying behind a small `ApplySink` trait so Phase 4 can swap in the ring -> core-pinned-worker handoff without touching openraft glue.

### Pattern 3: Three-lane transport with real pipelining (milestone 3.3)

- `RaftNetworkFactory::new_client` -> **replication lane**: one persistent TCP connection per peer, lazily connected, reconnect with `backoff()`. `stream_append` is implemented as: a writer task pulls requests from `input` and writes frames immediately (no waiting for the previous response) up to a bounded in-flight window (default 32 requests / 8 MiB), a reader task decodes responses; responses are yielded to openraft **in request order** (single TCP connection preserves order; the trait requires order). Server side: decode frames into a `Stream<AppendEntriesRequest>` and feed `Raft::stream_append`, writing each result back as a response frame.
- `new_heartbeat_client` -> **heartbeat lane**: its own connection, so a large AppendEntries batch or snapshot chunk cannot delay liveness probes (this is what openraft's docs recommend for transports that cannot multiplex).
- `new_snapshot_client` -> **snapshot lane**: its own connection opened on demand. This is how D-12's "separate logical channel" is satisfied with the least machinery. It is an interpretation of D-07/D-12 (one persistent *replication* connection per peer plus dedicated heartbeat and snapshot connections); see A4.
- Unary RPCs (`vote`, `pre_vote`, `transfer_leader`) ride the replication lane as request/response frames.
- **Framing:** `LengthDelimitedCodec` (`length_field_length(4)`, `max_frame_length(...)` set to snapshot chunk + slack, not the 8MB default) for the outer length; inside each frame a fixed header `{kind: u8, flags: u8, lane: u8, req_id: u64}` then the body. Control-plane bodies (vote, membership, meta, errors) = `postcard`; AppendEntries entries go through the same serde path at first (see Don't Hand-Roll note on a later zero-copy entry encoding).
- **Handshake (cheap insurance for the costly D-05 contract):** first frame on every connection is `Hello{magic "LCYC", wire_version: u16, cluster_id: u64, node_id: u64, lane}`; reject mismatches. Put `wire_version` in it so the format can evolve without silent corruption.
- **Raft node addresses** travel in the membership (`BasicNode{addr}`), so `new_client(target, node)` already receives the peer address; there is no separate discovery layer (D-13).

### Pattern 4: Fuzzy snapshot, stamped (milestone 3.4)

- `try_create_snapshot_builder(false)` is called by openraft's SM worker *between* apply commands, then the build is `spawn`ed and apply continues [VERIFIED: core/sm/worker.rs `build_snapshot`: "Building snapshot is a read-only operation that runs in a spawned task"]. So read `a = last_applied` inside `try_create_snapshot_builder` (serialized with apply) and capture `last_membership` there. The builder then runs the scan on `tokio::task::spawn_blocking`.
- **Scan:** iterate `SkipMap` in key order; per key take a *short* `epoch::pin()`, load the `Versioned`, clone the `Bytes`/stamp, drop the pin. `SkipMap::iter().next()` itself pins per step [VERIFIED: crossbeam-skiplist-0.1.3/src/map.rs:692-694 `let guard = &epoch::pin();`], so a long scan does not stall the global epoch as long as we do the same. Include: live keys (any stamp), tombstones with `stamp > a`; **drop tombstones with `stamp <= a`** (R-2).
- **File (D-11):** `snap-<a_index:020>-<a_term>.tmp` -> fsync file -> rename to `.snap` -> fsync directory -> delete older snapshot. Layout: header frame (magic `LCSNAP`, `format_version`, postcard `SnapshotMeta`, created-at), then record-block frames (up to ~64 KiB each: `[klen u32][vlen u32][stamp u64][flags u8][key][value]`* + a block CRC32C), then a footer frame (record count, total bytes, rolling CRC32C). All frames use the shared `wire::frame` codec so a chunk is just "N bytes of the file at offset X" (no re-encoding for InstallSnapshot).
- **Transfer (`full_snapshot`)**: on the snapshot lane send `SnapBegin{vote, meta, total_len}`, then chunks of `option.snapshot_chunk_size().unwrap_or(1 MiB)` (openraft sets it from `Config.snapshot_max_chunk_size`; set that to 1 MiB for D-12) as `SnapChunk{offset, bytes}` with an ack per chunk window (backpressure), then `SnapEnd`; honor `cancel`. Receiver writes chunks into `snap-recv-<id>.tmp`, verifies frames/CRC, then calls `Raft::install_full_snapshot(vote, Snapshot{meta, snapshot: <file handle>})`.
- **Install:** build a *fresh* `LocalEngine` from the file on a blocking thread, then `ArcSwap::store`; persist the file as the current snapshot (rename + dir fsync), delete older snapshots; `applied_state()` then reports `(a, membership)`. Old readers keep their `Arc<LocalEngine>`.
- **Order of durability:** the snapshot file (and its directory entry) must be durable *before* `build_snapshot` returns, because openraft purges the log right after (Pitfall 3).
- **Trigger (D-10):** `SnapshotPolicy::LogsSinceLast(n)` is exactly "every N applied entries". Also set `max_in_snapshot_log_to_keep` and `purge_batch_size` >= entries-per-segment so purge calls actually free whole segments, and keep `replication_lag_threshold` > `n` (openraft's config doc: it "should be greater than snapshot_policy.SnapshotPolicy.LogsSinceLast").

### Pattern 5: Membership and bootstrap (milestone 3.3)

- Static list from `Config`/CLI (`--peers`, existing `Config.peers` string in src/main.rs [VERIFIED: src/main.rs `pub peers: String`]). Exactly one designated bootstrap node runs `if !raft.is_initialized().await? { raft.initialize(members).await? }` with the full static member map; other nodes just start and wait. Never call `initialize` unconditionally on restart.
- Dynamic change: `add_learner(id, node, true)` (blocking: waits for catch-up) then `change_membership(...)`; removal via `RemoveVoters`. In 0.10 openraft skips the joint entry when old/new quorums provably intersect ("skip the joint entry when every old quorum intersects every new quorum"), so test both the joint path (replace >1 voter) and the single-step path.
- 1-node cluster (D-14): `initialize` with one member; same WAL/transport code; the replication lane is simply never opened.

### Anti-Patterns to Avoid
- **Holding one `epoch::pin()` across the snapshot scan.** Phase 2's `KvIndex::len` does this for its whole iteration (src/storage/index.rs:147 `let guard = epoch::pin();` then `self.map.iter()`); copying that pattern for snapshots pins the global epoch for seconds and lets retired values and slab blocks accumulate (violates NFR-3/NFR-4).
- **Running the snapshot scan or WAL I/O on a Tokio worker.** Blocks reactor/IO tasks and can trip election timeouts.
- **Sharing one connection for replication, heartbeats and snapshots** (head-of-line blocking; the exact failure D-12 wants to avoid).
- **In-place WAL truncation** (stale-tail resurrection; use markers).
- **Unconditional `initialize` on startup** (fails or re-bootstraps).
- **Treating macOS timings as evidence for NFR-1.**

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Consensus (elections, log matching, membership, commit tracking) | A Raft state machine | `openraft` | STATE.md locked; subtle edge cases (joint consensus, pre-vote, log revert) |
| CRC32C | Table-driven CRC | `crc32c` crate | HW-accelerated on x86 and aarch64, verified standard vector |
| Length-prefixed frame decode/encode | Manual partial-read loops | `tokio_util::codec::LengthDelimitedCodec` | Partial reads, max-frame enforcement, `BytesMut` split without copy |
| Serialization of openraft structs (Entry, Vote, Membership, requests, errors) | Hand-written encoders for every openraft type | `serde` + `postcard` (openraft `serde` feature) | Many nested types incl. remote errors; hand-roll only the hot `KvRequest` payload later if Phase 6 profiling demands zero-copy decode |
| Temp dirs / file cleanup in tests | `/tmp` juggling | `tempfile` | Isolation, cleanup on panic |
| Randomized model tests | Ad-hoc loops | `proptest` | Shrinking finds minimal CAS counter-examples |
| Atomic engine replacement at install | `RwLock<Arc<..>>` on the read path | `arc-swap` | Lock-free reads (Phase 4 hot path) |

**Key insight:** the only components that are genuinely custom in this phase are the ones CONTEXT.md locked: the WAL (D-01..D-03), the wire format (D-05), and the fuzzy snapshot (D-09). Everything around them should be library code.

## Resolution of Flagged Risks

### R-1: fuzzy snapshot vs CAS -> RESOLVED: per-key stamping is sound for single-key ops

**Naive replay is wrong (counter-example, verified by model):** initial `k=x`. Log: `j1: CAS(k, expected=y, desired=z)` (fails, current is `x`); `j2: CAS(k, expected=x, desired=y)` (succeeds, `k=y`). A fuzzy snapshot with start index `a=0` observes `k=y` (after `j2`). Replay from `a+1`: `j1` now *succeeds* (`k=z`), `j2` then fails. Final `z`, correct is `y`.

**Fix:** store the Raft index of the last successful write with each key in the same atomic cell as the value. On replay of entry `j`: if `stamp(k) >= j` skip, else apply (and stamp `j` on a successful write). Soundness argument: per key, a snapshot cell `(v, s)` equals "the key's state after all entries on that key with index <= s"; replay skips exactly those and re-executes the rest in log order from that base, so the per-key operation sequence is identical to the original. Failed CASes need no stamp (they change nothing; replay either skips them via `j <= s` or re-evaluates them on the same base state as the original).

**Model-check evidence** (Python model run in the scratchpad this session: random single-key SET/DEL/CAS histories, random snapshot window `[a,b]`, per-key random observation time, then restore+replay vs sequential ground truth; 3 keys x 14 ops x 200k trials, and 5 keys x 40 ops x 100k trials):

| Variant | Failures (3 keys) | Failures (5 keys) |
|---------|-------------------|-------------------|
| Naive replay, no stamps | 396 / 200000 | 310 / 100000 |
| Stamped replay, drop **all** tombstones | 51 / 200000 | 59 / 100000 |
| Stamped replay, drop only tombstones with `stamp <= a` | **0 / 200000** | **0 / 100000** |

Why dropping a newer tombstone fails: `k=x`; `j1: CAS(k, absent->q)` fails; `j2: CAS(k, x->None)` (conditional delete) succeeds. Snapshot sees `absent` with no stamp; replay makes `j1` succeed (`q`) and `j2` fail. So tombstones with `stamp > a` must keep their stamp in the snapshot.

**Constraints this imposes (record in the plan):**
1. The (value, stamp) pair must be one atomic cell (a separate `AtomicU64` would tear).
2. The snapshot's `last_log_id` must be `a` = applied index at builder creation (serialized with apply), not the index at scan end.
3. Only **single-key** operations are covered. A multi-key conditional (FR-2 `WATCH`/multi-key CAS) can observe key A post-entry and key B pre-entry and re-evaluate differently; that would need a consistent-cut snapshot (e.g. copy-before-overwrite undo log during snapshotting) or a different design. Not in any current Phase 3-5 milestone, but must be re-opened before such an operation is added (A12).
4. Per-key apply order must be log order (true for a single applier, and for key-hash-sharded workers in Phase 4).

### R-2: tombstones -> RESOLVED
Include tombstones with `stamp > a`; drop tombstones with `stamp <= a`. Restoring from a snapshot therefore also compacts old tombstones (bounded growth across restarts/installs). **Live** tombstone compaction on a running node is still not solved by this phase (Phase 2 known limitation; NFR-4); it is safe in principle to physically remove a tombstone on a live node once `stamp <= last_snapshot.a`, but that coordination with concurrent `put` is out of Phase 3 scope.

### R-3: threading -> RESOLVED for Phase 3
Inline chunked apply on the openraft SM worker task behind an `ApplySink` seam (Pattern 2). WAL writer is a dedicated `std::thread`, never on Tokio. Snapshot scan on `spawn_blocking`. Core-pinned arena worker handoff is the Phase 4 milestone (4.1 "Bridge Raft committed entries to lock-free memory arena").

## Common Pitfalls

### Pitfall 1: Epoch guard held across the snapshot scan
**What goes wrong:** retired `Bytes` boxes and slab blocks cannot be reclaimed for the whole scan; memory balloons on a busy node.
**Why:** `KvIndex::len`-style code pins once for the entire iteration.
**How to avoid:** pin per key (or per small batch, `guard.repin()`), clone `Bytes`, release. Add a test asserting `slab().live_blocks()` stays bounded during a scan with concurrent writers.
**Warning signs:** `live_blocks()` growing linearly during snapshot.

### Pitfall 2: Tokio starvation by snapshot/WAL work
**What goes wrong:** election timeouts under snapshot or large sync work.
**How to avoid:** `spawn_blocking` for scans/loads; WAL on its own thread; never `std::fs` on async tasks except via those.

### Pitfall 3: Log purged before the snapshot is durable
**What goes wrong:** crash after purge but before the snapshot file/dir entry hit disk loses committed data.
**How to avoid:** `build_snapshot` returns only after file fsync + rename + directory fsync. Test: inject a crash between those steps via `SegmentIo`/snapshot-io fakes.

### Pitfall 4: Vote, purged-id or committed lost with a deleted segment
**How to avoid:** segment preamble re-emission and the purge rule in Pattern 1. Test: purge everything purgeable, restart, assert the vote survives.

### Pitfall 5: Treating mid-log corruption as a torn tail
**How to avoid:** torn-tail discard only in the last segment; hard error elsewhere. Test with a flipped byte in an older segment.

### Pitfall 6: Lost wake-up in the park/unpark handshake
**How to avoid:** SeqCst fences on both sides, re-check the ring after setting `parked`, plus the long `park_timeout` backstop. Test with a stress loop (many publish/idle cycles; assert no op waits > backstop).

### Pitfall 7: macOS fsync is a full-device flush
**What goes wrong:** every `sync_data` costs milliseconds. Rust std's `File::sync_data` and `File::sync_all` both call `fcntl(fd, F_FULLFSYNC)` on `target_vendor = "apple"`, and `fdatasync` on Linux [CITED: rust-lang/rust library/std/src/sys/fs/unix.rs `os_datasync`/`os_fsync`]. Measured here (Apple M4 Pro, APFS, 4KB write + sync, 300 iterations): `sync_data` p50 4011us, p99 7496us; `sync_all` p50 4003us, p99 5795us; write-only p50 6us [VERIFIED: ran fsync probe this session].
**Consequence:** NFR-1 (P99 < 2ms durable) cannot be validated on this machine; use it for correctness only. Provide `Fsync::Periodic` for local dev, and run perf validation on Linux NVMe (Phase 6). Record fsync latency in a histogram from day one.

### Pitfall 8: `install_snapshot_timeout` default is 200 ms
**What goes wrong:** openraft passes it as the RPC TTL for a snapshot send; a multi-GB transfer would be cut short if the transport honours it as a total.
**How to avoid:** set it to tens of seconds and apply `option.soft_ttl()` as an *idle/per-chunk* timeout in `full_snapshot`; keep `heartbeat_interval` (50ms default) bound to the heartbeat lane only.

### Pitfall 9: Callback not completed exactly once
**What goes wrong:** if the writer thread drops an `IOFlushed` (panic, early return), `RaftCore` waits forever; completing twice is impossible (consumed by value) but a panic mid-batch can orphan the rest.
**How to avoid:** wrap batch processing so every callback is completed with `Ok` or `Err` even on panic (`catch_unwind` + fail-stop, or `Drop` guard completing with `Err`).

### Pitfall 10: Election timing vs fsync stalls
**What goes wrong:** with a 4-7ms fsync (Mac) or a slow cloud disk, defaults can still work (150ms min) but a disk stall >150ms causes spurious elections, especially with pre-vote off.
**How to avoid:** enable pre-vote (`enable_pre_vote: Some(true)`), set election_timeout_min to a multiple of the worst expected fsync, and expose all timing values in config.

### Pitfall 11: Snapshot cadence vs throughput
**What goes wrong:** with D-10's "e.g. 100k" and a 500k ops/s target, a snapshot would trigger roughly every 0.2 s; each scan of a large keyspace takes seconds, so snapshots would run back to back.
**How to avoid:** choose N from `N >= throughput x snapshot_duration x safety` (default recommended 1,000,000 entries, configurable), bound log memory by the byte-budgeted cache not by N, and make `try_create_snapshot_builder` return `None` while a build is already running. Raise as A3.

### Pitfall 12: Frame/allocation bombs and ordering bugs in the transport
**How to avoid:** `max_frame_length` set explicitly; per-lane in-flight window; verify response order equals request order in `stream_append` (test with reordered fake delays); close the connection on any decode error (no resync).

### Pitfall 13: Sparse preallocation does not remove metadata cost
`File::set_len` produces a sparse file on most filesystems; `fdatasync` on a write into an unallocated region still has to commit block-allocation metadata [ASSUMED]. Baseline with `set_len`; add an optional background pre-zeroing thread for the *next* segment (portable, no `libc`) if the Linux benchmark shows it matters. New zero-filled segments are safe for torn-tail detection because a zero length/CRC reads as end-of-log.

## Code Examples

### Type configuration (compiled in the scratchpad spike against `=0.10.0-alpha.38`)
```rust
// Source: scratchpad spike, compiled and tested this session [VERIFIED]
use bytes::Bytes;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvRequest {
    Set { key: Bytes, value: Bytes },
    Delete { key: Bytes },
    Cas { key: Bytes, expected: Option<Bytes>, desired: Option<Bytes> },
}
impl std::fmt::Display for KvRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{:?}", self) }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResponse { Ok, Value(Option<Bytes>), CasFailed { current: Option<Bytes> } }

openraft::declare_raft_types!(
    pub TypeConfig:
        D = KvRequest,
        R = KvResponse,
        Node = BasicNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, u64>,
);
```
`postcard::to_stdvec(&entry)` / `postcard::from_bytes` round-trips `EntryOf<TypeConfig>` (verified, encoded size 8 bytes for a 1-byte key/value entry).

### Pipelined `stream_append` client (design sketch using the verified signatures)
```rust
// impl RaftNetworkV2<TypeConfig> for PeerClient  (replication lane)
fn stream_append<'s, S>(&'s mut self, input: S, option: RPCOption)
    -> BoxFuture<'s, Result<BoxStream<'s, Result<AppendResult<C>, RPCError<C>>>, RPCError<C>>>
where S: Stream<Item = AppendEntriesRequest<C>> + OptionalSend + Unpin + 'static
{
    // 1. ensure connection (Hello handshake, lane = Replication)
    // 2. spawn writer: while let Some(req) = input.next().await { acquire in-flight permit; write frame(req_id++) }
    // 3. return reader stream: decode response frames in order, release permit per frame,
    //    map to Ok(AppendResult) / Err(RPCError); end the stream on socket error (openraft backs off + restarts)
}
```

### Group-commit writer loop (design sketch)
```rust
loop {
    let n = rx.consume_batch(MAX_OPS, |op| batch.push(op));     // Phase 2 ring Consumer
    if n == 0 { park_if_still_empty(); continue; }
    buf.clear();
    for op in &batch { record::encode_into(&mut buf, op); }     // CRC32C per record
    io.write_all_at_tail(&buf)?;                                // may roll segment
    if cfg.fsync == Fsync::Always { io.sync()?; }               // ONE fsync for the whole batch
    for op in batch.drain(..) { op.ack.io_completed(Ok(())); }  // after durable
}
// any Err above: ack all pending with Err, set failed flag, exit (fail-stop)
```

### Stamped replay skip (design sketch; semantics validated by the model check)
```rust
fn apply_at(&self, index: u64, req: &KvRequest) -> KvResponse {
    // pin; load cell; if cell.index >= index { return KvResponse::Ok }   // already reflected in snapshot
    // Set/Delete: swap Versioned{index, ..}, defer_destroy(old)
    // Cas: compare cell.value with expected; on match compare_exchange Versioned{index, desired}; else CasFailed{current}
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| openraft 0.9 `RaftNetwork` (v1), one in-flight append | `0.10` `RaftNetworkV2::stream_append`, `Inflight::LogsSince` streaming mode | 0.10 alpha line | Real pipelining; D-07 achievable |
| `Sealed` storage traits behind `storage-v2` | Unsealed `RaftLogStorage`/`RaftStateMachine`, `io::Error`, `IOFlushed`, `truncate_after` | 0.10 | Simpler glue; errors are `io::Error` |
| `SnapshotData` in `RaftTypeConfig` with `AsyncRead+AsyncWrite+AsyncSeek` bound (or `generic-snapshot-data`) | `SnapshotData` is an associated type on the state machine/network, no bound | 0.10 | Snapshot data can be a file handle directly |
| `SnapshotMeta.snapshot_id` | removed (meta = `last_log_id` + `last_membership`) | 0.10 | Name snapshot files from `last_log_id` |
| `single-term-leader` feature | `leader_id_std::LeaderId` type | 0.10 | Choose `leader_id_std` explicitly |
| Heartbeats share the replication client | `new_heartbeat_client` / `new_snapshot_client` | 0.10 | Native support for D-12 |
| bincode for serde binary | postcard (bincode unmaintained, RUSTSEC-2025-0141) | Dec 2025 | Use postcard |

**Deprecated/outdated:** `bincode` (unmaintained); openraft `LogFlushed` (deprecated alias of `IOFlushed` in 0.10); `RaftNetwork` v1 in 0.10 (moved to `openraft-legacy`).

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Using the **alpha** `openraft =0.10.0-alpha.38` is acceptable (38 alphas in ~7 months; API still moving; pinned with `=` and Cargo.lock). Required to honour D-07 pipelining and D-12 dedicated lanes | Summary, Standard Stack | If rejected: fall back to 0.9.26, relax D-07 to "batched, one in flight", and rewrite storage glue for sealed/`storage-v2` v2 traits + 0.9 `RaftNetwork`. **Needs user confirmation** |
| A2 | `leader_id_std` (one leader per term) is preferable to the macro default `leader_id_adv` for textbook semantics and Phase 4 Jepsen reasoning | Code Examples | Minor; `adv` allows faster elections but weaker mental model |
| A3 | Default snapshot interval N = 1,000,000 entries (not 100k), plus "no overlapping builds" | Pitfall 11 | If throughput is far lower than target, snapshots are rarer than ideal and the log is larger |
| A4 | D-07/D-12 are satisfied by one persistent pipelined replication connection plus separate heartbeat and snapshot connections ("lanes"), rather than multiplexing all on one TCP connection | Pattern 3 | If the user meant a strictly single TCP connection per peer, a multiplexed priority writer is needed (more code, HOL risk) |
| A5 | `SkipMap` iteration visits every key present for the whole scan (successor-by-last-key iteration, base.rs RefIter); keys inserted mid-scan may or may not appear | Pattern 4 | A missed key would be silently absent from the snapshot; mitigated by the required concurrent-writer snapshot test |
| A6 | postcard/serde deserialization of a size-prefixed `Vec` does not preallocate the claimed length unboundedly (serde "cautious" preallocation) | Security | Frame size cap is the real bound regardless; low |
| A7 | `set_len` sparse files still pay block-allocation metadata on `fdatasync` | Pitfall 13 | Only affects an optimization, not correctness |
| A8 | Inline chunked apply (<~1ms per 4096-entry batch) is acceptable on the SM worker task | Pattern 2 | If larger values/oversize heap fallback make batches slower, yield more often or move to `block_in_place` |
| A9 | NVMe with power-loss protection gives sub-millisecond fsync; consumer/cloud disks often 1-5ms+ | Summary | Governs whether NFR-1 is attainable on the target hardware |
| A10 | An etcd-style "later record overrides" scheme and the fsyncgate behaviour (never retry a failed fsync) are accurate general knowledge | Pattern 1 | Design choice stands on its own reasoning; low |
| A11 | `openraft-rt-sim` is mature enough for deterministic consensus tests | Standard Stack | Optional; fall back to real Tokio + `Raft::wait()` |
| A12 | Multi-key transactions (FR-2 `WATCH`/multi-key conditional) are not part of Phase 3-5 milestones and will need a different snapshot cut when added | R-1 | If they are added without revisiting, replay can diverge silently |
| A13 | A directory fsync is required for durable segment/snapshot create, rename and delete | Pattern 1/4 | Skipping it risks losing a freshly created file after power loss |

## Open Questions (RESOLVED)

1. **Accept the openraft alpha (A1)?**
   - Known: 0.9.26 cannot pipeline AppendEntries and only seals the storage traits behind an unstable feature; 0.10 alpha.38 compiles with our toolchain and has the needed hooks.
   - Unclear: user appetite for a pre-release dependency.
   - Recommendation: confirm before planning; plan a Wave 0 "API gate" task that compiles the three trait skeletons against alpha.38 and aborts to the 0.9.26 plan if any hook is missing.
   - **RESOLVED:** the user accepted `openraft =0.10.0-alpha.38`; plan 03-01 Task 1 carries the API gate and stops at a checkpoint (no fallback) if a hook is missing.
2. **Snapshot default N and time/size floors (A3).** Recommend 1M entries; confirm or give the target dataset size so the planner can bound snapshot duration.
   - **RESOLVED:** the snapshot default is 1,000,000 applied entries (configurable), with no overlapping builds; see plans 03-12 and 03-13.
3. **Snapshot retention.** Recommend keep exactly one valid snapshot; if it is corrupt at startup the node must be wiped and rejoined (the log below it was purged). Keeping two costs disk and delays purge. Confirm.
   - **RESOLVED:** keep exactly one snapshot; a corrupt latest snapshot is a hard error requiring wipe and rejoin (plan 03-10).
4. **Deterministic cluster tests.** Real Tokio + `FakeNetwork` + `Raft::wait()` is the baseline; `openraft-rt-sim` gives virtual time but needs an in-memory log store (the WAL writer thread is real-time). Decide whether the stricter determinism is worth the SUS-flagged dependency.
   - **RESOLVED:** `openraft-rt-sim` and `openraft-memstore` are excluded; cluster tests use real Tokio, `FakeNetwork` and `Raft::wait()` (plans 03-06, 03-14, 03-16).
5. **Linux NVMe validation environment.** Docker (Rancher Desktop VM) is available for Linux correctness runs but its disk is virtualized; NFR-1 needs real hardware (Phase 6). Is such a machine available?
   - **RESOLVED (deferred):** NFR-1 and NFR-2 hardware validation is deferred; the user will provide a Linux NVMe host later. Phase 3 records an informational bench only (plan 03-11) and does not claim NFR-1 or NFR-2.
6. **Scope: modifying Phase 2 code.** `KvIndex`/`LocalEngine` must change (stamped cell, scan API, `apply_at`). Plans should include updating Phase 2 tests/benches and re-running the Phase 2 bench to confirm no read-path regression (Phase 2 GET p99 was 250ns/542ns).
   - **RESOLVED:** the Phase 2 edits are made in plan 03-02, which re-runs the 17 Phase 2 tests and the GET p99 regression check (`scripts/check-get-regression.sh`).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| rustc / cargo | everything | yes | 1.99.0 | - (openraft 0.10 needs >= 1.88) |
| crates.io network | fetching deps | yes | - | `cargo vendor` |
| Docker (Rancher Desktop) | Linux-only checks (fdatasync path, future io_uring) | yes | server 28.4.0 | skip Linux checks locally |
| Linux NVMe host | NFR-1/NFR-2 validation | no | - | defer to Phase 6; use `Fsync::Periodic` for local functional runs |
| cargo-miri | `unsafe` review of ring/slab changes | binary present (`~/.cargo/bin/cargo-miri`); nightly toolchain not confirmed | - | skip |
| cargo-nextest | faster test runs | no | - | plain `cargo test` |
| fio | disk characterisation | no | - | in-repo Criterion fsync bench |
| macOS APFS | local dev | yes (arm64, Apple M4 Pro) | - | note F_FULLFSYNC behaviour (Pitfall 7) |

**Missing with no fallback:** a real Linux NVMe host for latency validation (does not block Phase 3 functionality; blocks the NFR-1 claim).
**Missing with fallback:** nextest, fio.

## Validation Architecture

(No `.planning/config.json` exists, so `workflow.nyquist_validation` is treated as enabled.)

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in `cargo test` (17 existing tests green) + Criterion 0.5 benches; add `proptest` 1.12, `tempfile` 3 |
| Config file | none (Cargo.toml only); add `[[bench]] name = "wal"` with `harness = false` |
| Quick run command | `cargo test --lib -- wal:: wire:: snapshot::` (module-scoped), `cargo test --lib raft::` |
| Full suite command | `cargo test` (add `--release` for the multi-node tests that need throughput) |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FR-4 / D-01 | Record encode/decode round-trip; CRC detects every single-bit flip | unit | `cargo test --lib wal::record` | Wave 0 |
| FR-4 / D-16 | Truncate last segment at every byte offset of the final batch -> recovery yields a valid prefix, no panic, torn tail discarded | unit/property | `cargo test --lib wal::recovery` | Wave 0 |
| FR-4 / D-16 | Bad CRC in a non-last segment -> hard error | unit | `cargo test --lib wal::recovery::corrupt_middle` | Wave 0 |
| FR-4 / D-02 | Group commit: 1000 rapid appends with a delayed fake fsync -> far fewer syncs than appends; no ack before its covering sync | unit (FakeIo) | `cargo test --lib wal::writer::group_commit` | Wave 0 |
| FR-4 | Vote/purged/committed survive purge of old segments + restart | unit | `cargo test --lib wal::store::preamble_survives_purge` | Wave 0 |
| FR-4 | Truncate marker then append; restart replays to the same log | unit | `cargo test --lib wal::store::truncate_marker` | Wave 0 |
| FR-4 | Writer failure (fake fsync error) fails all pending acks and stops | unit | `cargo test --lib wal::writer::fail_stop` | Wave 0 |
| FR-4 / R-1 | Stamped fuzzy-snapshot replay equals sequential apply (Rust port of the Python model; CAS, SET, DEL; tombstone rule) | proptest | `cargo test --lib raft::state_machine::fuzzy_replay_model` | Wave 0 |
| FR-4 / R-1 | Deterministic counter-example (`CAS y->z` fails, `CAS x->y`) restored+replayed correctly | unit | `cargo test --lib raft::state_machine::cas_counterexample` | Wave 0 |
| FR-4 / NFR-4 | Snapshot with concurrent writers: restore+replay equals live state; `slab().live_blocks()` bounded during scan | stress | `cargo test --release --lib snapshot::concurrent_scan` | Wave 0 |
| FR-4 | Snapshot file CRC/footer detects truncation and bit flips; chunked copy reassembles identical bytes | unit | `cargo test --lib snapshot::format` | Wave 0 |
| FR-3 / D-05 | Frame codec round-trip, oversize frame rejected, handshake mismatch rejected | unit | `cargo test --lib wire::` | Wave 0 |
| FR-3 / D-07 | `stream_append` keeps >1 request in flight and yields responses in order (fake delays) | integration | `cargo test --test transport pipelined_in_order` | Wave 0 |
| FR-3 / D-12 | Snapshot transfer in progress does not delay heartbeats (separate connection) | integration | `cargo test --test transport snapshot_does_not_starve_heartbeat` | Wave 0 |
| FR-3 / D-15 | 3-node in-process cluster elects a leader; client writes replicate; leader killed -> new leader; partition heals | integration (FakeNetwork) | `cargo test --test cluster` | Wave 0 |
| FR-6 / D-13 | `add_learner` + `change_membership` add and remove a voter (joint and single-step) | integration | `cargo test --test cluster membership_` | Wave 0 |
| FR-6 / D-14 | 1-node cluster full write/restart via WAL | integration | `cargo test --test cluster single_node_restart` | Wave 0 |
| FR-6 / D-16 | Kill-and-restart: committed writes survive; node rejoins; lagging follower catches up via snapshot install | integration | `cargo test --test cluster kill_restart_snapshot_catchup` | Wave 0 |
| D-15 | Real-TCP localhost smoke: 3 nodes over actual codec elect and replicate | integration | `cargo test --test tcp_smoke` | Wave 0 |
| NFR-1/2 (informational) | WAL append->ack latency and throughput histogram; fsync latency | bench | `cargo bench --bench wal` | Wave 0 |

### Sampling Rate
- **Per task commit:** the module-scoped quick command for the module touched (< 30 s).
- **Per wave merge:** `cargo test` (all unit + integration) plus `cargo check --all-targets` with zero warnings.
- **Phase gate:** full suite green (including the 17 Phase 2 tests), Phase 2 bench re-run showing no GET regression, before `/gsd-verify-work`.

### Wave 0 Gaps
- [ ] `tests/cluster.rs`, `tests/transport.rs`, `tests/tcp_smoke.rs` — integration harness + `FakeNetwork`/`NetController`
- [ ] `src/wal/io.rs` `FakeIo` (counts syncs, injects delay/errors, simulates torn writes) — makes every WAL test deterministic
- [ ] Port of the model check (`fuzzy.py`) to a proptest — must exist and pass before the snapshot milestone is planned in detail
- [ ] Dev-deps: `tempfile`, `proptest`; bench `benches/wal.rs`
- [ ] openraft API gate task (compile skeleton of the three traits against alpha.38; decide A1)

## Security Domain

`security_enforcement` is not set to false (no config file), so this section applies. D-08 explicitly accepts no TLS/auth for this phase.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no (deferred, D-08) | none this phase; document that any host reaching the Raft port can inject RPCs and take over the cluster -> bind to a private interface, call out in README |
| V3 Session Management | no | - |
| V4 Access Control | no (deferred) | - |
| V5 Input Validation | yes | explicit `max_frame_length`; bounded in-flight window; reject Hello mismatch; validate every decoded frame/record length against remaining bytes; postcard decode errors close the connection |
| V6 Cryptography | no | CRC32C is integrity-against-corruption only, NOT authentication; do not describe it as security |
| V8 Data Protection | partial | WAL/snapshot files are plaintext; create dirs `0700` / files `0600`; encryption at rest out of scope |
| V12 Files | yes | file names derived only from numeric ids (no peer-supplied path components); snapshot receive goes to `*.tmp` and is validated before rename; `O_EXCL`-style create for segments |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Oversized length prefix -> memory exhaustion | DoS | `LengthDelimitedCodec::max_frame_length`, per-connection byte budget |
| Rogue peer sends crafted AppendEntries / Vote | Spoofing, Tampering | accepted risk (D-08); Hello `cluster_id` check blocks accidental cross-cluster traffic only |
| Snapshot chunk flood / bogus offsets | DoS, Tampering | single active receive per lane, enforce `offset == bytes_written`, size cap from `SnapBegin.total_len`, CRC validate before install |
| Torn/corrupt WAL accepted as valid | Tampering (integrity) | per-record CRC32C, fail hard on non-tail corruption |
| Path traversal via snapshot/segment names | Tampering | numeric-only names, join under a fixed directory |
| Slow-loris on handshake | DoS | handshake timeout, close idle unauthenticated connections |

## Sources

### Primary (HIGH confidence)
- openraft `v0.10.0-alpha.38` source (shallow clone at tag, read this session): `openraft/src/storage/v2/{raft_log_storage,raft_state_machine,raft_snapshot_builder,raft_log_reader,entry_responder}.rs`, `storage/callback.rs`, `storage/snapshot_meta.rs`, `network/v2/network.rs`, `network/factory.rs`, `network/stream_append_trait.rs`, `network/rpc_option.rs`, `replication/mod.rs` (`run_stream_session`, `new_request_stream`), `replication/snapshot_transmitter.rs`, `core/sm/worker.rs`, `core/raft_core.rs`, `core/heartbeat/{handle,worker}.rs`, `config/config.rs`, `raft/mod.rs` (`declare_raft_types!`, `Raft::*`), `raft/impl_raft_blocking_write.rs`, `docs/features.md`, `docs/protocol/log_replication.md`; `examples/log-wal`, `examples/network-v2-http`, `examples/sm-mem`
- openraft `v0.9.26` source: `replication/mod.rs` ("at most 1 in flight data"; separate `snapshot_network`), `storage/v2.rs` (`Sealed`, `storage-v2`)
- crates.io API: versions, dates, features, dependencies for openraft, crc32c, postcard, tokio-util, futures, arc-swap, tempfile, thiserror, bincode, proptest, raft-log
- `gsd-tools query package-legitimacy check` output (this session)
- Repo files read this session: `Cargo.toml`, `src/{lib,main,engine}.rs`, `src/storage/index.rs`, `src/ipc/ring_buffer.rs`, `.planning/{REQUIREMENTS,STATE,ROADMAP}.md`, `.planning/phases/02-lock-free-core/{02-01-SUMMARY,02-VERIFICATION}.md`, `.planning/research/ARCHITECTURE_SPIKES.md`, `03-CONTEXT.md`, `03-DISCUSSION-LOG.md`
- `crossbeam-skiplist-0.1.3/src/map.rs` (local registry copy)
- Experiments run this session (scratchpad): openraft 0.10 spike (compile + postcard round-trip), fsync latency probe, crc32c benchmark, Python fuzzy-snapshot model check

### Secondary (MEDIUM confidence)
- docs.rs: openraft 0.9.26 trait pages (`RaftLogStorage`, `RaftStateMachine`, `RaftNetwork`, `RaftNetworkFactory`, features); tokio-util 0.7.20 `length_delimited::Builder`; crc32c 0.6.9 overview
- raw.githubusercontent rust-lang/rust `library/std/src/sys/fs/unix.rs` (F_FULLFSYNC on Apple)
- rustsec.org RUSTSEC-2025-0141 (bincode unmaintained)

### Tertiary (LOW confidence)
- General-knowledge items tagged `[ASSUMED]`: etcd-style override semantics, fsyncgate behaviour, directory-fsync requirement, sparse-file metadata cost, serde cautious preallocation, NVMe fsync ranges

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH for openraft API/behaviour and versions (read from source at the tag, compiled a spike); MEDIUM overall because the recommended openraft line is a pre-release (A1)
- Architecture: MEDIUM-HIGH. WAL/transport/snapshot designs are reasoned and cross-checked against openraft's own examples and docs, but none is implemented yet; R-1 resolution is backed by a 300k-trial model check
- Pitfalls: HIGH for the verified ones (macOS fsync, epoch pinning, 200ms snapshot timeout, purge ordering), MEDIUM for the `[ASSUMED]` ones

**Research date:** 2026-11-09
**Valid until:** 2026-11-16 for the openraft pin (alpha releases land roughly every one to two weeks; re-check `crates.io` before locking), 30 days for the rest
