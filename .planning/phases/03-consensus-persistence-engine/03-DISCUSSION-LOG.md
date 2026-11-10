# Phase 3: Consensus & Persistence Engine - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-11-09
**Phase:** 3-Consensus & Persistence Engine
**Areas discussed:** WAL design & fsync, Raft network transport, Snapshot format & trigger, Membership & test harness

---

## WAL design & fsync

| Question | Options considered | Selected |
|---|---|---|
| On-disk layout | Segmented append-only files; single ever-growing file; embedded KV store (RocksDB/redb) | Segmented append-only files |
| Group commit | Dedicated writer thread batching per fsync; timer-based batching window; fsync per append | Dedicated writer thread |
| I/O backend | std::fs on writer thread; io_uring; tokio::fs | std::fs on writer thread |
| Durability default | fsync before ack, configurable; always strict; you decide | fsync before ack, configurable |

## Raft network transport

| Question | Options considered | Selected |
|---|---|---|
| Transport | Custom length-prefixed binary over Tokio TCP; gRPC (tonic); QUIC | Custom binary over TCP |
| Relation to Phase 5 RPC | Separate port + shared codec; one multiplexed port; fully independent | Separate port, shared codec |
| Connections | One persistent pipelined connection per peer; request/response; you decide | Persistent pipelined |
| Security | Plaintext, TLS deferred; shared-secret handshake; mTLS now | Plaintext, TLS deferred |

## Snapshot format & trigger

| Question | Options considered | Selected |
|---|---|---|
| Capture method | Epoch-pinned live iteration; stop-the-world copy; MVCC/copy-on-write | Epoch-pinned live iteration |
| Trigger | Entries since last snapshot; WAL size; both | Entries since last snapshot |
| File format | Framed header + KV records + CRC; reuse WAL format; you decide | Framed file |
| Transfer | Chunked streaming; single large message; out-of-band file copy | Chunked streaming |

## Membership & test harness

| Question | Options considered | Selected |
|---|---|---|
| Membership | Static bootstrap + dynamic joint consensus; static only; gossip | Static bootstrap + dynamic |
| Single-node mode | Yes (1-node cluster); no (min 3) | Yes |
| Testing | In-process cluster with fake network; real sockets only; unit tests only | In-process + fake network |
| Recovery bar | Kill-and-restart incl. torn WAL records; clean shutdown only; you decide | Kill-and-restart tests |

**Notes:** All recommended options were chosen. Assistant flagged risk R-1 (fuzzy snapshot vs non-idempotent CAS) for the researcher rather than deciding it.

## Claude's Discretion

Segment/chunk sizes, snapshot-N default, Raft timing parameters, module layout.

## Deferred Ideas

TLS/mTLS, io_uring backend, gossip discovery.
