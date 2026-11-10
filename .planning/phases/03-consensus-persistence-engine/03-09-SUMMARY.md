---
phase: 03-consensus-persistence-engine
plan: 09
subsystem: consensus
tags: [tcp-transport, openraft, pipelined-appendentries, frame-codec, raft-network]
requires: ["03-01", "03-04", "03-06"]
provides:
  - "net::protocol: frame kinds 16-26, WireAppendResult (openraft AppendResult has no serde), RemoteFailure, SnapBegin/SnapAck/snap-chunk helpers, AbortOnDrop"
  - "net::server::{PeerServer, ServerStats}: Hello-gated accept loop, per-lane frame caps, vote/pre-vote and pipelined AppendEntries dispatch into Arc<dyn RaftHandler>"
  - "net::client::{TcpNetConfig, TcpNetworkFactory, PeerClient}: lazy lane-tagged connections; unary vote/pre_vote/append_entries; pipelined stream_append; full_snapshot stubbed"
  - "tests/transport.rs: 22 loopback TCP tests against a stub RaftHandler"
affects: [03-13, 03-15, 03-16, 03-17]
tech-stack:
  added: []
  patterns: [id-before-frame queueing for in-order pipelining, semaphore request and byte windows, single writer task per connection, drop-guard connection cancellation, wire-owned result type for non-serde openraft types]
key-files:
  created:
    - src/net/protocol.rs
    - src/net/client.rs
    - src/net/server.rs
    - tests/transport.rs
  modified:
    - src/net/mod.rs
    - src/net/fake.rs
    - src/wire/frame.rs
key-decisions:
  - "Interpretation A4 (user must be told, repeat in 03-16): each peer gets up to THREE TCP connections (replication, heartbeat, snapshot), each lazily dialed and Hello-tagged with its lane. D-07 literally says one persistent connection per peer; separate sockets were chosen so a bulk lane cannot head-of-line block heartbeats (D-12). Strictly one connection per peer would need a priority-multiplexing writer, which is a user decision and is not built."
  - "Unary AppendEntries on the server also goes through handler.stream_append, so ordering and the request/byte windows apply uniformly; RaftHandler::append_entries is unused by the TCP server."
  - "openraft AppendResult is not serde, so the wire carries WireAppendResult including the optional conflict hint; no information is dropped."
  - "Error mapping: connect/EOF/socket -> Unreachable; connect, handshake and response timeouts -> Timeout; codec, frame, req_id mismatch and wrong-kind -> Network; handler error frame -> Network (connection kept); Fatal from the remote -> Unreachable. A peer-rejected Hello (wrong cluster or version) -> Unreachable so the leader backs off instead of retrying a permanent rejection."
  - "Server windows bound memory: request permits and KiB byte permits travel with each queued request and are released when its response is written; a body larger than the whole byte window is admitted alone."
metrics:
  tasks: 2
  completed: 2026-11-09
status: complete
actuals:
  tokens: 21000
  tasks: 2
  commits: 4
plan_head_before: ad1a45623822cf0783e856f47c8d2e93a973086b
plan_head_after: 4a0018819db6634f5650452395c45e6660c765d3
---

# Phase 3 Plan 09: Raft TCP peer transport, first half Summary

A real Tokio TCP transport for Raft RPCs on the shared frame codec: Hello-gated lane-tagged connections, unary Vote/Pre-Vote/AppendEntries, and a pipelined in-order replication lane with request and byte windows, all proven hermetically against a stub `RaftHandler`.

## What was built

- **`src/net/protocol.rs`:** kinds 16-26 (APPEND, VOTE, PREVOTE, SNAP_*), `encode_body`/`decode_body` (postcard, trailing bytes rejected), `WireAppendResult` with `From` conversions to and from openraft's `AppendResult` (conflict hint preserved), `RemoteFailure` (fatal flag plus a message capped at 256 bytes, carried in `KIND_ERROR`), `SnapBegin`/`SnapAck`/snapshot-chunk helpers for plan 03-17.
- **`src/net/server.rs`:** `PeerServer::bind/local_addr/stats/shutdown`. 4 KiB frame cap until the Hello is accepted, then `max_frame_len` (replication, heartbeat) or `snapshot_chunk_size + 64 KiB` (snapshot). One writer task per connection owns the sink. Vote/Pre-Vote run as concurrent tasks (bounded by a semaphore). AppendEntries create a lazy per-connection session: ids are queued before requests, a responder pairs each `stream_append` result with the oldest id and writes `APPEND_RESP` in order. Decode errors, unknown kinds, lane-byte mismatch, bad bodies and oversize prefixes close the connection and count `decode_errors`. Snapshot-lane connections are accepted, lane-counted and capped, but every `SNAP_*` frame is an unknown kind until plan 03-17.
- **`src/net/client.rs`:** `TcpNetConfig` (documented defaults, `lane_frame_cap`), `TcpNetworkFactory` (`new_client`/`new_heartbeat_client`/`new_snapshot_client` -> `PeerClient` on Replication/Heartbeat/Snapshot), `PeerClient` with lazy connect+Hello. `stream_append` takes the connection for the session: a writer task acquires a request permit and KiB permits, queues the id, then sends; the returned stream checks each response against the oldest unanswered `req_id`, applies `soft_ttl` per response, and ends with one error item on any mismatch, wrong kind, decode failure, timeout or EOF (connection dropped, no resync). `full_snapshot` returns `Unreachable` (plan 03-17).
- **`tests/transport.rs`:** 22 tests including `vote_roundtrip_over_tcp`, `hello_cluster_mismatch_rejected_over_tcp`, `handshake_timeout_closes_silent_connection`, `pipelined_in_order`, `inflight_window_bounded`, `oversize_frame_closes_connection`, byte window, garbage body, lane mismatch, wrong kind, req_id mismatch, EOF, response timeout, vote served while appends are withheld, reconnect after restart.

## Verification

- `cargo test --test transport`: 22 passed, stable over 5 consecutive runs. `cargo test --lib`: 114 passed. `tests/openraft_api_gate.rs`: 2 passed.
- `bash scripts/check-warnings.sh` exits 0. Tracer gate: Task 1 `<verify>` re-ran green before Task 2 expanded the slice.
- `pipelined_in_order`: 20 requests answered after 20 ms each arrive in order and `max_inflight_observed >= 2`. `inflight_window_bounded`: with a window of 4 and withheld responses the server sees exactly 4 frames (max in flight 4) until responses are released.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] FrameCodec misread body bytes as a length prefix (plan 03-04 code)**
- **Found during:** Task 2 (`byte_window_admits_oversized_request_alone`; server logged `frame too large: declared 268435456`).
- **Issue:** `FrameCodec::decode` peeked at the buffer front on every call. After the inner `LengthDelimitedCodec` consumed a prefix and waited for the rest of the body, the front is body data, so any frame split across reads could be rejected as `TooLarge` (and a real oversize prefix could be missed). Invisible in 03-04's tests, which only deliver whole frames; fatal for real TCP.
- **Fix:** track a `mid_frame` flag and only peek when a prefix is expected; regression test feeds a frame in small pieces whose body starts with `0x10000000`.
- **Files modified:** src/wire/frame.rs
- **Commit:** be93465

**2. [Rule 3 - Blocking/cleanup, per orchestrator note] fake network reuses `wire::frame::Lane`**
- 03-06's local `Lane` enum was replaced by `pub use crate::wire::frame::Lane` (identical variants and derives, so `fake::Lane` paths keep working).
- **Files modified:** src/net/fake.rs  **Commit:** 356b5bf

**3. [Interpretation] Handler errors and `AppendResult` on the wire**
- openraft's `RPCError<TypeConfig>` has no constructible remote-error variant (same finding as 03-06), so a handler failure frame maps to `Network` and a remote `Fatal` to `Unreachable`, matching the fake network. `AppendResult` has no serde, so `WireAppendResult` was added to protocol.rs (the plan only named request/response types).

Otherwise the plan executed as written. `Cargo.toml` was not touched; no packages were added.

## Notes for downstream plans

- **03-17 (snapshot and heartbeat lanes):** extend `run_lane`'s `match` in server.rs for `SNAP_*` on `Lane::Snapshot` and replace the `full_snapshot` stub; `TcpNetConfig` already carries `snapshot_chunk_size`, `snapshot_ack_window`, `snapshot_idle_timeout`, `max_snapshot_bytes`, `recv_dir`; `ServerStats::snapshot_chunks` and `protocol::{SnapBegin, SnapAck, encode_snap_chunk, decode_snap_chunk}` are ready. The heartbeat lane already shares replication framing and sessions.
- **Known limits (not stubs):** the server does not cap concurrent connections; a handler `Fatal` error frame can be lost if the connection teardown races the writer, in which case the client sees EOF, which maps to the same `Unreachable`. Plaintext, unauthenticated by design (D-08, T-03-22 accepted); README guidance lands in plan 03-15.

## Threat Flags

None beyond the plan's register. T-03-20 mitigated: 4 KiB pre-Hello cap, per-lane caps, request and byte windows on both client and server, bounded channels, close on any decode error.

## Known Stubs

- `PeerClient::full_snapshot` returns `Unreachable("snapshot transfer not implemented yet")` by design until plan 03-17 (src/net/client.rs).

## Self-Check: PASSED

- Files found: src/net/protocol.rs, src/net/client.rs, src/net/server.rs, tests/transport.rs, src/net/mod.rs
- Commits found on branch: 3c4f20d, be93465, 356b5bf, 4a00188
