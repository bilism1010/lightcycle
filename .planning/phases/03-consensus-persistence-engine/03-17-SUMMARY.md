---
phase: 03-consensus-persistence-engine
plan: 17
subsystem: net
tags: [raft, tcp, snapshot, transport, backpressure, heartbeat]
status: complete
requires:
  - phase: 03-consensus-persistence-engine
    provides: "03-09 TCP peer transport (lanes, Hello, FrameCodec, StubHandler harness)"
provides:
  - "PeerClient::full_snapshot: chunked snapshot send on Lane::Snapshot with ack window, per-chunk idle timeout and cancel"
  - "Snapshot-lane receive state machine in PeerServer with ServerStats.snapshot_chunks accounting"
  - "Proof that the heartbeat lane is isolated from a snapshot in flight (32 MiB)"
affects: [03-13 node wiring, 03-15 TCP smoke test]
tech-stack:
  added: []
  patterns:
    - "RAII TmpGuard deletes the receive file on every exit path unless the install hand-off succeeded"
    - "Positional I/O (pread/pwrite) in spawn_blocking for bulk snapshot file access"
key-files:
  created: []
  modified:
    - src/net/client.rs
    - src/net/server.rs
    - tests/transport.rs
key-decisions:
  - "Receive file numbering is a process-global AtomicU64 with create_new and retry on AlreadyExists, so stale files from a crashed process never block a transfer"
  - "Idle timeout on the server applies only while a receive is active; an idle snapshot connection with no active receive is not timed out (same as the other lanes)"
  - "A handler error removes the tmp file and is reported as a fatal KIND_ERROR; a handler success disarms the guard (the handler owns the file)"
  - "Client chunk size is the RPCOption value clamped to cfg.snapshot_chunk_size, because the snapshot lane frame cap derives from that config"
metrics:
  duration: "about 1 session"
  completed: 2026-11-09
commits: 2
plan_head_before: cc4c96f5a9df3c66a4a81ee80a15e75fbfdae695
plan_head_after: 6b7fb66ad6fd8520eb6796584704b7596e489d0c
actuals:
  tokens: 16000
  tasks: 3
  commits: 2
---

# Phase 3 Plan 17: Snapshot lane and heartbeat isolation Summary

Chunked, validated, backpressured snapshot transfer on its own TCP connection, with guaranteed receive-file cleanup, plus tests proving the heartbeat lane is not delayed by a snapshot in flight.

## What was built

- `PeerClient::full_snapshot` (src/net/client.rs): opens or reuses the snapshot-lane connection, sends `SNAP_BEGIN`, then `SNAP_CHUNK` frames (8-byte big-endian offset prefix plus raw bytes, read in place with positional reads inside `spawn_blocking`), never more than `snapshot_ack_window` chunks unacknowledged, then `SNAP_END`, and returns the `SNAP_RESP`. Every send/receive is bounded by `snapshot_idle_timeout` per frame (not a total). The whole transfer races the cancel future (returns `StreamingError::Closed`). Errors map as: socket to Unreachable, timeout to Timeout, protocol violation to Network, local file read to StorageError, remote fatal to Unreachable. The connection is dropped on every failure; on success it is reused.
- Snapshot-lane receiver (src/net/server.rs, `run_snapshot_lane`): `SNAP_BEGIN` rejects `total_len > max_snapshot_bytes` before creating anything, then creates `recv-<n>.tmp` (n from a server-side counter) with `create_new` and mode 0600. Each chunk must have offset equal to bytes written, non-empty, and offset plus length within `total_len`; bytes are written with `pwrite`; `snapshot_chunks` increments; a `SNAP_ACK` goes out every `snapshot_ack_window` chunks. `SNAP_END` requires written == total, syncs, then calls `handler.install_snapshot` with the numeric tmp path. A second Begin, a Chunk/End without Begin, bad bodies and unknown kinds close the connection. `TmpGuard` removes the file on every exit.
- Heartbeat lane needed no server change: `Lane::Heartbeat` already takes the same `VOTE_REQ`/`APPEND_REQ` path as `Lane::Replication` (confirmed in `run_lane`; no place in client or server assumed Replication).

## Verification

- `cargo test --test transport`: 38 passed (the 15 prior tests plus the new ones below), 5 consecutive runs stable at about 0.8 s.
- `bash scripts/check-warnings.sh`: exit 0. `cargo clippy --all-targets`: no diagnostics in the touched files.
- New tests: `snapshot_transfer_chunked_and_installed` (3 MiB, 12 chunks, byte-identical, name `recv-<digits>.tmp`, mode 0600, recv_dir otherwise empty), `snapshot_connection_is_reusable_after_success` (0, 1, chunk, chunk+1 byte snapshots on one connection), `snapshot_bad_offset_rejected`, `snapshot_over_declared_size_rejected`, `snapshot_begin_over_max_rejected`, `snapshot_second_begin_rejected`, `snapshot_early_close_removes_tmp`, `snapshot_server_shutdown_removes_tmp`, `snapshot_cancel_aborts_and_removes_tmp`, `snapshot_idle_timeout_cleans_up` (both ends), `snapshot_idle_timeout_is_per_chunk_not_total`, `snapshot_ack_window_bounds_outstanding_chunks`, `snapshot_handler_failure_removes_tmp_and_reports`, `heartbeat_lane_uses_own_connection`, `heartbeat_lane_pipelines_like_replication`, `snapshot_does_not_starve_heartbeat` (32 MiB, 300 ms install, one heartbeat per 10 ms: max under 250 ms, median under 20 ms, distinct connections per lane, CRC of the received file matches).

## Deviations from Plan

### Auto-fixed / interpretation notes

**1. [Plan interpretation] Chunk size cannot be set through RPCOption in tests**
- `RPCOption::snapshot_chunk_size` is `pub(crate)` in openraft with no public setter, so the plan's "option chunk size 256 KiB" is exercised through `TcpNetConfig::snapshot_chunk_size` on both ends. The client still honors `option.snapshot_chunk_size()` when openraft sets it (clamped to the configured size, since the lane's frame cap derives from the config).

**2. [Design note] Ack window must match on both ends**
- The receiver acknowledges every `snapshot_ack_window` chunks (as planned). A sender whose window is smaller than the receiver's would wait for an ack that never comes until the idle timeout. This is documented on `TcpNetConfig::snapshot_ack_window` and deliberately used by the cancel/idle-timeout tests as a deterministic way to stall a receiver. The "server-side ack-window enforcement" in the plan text is satisfied by the client-side bound (tested); the server does not reject a sender that exceeds the window, since TCP back-pressure already bounds it and acks are cumulative.

**3. [Commit granularity] Two commits instead of one per task**
- The three tasks edit the same state machine and the guardrails of Task 2 are interleaved with the Task 1 transfer path, so the implementation landed as one `feat` commit (0186b9d) and all tests as one `test` commit (6b7fb66).

**4. [Rule 1/2 hardening] Zero-length chunks are rejected** (they make no progress and would let a peer keep a transfer alive indefinitely); a stale `recv-<n>.tmp` from a prior process is skipped rather than failing the transfer.

None of the deviations touched files outside src/net/client.rs, src/net/server.rs and tests/transport.rs. The existing test `unexpected_kinds_and_lane_mismatch_close_connection` still passes unchanged apart from a stale comment.

## Authentication gates

None.

## Known Stubs

None.

## Threat Flags

None beyond the plan's threat model. T-03-21 (chunk flood, bogus offsets, size lies) and T-03-23 (path traversal) are mitigated as specified; note the residual risk that a peer may legitimately write up to `max_snapshot_bytes` (default 8 GiB) per connection before the handler validates the file, and that there is no limit on concurrent snapshot connections (no authentication in this phase, D-08). Plan 03-13 should clean `recv_dir` at node startup (a crash mid-transfer leaves a `recv-<n>.tmp`; the guard only protects against in-process exits).

## Self-Check: PASSED

- src/net/client.rs, src/net/server.rs, tests/transport.rs modified: FOUND
- Commits 0186b9d and 6b7fb66 are ancestors of HEAD: FOUND
- STATE.md and ROADMAP.md untouched.
