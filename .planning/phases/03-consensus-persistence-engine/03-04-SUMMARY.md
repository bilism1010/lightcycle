---
phase: 03-consensus-persistence-engine
plan: 04
subsystem: wire
tags: [framing, handshake, tokio-util, length-delimited, transport]
requires: ["03-01"]
provides:
  - "FrameCodec: 4-byte BE length prefix + 11-byte header (kind, flags, lane, req_id) + body, with explicit max_frame_length and set_max_frame_length"
  - "Lane, FrameHeader, Frame, FrameError, kind constants (KIND_HELLO=1, KIND_HELLO_ACK=2, KIND_ERROR=3, KIND_APP_BASE=16)"
  - "Hello fixed 23-byte layout, client_handshake, server_handshake, HandshakeError, WIRE_VERSION=1, DEFAULT_HANDSHAKE_TIMEOUT=5s"
affects: [03-09, 03-10]
tech-stack:
  added: []
  patterns:
    - "oversize check on the 4-byte prefix before the inner codec reserves body capacity"
    - "hand-encoded Hello so version negotiation does not depend on a serialization library"
key-files:
  created:
    - src/wire/frame.rs
    - src/wire/hello.rs
  modified:
    - src/wire/mod.rs
key-decisions:
  - "Encoder writes prefix, header and body straight into the output BytesMut (body copied once); LengthDelimitedCodec is used for the decode side"
  - "Hello::decode checks magic then wire_version before layout length, so a future-version Hello with a different layout reports VersionMismatch"
  - "Added HandshakeError::Remote(String) so the client surfaces the server's KIND_ERROR reason, and FrameError::UnknownLane for Lane::try_from"
requirements-completed: [FR-3]
duration: ~15 min
completed: 2026-11-09
status: complete
actuals:
  tokens: 9000
  tasks: 2
  commits: 2
plan_head_before: 60abfd47818790e2917e1eebb2873ac016805efa
plan_head_after: bcbb42897f9315e5aea52e33fbfbfb2dc4d1de2c
commits: 2
---

# Phase 3 Plan 04: Wire frame codec and Hello handshake Summary

**Consensus-agnostic length-prefixed frame codec with a pre-buffer oversize guard, plus a fixed-layout versioned Hello handshake with cluster check and timeout, reusable by the Raft transport, snapshot files and Phase 5.**

## Tasks

| Task | Name | Commit |
| ---- | ---- | ------ |
| 1 (tracer) | Frame codec: round trip over sync buffer and duplex, oversize guard | a66995e |
| 2 | Hello handshake with version, cluster check and timeout | bcbb428 |

## What was built

- `src/wire/frame.rs`: `FrameCodec` implements `Encoder<Frame>` and `Decoder`. `length` counts header plus body; the cap is set explicitly on the inner `LengthDelimitedCodec` (never the 8 MB default). Decode peeks the declared length from the first 4 bytes and returns `FrameError::TooLarge { declared, max }` before any body is buffered (test declares 1 GiB against a 1 MiB cap and asserts the buffer capacity is unchanged). Payloads under 11 bytes yield `Truncated`. Encode also enforces the cap. Documented: any `FrameError` is terminal, callers close the connection.
- `src/wire/hello.rs`: `Hello` encodes to exactly 23 bytes (`LCYC`, version u16, cluster u64, node u64, lane u8). `server_handshake` reads the first frame under `tokio::time::timeout`, maps non-Hello to `UnexpectedFrame`, bad magic, `VersionMismatch`, `ClusterMismatch`, sends one best-effort `KIND_ERROR` frame (ASCII reason) on rejection, and on success replies `KIND_HELLO_ACK` with its own Hello (lane echoed). `client_handshake` sends the Hello and waits under the same timeout for an ack or error, also verifying the cluster id in the ack. The module doc states the cluster_id check is not a security control (D-08).
- `src/wire/mod.rs`: module doc rewritten to say "no consensus-crate dependency" without naming the crate; re-exports added.

## Verification

- `cargo test --lib -- wire::` : 16 passed (7 frame, 9 hello).
- `grep -rn openraft src/wire/` : 0 matches.
- `bash scripts/check-warnings.sh` : exit 0.

## Threat model

- T-03-08 mitigated (explicit cap, rejection from prefix, 1 GiB test). T-03-09 mitigated (handshake timeout on server and client, 50 ms tests). T-03-10 accepted per D-08.

## Deviations from Plan

None - plan executed exactly as written. Minor additive items within the interface intent: `HandshakeError::Remote`, `FrameError::UnknownLane`, `KIND_APP_BASE`, extra tests (`encode_over_cap_rejected`, `set_max_frame_length_widens_cap`, `rejects_non_hello_first_frame`, `client_times_out_when_server_silent`, `hello_decode_rejects_truncated_and_short`).

## Known Stubs

None.

## Notes for downstream plans

- Servers should cap a fresh connection at a few KiB (`FrameCodec::new(small)`) and call `set_max_frame_length` after `server_handshake` succeeds.
- Tests ran with an isolated `CARGO_TARGET_DIR` to avoid sibling-worktree interference.

## Self-Check: PASSED

Files `src/wire/frame.rs`, `src/wire/hello.rs`, `src/wire/mod.rs` exist; commits a66995e and bcbb428 are ancestors of HEAD.
