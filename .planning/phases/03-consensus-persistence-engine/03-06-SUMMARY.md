---
phase: 03-consensus-persistence-engine
plan: 06
subsystem: consensus
tags: [openraft, fake-network, test-seam, raft-handler]
requires: ["03-01"]
provides:
  - "net::handler::RaftHandler: object-safe server-side dispatch trait, blanket impl for openraft::Raft<TypeConfig, SM>"
  - "net::fake::{NetController, FakeNetworkFactory, FakePeer, Lane, NetStats, LaneCounts}: deterministic in-process fake network (cfg test or feature test-util)"
affects: [03-09, 03-13, 03-14, 03-15, 03-16, 03-17]
tech-stack:
  added: []
  patterns: [handler-trait seam shared by fake and TCP transport, directed link-state fault injection, paused-time tests]
key-files:
  created:
    - src/net/handler.rs
    - src/net/fake.rs
  modified:
    - src/net/mod.rs
    - Cargo.toml
key-decisions:
  - "Handler errors surface as RPCError::Network (RPCError<TypeConfig> has an uninhabited RemoteError payload); a stopped remote (Fatal) surfaces as Unreachable"
  - "Link checks apply to the request direction only; a blocked call returns Unreachable immediately"
  - "isolate() is a node set, not enumerated pairs, so it also covers nodes registered later"
metrics:
  tasks: 2
  completed: 2026-11-09
status: complete
actuals:
  tokens: 11500
  tasks: 2
  commits: 2
plan_head_before: 60abfd47818790e2917e1eebb2873ac016805efa
plan_head_after: 3c83b7a44d1b31f900809612e4b5ed4159ba5e33
---

# Phase 3 Plan 06: RaftHandler seam and deterministic FakeNetwork Summary

A `RaftHandler` trait is now the single server-side dispatch target, and an in-process `FakeNetwork` implements `RaftNetworkFactory` / `RaftNetworkV2` with distinct replication, heartbeat and snapshot clients, so multi-node consensus tests (plans 03-14, 03-16) run without sockets.

## What was built

- **`src/net/handler.rs`:** `RaftHandler: Send + Sync + 'static` with boxed-future/stream methods (`append_entries`, `stream_append`, `vote`, `pre_vote`, `install_snapshot`). Blanket impl for `Raft<TypeConfig, SM>` where `SM: RaftStateMachine<TypeConfig, SnapshotData = SnapshotFile>`; `install_snapshot` builds `Snapshot { meta, snapshot: file }` and calls `Raft::install_full_snapshot`.
- **`src/net/fake.rs`** (gated `cfg(any(test, feature = "test-util"))`):
  - `NetController` (Clone): `register`, `unregister`, `drop_link`, `restore_link`, `partition`, `isolate`, `heal`, `set_delay`, `stats`, `factory(me)`. No randomness anywhere.
  - `FakeNetworkFactory` returns `FakePeer` for `new_client` / `new_heartbeat_client` / `new_snapshot_client`, tagged `Lane::Replication` / `Heartbeat` / `Snapshot`.
  - `FakePeer`: `append_entries`, `pre_vote`, `vote` check the link, apply the delay, forward to the registered handler. `stream_append` wraps the input so each request is delayed and the link re-checked before forwarding; a link that goes down mid-stream ends the request stream and therefore the output stream; response order equals request order. `full_snapshot` checks the link, delays once, copies the file with `std::fs::copy` into `recv_dir/recv-<n>.tmp` (n from an `AtomicU64`), calls `install_snapshot` with the copy, and races a biased `cancel` future that returns `StreamingError::Closed`. `transfer_leader` and `backoff` keep trait defaults.
  - `stats()` returns per-lane delivered/dropped counters.
- 13 unit tests, all passing (2 in Task 1, 11 more in Task 2), including paused-time tests for delay ordering and snapshot cancel.

## Verification

- `cargo test --lib net::fake`: 13 passed. Full `cargo test --lib`: 48 passed. `tests/openraft_api_gate.rs`: 2 passed.
- `cargo build` (no test feature) succeeds; `bash scripts/check-warnings.sh` exits 0.
- Tracer gate: Task 1 `<verify>` re-ran green before Task 2 expanded the slice.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `wire::Lane` does not exist in this wave's base**
- **Found during:** Task 1
- **Issue:** The plan tags clients with `wire::Lane`, but `src/wire/` is built by sibling plan 03-04 in the same wave and is a doc-only stub in this worktree; editing or depending on it would break parallel isolation.
- **Fix:** Defined a local `Lane { Replication, Heartbeat, Snapshot }` in `src/net/fake.rs` with identical variant names. After 03-04 merges, it can be swapped for `crate::wire::Lane` (and `net/client.rs` in 03-09 should keep the same variant names).
- **Files modified:** src/net/fake.rs
- **Commit:** 1af6e57

**2. [Rule 3 - Blocking] `start_paused` needs tokio `test-util`**
- **Found during:** Task 2
- **Issue:** `#[tokio::test(start_paused = true)]` does not compile with only `features = ["full"]`.
- **Fix:** Added `tokio = { version = "1.40", features = ["test-util"] }` under `[dev-dependencies]`. No new package; `Cargo.lock` unchanged. Note for merge: this is a one-line edit in `[dev-dependencies]` of `Cargo.toml`, outside the plan's `files_modified`.
- **Files modified:** Cargo.toml
- **Commit:** 3c83b7a

**3. [Rule 1 - API mismatch] No remote-error construction in `RPCError<TypeConfig>`**
- **Issue:** The plan says to map a handler `RaftError` into the RPCError remote-error variant. In alpha.38 `RPCError<C>` defaults `E = Infallible`, so a `RemoteError` cannot be built from a `RaftError`.
- **Fix:** Handler errors map to `RPCError::Network`; a `Fatal` from a stopped remote (stream and snapshot paths) maps to `Unreachable` so openraft backs off. Plan 03-09 should apply the same mapping.

Otherwise the plan executed as written.

## Notes for downstream plans

- Stub handlers in `fake.rs` tests echo `prev_log_id` on `stream_append`; they live in the test module (private), so 03-09/03-14 write their own stubs against `RaftHandler`.
- Delay is serial per request in `stream_append` (not overlapped like a real pipelined link); this is deliberate to keep ordering deterministic.
- Only the request direction is link-checked; a lost reply (request delivered, response dropped) cannot be simulated.

## Known Stubs

None. (Task 1 placeholder `Unreachable` bodies were replaced in Task 2.)

## Threat Flags

None. T-03-13 mitigated: `pub mod fake` is gated by `cfg(any(test, feature = "test-util"))`, and `cargo build` without the feature succeeds.

## Self-Check: PASSED

- Files exist: src/net/handler.rs, src/net/fake.rs, src/net/mod.rs.
- Commits 1af6e57 and 3c83b7a are ancestors of HEAD.
