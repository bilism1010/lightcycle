---
phase: 03-consensus-persistence-engine
plan: 01
subsystem: consensus
tags: [openraft, postcard, raft, api-gate, zero-warning-gate]
requires: []
provides:
  - "Pinned openraft =0.10.0-alpha.38 dependency set and test-util feature"
  - "KvRequest, KvResponse, TypeConfig (leader_id_std), SnapshotFile in src/raft/types.rs"
  - "tests/openraft_api_gate.rs permanent compile-time pin of the alpha.38 trait surface"
  - "Module roots wal, wire, net, raft, snapshot"
  - "scripts/check-warnings.sh shared zero-warning gate with empty recorded baseline"
affects: [03-03, 03-04, 03-05, 03-06, 03-07, 03-08, 03-09, 03-10, 03-11]
tech-stack:
  added: [openraft =0.10.0-alpha.38, crc32c 0.6, postcard 1.1, tokio-util 0.7, futures 0.3, arc-swap 1.9, thiserror 2, tempfile 3, proptest 1.12]
  patterns: [self dev-dependency to unify the test-util feature, recorded-baseline warning gate]
key-files:
  created:
    - src/raft/mod.rs
    - src/raft/types.rs
    - src/wal/mod.rs
    - src/wire/mod.rs
    - src/net/mod.rs
    - src/snapshot/mod.rs
    - tests/openraft_api_gate.rs
    - scripts/check-warnings.sh
    - scripts/cargo-check-baseline.txt
  modified:
    - Cargo.toml
    - Cargo.lock
    - src/lib.rs
key-decisions:
  - "A2 adopted: LeaderId = leader_id_std::LeaderId<u64, u64> (single leader per term); serialized into every log id"
  - "bytes gets the serde feature; no other crates beyond the user-approved list"
metrics:
  tasks: 2
  completed: 2026-11-09
status: complete
actuals:
  tokens: 14000
  tasks: 2
  commits: 2
plan_head_before: f96c9da3a1c40274a36899e9bbe1d1bb4d5902fa
plan_head_after: bfb839a60af0bd2392149d572bccd4a670a64c55
---

# Phase 3 Plan 01: openraft API gate and Wave 0 foundation Summary

openraft `=0.10.0-alpha.38` is pinned and a permanent gate test proves the whole trait surface Phase 3 depends on (unsealed `RaftLogStorage` / `RaftStateMachine`, `new_heartbeat_client`, `new_snapshot_client`, `stream_append`, `full_snapshot`) compiles, so no checkpoint or fallback was needed.

## What was built

- **Dependencies:** exactly the approved set added to `Cargo.toml` (openraft with `default-features = false, features = ["tokio-rt", "serde"]`, crc32c, postcard, tokio-util, futures, arc-swap, thiserror; dev: tempfile, proptest, self-reference with `test-util`). `Cargo.lock` committed.
- **Types (`src/raft/types.rs`):** `KvRequest` (Set/Delete/Cas) with `Display`, `KvResponse` (Ok/Value/CasFailed), `TypeConfig` via `declare_raft_types!` with `leader_id_std`, and `SnapshotFile`. Postcard round-trips of every `KvRequest` variant and of an `EntryOf<TypeConfig>` pass.
- **API gate (`tests/openraft_api_gate.rs`):** skeleton `GateLog`/`GateLogReader`, `GateSm`/`GateSnapshotBuilder`, `GateFactory` (overrides all three client constructors) and `GatePeer` (overrides `stream_append` and `full_snapshot`); a type-check-only function proves `Raft::new` accepts them; `openraft_config_literal_validates` and `test_util_visible_to_integration_tests` run.
- **Module roots:** `wal`, `wire`, `net`, `snapshot` (doc-only) plus `raft`; `src/lib.rs` alphabetical with `test_support::ENABLED` under `test-util` and `feature_gate_tests`.
- **Zero-warning gate:** `scripts/check-warnings.sh` fails on compile errors or any cargo diagnostic not in `scripts/cargo-check-baseline.txt`. The baseline recorded after the dependency add is empty. Proven to exit 1 on an injected unused variable (reverted).

## Verification

- `cargo test`: 20 lib tests (17 Phase 2 + 3 new) and 2 gate tests pass.
- `bash scripts/check-warnings.sh` exits 0; baseline names no path under src/, tests/ or benches/; no bincode / openraft-memstore / openraft-rt-sim in `Cargo.toml`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Skeleton signature mismatches in the gate and test**
- **Found during:** Task 1
- **Issue:** `Entry` takes two generics in alpha.38 and `RaftLogStorage` / `RaftStateMachine` are under `openraft::storage`, not the crate root. These are signature mismatches only, not missing hooks.
- **Fix:** Used `EntryOf<TypeConfig>` / `LogIdOf` aliases and `openraft::storage` imports.
- **Files modified:** `tests/openraft_api_gate.rs`, `src/raft/types.rs`
- **Commit:** 8bdd0c3

**2. [Ordering] Module roots and the lib.rs feature gate landed in the Task 1 commit**
- `src/lib.rs` must declare the module roots to compile once the alphabetical list and `test_support` are present, so the module roots, `test_support` and `feature_gate_tests` were committed with Task 1. Task 2's commit therefore contains only the gate script and baseline. No behavior change.

Otherwise the plan executed as written. No tracer gate failure; `<verify>` re-ran green.

## Notes for the phase summary

- Interpretation A4 (up to three TCP connections per peer: pipelined replication plus separate heartbeat and snapshot connections) is implemented in plans 03-09 and 03-17 and must be reported to the user.
- `commits: 2` counts the two task commits (`plan_head_after` is the second task commit); the SUMMARY commit follows it.

## Known Stubs

None. The gate skeleton method bodies return defaults or `unimplemented!` by design and are never run.

## Threat Flags

None.

## Self-Check: PASSED

- Files exist: Cargo.toml, src/raft/types.rs, tests/openraft_api_gate.rs, scripts/check-warnings.sh, scripts/cargo-check-baseline.txt, and the four module roots.
- Commits 8bdd0c3 and bfb839a are ancestors of HEAD.
