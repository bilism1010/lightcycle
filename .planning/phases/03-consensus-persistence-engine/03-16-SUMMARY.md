---
phase: 03-consensus-persistence-engine
plan: 16
subsystem: testing
tags: [raft, openraft, membership, joint-consensus, snapshot-catchup, phase-gate, cluster-tests]
requires:
  - phase: 03-14
    provides: TestCluster harness, ClusterOpts, wait_converged, assert_contain_and_identical, write_among, spawn_writer
  - phase: 03-15
    provides: tcp_smoke and cli_smoke tests, binary wiring
  - phase: 03-17
    provides: snapshot lane on the TCP server
provides:
  - Dynamic membership tests (add voter, remove voter, joint-consensus replace, remove leader)
  - Snapshot catch-up test with kill/restart and engine() identity check
  - Harness additions start_extra, add_learner, change_membership, remove_voters, wait_voters, MembershipLog
  - Phase 3 gate results recorded in 03-VALIDATION.md
  - Two load-flakiness fixes found by the gate in the tcp_smoke snapshot test and the snapshot receive path
affects: [phase-3-verification]
tech-stack:
  added: []
  patterns:
    - "MembershipLog: a task per node follows raft.metrics() and records each distinct membership configuration, so tests can assert a joint configuration was (or was not) observed"
    - "Membership operations go through TestCluster::admin, which retries on NotLeader against the leader among a chosen id set"
key-files:
  created: []
  modified:
    - tests/common/mod.rs
    - tests/cluster.rs
    - tests/tcp_smoke.rs
    - src/net/server.rs
    - .planning/phases/03-consensus-persistence-engine/03-VALIDATION.md
key-decisions:
  - "Removing a leader is tested from a 4-voter cluster (3 voters remain) so a quorum survives"
  - "The receive-file guard in the snapshot lane now always removes the file after the hand-off (installer rename makes that a no-op; a skipped stale or duplicate snapshot would otherwise leak)"
requirements-completed: [FR-6, FR-3, FR-4, NFR-4]
duration: ~2h (most of it chasing the tcp_smoke flake)
completed: 2026-11-10
status: complete
commits: 5
plan_head_before: 1e60fb9de904491c3bf19f8dd0856af09a0b5368
plan_head_after: e70547c451c124474a306db5e2b75ad62bd3195c
actuals:
  tokens: 22000
  tasks: 3
  commits: 5
---

# Phase 3 Plan 16: Consensus behavior tests, part two, and the phase gate Summary

**Dynamic membership (single-step add, remove, observed joint-consensus replace, remove-the-leader) and snapshot catch-up after log purge are proven on the 3-node harness, and the whole Phase 3 gate is green, after the gate exposed and fixed two real flakiness defects in the snapshot receive path.**

The commit count above (5) was measured before this SUMMARY's own commit; the SUMMARY commit makes 6.

## What the user must know first

1. **NFR-1 and NFR-2 were NOT validated in this phase.** They are deferred manual verification and are NOT claimed as met. They are in no plan's `requirements` frontmatter, REQUIREMENTS.md must not mark them complete, and 03-VALIDATION.md keeps the row open with the text "Deferred: needs a Linux NVMe host; not claimed". The WAL bench was only run as `--test` (informational, macOS `F_FULLFSYNC`, fsync p50 about 4 ms). The user will provide a Linux NVMe host later.
2. **Multi-key transaction limit (assumption A12) still stands.** Single-key stamping does not cover multi-key conditional operations (FR-2 WATCH, multi-key CAS). The snapshot design must be reopened before any such operation is added. Nothing in this plan changed that.
3. **Interpretation A4 needs your confirmation or veto.** Each peer uses up to three TCP connections (replication, heartbeat, snapshot lane). Decision D-07 literally says one persistent connection per peer; I (via earlier plans) read it as one persistent connection per lane per peer, to satisfy D-12's separate snapshot channel without head-of-line blocking behind replication traffic. Please confirm that reading or overrule it. The fake network and the real TCP transport both follow it, and the tests (`three_node_election_and_replication` counts replication and heartbeat lanes separately, `tcp_smoke` counts accepted connections per lane) assert it.

## Accomplishments

- **Harness** (`tests/common/mod.rs`): `start_extra(id)` starts a node outside the bootstrap member map; `add_learner`, `change_membership`, `remove_voters` run against the leader among a set of ids and retry on `NotLeader`; `wait_voters` waits for every node's metrics to show an exact voter set; `MembershipLog` records every distinct membership configuration (list of voter sets, two sets meaning joint) reported by watched nodes. `ClusterOpts` is now kept on the cluster so late-started nodes get the same snapshot, purge and segment settings.
- **`membership_add_voter`** (tracer): 3 nodes, 20 keys, node 4 added as a learner (blocking), its engine converges on the 20 keys while still a learner, then `change_membership({1,2,3,4})`, 20 more keys reach all four, and the recorded configurations contain no joint configuration.
- **`membership_remove_voter`**: 4 voters, a follower removed with `remove_voters`; it disappears from every remaining node's membership; the other three keep committing; the removed node's engine stays exactly at the pre-removal contents.
- **`membership_replace_voters_joint`**: voters {1,2,3} plus learners 4 and 5, then `change_membership({3,4,5})` while a background writer runs. A joint configuration naming both {1,2,3} and {3,4,5} is observed, the final voters are {3,4,5}, the cluster stayed writable, and every acknowledged write is on 3, 4 and 5 and identical.
- **`membership_remove_leader_new_leader_elected`**: the leader is removed from a 4-voter cluster while a writer runs; a new leader among the remaining voters is elected within 15 s and accepts writes; every acknowledged write is present and identical on the remaining three.
- **`kill_restart_snapshot_catchup`**: snapshot every 50 entries, keep 10, purge batch 10, 4 KiB WAL segments. A follower is isolated and its engine handle `before` is taken; 300 keys are written through the other two; the leader's snapshot index reaches at least 50 and its purged index passes the isolated node's last log index; the other follower is killed and restarted from its own snapshot plus log and the pair keeps committing 20 more keys; after the heal the isolated node converges to all 320 keys (polled through a fresh `engine()` call each iteration), `!Arc::ptr_eq(before, after)` holds, `before` still equals the pre-isolation contents, the snapshot lane delivered at least once, the node's own metrics show a snapshot at index 50 or later, and all three nodes are identical. Passed 6 of 6 repeated runs.

## Phase gate results (macOS dev host, debug profile unless stated)

| Check | Result |
| ----- | ------ |
| `cargo test` (all binaries) | green: 227 lib, 1 cli_smoke, 13 cluster, 2 openraft_api_gate, 2 tcp_smoke, 38 transport, 1 wal_test_util_reachable |
| `cargo test --test cluster` three consecutive runs | 13 passed each time (about 11 s) |
| `cargo test --lib -- storage:: engine:: ipc:: arena::` | 32 passed (the 17 Phase 2 tests plus later additions in those modules) |
| `bash scripts/check-get-regression.sh` | PASS: GET p99 250 ns at 1 thread (limit 334), 542 ns at 8 threads (limit 667), same as the Phase 2 baseline |
| `cargo test --release --lib snapshot::concurrent_scan` | 1 passed |
| `cargo bench --bench wal -- --test` | ran; informational only (fakeio zero delay p99 about 2.3 ms at window 256; stdfs always, 1 in flight p50 4.0 ms; these are macOS numbers, not an NFR measurement) |
| `bash scripts/check-warnings.sh` | exit 0 |
| Static greps (baseline names no src/tests/benches path, no banned crates, wire has no openraft import, README Raft port and --bootstrap) | pass |

03-VALIDATION.md: every per-task Status cell is set to green, `wave_0_complete: true`, the Wave 0 boxes are ticked, and the Manual-Only NFR row stays open. The "Feedback latency < 30s" sign-off box and the Approval line are left unticked/pending on purpose (integration tests are heavier than that target, and approval is not mine to give).

## Task Commits

| Task | Name | Commit |
| ---- | ---- | ------ |
| 1 | Add a voter (tracer) plus harness | 72a5da6 |
| 2 | Remove voter, joint replace, remove leader | 768b421 |
| 3 | Snapshot catch-up test | d52c508 |
| 3 | Gate-found fix: snapshot receive path and tcp_smoke race | 72ccdb7 |
| 3 | Record gate results in 03-VALIDATION.md | e70547c |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Snapshot receive file leaked when openraft skips a stale or duplicate snapshot**
- **Found during:** Task 3 phase gate. `tcp_smoke tcp_snapshot_lane_install` failed in 1 of 3 plain full-suite runs and was reproduced under CPU load.
- **Issue:** `PeerServer` marked the receive file as owned by the handler after any `Ok` and never removed it. When a leader change resends a snapshot the follower already has, openraft answers `Ok` without consuming the file, leaving `recv-N.tmp` forever (observed: `tmps=["recv-2.tmp"]` after the term moved from 1 to 3).
- **Fix:** `TmpGuard` in `src/net/server.rs` now always removes the receive file when dropped, including after a successful hand-off. For an installed snapshot the file was already renamed away, so this is a no-op. openraft only answers `Ok` for an accepted snapshot after it is installed (it waits on the snapshot condition), and answers immediately for a skipped one, so no needed file is removed.
- **Files modified:** `src/net/server.rs`
- **Commit:** 72ccdb7

**2. [Rule 1 - Bug, test harness] tcp_smoke binds the server before the node starts, racing the recv-dir cleanup**
- **Found during:** same investigation. A snapshot transfer begun while node 3 was still starting had its file deleted by `RaftNode::start`'s `clean_recv_dir`; the later install hit ENOENT, which openraft treats as fatal, so node 3's Raft core died and never converged (`running_state: Err(StorageError ... No such file or directory)`).
- **Fix:** the test's `DeferredHandler::install_snapshot` refuses a transfer whose file no longer exists, checked only after the handler is set (cleanup has finished by then, so the check cannot race). The leader retries.
- **Scope note:** `src/main.rs` starts the node first and binds the server afterwards, so the production binary does not have this race; only the test layout does.
- **Files modified:** `tests/tcp_smoke.rs`
- **Commit:** 72ccdb7
- **Evidence:** before the fixes the test failed once in a plain full-suite run and several more times when other test processes loaded the machine (first mode ENOENT/dead core several times, leftover-file mode twice); after them 60 of 60 and then the full gate passed under the same heavy load (four concurrent cluster test processes).

**3. [Rule 3 - Blocking] Plan 03-17's server file edited from this plan**
Both fixes touch files owned by earlier plans (`src/net/server.rs` from 03-17, `tests/tcp_smoke.rs` from 03-15). The plan's gate language ("fix the test's synchronization or the product bug, never raise a timeout blindly") required fixing them here; no timeout was changed.

### Observations not acted on

- `RaftNode::start` empties the receive directory while a `PeerServer` that shares it could already be accepting transfers. The shipped binary orders start and bind correctly, but the API does not enforce it. A later plan could document the ordering or move cleanup into the server constructor. Not changed here.

## Known Stubs

None.

## Threat Flags

None. Test-only changes plus one narrowing of file retention in the snapshot receive path (it removes more, not less, and the threat register T-03-38 split-brain mitigation is covered by `membership_replace_voters_joint` and the remove tests asserting exact acknowledged-write equality).

## Self-Check: PASSED

- tests/common/mod.rs, tests/cluster.rs, tests/tcp_smoke.rs, src/net/server.rs and 03-VALIDATION.md contain the described changes.
- Commits 72a5da6, 768b421, d52c508, 72ccdb7 and e70547c are ancestors of HEAD.
- `bash scripts/check-warnings.sh` exits 0. STATE.md and ROADMAP.md were not modified.
