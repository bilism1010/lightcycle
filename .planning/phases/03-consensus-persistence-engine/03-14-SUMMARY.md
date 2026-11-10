---
phase: 03-consensus-persistence-engine
plan: 14
subsystem: testing
tags: [raft, openraft, cluster-tests, fake-network, fake-io, crash-recovery, torn-wal]
requires:
  - phase: 03-13
    provides: RaftNode assembly, fast_config/eventually/wait_for_leader helpers
  - phase: 03-06
    provides: NetController / FakeNetwork (isolate, heal, stats)
  - phase: 03-03
    provides: FakeIo crash/revive/set_sync_delay/unsynced_bytes
provides:
  - TestCluster harness (start, kill, crash, crash_with_unsynced_tail, restart, leader election helpers, convergence checks, background writer)
  - Four 3-node cluster tests covering FR-3, FR-4, FR-6 and the D-16 crash-recovery bar
affects: [03-16, 03-15]
tech-stack:
  added: []
  patterns:
    - "Crash via FakeIo::crash before node shutdown so the live writer thread cannot persist"
    - "Anti-vacuity: delayed fsync + wait for unsynced bytes + assert torn_bytes_discarded > 0"
key-files:
  created: []
  modified:
    - tests/common/mod.rs
    - tests/cluster.rs
key-decisions:
  - "Harness gained write_among(ids, req) and leader_among(ids): an isolated old leader still believes it leads, so cluster-wide leader agreement never holds during a partition"
  - "Default ClusterOpts segment_size is 256 KiB (not the 64 MiB production default) because FakeIo clones the whole segment image on every sync"
  - "Background writer holds Raft handle clones and round-robins on failure, so it survives the writer's target node being crashed"
requirements-completed: [FR-3, FR-6, FR-4]
duration: ~35min
completed: 2026-11-10
status: complete
commits: 3
plan_head_before: 91da9bf17730d7fec89c2ef6172715f72b3023e8
plan_head_after: cdb0c56
actuals:
  tokens: 5500
  tasks: 3
  commits: 3
---

# Phase 3 Plan 14: Consensus behavior tests, part one Summary

**Deterministic 3-node in-process cluster over FakeNetwork/FakeIo proving election, replication on separate lanes, leader failover, partition healing and torn-WAL-tail crash recovery with no acknowledged write lost.**

## Accomplishments

- `TestCluster` and `ClusterOpts` in `tests/common/mod.rs` implement the planned interface (start, node, fake_io, leader, write_via_leader, kill, crash, crash_with_unsynced_tail, restart, wait_converged, assert_contain_and_identical) plus `leader_among`, `write_among`, `live_ids`, `spawn_writer`/`BgWriter`.
- `three_node_election_and_replication`: 50 writes reach all three engines; NetController stats show delivered traffic on both the replication and the heartbeat lane; all nodes agree on leader and term.
- `leader_failover_new_leader_elected`: kill the leader, survivors elect a different leader, 20 more writes commit, both survivors hold all 40 keys.
- `partition_heals_and_minority_converges`: isolated leader's write never returns Ok within 2 s; majority elects in a higher term and keeps committing; after heal all three engines equal the expected map and the old leader's uncommitted key is absent.
- `kill_restart_keeps_committed_writes`: FakeIo-backed cluster; follower crashed with keep=7 and leader crashed with keep=301, each after a 250 ms fsync delay and a wait for genuinely unsynced bytes while a background task keeps writing. Observed recovery: follower `torn_bytes_discarded: 7`, ex-leader `torn_bytes_discarded: 20` (the 301 kept bytes held whole records plus a 20-byte partial). Restart never failed; every acknowledged write is on all three nodes and the engines are identical.

## Task Commits

| Task | Name | Commit |
|------|------|--------|
| 1 | Harness + election/replication tracer | 02453b4 |
| 2 | Leader failover and partition healing | 83b5bef |
| 3 | Crash with torn WAL tail | cdb0c56 |

## Verification

- `cargo test --test cluster`: run 4 times in a row (8 tests each, ~5 s), all green; `kill_restart_keeps_committed_writes` additionally run 8 more times alone, all green. No flakiness observed.
- `bash scripts/check-warnings.sh` exits 0.
- Tracer gate: the `<verify>` for Task 1 passed end-to-end before the expansion tasks.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] write_via_leader cannot be used during a partition**
- **Found during:** Task 2 (partition test failed with "all nodes agree on a leader" timeout)
- **Issue:** `leader()` required every live node to agree; the isolated old leader still reports itself as leader, so agreement never holds.
- **Fix:** Added `write_among(ids, req)`; `write_via_leader` delegates to it with all live ids. The partition test writes through the majority ids.
- **Files modified:** tests/common/mod.rs, tests/cluster.rs
- **Commit:** 83b5bef

Otherwise the plan executed as written. The harness additions in Task 3 (crash, crash_with_unsynced_tail, restart, fake_io) were implemented in Task 1's commit together with the rest of the harness, since they share one struct.

## Known Stubs

None.

## Threat Flags

None. Test-only changes; no new production surface.

## Notes for downstream plans

- 03-16 can reuse `ClusterOpts` (snapshot_every, segment_size, max_in_snapshot_log_to_keep, purge_batch_size), `wait_converged` and `assert_contain_and_identical`.
- Only `tests/cluster.rs` and `tests/common/mod.rs` were modified; STATE.md and ROADMAP.md untouched.

## Self-Check: PASSED

- tests/common/mod.rs and tests/cluster.rs exist and contain the new code.
- Commits 02453b4, 83b5bef, cdb0c56 are ancestors of HEAD.
