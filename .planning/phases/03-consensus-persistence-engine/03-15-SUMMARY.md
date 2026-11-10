---
phase: 03-consensus-persistence-engine
plan: 15
subsystem: net
tags: [raft, tcp, snapshot, cli, readme, security, acceptance]
status: complete
requires:
  - phase: 03-consensus-persistence-engine
    provides: "03-09 PeerServer/TcpNetworkFactory, 03-13 RaftNode/NodeConfig, 03-17 snapshot lane"
provides:
  - "tests/tcp_smoke.rs: three real Raft nodes with on-disk WALs over loopback TCP (election, replication, snapshot-lane install)"
  - "src/main.rs: the binary starts a real Raft node from CLI flags (static bootstrap, standalone mode, ordered shutdown)"
  - "tests/cli_smoke.rs: spawns the real binary as a single-node cluster"
  - "README quickstart with --bootstrap and the Raft port security section (D-08)"
affects: [03-16]
key-files:
  created:
    - tests/tcp_smoke.rs
    - tests/cli_smoke.rs
  modified:
    - src/main.rs
    - README.md
key-decisions:
  - "main starts the RaftNode first, then binds the PeerServer with the node's Raft handle as its handler (no deferred handler needed in the binary), then calls bootstrap_if_needed with the server's real local_addr; the DeferredHandler exists only in the test file where all servers must bind before any node starts"
  - "A node with no --peers always bootstraps itself (single-member cluster, same WAL path); with peers it bootstraps only when --bootstrap is given"
  - "--cluster-id is a u64 on the wire (TcpNetConfig) and stored as its decimal string in NodeConfig.cluster_id (a log-context name only)"
  - "--fsync periodic uses a fixed 10 ms sync interval (no extra flag); both relaxed modes print an UNSAFE warning to stderr and the node logs its own warning"
requirements-completed: [FR-3, FR-6]
metrics:
  duration: "about 1 session"
  completed: 2026-11-10
commits: 3
plan_head_before: 91da9bf17730d7fec89c2ef6172715f72b3023e8
plan_head_after: 34790a4b6cc1291633b8312ff192afd0a30c9d1b
actuals:
  tokens: 14000
  tasks: 3
  commits: 3
---

# Phase 3 Plan 15: Real-transport acceptance, binary wiring and README security note Summary

Three real Raft nodes with on-disk WALs now elect, replicate and snapshot-install over actual loopback TCP, and the `lightcycle` binary starts a real node from CLI flags, with the README stating plainly that the Raft port is unauthenticated and that NFR-1/NFR-2 are not yet validated.

## Accomplishments

- **Task 1 (tracer), tests/tcp_smoke.rs:** `DeferredHandler` (a `RaftHandler` backed by a `OnceLock`; before it is set every call fails like a stopped node, so an early dialer gets a retryable error) lets all three `PeerServer`s bind on `127.0.0.1:0` up front. `three_nodes_over_tcp_elect_and_replicate` bootstraps node 1 with the real addresses, waits for all three to agree on a leader, writes 50 keys, and waits until every engine holds them. It asserts replication-lane and heartbeat-lane connections were accepted, every follower received `append_req_frames > 0`, and `decode_errors` and `handshake_failures` are zero on all three servers. Tracer gate re-run green before expanding.
- **Task 2, tcp_snapshot_lane_install:** node 3's server is bound (address fixed in the membership) but its node is not started; with snapshot every 50, keep 10, purge batch 10 and 4096-byte segments, 300 writes commit on two of three nodes. After both running nodes snapshotted and purged, node 3 starts on a fresh directory and converges to all 300 keys. The poll closure calls `node3.engine()` afresh each iteration; the test also captures the engine `Arc` before the install and asserts afterwards that it is a different `Arc` that never saw the data, which proves the swap actually happened and that a captured handle would have been stale. Node 3's server stats show `snapshot_chunks > 0` and an accepted snapshot-lane connection; its snapshot directory ends with exactly one `.snap` and its recv directory has no `.tmp` leftovers.
- **Task 3, binary, CLI smoke and README:** `src/main.rs` keeps `--node-id/-n`, `--bind/-b`, `--peers` unchanged and adds `--data-dir` (default `./lightcycle-data/node-<id>`), `--cluster-id` (u64, default 1), `--bootstrap`, `--fsync always|periodic|never` and `--snapshot-every` (default 1,000,000). Startup order: parse peers, build `NodeConfig`, `RaftNode::start` with `TcpNetworkFactory`, bind `PeerServer` on `--bind` with the node's Raft handle (receive directory is `cfg.recv_dir()` on both client and server config, per the 03-13 carry-over), `bootstrap_if_needed` with self at the real bound address plus the parsed peers, print `[Lightcycle] node <id> listening on <addr>`, spawn the leader watcher (prints `node <id> is leader (term <t>)` each time it becomes leader), wait for Ctrl-C or SIGTERM, then shut down watcher, server and node in that order. `tests/cli_smoke.rs` spawns the real binary (`--bind 127.0.0.1:0`), reads stdout on a thread with a 15 s deadline until the leader line appears, kills the child (guard also kills on a failed assertion), and checks a `<16 hex>.wal` file exists and the wal directory mode is `0700`. README: quickstart gets `--bootstrap` on node 1 with an explanatory comment, a flags paragraph, and a "Raft port security (Phase 3)" section covering D-08 (plaintext, unauthenticated, any reachable host can take over, bind privately or firewall), the up-to-three-connections-per-peer note (replication, heartbeat, snapshot lanes), CRC32C is not authentication, plaintext files at 0700/0600, unsafe fsync modes, and that NFR-1/NFR-2 hardware validation is pending so no latency claim is made for the replicated write path.

## Task Commits

1. Task 1: `579b9d5` test(03-15): three nodes over real TCP elect and replicate
2. Task 2: `69e534d` test(03-15): lagging follower installs a snapshot over the TCP snapshot lane
3. Task 3: `34790a4` feat(03-15): wire the binary to the Raft node, CLI smoke test and README security note

## Verification

- `cargo test --features test-util` (whole suite): lib 227, cli_smoke 1, cluster 4, openraft_api_gate 2, tcp_smoke 2, transport 38, wal_test_util_reachable 1; all passed.
- `cargo test --test tcp_smoke` run 10 consecutive times (both tests running in parallel): all passed, 0.5 to 1.1 s each. `cargo test --test cli_smoke` run 3 times: passed (about 0.4 s).
- `bash scripts/check-warnings.sh`: exit 0.
- README gates: `grep -c "Raft port" README.md` = 1 (baseline 0); `grep -c -- "--bootstrap" README.md` = 2 (baseline 0).
- Manual check (not a committed test): three CLI processes (node 1 with `--bootstrap`) on loopback; node 1 printed "initialized a new cluster" and "is leader (term 1)"; SIGINT to all three produced exit code 0 for each with the "shutting down" line.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Test flake] Writes through a fixed leader failed with `NotLeader { leader: None }`**
- **Found during:** Task 2, first full run of `tcp_smoke` (both tests running concurrently on a loaded machine; the single-test runs passed).
- **Issue:** the test picked one leader after `wait_for_leader` and wrote everything through it. With the fast test timers (election 100-200 ms) a debug-build node can miss a heartbeat under CPU load and a legitimate leadership change then makes a write fail. This is correct Raft behaviour, not a transport bug.
- **Fix:** `write_anywhere` tries each running node in turn and retries on `NotLeader` for up to 20 s (`Set` is idempotent, so retrying the ambiguous `LogEntryDiscarded` case is safe); the purge wait now requires both running nodes to have snapshotted and purged. Any non-`NotLeader` error still panics. 10 consecutive runs afterwards were clean.
- **Files modified:** tests/tcp_smoke.rs. **Commit:** 69e534d.

### Interpretation notes

- The plan describes the received file as observable "before install" under a `recv-<digits>.tmp` name. The transfer of a few KiB takes milliseconds, so a guaranteed sighting is not deterministic. The test runs a 200 microsecond polling thread over node 3's recv directory and asserts that anything it sees is a well-formed `recv-<digits>.tmp` name, without asserting a sighting; the deterministic proof that the file path was used is `snapshot_chunks > 0` (chunks are only counted after being written into the recv file), plus the empty recv directory afterwards. The exact-name and 0600-mode checks for the receive file are covered by `snapshot_transfer_chunked_and_installed` in tests/transport.rs (03-17).
- `parse_peers` is a free function in `lightcycle::raft::config`, not an associated function of `NodeConfig`; main calls it directly.
- tests/tcp_smoke.rs uses `tests/common/mod.rs` read-only (`fast_config`, `eventually`, `wait_for_leader`); the sibling plan 03-14's files were not touched.

No bugs were found in prior plans' files; no files outside the plan's `files_modified` were changed.

## Authentication gates

None.

## Known Stubs

None.

## Threat Flags

None beyond the plan's register. T-03-36 (unauthenticated Raft port) and T-03-37 (plaintext traffic and files) are accepted per D-08 and documented in the README. Operational notes worth carrying to the hardening phase: the binary accepts any `--bind` address including `0.0.0.0` (a stderr warning for non-loopback binds was not in the plan and was not added), and a peer can still make a receiver write up to `max_snapshot_bytes` (8 GiB default) into the recv directory before validation (noted by 03-17).

## Notes for downstream plans

- 03-16 (documentation/verification) can cite: three TCP connections per peer (replication, heartbeat, snapshot), README security section title "Raft port security (Phase 3)", and the pending NFR-1/NFR-2 hardware validation statement.
- `tests/tcp_smoke.rs` contains reusable `TestNode`/`DeferredHandler`/`write_anywhere` helpers if a later plan needs a real-TCP cluster harness (they are private to that test file, not in `tests/common`).
- The binary's data layout is `<data-dir>/wal`, `<data-dir>/snapshots`, `<data-dir>/recv`; reusing a data directory from a standalone run for a clustered node 1 keeps its single-member state (the README says to use fresh directories).

## Self-Check: PASSED

- tests/tcp_smoke.rs, tests/cli_smoke.rs, src/main.rs, README.md: FOUND
- Commits 579b9d5, 69e534d, 34790a4 are ancestors of HEAD: verified via `git log`
- STATE.md and ROADMAP.md untouched
