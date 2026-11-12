---
status: complete
phase: 03-consensus-persistence-engine
source: [03-VERIFICATION.md]
started: 2026-11-10T12:10:00Z
updated: 2026-11-12T03:47:57.531Z
---

## Current Test

[testing complete]

## Tests

### 1. Decide whether up to three TCP connections per peer (replication, heartbeat, snapshot lanes) is acceptable against decision D-07 (one persistent connection per peer)
expected: Either accept interpretation A4 (one persistent pipelined replication connection plus dedicated heartbeat and snapshot connections), recording it as a CONTEXT.md amendment, or require a single multiplexed connection (priority writer, head-of-line risk), which means rework of src/net/client.rs, src/net/server.rs and the lane tests.
result: pass
note: "User accepted interpretation A4 (2026-11-10). Recorded as an amendment to D-07 in 03-CONTEXT.md."

### 2. Decide the status of FR-2 (atomic CAS plus WATCH/multi-key conditional updates)
expected: Either accept FR-2 as split (single-key CAS done in Phases 2-3, WATCH/multi-key explicitly unscheduled) and add a roadmap item for it, or require it before Phase 3 closes. Plans 03-02 and 03-05 record requirements-completed FR-2, which overstates this.
result: pass
note: "User: \"FR-2 add roadmap item for WATCH/multi-key\". Accepted as split; added ROADMAP.md Milestone 4.4 and a status note on FR-2 in REQUIREMENTS.md."

### 3. Decide whether the binary should warn on, or refuse, non-private --bind addresses
expected: Either accept the current behaviour, or ask for something different (warn-only, refusal of all non-loopback binds, or a revert). Current behaviour after review fixes 48b2983 and f528805: any --bind is accepted; --advertise <host:port> sets the address published in the membership; a wildcard address (0.0.0.0, [::]) is refused only when this start initializes a multi-node cluster without a routable --advertise; restarts and standalone nodes are not checked. The README states the peer port is plaintext and unauthenticated (D-08) and must be bound to a private interface or firewalled. (Original finding: RAFT-CR-01 / CR-03 - a wildcard bind used to be published into the membership silently.)
result: pass
note: "User accepted the current --bind/--advertise behaviour (2026-11-11). Peer-port authentication stays with the TLS hardening phase (D-08)."

### 4. Run cargo bench --bench wal and an end-to-end 3-node write benchmark on a Linux NVMe host
expected: WAL fsync percentiles on NVMe, and P99 write < 2ms (3 nodes), >= 500k ops/s, as NFR-1 and NFR-2 require. Not a Phase 3 claim; 03-VALIDATION.md keeps this row open and it is carried to Phase 6.
result: skipped
reason: "Deferred follow-up: I will test these later on Linux. let me know when we've done the work enough that I can git clone and run from Linux Later."

## Summary

total: 4
passed: 3
issues: 0
pending: 0
skipped: 1
blocked: 0

## Gaps

## Deferred Follow-Ups

- test: 4
  idea: "I will test these later on Linux. let me know when we've done the work enough that I can git clone and run from Linux Later."
  deferred_at: 2026-11-10
