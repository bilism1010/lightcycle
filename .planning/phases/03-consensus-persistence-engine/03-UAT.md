---
status: testing
phase: 03-consensus-persistence-engine
source: [03-VERIFICATION.md]
started: 2026-11-10T12:10:00Z
updated: 2026-11-10T12:10:00Z
---

## Current Test

number: 1
name: Decide whether up to three TCP connections per peer is acceptable against decision D-07
expected: |
  Either accept interpretation A4 (one persistent pipelined replication connection plus
  dedicated heartbeat and snapshot connections), recording it as a CONTEXT.md amendment,
  or require a single multiplexed connection (priority writer, head-of-line risk), which
  means rework of src/net/client.rs, src/net/server.rs and the lane tests.
awaiting: user response

## Tests

### 1. Decide whether up to three TCP connections per peer (replication, heartbeat, snapshot lanes) is acceptable against decision D-07 (one persistent connection per peer)
expected: Either accept interpretation A4 (one persistent pipelined replication connection plus dedicated heartbeat and snapshot connections), recording it as a CONTEXT.md amendment, or require a single multiplexed connection (priority writer, head-of-line risk), which means rework of src/net/client.rs, src/net/server.rs and the lane tests.
result: [pending]

### 2. Decide the status of FR-2 (atomic CAS plus WATCH/multi-key conditional updates)
expected: Either accept FR-2 as split (single-key CAS done in Phases 2-3, WATCH/multi-key explicitly unscheduled) and add a roadmap item for it, or require it before Phase 3 closes. Plans 03-02 and 03-05 record requirements-completed FR-2, which overstates this.
result: [pending]

### 3. Decide whether the binary should warn on, or refuse, non-private --bind addresses
expected: Either accept README-only guidance, or add a warning / refusal for 0.0.0.0 and non-loopback binds. Functional hazard - any --bind is accepted silently and the bootstrap membership advertises the listener's local_addr, so --bind 0.0.0.0:PORT publishes the unroutable address 0.0.0.0:PORT as this node's membership address (also code review finding RAFT-CR-01).
result: [pending]

### 4. Run cargo bench --bench wal and an end-to-end 3-node write benchmark on a Linux NVMe host
expected: WAL fsync percentiles on NVMe, and P99 write < 2ms (3 nodes), >= 500k ops/s, as NFR-1 and NFR-2 require. Not a Phase 3 claim; 03-VALIDATION.md keeps this row open and it is carried to Phase 6.
result: [pending]

## Summary

total: 4
passed: 0
issues: 0
pending: 4
skipped: 0
blocked: 0

## Gaps
