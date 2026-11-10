---
phase: 03
review: 03-REVIEW.md
titles: json
findings:
  - id: CR-01
    severity: critical
    disposition: fixed
    title: "AppendEntries batches are not bounded by the frame cap, so replication of large entries wedges permanently"
  - id: WR-01
    severity: warning
    disposition: fixed
    title: "The CR-01 recovery fix makes a legitimate crash unrecoverable (non-prefix tear, or a value that embeds a WAL record)"
  - id: WR-02
    severity: warning
    disposition: fixed
    title: "The CR-03 wildcard check runs on restarts and on standalone nodes where no address is published (regression)"
  - id: WR-03
    severity: warning
    disposition: fixed
    title: "The WR-07 warning tells the operator to \"stop now\" but the node initializes the cluster immediately"
  - id: WR-04
    severity: warning
    disposition: fixed
    title: "A corrupt or mismatched received snapshot is a fatal storage error for the receiving node"
  - id: IN-01
    severity: info
    disposition: fixed
    title: "Dead error variants and a stale doc comment"
  - id: IN-02
    severity: info
    disposition: fixed
    title: "`WalLogStore::append` can return `Err` without completing the openraft callback"
  - id: IN-03
    severity: info
    disposition: fixed
    title: "Recovery hard-fails on a segment gap that a partially persisted purge can legitimately produce"
  - id: IN-04
    severity: info
    disposition: fixed
    title: "`Display for KvRequest` dumps keys and values into any log line that formats an entry"
  - id: IN-05
    severity: info
    disposition: fixed
    title: "Declared minimum `bytes = \"1.7\"` is too low for `Bytes::from_owner`"
  - id: IN-06
    severity: info
    disposition: fixed
    title: "Leader watcher re-announces leadership every hour"
  - id: IN-07
    severity: info
    disposition: fixed
    title: "`SnapshotStore::read_chunk` has no production caller"
  - id: IN-08
    severity: info
    disposition: fixed
    title: "Generality without instantiation, `NodeConfig.bind` is never read"
  - id: IN-09
    severity: info
    disposition: fixed
    title: "`RaftNode::start` creates the WAL directory before the config is validated"
  - id: IN-10
    severity: info
    disposition: fixed
    title: "Server ignores the Hello `node_id`"
  - id: IN-11
    severity: info
    disposition: fixed
    title: "Settings that must match across peers are not negotiated, so mismatches fail late and opaquely"
  - id: IN-12
    severity: info
    disposition: fixed
    title: "Fake snapshot delivery does not remove the received copy, unlike the real server"
  - id: IN-13
    severity: info
    disposition: fixed
    title: "Connection task panics/failures are discarded and `shutdown` does not wait for connection cleanup"
  - id: IN-14
    severity: info
    disposition: fixed
    title: "`check-get-regression.sh` no-match case exits silently; the diagnostic branch is dead"
  - id: IN-15
    severity: info
    disposition: fixed
    title: "`check-get-regression.sh` thresholds are a single-machine absolute baseline"
  - id: IN-16
    severity: info
    disposition: fixed
    title: "`check-warnings.sh` continues after a failed `cd`"
  - id: IN-17
    severity: info
    disposition: fixed
    title: "WAL bench report runs unconditionally, ignoring Criterion filters and `--test`"
  - id: IN-18
    severity: info
    disposition: fixed
    title: "Bench statistics are not trustworthy at the small op counts"
  - id: IN-19
    severity: info
    disposition: fixed
    title: "Several tests make claims their assertions only partly support"
  - id: IN-20
    severity: info
    disposition: fixed
    title: "Minor timing and diagnosability risks in test helpers"
  - id: IN-21
    severity: info
    disposition: skipped
    title: "Snapshot receive hardening is only partly done (no free-space check; two connections can hold both slots)"
  - id: IN-22
    severity: info
    disposition: fixed
    title: "`estimate_size` duplicates `KvRequest::payload_len`"
  - id: IN-23
    severity: info
    disposition: fixed
    title: "A wrong lane in the HelloAck is reported as \"unexpected frame\", and only when a node id was expected"
  - id: IN-24
    severity: info
    disposition: fixed
    title: "`--advertise` accepts an unbracketed IPv6 address and publishes a malformed `host:port`"
  - id: IN-25
    severity: info
    disposition: fixed
    title: "The entry-size limit is per node, so a follower with a smaller segment size goes fatal on an entry the leader accepted"
  - id: IN-26
    severity: info
    disposition: fixed
    title: "Test gaps left by the fix commits"
  - id: CR-02
    severity: critical
    disposition: fixed
    title: "Oversized entries are not rejected up front; the writer can drop them silently and leave a durable index gap"
  - id: CR-03
    severity: critical
    disposition: fixed
    title: "Wildcard `--bind` publishes an unroutable address (`0.0.0.0:port`) into replicated cluster membership"
  - id: WR-05
    severity: warning
    disposition: fixed
    title: "Snapshot store is not serialized against concurrent build / install / `get_current_snapshot`; a lost race becomes a spurious I/O error"
  - id: WR-06
    severity: warning
    disposition: fixed
    title: "A failed `rename` in `build_with_hook` leaks a full-size `.tmp`; the test named for this behaviour does not assert it"
  - id: WR-07
    severity: warning
    disposition: fixed
    title: "A node started with no `--peers` silently initializes its own one-node cluster"
  - id: WR-08
    severity: warning
    disposition: fixed
    title: "Any unreadable-header error is reported as \"wipe this node\", including a snapshot written by a newer format version"
  - id: WR-09
    severity: warning
    disposition: fixed
    title: "Client never checks that the node it connected to is the intended target"
  - id: WR-10
    severity: warning
    disposition: fixed
    title: "No bound on concurrent connections or concurrent snapshot receives; per-connection memory and disk are large"
  - id: WR-11
    severity: warning
    disposition: fixed
    title: "No idle detection, TCP keepalive or write timeout on replication/heartbeat connections, or on an idle snapshot connection"
  - id: WR-12
    severity: warning
    disposition: fixed
    title: "Writer task is aborted without draining, so the final response/error frame can be lost"
  - id: WR-13
    severity: warning
    disposition: fixed
    title: "Server closes the connection after every Conflict/HigherVote AppendEntries result, but the unary client path reuses the connection"
  - id: WR-14
    severity: warning
    disposition: fixed
    title: "Snapshot response is awaited with the 30 s per-chunk idle timeout, but install includes full double validation of up to 8 GiB"
  - id: WR-15
    severity: warning
    disposition: fixed
    title: "Vote/pre-vote handler errors are always reported as non-fatal although they can only be Fatal"
  - id: WR-16
    severity: warning
    disposition: fixed
    title: "Second-crash torn-tail assertion is vacuous"
  - id: WR-17
    severity: warning
    disposition: fixed
    title: "\"Uncommitted write is discarded\" has no evidence the write was ever appended"
  - id: WR-18
    severity: warning
    disposition: fixed
    title: "Exact-count assertions after a fixed 300 ms sleep (flaky on loaded hosts)"
  - id: WR-19
    severity: warning
    disposition: fixed
    title: "`snapshot_does_not_starve_heartbeat` relies on absolute wall-clock bounds"
  - id: WR-20
    severity: warning
    disposition: fixed
    title: "WAL bench \"fsync\" scenarios use `tempfile::tempdir()`, which is often tmpfs on Linux"
  - id: WR-21
    severity: warning
    disposition: fixed
    title: "Bench counts failed acks as latencies and cannot detect a failing WAL"
open: 0
total: 50
recorded: 2026-11-10T12:41:52.618Z
---

# Phase 03: Code Review Disposition

| Finding | Severity | Disposition | Source |
|---------|----------|-------------|--------|
| CR-01 | critical | fixed | 03-REVIEW-FIX.md |
| WR-01 | warning | fixed | 03-REVIEW-FIX.md |
| WR-02 | warning | fixed | 03-REVIEW-FIX.md |
| WR-03 | warning | fixed | 03-REVIEW-FIX.md |
| WR-04 | warning | fixed | 03-REVIEW-FIX.md |
| IN-01 | info | fixed | 03-REVIEW-FIX.md |
| IN-02 | info | fixed | 03-REVIEW-FIX.md |
| IN-03 | info | fixed | 03-REVIEW-FIX.md |
| IN-04 | info | fixed | 03-REVIEW-FIX.md |
| IN-05 | info | fixed | 03-REVIEW-FIX.md |
| IN-06 | info | fixed | 03-REVIEW-FIX.md |
| IN-07 | info | fixed | 03-REVIEW-FIX.md |
| IN-08 | info | fixed | 03-REVIEW-FIX.md |
| IN-09 | info | fixed | 03-REVIEW-FIX.md |
| IN-10 | info | fixed | 03-REVIEW-FIX.md |
| IN-11 | info | fixed | 03-REVIEW-FIX.md |
| IN-12 | info | fixed | 03-REVIEW-FIX.md |
| IN-13 | info | fixed | 03-REVIEW-FIX.md |
| IN-14 | info | fixed | 03-REVIEW-FIX.md |
| IN-15 | info | fixed | 03-REVIEW-FIX.md |
| IN-16 | info | fixed | 03-REVIEW-FIX.md |
| IN-17 | info | fixed | 03-REVIEW-FIX.md |
| IN-18 | info | fixed | 03-REVIEW-FIX.md |
| IN-19 | info | fixed | 03-REVIEW-FIX.md |
| IN-20 | info | fixed | 03-REVIEW-FIX.md |
| IN-21 | info | skipped | 03-REVIEW-FIX.md |
| IN-22 | info | fixed | 03-REVIEW-FIX.md |
| IN-23 | info | fixed | 03-REVIEW-FIX.md |
| IN-24 | info | fixed | 03-REVIEW-FIX.md |
| IN-25 | info | fixed | 03-REVIEW-FIX.md |
| IN-26 | info | fixed | 03-REVIEW-FIX.md |
| CR-02 | critical | fixed | 03-REVIEW-FIX.md (not in the current review) |
| CR-03 | critical | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-05 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-06 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-07 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-08 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-09 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-10 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-11 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-12 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-13 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-14 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-15 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-16 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-17 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-18 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-19 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-20 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |
| WR-21 | warning | fixed | 03-REVIEW-FIX.md (not in the current review) |

Dispositions: `open` (recorded, not yet triaged), `fixed`, `skipped`, `deferred`.
Set `deferred` by hand and put the reason in the Source cell; both are preserved. A `|` in the reason is kept as prose and escaped on the next run.
Re-running the gate keeps every row it can. A row the current review no longer reports is kept and its Source cell flagged, so a finding does not leave this record silently. ONE exception: when a finding id is REUSED by a different finding, the earlier decision cannot keep a row — the id is taken — and it is dropped. A RECORDED decision (anything but `open`) is named on the console when that happens; a row still at `open` is replaced silently, because `open` records no decision to lose.
