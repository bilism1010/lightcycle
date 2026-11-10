---
phase: 03
review: 03-REVIEW.md
titles: json
findings:
  - id: CR-01
    severity: critical
    disposition: open
    title: "Corruption in the last segment is treated as a torn tail and silently drops acknowledged records"
  - id: CR-02
    severity: critical
    disposition: open
    title: "Oversized entries are not rejected up front; the writer can drop them silently and leave a durable index gap"
  - id: CR-03
    severity: critical
    disposition: open
    title: "Wildcard `--bind` publishes an unroutable address (`0.0.0.0:port`) into replicated cluster membership"
  - id: WR-01
    severity: warning
    disposition: open
    title: "Flushed watermark is raised past the truncation point when entries and a truncate marker share a batch"
  - id: WR-02
    severity: warning
    disposition: open
    title: "Purge deletes segment files before the index and cache drop the entries, so concurrent readers get a spurious I/O error"
  - id: WR-03
    severity: warning
    disposition: open
    title: "Recovery caps reads at the configured segment size, then destroys anything beyond it"
  - id: WR-04
    severity: warning
    disposition: open
    title: "Recovery never validates that the surviving log starts where the purge point says it should"
  - id: WR-05
    severity: warning
    disposition: open
    title: "Snapshot store is not serialized against concurrent build / install / `get_current_snapshot`; a lost race becomes a spurious I/O error"
  - id: WR-06
    severity: warning
    disposition: open
    title: "A failed `rename` in `build_with_hook` leaks a full-size `.tmp`; the test named for this behaviour does not assert it"
  - id: WR-07
    severity: warning
    disposition: open
    title: "A node started with no `--peers` silently initializes its own one-node cluster"
  - id: WR-08
    severity: warning
    disposition: open
    title: "Any unreadable-header error is reported as \"wipe this node\", including a snapshot written by a newer format version"
  - id: WR-09
    severity: warning
    disposition: open
    title: "Client never checks that the node it connected to is the intended target"
  - id: WR-10
    severity: warning
    disposition: open
    title: "No bound on concurrent connections or concurrent snapshot receives; per-connection memory and disk are large"
  - id: WR-11
    severity: warning
    disposition: open
    title: "No idle detection, TCP keepalive or write timeout on replication/heartbeat connections, or on an idle snapshot connection"
  - id: WR-12
    severity: warning
    disposition: open
    title: "Writer task is aborted without draining, so the final response/error frame can be lost"
  - id: WR-13
    severity: warning
    disposition: open
    title: "Server closes the connection after every Conflict/HigherVote AppendEntries result, but the unary client path reuses the connection"
  - id: WR-14
    severity: warning
    disposition: open
    title: "Snapshot response is awaited with the 30 s per-chunk idle timeout, but install includes full double validation of up to 8 GiB"
  - id: WR-15
    severity: warning
    disposition: open
    title: "Vote/pre-vote handler errors are always reported as non-fatal although they can only be Fatal"
  - id: WR-16
    severity: warning
    disposition: open
    title: "Second-crash torn-tail assertion is vacuous"
  - id: WR-17
    severity: warning
    disposition: open
    title: "\"Uncommitted write is discarded\" has no evidence the write was ever appended"
  - id: WR-18
    severity: warning
    disposition: open
    title: "Exact-count assertions after a fixed 300 ms sleep (flaky on loaded hosts)"
  - id: WR-19
    severity: warning
    disposition: open
    title: "`snapshot_does_not_starve_heartbeat` relies on absolute wall-clock bounds"
  - id: WR-20
    severity: warning
    disposition: open
    title: "WAL bench \"fsync\" scenarios use `tempfile::tempdir()`, which is often tmpfs on Linux"
  - id: WR-21
    severity: warning
    disposition: open
    title: "Bench counts failed acks as latencies and cannot detect a failing WAL"
  - id: IN-01
    severity: info
    disposition: open
    title: "Dead error variants and a stale doc comment"
  - id: IN-02
    severity: info
    disposition: open
    title: "`WalLogStore::append` can return `Err` without completing the openraft callback"
  - id: IN-03
    severity: info
    disposition: open
    title: "Recovery hard-fails on a segment gap that a partially persisted purge can legitimately produce"
  - id: IN-04
    severity: info
    disposition: open
    title: "`Display for KvRequest` dumps keys and values into any log line that formats an entry"
  - id: IN-05
    severity: info
    disposition: open
    title: "Declared minimum `bytes = \"1.7\"` is too low for `Bytes::from_owner`"
  - id: IN-06
    severity: info
    disposition: open
    title: "Leader watcher re-announces leadership every hour"
  - id: IN-07
    severity: info
    disposition: open
    title: "`SnapshotStore::read_chunk` has no production caller"
  - id: IN-08
    severity: info
    disposition: open
    title: "Generality without instantiation, `NodeConfig.bind` is never read"
  - id: IN-09
    severity: info
    disposition: open
    title: "`RaftNode::start` creates the WAL directory before the config is validated"
  - id: IN-10
    severity: info
    disposition: open
    title: "Server ignores the Hello `node_id`"
  - id: IN-11
    severity: info
    disposition: open
    title: "Settings that must match across peers are not negotiated, so mismatches fail late and opaquely"
  - id: IN-12
    severity: info
    disposition: open
    title: "Fake snapshot delivery does not remove the received copy, unlike the real server"
  - id: IN-13
    severity: info
    disposition: open
    title: "Connection task panics/failures are discarded and `shutdown` does not wait for connection cleanup"
  - id: IN-14
    severity: info
    disposition: open
    title: "`check-get-regression.sh` no-match case exits silently; the diagnostic branch is dead"
  - id: IN-15
    severity: info
    disposition: open
    title: "`check-get-regression.sh` thresholds are a single-machine absolute baseline"
  - id: IN-16
    severity: info
    disposition: open
    title: "`check-warnings.sh` continues after a failed `cd`"
  - id: IN-17
    severity: info
    disposition: open
    title: "WAL bench report runs unconditionally, ignoring Criterion filters and `--test`"
  - id: IN-18
    severity: info
    disposition: open
    title: "Bench statistics are not trustworthy at the small op counts"
  - id: IN-19
    severity: info
    disposition: open
    title: "Several tests make claims their assertions only partly support"
  - id: IN-20
    severity: info
    disposition: open
    title: "Minor timing and diagnosability risks in test helpers"
open: 44
total: 44
recorded: 2026-11-10T11:27:46.565Z
---

# Phase 03: Code Review Disposition

| Finding | Severity | Disposition | Source |
|---------|----------|-------------|--------|
| CR-01 | critical | open | - |
| CR-02 | critical | open | - |
| CR-03 | critical | open | - |
| WR-01 | warning | open | - |
| WR-02 | warning | open | - |
| WR-03 | warning | open | - |
| WR-04 | warning | open | - |
| WR-05 | warning | open | - |
| WR-06 | warning | open | - |
| WR-07 | warning | open | - |
| WR-08 | warning | open | - |
| WR-09 | warning | open | - |
| WR-10 | warning | open | - |
| WR-11 | warning | open | - |
| WR-12 | warning | open | - |
| WR-13 | warning | open | - |
| WR-14 | warning | open | - |
| WR-15 | warning | open | - |
| WR-16 | warning | open | - |
| WR-17 | warning | open | - |
| WR-18 | warning | open | - |
| WR-19 | warning | open | - |
| WR-20 | warning | open | - |
| WR-21 | warning | open | - |
| IN-01 | info | open | - |
| IN-02 | info | open | - |
| IN-03 | info | open | - |
| IN-04 | info | open | - |
| IN-05 | info | open | - |
| IN-06 | info | open | - |
| IN-07 | info | open | - |
| IN-08 | info | open | - |
| IN-09 | info | open | - |
| IN-10 | info | open | - |
| IN-11 | info | open | - |
| IN-12 | info | open | - |
| IN-13 | info | open | - |
| IN-14 | info | open | - |
| IN-15 | info | open | - |
| IN-16 | info | open | - |
| IN-17 | info | open | - |
| IN-18 | info | open | - |
| IN-19 | info | open | - |
| IN-20 | info | open | - |

Dispositions: `open` (recorded, not yet triaged), `fixed`, `skipped`, `deferred`.
Set `deferred` by hand and put the reason in the Source cell; both are preserved. A `|` in the reason is kept as prose and escaped on the next run.
Re-running the gate keeps every row it can. A row the current review no longer reports is kept and its Source cell flagged, so a finding does not leave this record silently. ONE exception: when a finding id is REUSED by a different finding, the earlier decision cannot keep a row — the id is taken — and it is dropped. A RECORDED decision (anything but `open`) is named on the console when that happens; a row still at `open` is replaced silently, because `open` records no decision to lose.
