//! Single-node cluster tests (plan 03-13): the WAL, the stamped state machine, the snapshot store
//! and a real openraft core running together, including restart from disk.

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{eventually, fast_config, wait_for_leader};
use lightcycle::engine::LocalEngine;
use lightcycle::net::fake::NetController;
use lightcycle::raft::config::NodeConfig;
use lightcycle::raft::node::RaftNode;
use lightcycle::raft::types::{KvRequest, KvResponse};
use openraft::BasicNode;

const WAIT: Duration = Duration::from_secs(10);

fn members() -> BTreeMap<u64, BasicNode> {
    BTreeMap::from([(1, BasicNode { addr: "fake".to_string() })])
}

fn set(key: &str, value: &str) -> KvRequest {
    KvRequest::Set { key: Bytes::copy_from_slice(key.as_bytes()), value: Bytes::copy_from_slice(value.as_bytes()) }
}

async fn start(cfg: NodeConfig) -> RaftNode {
    let ctl = NetController::new();
    RaftNode::start(cfg, ctl.factory(1)).await.expect("node starts")
}

async fn expect_value(node: &RaftNode, key: &str, value: &str) {
    eventually(WAIT, "value applied", || async {
        node.engine().get(key.as_bytes()).as_deref() == Some(value.as_bytes())
    })
    .await;
}

fn bootstrap_cfg(dir: &Path) -> NodeConfig {
    let mut cfg = fast_config(1, dir);
    cfg.bootstrap = true;
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_restart() {
    let dir = tempfile::tempdir().unwrap();

    let node = start(bootstrap_cfg(dir.path())).await;
    assert!(node.bootstrap_if_needed(members()).await.unwrap(), "first start initializes");
    assert_eq!(wait_for_leader(&[&node], WAIT).await, 1);

    assert_eq!(node.write(set("k", "v")).await.unwrap(), KvResponse::Ok);
    expect_value(&node, "k", "v").await;
    node.shutdown().await.unwrap();

    // Restart on the same directory: no re-initialize, state recovered through WAL replay.
    let node = start(bootstrap_cfg(dir.path())).await;
    assert!(!node.bootstrap_if_needed(members()).await.unwrap(), "restart never re-initializes");
    assert_eq!(wait_for_leader(&[&node], WAIT).await, 1);
    expect_value(&node, "k", "v").await;

    let stats = node.recovery_stats();
    assert!(stats.entries_recovered > 0, "restart recovered log entries: {stats:?}");
    assert_eq!(stats.torn_bytes_discarded, 0, "graceful shutdown leaves no torn tail");

    assert_eq!(node.write(set("k2", "v2")).await.unwrap(), KvResponse::Ok);
    expect_value(&node, "k2", "v2").await;
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_write_is_rejected_without_stopping_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(bootstrap_cfg(dir.path())).await;
    assert!(node.bootstrap_if_needed(members()).await.unwrap());
    assert_eq!(wait_for_leader(&[&node], WAIT).await, 1);

    // Larger than one WAL record: the log store could not store it, and openraft treats a storage
    // error as fatal, so it must be refused before it is proposed.
    let huge = KvRequest::Set { key: Bytes::from_static(b"big"), value: Bytes::from(vec![7u8; 17 << 20]) };
    match node.write(huge).await {
        Err(lightcycle::raft::node::NodeError::RequestTooLarge { size, max }) => assert!(size > max),
        other => panic!("expected RequestTooLarge, got {other:?}"),
    }
    assert!(node.engine().get(b"big").is_none());

    assert_eq!(node.write(set("after", "ok")).await.unwrap(), KvResponse::Ok);
    expect_value(&node, "after", "ok").await;
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_accessor_is_not_captured_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let node = start(bootstrap_cfg(dir.path())).await;
    node.bootstrap_if_needed(members()).await.unwrap();
    wait_for_leader(&[&node], WAIT).await;

    node.write(set("k", "v")).await.unwrap();
    expect_value(&node, "k", "v").await;

    let before = node.engine();

    // Same ArcSwap store that KvStateMachine::replace_engine performs during install_snapshot.
    let replacement_engine = LocalEngine::new();
    replacement_engine.set(b"k", b"replaced");
    let replacement = Arc::new(replacement_engine);
    node.engine_handle().store(replacement.clone());

    assert!(Arc::ptr_eq(&node.engine(), &replacement), "engine() reloads the shared handle");
    assert!(!Arc::ptr_eq(&node.engine(), &before), "engine() is not the one captured earlier");
    assert_eq!(node.engine().get(b"k").as_deref(), Some(&b"replaced"[..]));
    assert_eq!(before.get(b"k").as_deref(), Some(&b"v"[..]));

    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_if_needed_is_idempotent() {
    // Designated node: first call initializes, second is a no-op.
    let dir = tempfile::tempdir().unwrap();
    let node = start(bootstrap_cfg(dir.path())).await;
    assert!(node.bootstrap_if_needed(members()).await.unwrap());
    assert!(!node.bootstrap_if_needed(members()).await.unwrap());
    wait_for_leader(&[&node], WAIT).await;
    assert!(!node.bootstrap_if_needed(members()).await.unwrap());
    node.shutdown().await.unwrap();

    // Not the designated node: never initializes, and stays uninitialized.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = fast_config(1, dir.path());
    cfg.bootstrap = false;
    let node = start(cfg).await;
    assert!(!node.bootstrap_if_needed(members()).await.unwrap());
    assert!(!node.raft().is_initialized().await.unwrap(), "non-bootstrap node must not initialize");
    node.shutdown().await.unwrap();
}

/// Lowest WAL segment sequence present in the node's WAL directory.
fn lowest_segment(cfg: &NodeConfig) -> u64 {
    std::fs::read_dir(cfg.wal_dir())
        .unwrap()
        .filter_map(|e| {
            lightcycle::wal::segment::parse_segment_name(&e.unwrap().file_name().to_string_lossy())
        })
        .min()
        .expect("at least one wal segment")
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_snapshot_purge_restart() {
    const KEYS: usize = 400;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = bootstrap_cfg(dir.path());
    cfg.snapshot_every_entries = 50;
    cfg.max_in_snapshot_log_to_keep = 10;
    cfg.purge_batch_size = 10;
    cfg.wal.segment_size = 4096;

    let node = start(cfg.clone()).await;
    assert!(node.bootstrap_if_needed(members()).await.unwrap());
    wait_for_leader(&[&node], WAIT).await;
    assert_eq!(lowest_segment(&cfg), 1);

    for i in 0..KEYS {
        let resp = node.write(set(&format!("key-{i:04}"), &format!("val-{i:04}"))).await.unwrap();
        assert_eq!(resp, KvResponse::Ok);
    }

    // snapshot trigger -> durable snapshot -> openraft purge -> WAL purge marker -> segment delete
    eventually(Duration::from_secs(30), "snapshot built and old WAL segments deleted", || {
        let snap_index = node.metrics().snapshot.map(|l| l.index);
        let low = lowest_segment(&cfg);
        async move { snap_index.is_some_and(|i| i >= 50) && low > 1 }
    })
    .await;
    let m = node.metrics();
    assert!(m.purged.is_some(), "openraft purged the log: {m:?}");
    node.shutdown().await.unwrap();

    // Restart from snapshot plus the remaining log; stamped replay skips already-applied entries.
    let node = start(cfg.clone()).await;
    assert!(!node.bootstrap_if_needed(members()).await.unwrap());
    wait_for_leader(&[&node], WAIT).await;
    expect_value(&node, &format!("key-{:04}", KEYS - 1), &format!("val-{:04}", KEYS - 1)).await;
    for i in 0..KEYS {
        assert_eq!(
            node.engine().get(format!("key-{i:04}").as_bytes()).as_deref(),
            Some(format!("val-{i:04}").as_bytes()),
            "key {i} lost across snapshot + purge + restart"
        );
    }
    assert_eq!(node.write(set("after", "restart")).await.unwrap(), KvResponse::Ok);
    expect_value(&node, "after", "restart").await;
    node.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// Three-node cluster over the fake network (plan 03-14).
// ---------------------------------------------------------------------------------------------

use common::{set_req, ClusterOpts, TestCluster};

fn expect_map(prefix: &str, range: std::ops::Range<usize>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    range
        .map(|i| (format!("{prefix}-{i:03}").into_bytes(), format!("val-{i:03}").into_bytes()))
        .collect()
}

async fn write_keys(cluster: &TestCluster, prefix: &str, range: std::ops::Range<usize>) {
    write_keys_among(cluster, &cluster.live_ids(), prefix, range).await;
}

async fn write_keys_among(
    cluster: &TestCluster,
    ids: &[u64],
    prefix: &str,
    range: std::ops::Range<usize>,
) {
    for i in range {
        let resp = cluster
            .write_among(ids, set_req(&format!("{prefix}-{i:03}"), &format!("val-{i:03}")))
            .await
            .expect("write acknowledged");
        assert_eq!(resp, KvResponse::Ok);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_node_election_and_replication() {
    let cluster = TestCluster::start(3, ClusterOpts::default()).await;
    let leader = cluster.leader().await;
    assert!((1..=3).contains(&leader));

    write_keys(&cluster, "k", 0..50).await;
    cluster.wait_converged(&[1, 2, 3], &expect_map("k", 0..50)).await;

    // Replication and heartbeats travel on separate lanes: both carried traffic.
    let stats = cluster.ctl.stats();
    assert!(stats.replication.delivered > 0, "no replication traffic: {stats:?}");
    assert!(stats.heartbeat.delivered > 0, "no heartbeat traffic: {stats:?}");

    // All three agree on one leader and one term.
    eventually(WAIT, "all nodes agree on leader and term", || {
        let ms: Vec<_> = (1..=3).map(|id| cluster.node(id).metrics()).collect();
        let ok = ms.iter().all(|m| m.current_leader == Some(leader))
            && ms.iter().all(|m| m.current_term == ms[0].current_term);
        async move { ok }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_failover_new_leader_elected() {
    let mut cluster = TestCluster::start(3, ClusterOpts::default()).await;
    let old_leader = cluster.leader().await;

    write_keys(&cluster, "a", 0..20).await;
    let mut expected = expect_map("a", 0..20);
    cluster.wait_converged(&[1, 2, 3], &expected).await;

    cluster.kill(old_leader).await;
    let survivors: Vec<u64> = (1..=3).filter(|id| *id != old_leader).collect();
    let new_leader = cluster.leader_among(&survivors).await;
    assert_ne!(new_leader, old_leader, "a different node must lead after the kill");

    // Two of three nodes still accept and commit writes.
    write_keys(&cluster, "b", 0..20).await;
    expected.extend(expect_map("b", 0..20));
    cluster.wait_converged(&survivors, &expected).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_heals_and_minority_converges() {
    let cluster = TestCluster::start(3, ClusterOpts::default()).await;
    let old_leader = cluster.leader().await;
    let old_term = cluster.node(old_leader).metrics().current_term;

    write_keys(&cluster, "a", 0..10).await;
    let mut expected = expect_map("a", 0..10);
    cluster.wait_converged(&[1, 2, 3], &expected).await;

    cluster.ctl.isolate(old_leader);
    let majority: Vec<u64> = (1..=3).filter(|id| *id != old_leader).collect();
    let new_leader = cluster.leader_among(&majority).await;
    assert_ne!(new_leader, old_leader);
    assert!(
        cluster.node(new_leader).metrics().current_term > old_term,
        "the majority side must elect in a higher term"
    );

    // The majority keeps committing.
    write_keys_among(&cluster, &majority, "b", 0..10).await;
    expected.extend(expect_map("b", 0..10));
    cluster.wait_converged(&majority, &expected).await;

    // The isolated old leader cannot commit: its write never returns Ok.
    let res = tokio::time::timeout(
        Duration::from_secs(2),
        cluster.node(old_leader).write(set_req("minority-only", "must-not-commit")),
    )
    .await;
    assert!(!matches!(res, Ok(Ok(_))), "isolated leader committed a write: {res:?}");

    // Heal: the old leader steps down, discards its uncommitted entry and converges.
    cluster.ctl.heal();
    eventually(Duration::from_secs(15), "old leader steps down behind the new leader", || {
        let m = cluster.node(old_leader).metrics();
        async move { m.current_leader.is_some_and(|l| l != old_leader) }
    })
    .await;
    cluster.wait_converged(&[1, 2, 3], &expected).await;
    assert!(
        cluster.node(old_leader).engine().get(b"minority-only").is_none(),
        "the uncommitted write must be absent"
    );
}

/// Crash a node with a genuinely torn WAL tail (decision D-16): its fsync is delayed while writes
/// keep arriving, so a record is half-persisted when power is lost. Restart must discard the torn
/// bytes by CRC rather than fail, and no acknowledged write may be lost on any node.
#[tokio::test(flavor = "multi_thread")]
async fn kill_restart_keeps_committed_writes() {
    let opts = ClusterOpts { use_fake_io: true, ..ClusterOpts::default() };
    let mut cluster = TestCluster::start(3, opts).await;
    cluster.leader().await;

    write_keys(&cluster, "base", 0..100).await;
    let mut acked = expect_map("base", 0..100);
    cluster.wait_converged(&[1, 2, 3], &acked).await;

    // Crash 1: a follower, torn after 7 unsynced bytes.
    let leader = cluster.leader().await;
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    let writer = cluster.spawn_writer("bg1");
    writer.wait_acked(5).await;
    cluster.crash_with_unsynced_tail(follower, 7).await;
    let progress = writer.acked();
    writer.wait_acked(progress + 5).await; // two of three nodes keep committing
    acked.extend(writer.stop().await);

    cluster.restart(follower).await;
    let first = cluster.node(follower).recovery_stats();
    assert!(
        first.torn_bytes_discarded > 0,
        "follower restart must have discarded a torn tail, not found a clean log: {first:?}"
    );
    cluster.assert_contain_and_identical(&[1, 2, 3], &acked).await;

    // Crash 2: the leader, torn after 301 unsynced bytes, while writes continue through the
    // survivor that wins the next election. The tear must be proven for the leader itself: 301
    // bytes can in principle end exactly on a record boundary (a clean log), so that rare case
    // retries the crash instead of weakening the assertion.
    let mut second = None;
    for attempt in 0..5 {
        let leader = cluster.leader().await;
        let writer = cluster.spawn_writer(&format!("bg2-{attempt}"));
        writer.wait_acked(5).await;
        cluster.crash_with_unsynced_tail(leader, 301).await;
        let survivors: Vec<u64> = (1..=3).filter(|id| *id != leader).collect();
        let new_leader = cluster.leader_among(&survivors).await;
        assert_ne!(new_leader, leader);
        let progress = writer.acked();
        writer.wait_acked(progress + 5).await;
        acked.extend(writer.stop().await);

        cluster.restart(leader).await;
        let stats = cluster.node(leader).recovery_stats();
        if stats.torn_bytes_discarded > 0 {
            second = Some(stats);
            break;
        }
    }
    let second = second.expect("no leader crash in 5 attempts left a torn tail to discard");
    assert!(
        second.torn_bytes_discarded > 0,
        "leader restart must have discarded a torn tail: {second:?}"
    );

    // Every acknowledged write is on all three nodes, and the nodes are identical (a write that
    // was in flight at a crash and never acknowledged may be present or absent, but the same
    // everywhere).
    cluster.leader().await;
    cluster.assert_contain_and_identical(&[1, 2, 3], &acked).await;
    eprintln!("torn bytes discarded: follower {first:?}, ex-leader {second:?}");
}

// ---------------------------------------------------------------------------------------------
// Dynamic membership (plan 03-16).
// ---------------------------------------------------------------------------------------------

use common::{dump, MembershipLog};
use std::collections::BTreeSet;

fn set_of(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

/// Start node `id` outside the membership, add it as a learner (blocking until caught up), then
/// promote it to a voter alongside `current`. Returns the new voter list.
async fn grow(cluster: &mut TestCluster, current: &[u64], id: u64, log: &MembershipLog) -> Vec<u64> {
    cluster.start_extra(id).await;
    log.watch(cluster.node(id));
    cluster.add_learner(current, id).await.expect("add learner");
    let mut next = current.to_vec();
    next.push(id);
    cluster.change_membership(current, &set_of(&next), false).await.expect("promote learner");
    next
}

#[tokio::test(flavor = "multi_thread")]
async fn membership_add_voter() {
    let mut cluster = TestCluster::start(3, ClusterOpts::default()).await;
    let log = MembershipLog::new();
    for id in 1..=3 {
        log.watch(cluster.node(id));
    }
    cluster.leader().await;
    write_keys(&cluster, "a", 0..20).await;
    let mut expected = expect_map("a", 0..20);
    cluster.wait_converged(&[1, 2, 3], &expected).await;

    // Learner first: it catches up on the 20 keys before it may vote.
    cluster.start_extra(4).await;
    log.watch(cluster.node(4));
    cluster.add_learner(&[1, 2, 3], 4).await.expect("add learner 4");
    cluster.wait_converged(&[4], &expected).await;
    assert!(
        !cluster.node(4).metrics().membership_config.voter_ids().any(|v| v == 4),
        "node 4 is a learner, not yet a voter"
    );

    cluster.change_membership(&[1, 2, 3], &set_of(&[1, 2, 3, 4]), false).await.expect("change");
    cluster.wait_voters(&[1, 2, 3, 4], &set_of(&[1, 2, 3, 4])).await;

    write_keys(&cluster, "b", 0..20).await;
    expected.extend(expect_map("b", 0..20));
    cluster.wait_converged(&[1, 2, 3, 4], &expected).await;

    // Adding one voter to {1,2,3}: old and new quorums intersect, so openraft takes the
    // single-step path and no joint configuration is ever written.
    assert!(!log.saw_joint(), "unexpected joint configuration: {:?}", log.configs());
}

#[tokio::test(flavor = "multi_thread")]
async fn membership_remove_voter() {
    let mut cluster = TestCluster::start(3, ClusterOpts::default()).await;
    let log = MembershipLog::new();
    for id in 1..=3 {
        log.watch(cluster.node(id));
    }
    cluster.leader().await;
    write_keys(&cluster, "a", 0..20).await;
    let voters = grow(&mut cluster, &[1, 2, 3], 4, &log).await;
    cluster.wait_voters(&voters, &set_of(&voters)).await;
    let mut expected = expect_map("a", 0..20);
    cluster.wait_converged(&voters, &expected).await;

    // Remove a follower (never the leader here).
    let leader = cluster.leader().await;
    let removed = *voters.iter().find(|id| **id != leader).unwrap();
    let remaining: Vec<u64> = voters.iter().copied().filter(|id| *id != removed).collect();
    cluster.remove_voters(&voters, &set_of(&[removed])).await.expect("remove voter");
    cluster.wait_voters(&remaining, &set_of(&remaining)).await;
    for id in &remaining {
        let m = cluster.node(*id).metrics();
        assert!(
            !m.membership_config.nodes().any(|(n, _)| *n == removed),
            "node {id} still lists removed node {removed}: {}",
            m.membership_config
        );
    }

    // The remaining three keep committing; the removed node stops receiving writes.
    write_keys_among(&cluster, &remaining, "b", 0..20).await;
    let before_removal = expected.clone();
    expected.extend(expect_map("b", 0..20));
    cluster.wait_converged(&remaining, &expected).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        dump(&cluster.node(removed).engine()),
        before_removal,
        "the removed node must not see writes made after its removal"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn membership_replace_voters_joint() {
    let mut cluster = TestCluster::start(3, ClusterOpts::default()).await;
    let log = MembershipLog::new();
    for id in 1..=3 {
        log.watch(cluster.node(id));
    }
    cluster.leader().await;
    write_keys(&cluster, "a", 0..20).await;
    let mut acked = expect_map("a", 0..20);
    cluster.wait_converged(&[1, 2, 3], &acked).await;

    // Both new nodes join as learners first, so catch-up is not what the change waits for.
    for id in [4, 5] {
        cluster.start_extra(id).await;
        log.watch(cluster.node(id));
        cluster.add_learner(&[1, 2, 3], id).await.expect("add learner");
    }

    // {1,2,3} -> {3,4,5}: the old quorum {1,2} and the new quorum {4,5} do not intersect.
    let writer = cluster.spawn_writer("bg");
    writer.wait_acked(5).await;
    cluster.change_membership(&[1, 2, 3], &set_of(&[3, 4, 5]), false).await.expect("replace");
    let final_voters = [3u64, 4, 5];
    cluster.wait_voters(&final_voters, &set_of(&final_voters)).await;
    let progress = writer.acked();
    writer.wait_acked(progress + 5).await; // writable through the new voters
    acked.extend(writer.stop().await);

    let joint = log.configs().into_iter().find(|(_, c)| c.len() > 1);
    assert!(
        joint.is_some(),
        "no joint configuration observed replacing {{1,2,3}} with {{3,4,5}}; recorded: {:?}",
        log.configs()
    );
    let (_, joint_cfg) = joint.unwrap();
    assert!(
        joint_cfg.contains(&set_of(&[1, 2, 3])) && joint_cfg.contains(&set_of(&[3, 4, 5])),
        "joint configuration names the wrong voter sets: {joint_cfg:?}"
    );

    write_keys_among(&cluster, &final_voters, "c", 0..20).await;
    acked.extend(expect_map("c", 0..20));
    cluster.assert_contain_and_identical(&final_voters, &acked).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn membership_remove_leader_new_leader_elected() {
    let mut cluster = TestCluster::start(3, ClusterOpts::default()).await;
    let log = MembershipLog::new();
    for id in 1..=3 {
        log.watch(cluster.node(id));
    }
    cluster.leader().await;
    write_keys(&cluster, "a", 0..20).await;
    let voters = grow(&mut cluster, &[1, 2, 3], 4, &log).await;
    cluster.wait_voters(&voters, &set_of(&voters)).await;
    let mut acked = expect_map("a", 0..20);
    cluster.wait_converged(&voters, &acked).await;

    let old_leader = cluster.leader().await;
    let remaining: Vec<u64> = voters.iter().copied().filter(|id| *id != old_leader).collect();
    let writer = cluster.spawn_writer("bg");
    writer.wait_acked(5).await;
    cluster.remove_voters(&voters, &set_of(&[old_leader])).await.expect("remove leader");

    // A new leader among the remaining voters is elected and accepts writes.
    let new_leader = tokio::time::timeout(Duration::from_secs(15), cluster.leader_among(&remaining))
        .await
        .expect("a new leader within 15 s of removing the old one");
    assert_ne!(new_leader, old_leader);
    assert!(remaining.contains(&new_leader));
    let progress = writer.acked();
    writer.wait_acked(progress + 5).await;
    acked.extend(writer.stop().await);
    write_keys_among(&cluster, &remaining, "c", 0..20).await;
    acked.extend(expect_map("c", 0..20));

    cluster.wait_voters(&remaining, &set_of(&remaining)).await;
    cluster.assert_contain_and_identical(&remaining, &acked).await;
}

// ---------------------------------------------------------------------------------------------
// Snapshot catch-up after log purge (plan 03-16).
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn kill_restart_snapshot_catchup() {
    let opts = ClusterOpts {
        use_fake_io: false,
        snapshot_every: 50,
        segment_size: 4096,
        max_in_snapshot_log_to_keep: 10,
        purge_batch_size: 10,
    };
    let mut cluster = TestCluster::start(3, opts).await;
    let leader = cluster.leader().await;
    // Lag a follower and restart the other one, so the leader is never the one taken out.
    let mut followers = (1..=3u64).filter(|id| *id != leader);
    let lagging = followers.next().unwrap();
    let restarted = followers.next().unwrap();
    let up = [leader, restarted];

    write_keys(&cluster, "base", 0..5).await;
    let base = expect_map("base", 0..5);
    cluster.wait_converged(&[1, 2, 3], &base).await;

    // Isolate the lagging follower and hold on to its engine as of now.
    cluster.ctl.isolate(lagging);
    let before = cluster.node(lagging).engine();
    let lagging_last_log = cluster.node(lagging).metrics().last_log_index.expect("has a log");

    write_keys_among(&cluster, &up, "s", 0..300).await;
    let mut expected = base.clone();
    expected.extend(expect_map("s", 0..300));
    cluster.wait_converged(&up, &expected).await;

    // The leader snapshots and purges its log past everything the lagging follower has.
    eventually(Duration::from_secs(30), "leader snapshot at index >= 50 and log purged past the lagging follower", || {
        let m = cluster.node(leader).metrics();
        let snap = m.snapshot.map(|l| l.index);
        let purged = m.purged.map(|l| l.index);
        async move { snap.is_some_and(|i| i >= 50) && purged.is_some_and(|p| p > lagging_last_log) }
    })
    .await;

    // Graceful kill and restart of the other follower: it recovers from its own snapshot plus
    // log and the pair {leader, restarted} keeps committing while the third node is still cut off.
    cluster.kill(restarted).await;
    cluster.restart(restarted).await;
    write_keys_among(&cluster, &up, "t", 0..20).await;
    expected.extend(expect_map("t", 0..20));
    cluster.wait_converged(&up, &expected).await;

    // Heal: the lagging follower can only catch up from a snapshot because the log is purged.
    cluster.ctl.heal();
    cluster.wait_converged(&[lagging], &expected).await; // fresh engine() on every poll
    let after = cluster.node(lagging).engine();
    assert!(
        !Arc::ptr_eq(&before, &after),
        "engine() must follow the install_snapshot swap: the handle is still the pre-heal engine"
    );
    assert_eq!(dump(&before), base, "the pre-heal engine handle must not have gained the new keys");

    let stats = cluster.ctl.stats();
    assert!(stats.snapshot.delivered >= 1, "no snapshot lane delivery: {stats:?}");
    eventually(WAIT, "lagging follower reports an installed snapshot at index >= 50", || {
        let snap = cluster.node(lagging).metrics().snapshot.map(|l| l.index);
        async move { snap.is_some_and(|i| i >= 50) }
    })
    .await;

    // No acknowledged write is missing anywhere and the three nodes are identical.
    cluster.assert_contain_and_identical(&[1, 2, 3], &expected).await;
}
