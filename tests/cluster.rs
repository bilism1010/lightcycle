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
