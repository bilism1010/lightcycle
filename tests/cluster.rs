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
