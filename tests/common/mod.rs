//! Cluster test harness helpers shared by the integration tests (plan 03-13).
//!
//! Each test target compiles this module separately, so unused helpers are expected.
#![allow(dead_code)]

use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use lightcycle::raft::config::NodeConfig;
use lightcycle::raft::node::RaftNode;

/// A configuration with fast timers for tests: election 100-200 ms, heartbeat 20 ms, pre-vote on
/// and the default snapshot interval. `bootstrap` is off; tests designate the bootstrap node.
pub fn fast_config(node_id: u64, dir: &Path) -> NodeConfig {
    let mut cfg = NodeConfig::new(node_id, dir);
    cfg.election_timeout_min_ms = 100;
    cfg.election_timeout_max_ms = 200;
    cfg.heartbeat_interval_ms = 20;
    cfg.enable_pre_vote = true;
    cfg
}

/// Poll `f` every 10 ms until it returns true; panics with `what` after `timeout`.
pub async fn eventually<F, Fut>(timeout: Duration, what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out after {timeout:?} waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until every node reports the same leader (one of `nodes`) and return its id.
pub async fn wait_for_leader(nodes: &[&RaftNode], timeout: Duration) -> u64 {
    let ids: Vec<u64> = nodes.iter().map(|n| n.node_id()).collect();
    let mut found = 0;
    eventually(timeout, "all nodes agree on a leader", || {
        let leaders: Vec<Option<u64>> =
            nodes.iter().map(|n| n.metrics().current_leader).collect();
        let agreed = match leaders.first().copied().flatten() {
            Some(l) if ids.contains(&l) && leaders.iter().all(|x| *x == Some(l)) => Some(l),
            _ => None,
        };
        if let Some(l) = agreed {
            found = l;
        }
        async move { agreed.is_some() }
    })
    .await;
    found
}

// ---------------------------------------------------------------------------------------------
// In-process multi-node cluster over the fake network (plan 03-14).
// ---------------------------------------------------------------------------------------------

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use lightcycle::engine::LocalEngine;
use lightcycle::net::fake::NetController;
use lightcycle::raft::node::NodeError;
use lightcycle::raft::types::{KvRequest, KvResponse};
use lightcycle::wal::io::FakeIo;
use openraft::BasicNode;
use tempfile::TempDir;

/// A `Set` request from string slices.
pub fn set_req(key: &str, value: &str) -> KvRequest {
    KvRequest::Set {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
    }
}

/// Every live (non-tombstone) key and value of `engine`.
pub fn dump(engine: &LocalEngine) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    engine.scan_versioned(0, |e| {
        if let Some(v) = e.value {
            out.insert(e.key.to_vec(), v.to_vec());
        }
    });
    out
}

/// Options for [`TestCluster::start`].
#[derive(Debug, Clone)]
pub struct ClusterOpts {
    /// Back every node's WAL with a [`FakeIo`] (needed for crash simulation).
    pub use_fake_io: bool,
    /// Applied entries between automatic snapshots.
    pub snapshot_every: u64,
    /// WAL segment size in bytes. Kept small so FakeIo images stay cheap to copy and compare.
    pub segment_size: u64,
    /// Log entries kept behind a snapshot before purging.
    pub max_in_snapshot_log_to_keep: u64,
    /// Purge granularity in entries.
    pub purge_batch_size: u64,
}

impl Default for ClusterOpts {
    fn default() -> Self {
        ClusterOpts {
            use_fake_io: false,
            snapshot_every: 1_000_000,
            segment_size: 256 * 1024,
            max_in_snapshot_log_to_keep: 1000,
            purge_batch_size: 65_536,
        }
    }
}

/// An in-process cluster: one tempdir per node, one shared [`NetController`], optional per-node
/// [`FakeIo`]. Node 1 bootstraps the full member map.
pub struct TestCluster {
    /// The shared fake network (isolate, heal, stats).
    pub ctl: NetController,
    nodes: BTreeMap<u64, RaftNode>,
    dirs: BTreeMap<u64, TempDir>,
    ios: BTreeMap<u64, FakeIo>,
    cfgs: BTreeMap<u64, NodeConfig>,
    members: BTreeMap<u64, BasicNode>,
}

impl TestCluster {
    /// Start `n` nodes (ids `1..=n`), register them on one fake network and bootstrap from node 1.
    pub async fn start(n: usize, opts: ClusterOpts) -> TestCluster {
        let members: BTreeMap<u64, BasicNode> =
            (1..=n as u64).map(|id| (id, BasicNode { addr: format!("fake-{id}") })).collect();
        let mut cluster = TestCluster {
            ctl: NetController::new(),
            nodes: BTreeMap::new(),
            dirs: BTreeMap::new(),
            ios: BTreeMap::new(),
            cfgs: BTreeMap::new(),
            members,
        };
        for id in 1..=n as u64 {
            let dir = tempfile::tempdir().expect("tempdir");
            let mut cfg = fast_config(id, dir.path());
            cfg.bootstrap = id == 1;
            cfg.snapshot_every_entries = opts.snapshot_every;
            cfg.max_in_snapshot_log_to_keep = opts.max_in_snapshot_log_to_keep;
            cfg.purge_batch_size = opts.purge_batch_size;
            cfg.wal.segment_size = opts.segment_size;
            cluster.dirs.insert(id, dir);
            cluster.cfgs.insert(id, cfg);
            if opts.use_fake_io {
                cluster.ios.insert(id, FakeIo::new());
            }
            cluster.start_node(id).await;
        }
        let members = cluster.members.clone();
        cluster.node(1).bootstrap_if_needed(members).await.expect("bootstrap node 1");
        cluster
    }

    /// Start (or restart) node `id` from its stored config and storage, and register it.
    async fn start_node(&mut self, id: u64) {
        let cfg = self.cfgs[&id].clone();
        let factory = self.ctl.factory(id);
        let node = match self.ios.get(&id) {
            Some(io) => RaftNode::start_with_io(cfg.clone(), factory, Box::new(io.clone())).await,
            None => RaftNode::start(cfg.clone(), factory).await,
        }
        .unwrap_or_else(|e| panic!("node {id} failed to start: {e}"));
        self.ctl.register(id, Arc::new(node.raft().clone()), cfg.recv_dir());
        self.nodes.insert(id, node);
    }

    /// Ids of the nodes that are currently running.
    pub fn live_ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    /// A running node. Panics if `id` is stopped.
    pub fn node(&self, id: u64) -> &RaftNode {
        self.nodes.get(&id).unwrap_or_else(|| panic!("node {id} is not running"))
    }

    /// A clone of node `id`'s FakeIo handle. Panics without `use_fake_io`.
    pub fn fake_io(&self, id: u64) -> FakeIo {
        self.ios.get(&id).unwrap_or_else(|| panic!("cluster started without use_fake_io")).clone()
    }

    /// Wait until every node in `ids` reports the same leader, which is itself one of `ids`.
    pub async fn leader_among(&self, ids: &[u64]) -> u64 {
        let nodes: Vec<&RaftNode> = ids.iter().map(|id| self.node(*id)).collect();
        wait_for_leader(&nodes, Duration::from_secs(10)).await
    }

    /// Wait until all running nodes agree on a running leader.
    pub async fn leader(&self) -> u64 {
        self.leader_among(&self.live_ids()).await
    }

    /// Write through the current leader, retrying while leadership moves.
    pub async fn write_via_leader(&self, req: KvRequest) -> Result<KvResponse, NodeError> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let leader = self.leader().await;
            match self.node(leader).write(req.clone()).await {
                Err(NodeError::NotLeader { .. }) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                other => return other,
            }
        }
    }

    /// Stop node `id` and forget it (the caller has already decided how its storage ends).
    async fn stop_node(&mut self, id: u64) {
        self.ctl.unregister(id);
        if let Some(node) = self.nodes.remove(&id) {
            // A crashed node's core may report a fatal storage error on the way down; the bound
            // keeps a wedged shutdown from hanging the test.
            let _ = tokio::time::timeout(Duration::from_secs(10), node.shutdown()).await;
        }
    }

    /// Graceful shutdown.
    pub async fn kill(&mut self, id: u64) {
        self.stop_node(id).await;
    }

    /// Simulate a crash: the FakeIo loses power first (the still-running writer thread can no
    /// longer persist anything, like a killed process), keeping `keep` unsynced bytes as a torn
    /// write, then the node is shut down.
    pub async fn crash(&mut self, id: u64, keep: usize) {
        self.fake_io(id).crash(keep);
        self.stop_node(id).await;
    }

    /// Delay the node's fsync by 250 ms, wait until more than `keep` bytes are genuinely
    /// unsynced (a record is half-persisted), then [`crash`](Self::crash) it. The caller keeps
    /// writes in flight through a background task so there is something to tear.
    pub async fn crash_with_unsynced_tail(&mut self, id: u64, keep: usize) {
        let io = self.fake_io(id);
        io.set_sync_delay(Duration::from_millis(250));
        eventually(Duration::from_secs(10), "unsynced bytes beyond the keep budget", || {
            let unsynced = io.unsynced_bytes();
            async move { unsynced > keep }
        })
        .await;
        self.crash(id, keep).await;
    }

    /// Restart a stopped node from the same directory and FakeIo state.
    pub async fn restart(&mut self, id: u64) {
        assert!(!self.nodes.contains_key(&id), "node {id} is still running");
        if let Some(io) = self.ios.get(&id) {
            io.revive();
            io.set_sync_delay(Duration::ZERO);
        }
        self.start_node(id).await;
    }

    /// Wait until every node in `ids` holds exactly `expect`. The engine is re-read on every poll
    /// because installing a snapshot replaces it.
    pub async fn wait_converged(&self, ids: &[u64], expect: &BTreeMap<Vec<u8>, Vec<u8>>) {
        eventually(Duration::from_secs(15), "nodes converge to the expected contents", || {
            let ok = ids.iter().all(|id| &dump(&self.node(*id).engine()) == expect);
            async move { ok }
        })
        .await;
    }

    /// Wait until every node in `ids` contains every `acked` pair and all of them are identical.
    pub async fn assert_contain_and_identical(
        &self,
        ids: &[u64],
        acked: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let mut last = String::new();
        let mut poll = || {
            let dumps: Vec<BTreeMap<Vec<u8>, Vec<u8>>> =
                ids.iter().map(|id| dump(&self.node(*id).engine())).collect();
            let contains = dumps.iter().all(|d| acked.iter().all(|(k, v)| d.get(k) == Some(v)));
            let identical = dumps.windows(2).all(|w| w[0] == w[1]);
            last = format!(
                "contains={contains} identical={identical} sizes={:?} acked={}",
                dumps.iter().map(|d| d.len()).collect::<Vec<_>>(),
                acked.len()
            );
            contains && identical
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while !poll() {
            assert!(Instant::now() < deadline, "nodes did not converge: {last}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Start a background task that keeps writing `prefix-NNNNNN` keys through whichever live
    /// node accepts them, recording only acknowledged writes. Handles are captured now, so nodes
    /// stopped later simply fail fast and the task moves on to the others.
    pub fn spawn_writer(&self, prefix: &str) -> BgWriter {
        let handles: Vec<_> = self.nodes.values().map(|n| n.raft().clone()).collect();
        let stop = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let prefix = prefix.to_string();
        let task = {
            let (stop, count) = (stop.clone(), count.clone());
            tokio::spawn(async move {
                let mut acked = BTreeMap::new();
                let mut preferred = 0usize;
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("{prefix}-{n:06}");
                    let value = format!("val-{n}");
                    let mut done = false;
                    for attempt in 0..handles.len() {
                        let i = (preferred + attempt) % handles.len();
                        let req = set_req(&key, &value);
                        let res = tokio::time::timeout(
                            Duration::from_secs(3),
                            handles[i].client_write(req),
                        )
                        .await;
                        if matches!(res, Ok(Ok(_))) {
                            preferred = i;
                            done = true;
                            break;
                        }
                    }
                    if done {
                        acked.insert(key.into_bytes(), value.into_bytes());
                        count.fetch_add(1, Ordering::Relaxed);
                        n += 1;
                    } else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                acked
            })
        };
        BgWriter { stop, count, task }
    }
}

/// Handle to a [`TestCluster::spawn_writer`] task.
pub struct BgWriter {
    stop: Arc<AtomicBool>,
    count: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl BgWriter {
    /// Number of writes acknowledged so far.
    pub fn acked(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Wait until at least `n` writes have been acknowledged in total.
    pub async fn wait_acked(&self, n: usize) {
        eventually(Duration::from_secs(20), "background writer acknowledged writes", || {
            let c = self.acked();
            async move { c >= n }
        })
        .await;
    }

    /// Stop the task and return every acknowledged pair.
    pub async fn stop(self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        self.stop.store(true, Ordering::Relaxed);
        self.task.await.expect("background writer task")
    }
}
