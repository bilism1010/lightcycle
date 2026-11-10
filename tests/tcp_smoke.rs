//! Real-transport acceptance (plan 03-15): full Raft nodes with on-disk WALs talking over
//! loopback TCP through the actual frame codec, Hello handshake and three lanes.
//!
//! The fake network proves consensus logic; these tests prove the real wire path works with a
//! real Raft core. Servers are bound before the nodes start so the membership carries real
//! addresses; a `DeferredHandler` lets every server bind up front and have its Raft handle set
//! afterwards.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use common::{eventually, fast_config, wait_for_leader};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use lightcycle::net::client::{TcpNetConfig, TcpNetworkFactory};
use lightcycle::net::handler::RaftHandler;
use lightcycle::net::server::PeerServer;
use lightcycle::raft::config::NodeConfig;
use lightcycle::raft::node::{NodeError, RaftNode};
use lightcycle::raft::types::{KvRequest, KvResponse, SnapshotFile, TypeConfig};
use lightcycle::wire::frame::Lane;
use openraft::errors::{Fatal, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, VoteOf};
use openraft::BasicNode;

const CLUSTER_ID: u64 = 7;
const WAIT: Duration = Duration::from_secs(20);

/// A `RaftHandler` whose target is supplied after construction. Before it is set every call
/// fails like a stopped node, so a peer that dials early sees a retryable error.
#[derive(Default)]
struct DeferredHandler {
    inner: OnceLock<Arc<dyn RaftHandler>>,
}

impl DeferredHandler {
    fn set(&self, handler: Arc<dyn RaftHandler>) {
        assert!(self.inner.set(handler).is_ok(), "handler set twice");
    }
}

impl RaftHandler for DeferredHandler {
    fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<AppendResult<TypeConfig>, RaftError<TypeConfig>>> {
        match self.inner.get() {
            Some(h) => h.append_entries(req),
            None => Box::pin(async { Err(RaftError::Fatal(Fatal::Stopped)) }),
        }
    }

    fn stream_append(
        &self,
        reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
    ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>> {
        match self.inner.get() {
            Some(h) => h.stream_append(reqs),
            None => futures::stream::once(async { Err(Fatal::Stopped) }).boxed(),
        }
    }

    fn vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
        match self.inner.get() {
            Some(h) => h.vote(req),
            None => Box::pin(async { Err(RaftError::Fatal(Fatal::Stopped)) }),
        }
    }

    fn pre_vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
        match self.inner.get() {
            Some(h) => h.pre_vote(req),
            None => Box::pin(async { Err(RaftError::Fatal(Fatal::Stopped)) }),
        }
    }

    fn install_snapshot(
        &self,
        vote: VoteOf<TypeConfig>,
        meta: SnapshotMetaOf<TypeConfig>,
        file: SnapshotFile,
    ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>> {
        match self.inner.get() {
            // `RaftNode::start` empties the receive directory before this handler is set. This
            // test binds the server first, so a transfer that began while the node was starting
            // can have its file deleted by that cleanup. The handler is only visible after the
            // cleanup finished, so checking here is race-free: refuse such a transfer like a
            // not-yet-started node, and the leader simply retries the snapshot.
            Some(_) if !file.path.exists() => Box::pin(async { Err(Fatal::Stopped) }),
            Some(h) => h.install_snapshot(vote, meta, file),
            None => Box::pin(async { Err(Fatal::Stopped) }),
        }
    }

    fn validate_snapshot(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        file: &SnapshotFile,
    ) -> BoxFuture<'static, Result<(), String>> {
        match self.inner.get() {
            // The same startup race `install_snapshot` guards against: a receive file deleted by
            // the node's own recv-dir cleanup is answered there with the retryable "stopped".
            Some(_) if !file.path.exists() => Box::pin(async { Ok(()) }),
            Some(h) => h.validate_snapshot(meta, file),
            // Not started: let `install_snapshot` answer with its retryable "stopped" failure.
            None => Box::pin(async { Ok(()) }),
        }
    }
}

/// One node of the test cluster: its directory, bound server and (once started) Raft node.
struct TestNode {
    cfg: NodeConfig,
    server: PeerServer,
    deferred: Arc<DeferredHandler>,
    node: Option<RaftNode>,
    _dir: tempfile::TempDir,
}

impl TestNode {
    /// Create a fresh data directory and bind a server on an ephemeral loopback port. The Raft
    /// node is not started yet.
    async fn bind(node_id: u64, tune: impl Fn(&mut NodeConfig)) -> TestNode {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = fast_config(node_id, dir.path());
        tune(&mut cfg);
        let deferred = Arc::new(DeferredHandler::default());
        let server = PeerServer::bind(
            "127.0.0.1:0",
            TcpNetConfig::new(CLUSTER_ID, node_id, cfg.recv_dir()),
            deferred.clone(),
        )
        .await
        .expect("bind peer server");
        TestNode { cfg, server, deferred, node: None, _dir: dir }
    }

    fn addr(&self) -> String {
        self.server.local_addr().to_string()
    }

    /// Start the Raft node over real TCP and route the server's inbound RPCs to it.
    async fn start(&mut self) {
        let net = TcpNetworkFactory::new(TcpNetConfig::new(
            CLUSTER_ID,
            self.cfg.node_id,
            self.cfg.recv_dir(),
        ));
        let node = RaftNode::start(self.cfg.clone(), net).await.expect("node starts");
        self.deferred.set(Arc::new(node.raft().clone()));
        self.node = Some(node);
    }

    fn node(&self) -> &RaftNode {
        self.node.as_ref().expect("node started")
    }

    async fn shutdown(self) {
        self.server.shutdown().await;
        if let Some(node) = self.node {
            node.shutdown().await.unwrap();
        }
    }
}

fn members(nodes: &[&TestNode]) -> BTreeMap<u64, BasicNode> {
    nodes.iter().map(|n| (n.cfg.node_id, BasicNode { addr: n.addr() })).collect()
}

fn key(i: usize) -> String {
    format!("key-{i:04}")
}

fn val(i: usize) -> String {
    format!("val-{i:04}")
}

fn set(i: usize) -> KvRequest {
    KvRequest::Set {
        key: Bytes::copy_from_slice(key(i).as_bytes()),
        value: Bytes::copy_from_slice(val(i).as_bytes()),
    }
}

/// Commit one write through whichever of `nodes` is the leader, retrying while an election is in
/// progress. Under load a debug-build node can miss a heartbeat and trigger a legitimate
/// leadership change mid-test; `Set` is idempotent, so retrying an ambiguous NotLeader is safe.
async fn write_anywhere(nodes: &[&RaftNode], req: KvRequest) {
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        for n in nodes {
            match n.write(req.clone()).await {
                Ok(resp) => {
                    assert_eq!(resp, KvResponse::Ok);
                    return;
                }
                Err(NodeError::NotLeader { .. }) => {}
                Err(e) => panic!("write failed on node {}: {e}", n.node_id()),
            }
        }
        assert!(std::time::Instant::now() < deadline, "no node accepted the write as leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Write keys `range`, 25 at a time concurrently so group commit amortizes the per-write fsync of
/// the default `Always` mode.
async fn write_range(nodes: &[&RaftNode], range: std::ops::Range<usize>) {
    let keys: Vec<usize> = range.collect();
    for batch in keys.chunks(25) {
        futures::future::join_all(batch.iter().map(|&i| write_anywhere(nodes, set(i)))).await;
    }
}

/// True when `node`'s engine, loaded afresh, holds every key in `range`.
fn holds_all(node: &RaftNode, range: std::ops::Range<usize>) -> bool {
    // A new `engine()` call on every poll: install_snapshot swaps the engine.
    let engine = node.engine();
    range.into_iter().all(|i| engine.get(key(i).as_bytes()).as_deref() == Some(val(i).as_bytes()))
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_over_tcp_elect_and_replicate() {
    const KEYS: usize = 50;
    let mut n1 = TestNode::bind(1, |c| c.bootstrap = true).await;
    let mut n2 = TestNode::bind(2, |_| {}).await;
    let mut n3 = TestNode::bind(3, |_| {}).await;
    for n in [&mut n1, &mut n2, &mut n3] {
        n.start().await;
    }

    let map = members(&[&n1, &n2, &n3]);
    assert!(n1.node().bootstrap_if_needed(map.clone()).await.unwrap(), "node 1 initializes");
    for n in [&n2, &n3] {
        assert!(!n.node().bootstrap_if_needed(map.clone()).await.unwrap(), "only node 1 bootstraps");
    }

    let all = [n1.node(), n2.node(), n3.node()];
    let first_leader = wait_for_leader(&all, WAIT).await;

    write_range(&all, 0..KEYS).await;
    // Replication has completed, so the leader that acknowledged the writes is known.
    let leader_id = all
        .iter()
        .find_map(|n| n.metrics().current_leader)
        .unwrap_or(first_leader);

    for n in all {
        eventually(WAIT, &format!("node {} holds all {KEYS} keys", n.node_id()), || {
            let ok = holds_all(n, 0..KEYS);
            async move { ok }
        })
        .await;
    }

    // The real wire path was exercised on the lanes that matter, without protocol errors.
    let servers = [&n1.server, &n2.server, &n3.server];
    let stats: Vec<_> = servers.iter().map(|s| s.stats()).collect();
    let accepted = |lane| stats.iter().map(|s| s.accepted(lane)).sum::<u64>();
    assert!(accepted(Lane::Replication) > 0, "replication lane connections were accepted");
    assert!(accepted(Lane::Heartbeat) > 0, "heartbeat lane connections were accepted");
    for (i, s) in stats.iter().enumerate() {
        let id = (i + 1) as u64;
        assert_eq!(s.decode_errors.load(Ordering::Relaxed), 0, "node {id} decode errors");
        assert_eq!(s.handshake_failures.load(Ordering::Relaxed), 0, "node {id} handshake failures");
        if id != leader_id {
            // Replication-lane frames only: heartbeat-lane frames are also counted in
            // `append_req_frames` and would satisfy the check without any log entry travelling.
            assert!(
                s.append_frames(Lane::Replication) > 0,
                "follower {id} received AppendEntries frames on the replication lane over TCP"
            );
        }
    }

    for n in [n1, n2, n3] {
        n.shutdown().await;
    }
}

/// Settings that make a 300-write run snapshot and purge quickly (as in tests/cluster.rs).
fn snapshot_tuning(cfg: &mut NodeConfig) {
    cfg.snapshot_every_entries = 50;
    cfg.max_in_snapshot_log_to_keep = 10;
    cfg.purge_batch_size = 10;
    cfg.wal.segment_size = 4096;
}

/// File names in `dir` ending in `suffix`, sorted (empty when the directory does not exist).
fn names_with_suffix(dir: &std::path::Path, suffix: &str) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(suffix))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// `recv-<digits>.tmp`
fn is_recv_tmp_name(name: &str) -> bool {
    name.strip_prefix("recv-")
        .and_then(|r| r.strip_suffix(".tmp"))
        .is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_snapshot_lane_install() {
    const KEYS: usize = 300;
    let mut n1 = TestNode::bind(1, |c| {
        c.bootstrap = true;
        snapshot_tuning(c);
    })
    .await;
    let mut n2 = TestNode::bind(2, snapshot_tuning).await;
    // Node 3's server is bound (its address is in the membership) but its Raft node is not
    // started: the other two commit as a majority while it is absent.
    let mut n3 = TestNode::bind(3, snapshot_tuning).await;
    n1.start().await;
    n2.start().await;

    let map = members(&[&n1, &n2, &n3]);
    assert!(n1.node().bootstrap_if_needed(map).await.unwrap());
    let pair = [n1.node(), n2.node()];
    wait_for_leader(&pair, WAIT).await;

    write_range(&pair, 0..KEYS).await;

    // Wait until both running nodes have snapshotted and purged the head of their logs (so
    // whichever is leader, node 3 can no longer be caught up from the log).
    eventually(Duration::from_secs(30), "both running nodes snapshotted and purged", || {
        let ok = pair.iter().all(|n| {
            let m = n.metrics();
            m.snapshot.is_some_and(|l| l.index >= 50) && m.purged.is_some_and(|l| l.index >= 1)
        });
        async move { ok }
    })
    .await;

    // Watch node 3's receive directory from a plain thread: the transfer is only milliseconds
    // long, so a task on the test runtime could miss the window.
    let recv_dir = n3.cfg.recv_dir();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watcher = {
        let (recv_dir, stop) = (recv_dir.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut seen = std::collections::BTreeSet::new();
            while !stop.load(Ordering::Relaxed) {
                if let Ok(rd) = std::fs::read_dir(&recv_dir) {
                    for e in rd.filter_map(|e| e.ok()) {
                        seen.insert(e.file_name().to_string_lossy().into_owned());
                    }
                }
                std::thread::sleep(Duration::from_micros(200));
            }
            seen
        })
    };

    // Fresh directory, node starts now and must be brought up to date by a snapshot.
    n3.start().await;
    let stale_engine = n3.node().engine();

    eventually(Duration::from_secs(60), "node 3 converges to all 300 keys", || {
        let ok = holds_all(n3.node(), 0..KEYS);
        async move { ok }
    })
    .await;

    stop.store(true, Ordering::Relaxed);
    let seen = watcher.join().unwrap();
    // Best effort, and it can pass with no sightings at all: the transfer is milliseconds long.
    // It only checks that any file that WAS seen has a well-formed receive name. The proof that
    // the snapshot travelled over the snapshot lane is the chunk and lane counters below, and
    // the proof of cleanup is the "no recv leftovers" wait at the end.
    for name in &seen {
        assert!(is_recv_tmp_name(name), "unexpected file in recv dir: {name}");
    }

    // The engine the node held before the snapshot arrived was replaced, not mutated in place.
    assert!(
        !Arc::ptr_eq(&stale_engine, &n3.node().engine()),
        "install_snapshot swapped the engine; the pre-install Arc is stale"
    );
    assert!(stale_engine.get(key(KEYS - 1).as_bytes()).is_none(), "stale engine never saw the data");

    // The snapshot really travelled over the snapshot lane.
    let s3 = n3.server.stats();
    assert!(s3.snapshot_chunks.load(Ordering::Relaxed) > 0, "snapshot chunks received on node 3");
    assert!(s3.accepted(Lane::Snapshot) > 0, "a snapshot lane connection reached node 3");
    assert_eq!(s3.decode_errors.load(Ordering::Relaxed), 0);
    assert_eq!(s3.handshake_failures.load(Ordering::Relaxed), 0);

    // Nothing left behind: exactly one installed snapshot, no leftover receive files.
    eventually(WAIT, "node 3 holds exactly one .snap and no recv leftovers", || {
        let snaps = names_with_suffix(&n3.cfg.snapshot_dir(), ".snap");
        let tmps = names_with_suffix(&recv_dir, ".tmp");
        async move { snaps.len() == 1 && tmps.is_empty() }
    })
    .await;

    for n in [n1, n2, n3] {
        n.shutdown().await;
    }
}

/// A peer that streams a garbage snapshot is refused at the transport with a non-fatal failure.
/// Handed to openraft it would be a state machine error, which stops the receiving node, and the
/// peer port is unauthenticated.
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_snapshot_from_a_peer_does_not_stop_the_node() {
    use openraft::errors::StreamingError;
    use openraft::network::RPCOption;
    use openraft::{RaftNetworkFactory, RaftNetworkV2};

    let mut n1 = TestNode::bind(1, |c| c.bootstrap = true).await;
    n1.start().await;
    assert!(n1.node().bootstrap_if_needed(members(&[&n1])).await.unwrap());
    wait_for_leader(&[n1.node()], WAIT).await;
    write_anywhere(&[n1.node()], set(0)).await;

    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path().join("garbage.snap");
    std::fs::write(&path, vec![0xA5u8; 4096]).unwrap();
    let snapshot = openraft::Snapshot {
        meta: openraft::SnapshotMeta {
            last_log_id: Some(openraft::LogId::new_term_index(1, 5)),
            last_membership: Default::default(),
        },
        snapshot: SnapshotFile { path, len: 4096 },
    };
    // A vote above the node's own: without the transport check this reaches the state machine.
    let vote = VoteOf::<TypeConfig>::new_committed(1000, 2);

    let mut factory = TcpNetworkFactory::new(TcpNetConfig::new(
        CLUSTER_ID,
        2,
        scratch.path().join("recv"),
    ));
    let mut peer = factory.new_snapshot_client(1, &BasicNode { addr: n1.addr() }).await;
    let outcome = peer
        .full_snapshot(
            vote,
            snapshot,
            futures::future::pending::<openraft::errors::ReplicationClosed>(),
            RPCOption::new(Duration::from_secs(4)),
        )
        .await;
    assert!(
        matches!(outcome, Err(StreamingError::Network(_))),
        "the corrupt snapshot is refused: {outcome:?}"
    );

    // The node is still running and still takes writes.
    write_anywhere(&[n1.node()], set(1)).await;
    assert!(holds_all(n1.node(), 0..2));
    n1.shutdown().await;
}
