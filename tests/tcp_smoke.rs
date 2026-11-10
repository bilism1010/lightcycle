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
use lightcycle::raft::node::RaftNode;
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
            Some(h) => h.install_snapshot(vote, meta, file),
            None => Box::pin(async { Err(Fatal::Stopped) }),
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
        cfg.bind = server.local_addr().to_string();
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

/// Write keys `range` through `leader`, 25 at a time concurrently so group commit amortizes the
/// per-write fsync of the default `Always` mode.
async fn write_range(leader: &RaftNode, range: std::ops::Range<usize>) {
    let keys: Vec<usize> = range.collect();
    for batch in keys.chunks(25) {
        let results = futures::future::join_all(batch.iter().map(|&i| leader.write(set(i)))).await;
        for r in results {
            assert_eq!(r.expect("write commits"), KvResponse::Ok);
        }
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
    let leader_id = wait_for_leader(&all, WAIT).await;
    let leader = all.iter().find(|n| n.node_id() == leader_id).unwrap();

    write_range(leader, 0..KEYS).await;

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
            assert!(
                s.append_req_frames.load(Ordering::Relaxed) > 0,
                "follower {id} received AppendEntries frames over TCP"
            );
        }
    }

    for n in [n1, n2, n3] {
        n.shutdown().await;
    }
}
