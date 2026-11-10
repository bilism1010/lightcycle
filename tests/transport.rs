//! Raft peer transport over real loopback TCP, driven against a stub `RaftHandler` (plan 03-09).
//!
//! The transport is proven here before a Raft node exists; the real-node smoke test is plan
//! 03-15. Tests use real time with short timeouts; nothing here depends on wall-clock precision
//! beyond generous slack.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use lightcycle::net::client::{PeerClient, TcpNetConfig, TcpNetworkFactory};
use lightcycle::net::handler::RaftHandler;
use lightcycle::net::server::PeerServer;
use lightcycle::raft::types::{SnapshotFile, TypeConfig};
use lightcycle::wire::frame::Lane;
use openraft::errors::{Fatal, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, VoteOf};
use openraft::{BasicNode, RaftNetworkFactory, RaftNetworkV2};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

const CLUSTER: u64 = 7;
const SERVER_NODE: u64 = 2;
const CLIENT_NODE: u64 = 1;

/// Records calls; answers votes with a grant unless told to fail.
#[derive(Default)]
struct StubHandler {
    votes: AtomicUsize,
    pre_votes: AtomicUsize,
    fail_votes: AtomicBool,
}

impl RaftHandler for StubHandler {
    fn append_entries(
        &self,
        _req: AppendEntriesRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<AppendResult<TypeConfig>, RaftError<TypeConfig>>> {
        Box::pin(async { Err(RaftError::Fatal(Fatal::Stopped)) })
    }

    fn stream_append(
        &self,
        _reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
    ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>> {
        futures::stream::empty().boxed()
    }

    fn vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
        self.votes.fetch_add(1, Ordering::SeqCst);
        let fail = self.fail_votes.load(Ordering::SeqCst);
        Box::pin(async move {
            if fail {
                Err(RaftError::Fatal(Fatal::Stopped))
            } else {
                Ok(VoteResponse::new(req.vote, None, true))
            }
        })
    }

    fn pre_vote(
        &self,
        req: VoteRequest<TypeConfig>,
    ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
        self.pre_votes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(VoteResponse::new(req.vote, None, true)) })
    }

    fn install_snapshot(
        &self,
        vote: VoteOf<TypeConfig>,
        _meta: SnapshotMetaOf<TypeConfig>,
        _file: SnapshotFile,
    ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>> {
        Box::pin(async move { Ok(SnapshotResponse::new(vote)) })
    }
}

fn config(cluster: u64, node: u64) -> TcpNetConfig {
    TcpNetConfig::new(cluster, node, std::env::temp_dir().join("lightcycle-transport-test"))
}

async fn start_server(
    handler: Arc<dyn RaftHandler>,
    tweak: impl FnOnce(&mut TcpNetConfig),
) -> PeerServer {
    let mut cfg = config(CLUSTER, SERVER_NODE);
    tweak(&mut cfg);
    PeerServer::bind("127.0.0.1:0", cfg, handler)
        .await
        .expect("bind loopback")
}

async fn client(server: &PeerServer, cfg: TcpNetConfig, lane: Lane) -> PeerClient {
    let node = BasicNode::new(server.local_addr().to_string());
    let mut factory = TcpNetworkFactory::new(cfg);
    match lane {
        Lane::Replication => factory.new_client(SERVER_NODE, &node).await,
        Lane::Heartbeat => factory.new_heartbeat_client(SERVER_NODE, &node).await,
        Lane::Snapshot => factory.new_snapshot_client(SERVER_NODE, &node).await,
    }
}

fn vote_req() -> VoteRequest<TypeConfig> {
    VoteRequest::new(VoteOf::<TypeConfig>::new(2, 1), None)
}

fn opt() -> RPCOption {
    RPCOption::new(Duration::from_secs(4))
}

/// Poll `cond` until it holds or two seconds pass.
async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn vote_roundtrip_over_tcp() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;

    let resp = peer.vote(vote_req(), opt()).await.expect("vote over tcp");
    assert!(resp.vote_granted);
    assert_eq!(resp.vote, VoteOf::<TypeConfig>::new(2, 1));
    assert_eq!(stub.votes.load(Ordering::SeqCst), 1);

    // The connection is reused for a second call.
    peer.vote(vote_req(), opt()).await.expect("second vote");
    assert_eq!(stub.votes.load(Ordering::SeqCst), 2);
    assert_eq!(server.stats().accepted(Lane::Replication), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn prevote_and_vote_work_on_every_voting_lane() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    let stats = server.stats();

    for lane in [Lane::Replication, Lane::Heartbeat] {
        let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), lane).await;
        assert!(peer.vote(vote_req(), opt()).await.unwrap().vote_granted);
        assert!(peer.pre_vote(vote_req(), opt()).await.unwrap().vote_granted);
    }
    assert_eq!(stub.votes.load(Ordering::SeqCst), 2);
    assert_eq!(stub.pre_votes.load(Ordering::SeqCst), 2);
    // Each lane dialed its own connection, tagged by lane.
    assert_eq!(stats.accepted(Lane::Replication), 1);
    assert_eq!(stats.accepted(Lane::Heartbeat), 1);
    assert_eq!(stats.accepted(Lane::Snapshot), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn handler_error_is_network_and_keeps_connection() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;

    stub.fail_votes.store(true, Ordering::SeqCst);
    let err = peer.vote(vote_req(), opt()).await.expect_err("handler fails");
    assert!(matches!(err, RPCError::Network(_)), "got {err:?}");

    stub.fail_votes.store(false, Ordering::SeqCst);
    peer.vote(vote_req(), opt()).await.expect("connection still usable");
    assert_eq!(server.stats().accepted(Lane::Replication), 1, "no reconnect");
    server.shutdown().await;
}

#[tokio::test]
async fn hello_cluster_mismatch_rejected_over_tcp() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    let mut peer = client(&server, config(CLUSTER + 1, CLIENT_NODE), Lane::Replication).await;

    let err = peer.vote(vote_req(), opt()).await.expect_err("must be rejected");
    assert!(
        matches!(err, RPCError::Unreachable(_)),
        "a rejected Hello backs off, got {err:?}"
    );
    let stats = server.stats();
    eventually("handshake failure to be counted", || {
        stats.handshake_failures.load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(stub.votes.load(Ordering::SeqCst), 0);
    assert_eq!(stats.accepted(Lane::Replication), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn handshake_timeout_closes_silent_connection() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |cfg| {
        cfg.handshake_timeout = Duration::from_millis(200);
    })
    .await;

    let mut raw = TcpStream::connect(server.local_addr()).await.unwrap();
    let started = Instant::now();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(3), raw.read(&mut buf))
        .await
        .expect("server must close the silent connection")
        .expect("clean close");
    assert_eq!(n, 0, "EOF expected, server sends nothing");
    assert!(
        started.elapsed() < Duration::from_millis(1500),
        "closed after {:?}",
        started.elapsed()
    );
    assert_eq!(server.stats().handshake_failures.load(Ordering::Relaxed), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn connect_refused_is_unreachable() {
    // Grab a free port, then free it again so nothing listens there.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mut factory = TcpNetworkFactory::new(config(CLUSTER, CLIENT_NODE));
    let mut peer = factory
        .new_client(SERVER_NODE, &BasicNode::new(addr.to_string()))
        .await;
    let err = peer.vote(vote_req(), opt()).await.expect_err("nothing listens");
    assert!(matches!(err, RPCError::Unreachable(_)), "got {err:?}");
}

#[tokio::test]
async fn client_reconnects_after_server_restart_on_same_connection_object() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    let addr = server.local_addr();
    let mut factory = TcpNetworkFactory::new(config(CLUSTER, CLIENT_NODE));
    let mut peer = factory
        .new_client(SERVER_NODE, &BasicNode::new(addr.to_string()))
        .await;
    peer.vote(vote_req(), opt()).await.expect("first vote");

    server.shutdown().await;
    let err = peer.vote(vote_req(), opt()).await.expect_err("server is gone");
    assert!(
        matches!(err, RPCError::Unreachable(_)),
        "closed peer is unreachable, got {err:?}"
    );

    // A new server on the same address: the next call dials afresh.
    let cfg = config(CLUSTER, SERVER_NODE);
    let revived = PeerServer::bind(addr, cfg, stub.clone())
        .await
        .expect("rebind the freed loopback port");
    peer.vote(vote_req(), opt()).await.expect("reconnects lazily");
    revived.shutdown().await;
}
