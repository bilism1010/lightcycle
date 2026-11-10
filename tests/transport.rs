//! Raft peer transport over real loopback TCP, driven against a stub `RaftHandler` (plan 03-09).
//!
//! The transport is proven here before a Raft node exists; the real-node smoke test is plan
//! 03-15. Tests use real time with short timeouts; nothing here depends on wall-clock precision
//! beyond generous slack.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt};
use lightcycle::net::client::{PeerClient, TcpNetConfig, TcpNetworkFactory};
use lightcycle::net::handler::RaftHandler;
use lightcycle::net::protocol::{
    encode_body, WireAppendResult, APPEND_REQ, APPEND_RESP, SNAP_BEGIN, VOTE_REQ, VOTE_RESP,
};
use lightcycle::net::server::PeerServer;
use lightcycle::raft::types::{KvRequest, SnapshotFile, TypeConfig};
use lightcycle::wire::frame::{Frame, FrameCodec, Lane};
use lightcycle::wire::hello::{client_handshake, server_handshake, Hello};
use openraft::errors::{Fatal, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, VoteOf};
use openraft::{BasicNode, Entry, EntryPayload, RaftNetworkFactory, RaftNetworkV2};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::codec::Framed;

const CLUSTER: u64 = 7;
const SERVER_NODE: u64 = 2;
const CLIENT_NODE: u64 = 1;

/// Records calls; answers votes with a grant unless told to fail. Pipelined AppendEntries are
/// answered strictly in order, each after `append_delay` and, when set, after taking a permit
/// from `append_gate` (the withhold gate), echoing the request's `prev_log_id`.
#[derive(Default)]
struct StubHandler {
    votes: AtomicUsize,
    pre_votes: AtomicUsize,
    fail_votes: AtomicBool,
    append_delay: Duration,
    append_gate: Option<Arc<Semaphore>>,
}

impl StubHandler {
    fn with_appends(append_delay: Duration, append_gate: Option<Arc<Semaphore>>) -> Self {
        StubHandler {
            append_delay,
            append_gate,
            ..Default::default()
        }
    }
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
        reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
    ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>> {
        let delay = self.append_delay;
        let gate = self.append_gate.clone();
        reqs.then(move |req| {
            let gate = gate.clone();
            async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                if let Some(gate) = gate {
                    gate.acquire().await.expect("gate stays open").forget();
                }
                Ok(Ok(req.prev_log_id))
            }
        })
        .boxed()
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

// ---- replication lane: pipelined AppendEntries ----------------------------------------------

fn lid(index: u64) -> LogIdOf<TypeConfig> {
    openraft::LogId::new_term_index(1, index)
}

/// An empty AppendEntries whose `prev_log_id` identifies it (the stub echoes it back).
fn append_req(index: u64) -> AppendEntriesRequest<TypeConfig> {
    AppendEntriesRequest {
        vote: VoteOf::<TypeConfig>::new_committed(1, 1),
        prev_log_id: Some(lid(index)),
        entries: Vec::new(),
        leader_commit: None,
    }
}

/// An AppendEntries carrying one entry with a `payload_len` byte value.
fn big_append_req(index: u64, payload_len: usize) -> AppendEntriesRequest<TypeConfig> {
    let mut req = append_req(index);
    req.entries = vec![Entry {
        log_id: lid(index + 1),
        payload: EntryPayload::Normal(KvRequest::Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from(vec![7u8; payload_len]),
        }),
    }];
    req
}

fn echoed(result: AppendResult<TypeConfig>) -> u64 {
    result
        .expect("append accepted")
        .expect("log id echoed")
        .index
}

#[tokio::test]
async fn append_entries_unary_over_replication_lane() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |_| {}).await;
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;

    for i in [3, 4, 5] {
        let result = peer.append_entries(append_req(i), opt()).await.expect("rpc");
        assert_eq!(echoed(result), i);
    }
    let stats = server.stats();
    assert_eq!(stats.append_req_frames.load(Ordering::Relaxed), 3);
    assert_eq!(stats.accepted(Lane::Replication), 1, "one connection reused");
    server.shutdown().await;
}

#[tokio::test]
async fn pipelined_in_order() {
    let stub = Arc::new(StubHandler::with_appends(Duration::from_millis(20), None));
    let server = start_server(stub, |_| {}).await;
    let stats = server.stats();
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;

    let reqs: Vec<_> = (0..20).map(append_req).collect();
    let mut out = peer
        .stream_append(futures::stream::iter(reqs), opt())
        .await
        .expect("stream established");

    let mut seen = Vec::new();
    while let Some(item) = out.next().await {
        seen.push(echoed(item.expect("response")));
    }
    assert_eq!(seen, (0..20).collect::<Vec<u64>>(), "responses in request order");
    assert!(
        stats.max_inflight_observed.load(Ordering::Relaxed) >= 2,
        "requests were received while earlier ones were unanswered (max {})",
        stats.max_inflight_observed.load(Ordering::Relaxed)
    );
    assert_eq!(stats.append_req_frames.load(Ordering::Relaxed), 20);
    eventually("all responses counted", || {
        stats.append_resp_frames.load(Ordering::Relaxed) == 20
    })
    .await;
    assert_eq!(stats.accepted(Lane::Replication), 1, "one connection carried the session");
    server.shutdown().await;
}

#[tokio::test]
async fn inflight_window_bounded() {
    let gate = Arc::new(Semaphore::new(0));
    let stub = Arc::new(StubHandler::with_appends(Duration::ZERO, Some(gate.clone())));
    let tweak = |cfg: &mut TcpNetConfig| cfg.inflight_requests = 4;
    let server = start_server(stub, tweak).await;
    let stats = server.stats();
    let mut client_cfg = config(CLUSTER, CLIENT_NODE);
    client_cfg.inflight_requests = 4;
    let mut peer = client(&server, client_cfg, Lane::Replication).await;

    let reqs: Vec<_> = (0..8).map(append_req).collect();
    let mut out = peer
        .stream_append(futures::stream::iter(reqs), opt())
        .await
        .expect("stream established");

    // Responses are withheld: exactly one window of frames reaches the server.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(stats.append_req_frames.load(Ordering::Relaxed), 4);
    assert_eq!(stats.max_inflight_observed.load(Ordering::Relaxed), 4);

    // Releasing responses lets the rest flow, still in order.
    gate.add_permits(8);
    let mut seen = Vec::new();
    while let Some(item) = out.next().await {
        seen.push(echoed(item.expect("response")));
    }
    assert_eq!(seen, (0..8).collect::<Vec<u64>>());
    assert_eq!(stats.append_req_frames.load(Ordering::Relaxed), 8);
    assert!(stats.max_inflight_observed.load(Ordering::Relaxed) <= 4);
    server.shutdown().await;
}

#[tokio::test]
async fn byte_window_admits_oversized_request_alone() {
    // 8 KiB byte window, 3 KiB requests: at most two fit; a body above the whole window is
    // admitted alone rather than deadlocking.
    let gate = Arc::new(Semaphore::new(0));
    let stub = Arc::new(StubHandler::with_appends(Duration::ZERO, Some(gate.clone())));
    let tweak = |cfg: &mut TcpNetConfig| cfg.inflight_bytes = 8 * 1024;
    let server = start_server(stub, tweak).await;
    let stats = server.stats();
    let mut client_cfg = config(CLUSTER, CLIENT_NODE);
    client_cfg.inflight_bytes = 8 * 1024;
    let mut peer = client(&server, client_cfg, Lane::Replication).await;

    let mut reqs: Vec<_> = (0..5).map(|i| big_append_req(i, 3 * 1024)).collect();
    reqs.push(big_append_req(5, 20 * 1024));
    let mut out = peer
        .stream_append(futures::stream::iter(reqs), opt())
        .await
        .expect("stream established");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        stats.append_req_frames.load(Ordering::Relaxed),
        2,
        "byte window holds two 3 KiB requests"
    );
    gate.add_permits(16);
    let mut seen = Vec::new();
    while let Some(item) = out.next().await {
        seen.push(echoed(item.expect("response")));
    }
    assert_eq!(seen, (0..6).collect::<Vec<u64>>());
    server.shutdown().await;
}

#[tokio::test]
async fn oversize_request_fails_on_client_without_sending() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |_| {}).await;
    let mut client_cfg = config(CLUSTER, CLIENT_NODE);
    client_cfg.max_frame_len = 1024;
    let mut peer = client(&server, client_cfg.clone(), Lane::Replication).await;

    let err = peer
        .append_entries(big_append_req(1, 4096), opt())
        .await
        .expect_err("frame cap");
    assert!(matches!(err, RPCError::Network(_)), "got {err:?}");

    let mut out = peer
        .stream_append(futures::stream::iter(vec![big_append_req(1, 4096)]), opt())
        .await
        .expect("stream established");
    let first = out.next().await.expect("an error item");
    assert!(matches!(first, Err(RPCError::Network(_))), "got {first:?}");
    assert!(out.next().await.is_none(), "stream ends after the error");
    assert_eq!(server.stats().append_req_frames.load(Ordering::Relaxed), 0);
    server.shutdown().await;
}

// ---- raw-socket helpers for protocol-violation tests ----------------------------------------

/// A raw, handshaken connection speaking the frame codec directly.
async fn raw_conn(server: &PeerServer, lane: Lane) -> Framed<TcpStream, FrameCodec> {
    let socket = TcpStream::connect(server.local_addr()).await.unwrap();
    let mut framed = Framed::new(socket, FrameCodec::new(1 << 20));
    client_handshake(
        &mut framed,
        Hello::new(CLUSTER, CLIENT_NODE, lane),
        Duration::from_secs(2),
    )
    .await
    .expect("handshake accepted");
    framed
}

/// Wait for the server to close the connection (EOF or reset), discarding anything it sends.
async fn expect_closed(framed: &mut Framed<TcpStream, FrameCodec>) {
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match framed.next().await {
                None | Some(Err(_)) => break,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "server did not close the connection");
}

#[tokio::test]
async fn oversize_frame_closes_connection() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |cfg| cfg.max_frame_len = 1024).await;
    let stats = server.stats();
    let mut framed = raw_conn(&server, Lane::Replication).await;

    // A header declaring 1 MiB against a 1 KiB lane cap: refused from the prefix alone.
    framed
        .get_mut()
        .write_all(&(1u32 << 20).to_be_bytes())
        .await
        .unwrap();
    expect_closed(&mut framed).await;
    eventually("decode error to be counted", || {
        stats.decode_errors.load(Ordering::Relaxed) == 1
    })
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn oversize_before_hello_is_a_handshake_failure() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |_| {}).await;
    let stats = server.stats();
    let mut raw = TcpStream::connect(server.local_addr()).await.unwrap();
    // Far above the 4 KiB pre-Hello cap, well below the lane cap.
    raw.write_all(&(1u32 << 20).to_be_bytes()).await.unwrap();
    let mut buf = [0u8; 64];
    tokio::time::timeout(Duration::from_secs(2), async {
        while matches!(raw.read(&mut buf).await, Ok(n) if n > 0) {}
    })
    .await
    .expect("server closes");
    eventually("handshake failure to be counted", || {
        stats.handshake_failures.load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(stats.accepted(Lane::Replication), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn garbage_body_closes_connection() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |_| {}).await;
    let stats = server.stats();
    let mut framed = raw_conn(&server, Lane::Replication).await;

    framed
        .send(Frame::new(
            APPEND_REQ,
            Lane::Replication,
            1,
            Bytes::from_static(&[0xff; 32]),
        ))
        .await
        .unwrap();
    expect_closed(&mut framed).await;
    eventually("decode error to be counted", || {
        stats.decode_errors.load(Ordering::Relaxed) == 1
    })
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn unexpected_kinds_and_lane_mismatch_close_connection() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |_| {}).await;
    let stats = server.stats();

    // A response kind sent to the server.
    let mut framed = raw_conn(&server, Lane::Replication).await;
    framed
        .send(Frame::new(VOTE_RESP, Lane::Replication, 1, Bytes::new()))
        .await
        .unwrap();
    expect_closed(&mut framed).await;

    // A snapshot frame on a replication connection.
    let mut framed = raw_conn(&server, Lane::Replication).await;
    framed
        .send(Frame::new(SNAP_BEGIN, Lane::Replication, 1, Bytes::new()))
        .await
        .unwrap();
    expect_closed(&mut framed).await;

    // The snapshot lane is accepted and lane-counted, but every snapshot frame is unknown until
    // plan 03-17.
    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed
        .send(Frame::new(SNAP_BEGIN, Lane::Snapshot, 1, Bytes::new()))
        .await
        .unwrap();
    expect_closed(&mut framed).await;

    // A frame whose lane byte disagrees with the Hello.
    let mut framed = raw_conn(&server, Lane::Replication).await;
    framed
        .send(Frame::new(
            VOTE_REQ,
            Lane::Heartbeat,
            1,
            encode_body(&vote_req()).unwrap(),
        ))
        .await
        .unwrap();
    expect_closed(&mut framed).await;

    eventually("four decode errors", || {
        stats.decode_errors.load(Ordering::Relaxed) == 4
    })
    .await;
    assert_eq!(stats.accepted(Lane::Snapshot), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn vote_served_while_append_responses_withheld() {
    let gate = Arc::new(Semaphore::new(0));
    let stub = Arc::new(StubHandler::with_appends(Duration::ZERO, Some(gate.clone())));
    let server = start_server(stub, |_| {}).await;
    let mut framed = raw_conn(&server, Lane::Replication).await;

    framed
        .send(Frame::new(
            APPEND_REQ,
            Lane::Replication,
            10,
            encode_body(&append_req(1)).unwrap(),
        ))
        .await
        .unwrap();
    framed
        .send(Frame::new(
            VOTE_REQ,
            Lane::Replication,
            11,
            encode_body(&vote_req()).unwrap(),
        ))
        .await
        .unwrap();

    // The vote overtakes the withheld append on the same connection.
    let first = tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("vote answered while the append is withheld")
        .unwrap()
        .unwrap();
    assert_eq!((first.header.kind, first.header.req_id), (VOTE_RESP, 11));

    gate.add_permits(1);
    let second = tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("append answered once released")
        .unwrap()
        .unwrap();
    assert_eq!((second.header.kind, second.header.req_id), (APPEND_RESP, 10));
    server.shutdown().await;
}

// ---- misbehaving server: the client must end the stream and drop the connection --------------

#[derive(Clone, Copy)]
enum Misbehave {
    WrongReqId,
    WrongKind,
    CloseWithoutAnswer,
}

/// A fake peer that handshakes, reads one AppendEntries and then misbehaves. Returns its address
/// and a channel reporting whether the client closed the socket afterwards.
async fn evil_server(how: Misbehave) -> (std::net::SocketAddr, tokio::sync::oneshot::Receiver<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(socket, FrameCodec::new(1 << 20));
        server_handshake(&mut framed, CLUSTER, SERVER_NODE, Duration::from_secs(2))
            .await
            .unwrap();
        let req = framed.next().await.unwrap().unwrap();
        assert_eq!(req.header.kind, APPEND_REQ);
        let lane = Lane::Replication;
        match how {
            Misbehave::WrongReqId => {
                let body = encode_body(&WireAppendResult::Accepted(None)).unwrap();
                framed
                    .send(Frame::new(APPEND_RESP, lane, req.header.req_id + 100, body))
                    .await
                    .unwrap();
            }
            Misbehave::WrongKind => {
                framed
                    .send(Frame::new(VOTE_RESP, lane, req.header.req_id, Bytes::new()))
                    .await
                    .unwrap();
            }
            Misbehave::CloseWithoutAnswer => {
                drop(framed);
                let _ = tx.send(true);
                return;
            }
        }
        // The client must close its end rather than keep using the connection.
        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match framed.next().await {
                    None | Some(Err(_)) => break,
                    Some(Ok(_)) => continue,
                }
            }
        })
        .await
        .is_ok();
        let _ = tx.send(closed);
    });
    (addr, rx)
}

async fn run_misbehaving(how: Misbehave) -> RPCError<TypeConfig> {
    let (addr, closed) = evil_server(how).await;
    let mut factory = TcpNetworkFactory::new(config(CLUSTER, CLIENT_NODE));
    let mut peer = factory
        .new_client(SERVER_NODE, &BasicNode::new(addr.to_string()))
        .await;
    let mut out = peer
        .stream_append(futures::stream::iter(vec![append_req(1), append_req(2)]), opt())
        .await
        .expect("stream established");
    let first = out.next().await.expect("an error item");
    let err = first.expect_err("must be an error, not a response");
    assert!(out.next().await.is_none(), "stream ends after the error");
    drop(out);
    assert!(closed.await.unwrap(), "connection dropped, no resynchronization");
    err
}

#[tokio::test]
async fn response_req_id_mismatch_ends_stream_and_drops_connection() {
    let err = run_misbehaving(Misbehave::WrongReqId).await;
    assert!(matches!(err, RPCError::Network(_)), "got {err:?}");
}

#[tokio::test]
async fn wrong_response_kind_ends_stream_and_drops_connection() {
    let err = run_misbehaving(Misbehave::WrongKind).await;
    assert!(matches!(err, RPCError::Network(_)), "got {err:?}");
}

#[tokio::test]
async fn eof_mid_stream_ends_stream_with_unreachable() {
    let err = run_misbehaving(Misbehave::CloseWithoutAnswer).await;
    assert!(matches!(err, RPCError::Unreachable(_)), "got {err:?}");
}

#[tokio::test]
async fn silent_server_times_out_stream_response() {
    // The gate never opens, so the one request is never answered.
    let gate = Arc::new(Semaphore::new(0));
    let stub = Arc::new(StubHandler::with_appends(Duration::ZERO, Some(gate)));
    let server = start_server(stub, |_| {}).await;
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;
    let mut out = peer
        .stream_append(
            futures::stream::iter(vec![append_req(1)]),
            RPCOption::new(Duration::from_millis(400)),
        )
        .await
        .expect("stream established");
    let first = out.next().await.expect("an error item");
    assert!(matches!(first, Err(RPCError::Timeout(_))), "got {first:?}");
    assert!(out.next().await.is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn handler_fatal_on_stream_maps_to_unreachable() {
    // Unit check of the KIND_ERROR path on the server: a stream handler that yields Fatal.
    struct FatalStream;
    impl RaftHandler for FatalStream {
        fn append_entries(
            &self,
            _req: AppendEntriesRequest<TypeConfig>,
        ) -> BoxFuture<'static, Result<AppendResult<TypeConfig>, RaftError<TypeConfig>>> {
            Box::pin(async { Err(RaftError::Fatal(Fatal::Stopped)) })
        }
        fn stream_append(
            &self,
            reqs: BoxStream<'static, AppendEntriesRequest<TypeConfig>>,
        ) -> BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>> {
            reqs.map(|_| Err(Fatal::Stopped)).boxed()
        }
        fn vote(
            &self,
            _req: VoteRequest<TypeConfig>,
        ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
            Box::pin(async { Err(RaftError::Fatal(Fatal::Stopped)) })
        }
        fn pre_vote(
            &self,
            _req: VoteRequest<TypeConfig>,
        ) -> BoxFuture<'static, Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>> {
            Box::pin(async { Err(RaftError::Fatal(Fatal::Stopped)) })
        }
        fn install_snapshot(
            &self,
            _vote: VoteOf<TypeConfig>,
            _meta: SnapshotMetaOf<TypeConfig>,
            _file: SnapshotFile,
        ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>> {
            Box::pin(async { Err(Fatal::Stopped) })
        }
    }

    let server = start_server(Arc::new(FatalStream), |_| {}).await;
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;
    let mut out = peer
        .stream_append(futures::stream::iter(vec![append_req(1)]), opt())
        .await
        .expect("stream established");
    let first = out.next().await.expect("an error item");
    // Whether the Fatal error frame or only the closing socket reaches the client first, a
    // stopped remote backs the leader off.
    assert!(
        matches!(first, Err(RPCError::Unreachable(_))),
        "a stopped remote must surface as Unreachable, got {first:?}"
    );
    server.shutdown().await;
}
