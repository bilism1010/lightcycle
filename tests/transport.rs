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
    decode_body, decode_snap_chunk, encode_body, encode_snap_chunk, SnapAck, SnapBegin,
    WireAppendResult, APPEND_REQ, APPEND_RESP, SNAP_ACK, SNAP_BEGIN, SNAP_CHUNK, SNAP_END,
    SNAP_RESP, VOTE_REQ, VOTE_RESP,
};
use lightcycle::net::server::PeerServer;
use lightcycle::raft::types::{KvRequest, SnapshotFile, TypeConfig};
use lightcycle::wire::frame::{Frame, FrameCodec, Lane, KIND_ERROR};
use lightcycle::wire::hello::{client_handshake, server_handshake, Hello};
use openraft::errors::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
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
    /// The next pipelined session answers its first request with a Conflict and then ends, as
    /// openraft's `stream_append` does after an API error.
    conflict_next_session: AtomicBool,
    append_delay: Duration,
    append_gate: Option<Arc<Semaphore>>,
    /// Time `install_snapshot` takes after inspecting the received file.
    install_delay: Duration,
    /// `install_snapshot` calls started.
    install_calls: AtomicUsize,
    /// What each `install_snapshot` call saw (the file is removed afterwards, as a real
    /// installer would consume it).
    installed: std::sync::Mutex<Vec<Installed>>,
}

/// Observations made inside `install_snapshot`.
#[derive(Debug)]
struct Installed {
    file_name: String,
    parent: std::path::PathBuf,
    len: u64,
    /// Whole file, kept only up to 4 MiB.
    contents: Vec<u8>,
    crc: u32,
    mode: Option<u32>,
    /// File names in the parent directory at that moment.
    siblings: Vec<String>,
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
        if self.conflict_next_session.swap(false, Ordering::SeqCst) {
            return reqs
                .take(1)
                .map(|req| {
                    Ok(Err(AppendError::Conflict {
                        log_id: req.prev_log_id.expect("prev_log_id set"),
                        hint: None,
                    }))
                })
                .boxed();
        }
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
        file: SnapshotFile,
    ) -> BoxFuture<'static, Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>>> {
        self.install_calls.fetch_add(1, Ordering::SeqCst);
        let delay = self.install_delay;
        // Inspect and consume the file synchronously, as the real installer would take it over.
        let observed = observe_received(&file);
        self.installed.lock().unwrap().push(observed);
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(SnapshotResponse::new(vote))
        })
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
async fn stopped_remote_on_vote_is_unreachable_and_the_next_call_redials() {
    // The vote handler can only fail with Fatal (the node stopped), which backs the caller off.
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;

    stub.fail_votes.store(true, Ordering::SeqCst);
    let err = peer.vote(vote_req(), opt()).await.expect_err("handler fails");
    assert!(matches!(err, RPCError::Unreachable(_)), "got {err:?}");

    stub.fail_votes.store(false, Ordering::SeqCst);
    peer.vote(vote_req(), opt()).await.expect("the next call dials afresh");
    assert_eq!(server.stats().accepted(Lane::Replication), 2, "fatal drops the connection");
    server.shutdown().await;
}

#[tokio::test]
async fn non_fatal_remote_failure_is_network_and_keeps_connection() {
    // A hand-rolled peer that answers the first vote with a non-fatal KIND_ERROR frame.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let seen = accepts.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            seen.fetch_add(1, Ordering::SeqCst);
            let mut framed = Framed::new(socket, FrameCodec::new(4096));
            if server_handshake(&mut framed, CLUSTER, SERVER_NODE, Duration::from_secs(2))
                .await
                .is_err()
            {
                continue;
            }
            framed.codec_mut().set_max_frame_length(1 << 20);
            let mut first = true;
            while let Some(Ok(req)) = framed.next().await {
                let id = req.header.req_id;
                let reply = if first {
                    first = false;
                    let failure = lightcycle::net::protocol::RemoteFailure::new(false, "refused");
                    Frame::new(KIND_ERROR, Lane::Replication, id, failure.encode())
                } else {
                    let resp: VoteResponse<TypeConfig> =
                        VoteResponse::new(VoteOf::<TypeConfig>::new(2, 1), None, true);
                    Frame::new(VOTE_RESP, Lane::Replication, id, encode_body(&resp).unwrap())
                };
                if framed.send(reply).await.is_err() {
                    break;
                }
            }
        }
    });

    let node = BasicNode::new(addr.to_string());
    let mut factory = TcpNetworkFactory::new(config(CLUSTER, CLIENT_NODE));
    let mut peer = factory.new_client(SERVER_NODE, &node).await;
    let err = peer.vote(vote_req(), opt()).await.expect_err("remote refuses");
    assert!(matches!(err, RPCError::Network(_)), "got {err:?}");
    peer.vote(vote_req(), opt()).await.expect("connection still usable");
    assert_eq!(accepts.load(Ordering::SeqCst), 1, "no reconnect");
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
async fn wrong_node_at_the_dialed_address_is_rejected_over_tcp() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    // The membership says node SERVER_NODE + 1 lives at this address, but node SERVER_NODE
    // answers: its votes must never be attributed to the other node.
    let node = BasicNode::new(server.local_addr().to_string());
    let mut factory = TcpNetworkFactory::new(config(CLUSTER, CLIENT_NODE));
    let mut peer = factory.new_client(SERVER_NODE + 1, &node).await;

    let err = peer.vote(vote_req(), opt()).await.expect_err("must be rejected");
    assert!(matches!(err, RPCError::Unreachable(_)), "got {err:?}");
    assert_eq!(stub.votes.load(Ordering::SeqCst), 0, "no vote may reach the handler");

    // The right target is served normally.
    let mut right = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;
    right.vote(vote_req(), opt()).await.expect("vote");
    assert_eq!(stub.votes.load(Ordering::SeqCst), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn connections_over_the_cap_are_dropped_and_slots_are_reused() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |cfg| cfg.max_connections = 2).await;
    let stats = server.stats();

    let first = raw_conn(&server, Lane::Replication).await;
    let second = raw_conn(&server, Lane::Heartbeat).await;
    eventually("both connections to be accepted", || {
        stats.accepted(Lane::Replication) + stats.accepted(Lane::Heartbeat) == 2
    })
    .await;

    // The third socket is accepted by the kernel but closed at once, before any handshake.
    let mut third = TcpStream::connect(server.local_addr()).await.unwrap();
    let mut buf = [0u8; 8];
    let n = tokio::time::timeout(Duration::from_secs(2), third.read(&mut buf))
        .await
        .expect("over-cap connection must be closed promptly")
        .unwrap_or(0);
    assert_eq!(n, 0);
    assert_eq!(stats.connections_rejected.load(Ordering::Relaxed), 1);

    // Closing one frees its slot.
    drop(first);
    let mut replacement = None;
    for _ in 0..50 {
        let socket = TcpStream::connect(server.local_addr()).await.unwrap();
        let mut framed = Framed::new(socket, FrameCodec::new(1 << 20));
        let hello = Hello::new(CLUSTER, CLIENT_NODE, Lane::Replication);
        if client_handshake(&mut framed, hello, Duration::from_millis(500)).await.is_ok() {
            replacement = Some(framed);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(replacement.is_some(), "a freed slot must be reusable");
    drop(second);
    server.shutdown().await;
}

#[tokio::test]
async fn concurrent_snapshot_receives_are_capped() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |cfg| {
        cfg.recv_dir = recv_dir.clone();
        cfg.max_snapshot_receives = 1;
    })
    .await;
    let stats = server.stats();

    // The first receive begins and stays open.
    let mut first = raw_conn(&server, Lane::Snapshot).await;
    first.send(begin_frame(1024)).await.unwrap();
    {
        let dir = recv_dir.clone();
        eventually("the first receive file to exist", move || dir_entries(&dir).len() == 1).await;
    }

    // A second sender is refused with a reason and disconnected; no second file is claimed.
    let mut second = raw_conn(&server, Lane::Snapshot).await;
    second.send(begin_frame(1024)).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), second.next())
        .await
        .expect("a refusal is sent")
        .expect("frame")
        .expect("decodes");
    assert_eq!(reply.header.kind, KIND_ERROR);
    expect_closed(&mut second).await;
    assert_eq!(stats.snapshot_receives_rejected.load(Ordering::Relaxed), 1);
    assert_eq!(dir_entries(&recv_dir).len(), 1);

    // Ending the first receive frees the slot.
    drop(first);
    let mut third = None;
    for _ in 0..50 {
        let mut conn = raw_conn(&server, Lane::Snapshot).await;
        conn.send(begin_frame(1024)).await.unwrap();
        let waited = tokio::time::timeout(Duration::from_millis(150), conn.next()).await;
        if waited.is_err() {
            third = Some(conn); // no refusal: the receive began
            break;
        }
    }
    assert!(third.is_some(), "a freed snapshot slot must be reusable");
    server.shutdown().await;
}

#[tokio::test]
async fn silent_heartbeat_connection_is_closed_after_the_idle_bound() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub, |cfg| {
        cfg.heartbeat_idle_timeout = Duration::from_millis(200);
    })
    .await;

    // Handshaken but then silent: no heartbeat ever arrives.
    let mut silent = raw_conn(&server, Lane::Heartbeat).await;
    expect_closed(&mut silent).await;

    // The replication lane has no such bound (it is legitimately quiet between writes).
    let mut quiet = raw_conn(&server, Lane::Replication).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let still_open = tokio::time::timeout(Duration::from_millis(100), quiet.next()).await;
    assert!(still_open.is_err(), "an idle replication connection stays open");
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
async fn unary_append_after_a_conflict_redials_instead_of_failing() {
    // After a Conflict answer the server ends the pipelined session and closes the connection;
    // the unary client must not reuse that dead connection for the next call.
    let stub = Arc::new(StubHandler::default());
    stub.conflict_next_session.store(true, Ordering::SeqCst);
    let server = start_server(stub, |_| {}).await;
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;

    let first = peer.append_entries(append_req(3), opt()).await.expect("conflict is an answer");
    assert!(matches!(first, Err(AppendError::Conflict { .. })), "got {first:?}");

    let second = peer.append_entries(append_req(4), opt()).await.expect("second call succeeds");
    assert_eq!(echoed(second), 4);
    assert_eq!(server.stats().accepted(Lane::Replication), 2, "a deliberate redial");
    server.shutdown().await;
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

    // Responses are withheld: exactly one window of frames reaches the server. Wait for the
    // window to fill (no fixed sleep decides that), then hold it open briefly to show the count
    // does not exceed the window.
    eventually("the request window to fill", || {
        stats.append_req_frames.load(Ordering::Relaxed) >= 4
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
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

    eventually("the byte window to fill", || {
        stats.append_req_frames.load(Ordering::Relaxed) >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
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

    // The snapshot lane is accepted and lane-counted; a SNAP_BEGIN whose body is not a SnapBegin
    // is a decode error.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_fatal_frame_is_delivered_before_the_connection_closes() {
    // The responder's last frame (the Fatal reason) must be flushed before the connection is torn
    // down; losing it leaves the peer with a bare EOF and no reason.
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
    for round in 0..40 {
        let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Replication).await;
        let mut out = peer
            .stream_append(futures::stream::iter(vec![append_req(1)]), opt())
            .await
            .expect("stream established");
        let first = out.next().await.expect("an error item");
        let text = format!("{first:?}");
        assert!(
            matches!(first, Err(RPCError::Unreachable(_))) && text.contains("remote node stopped"),
            "round {round}: the Fatal reason was lost: {text}"
        );
    }
    server.shutdown().await;
}

// ---- snapshot lane (plan 03-17) -------------------------------------------------------------

use std::path::Path;

fn snap_vote() -> VoteOf<TypeConfig> {
    VoteOf::<TypeConfig>::new_committed(3, 1)
}

fn snap_meta() -> SnapshotMetaOf<TypeConfig> {
    openraft::SnapshotMeta {
        last_log_id: Some(lid(5)),
        last_membership: Default::default(),
    }
}

/// Config for either end of a snapshot test. The client's chunk size must equal the server's
/// (openraft's per-call `RPCOption` chunk size cannot be set from outside the crate, so the
/// configured size is what the tests vary).
fn snap_cfg(
    node: u64,
    recv_dir: &Path,
    chunk: usize,
    window: usize,
    idle: Duration,
) -> TcpNetConfig {
    let mut cfg = TcpNetConfig::new(CLUSTER, node, recv_dir.to_path_buf());
    cfg.snapshot_chunk_size = chunk;
    cfg.snapshot_ack_window = window;
    cfg.snapshot_idle_timeout = idle;
    cfg
}

/// Write `len` non-repeating bytes to `dir/name`.
fn make_file(dir: &Path, name: &str, len: usize) -> SnapshotFile {
    let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ len as u64;
    let mut bytes = Vec::with_capacity(len + 8);
    while bytes.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes.truncate(len);
    let path = dir.join(name);
    std::fs::write(&path, &bytes).unwrap();
    SnapshotFile {
        path,
        len: len as u64,
    }
}

fn snapshot_of(
    file: SnapshotFile,
) -> openraft::type_config::alias::SnapshotOf<TypeConfig, SnapshotFile> {
    openraft::Snapshot {
        meta: snap_meta(),
        snapshot: file,
    }
}

fn never_cancelled() -> impl std::future::Future<Output = ReplicationClosed> + Send + 'static {
    futures::future::pending::<ReplicationClosed>()
}

/// Inspect the received file, then consume it (a real installer renames or removes it).
fn observe_received(file: &SnapshotFile) -> Installed {
    let meta = std::fs::metadata(&file.path).expect("received file exists");
    let bytes = std::fs::read(&file.path).unwrap();
    let parent = file.path.parent().unwrap().to_path_buf();
    let siblings = dir_entries(&parent);
    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        Some(meta.permissions().mode() & 0o777)
    };
    #[cfg(not(unix))]
    let mode = None;
    let installed = Installed {
        file_name: file.path.file_name().unwrap().to_string_lossy().into_owned(),
        parent,
        len: file.len.max(meta.len()),
        crc: crc32c::crc32c(&bytes),
        contents: if bytes.len() <= 4 * 1024 * 1024 {
            bytes
        } else {
            Vec::new()
        },
        mode,
        siblings,
    };
    std::fs::remove_file(&file.path).unwrap();
    installed
}

/// Names in `dir`, sorted; empty when the directory does not exist.
fn dir_entries(dir: &Path) -> Vec<String> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = rd
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

async fn wait_dir_empty(dir: &Path) {
    let dir = dir.to_path_buf();
    eventually("receive directory to be empty", move || {
        dir_entries(&dir).is_empty()
    })
    .await;
}

fn begin_frame(total_len: u64) -> Frame {
    let begin = SnapBegin {
        vote: snap_vote(),
        meta: snap_meta(),
        total_len,
    };
    Frame::new(SNAP_BEGIN, Lane::Snapshot, 1, encode_body(&begin).unwrap())
}

fn chunk_frame(offset: u64, len: usize) -> Frame {
    Frame::new(
        SNAP_CHUNK,
        Lane::Snapshot,
        1,
        encode_snap_chunk(offset, &vec![0xAB; len]),
    )
}

fn end_frame() -> Frame {
    Frame::new(SNAP_END, Lane::Snapshot, 1, Bytes::new())
}

/// The server must answer with a KIND_ERROR frame and then close the connection.
async fn expect_error_then_closed(framed: &mut Framed<TcpStream, FrameCodec>) {
    let mut saw_error = false;
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match framed.next().await {
                None | Some(Err(_)) => break,
                Some(Ok(frame)) => saw_error |= frame.header.kind == KIND_ERROR,
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "server did not close the connection");
    assert!(saw_error, "server did not report the violation");
}

const IDLE: Duration = Duration::from_secs(30);

async fn start_snapshot_server(
    stub: Arc<StubHandler>,
    recv_dir: &Path,
    chunk: usize,
    window: usize,
    idle: Duration,
    tweak: impl FnOnce(&mut TcpNetConfig),
) -> PeerServer {
    start_server(stub, |cfg| {
        *cfg = snap_cfg(SERVER_NODE, recv_dir, chunk, window, idle);
        tweak(cfg);
    })
    .await
}

#[tokio::test]
async fn snapshot_transfer_chunked_and_installed() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let src = make_file(td.path(), "snap.bin", 3 * 1024 * 1024);
    let expected = std::fs::read(&src.path).unwrap();
    let chunk = 256 * 1024;

    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, chunk, 4, IDLE, |_| {}).await;
    let mut peer = client(
        &server,
        snap_cfg(CLIENT_NODE, &recv_dir, chunk, 4, IDLE),
        Lane::Snapshot,
    )
    .await;

    let resp = peer
        .full_snapshot(snap_vote(), snapshot_of(src), never_cancelled(), opt())
        .await
        .expect("snapshot transfer");
    assert_eq!(resp.vote, snap_vote(), "the handler's response reaches the sender");

    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 1);
    let installed = std::mem::take(&mut *stub.installed.lock().unwrap());
    let got = &installed[0];
    assert_eq!(got.contents, expected, "byte-identical");
    assert_eq!(got.len, expected.len() as u64);
    assert_eq!(got.parent, recv_dir, "file lives directly in recv_dir");
    assert_eq!(got.siblings, vec![got.file_name.clone()], "no other file in recv_dir");
    let digits = got
        .file_name
        .strip_prefix("recv-")
        .and_then(|rest| rest.strip_suffix(".tmp"))
        .expect("name is recv-<digits>.tmp");
    assert!(!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()), "{digits}");
    #[cfg(unix)]
    assert_eq!(got.mode, Some(0o600));

    let stats = server.stats();
    assert_eq!(stats.snapshot_chunks.load(Ordering::Relaxed), 12);
    assert_eq!(stats.accepted(Lane::Snapshot), 1);
    assert_eq!(stats.accepted(Lane::Replication), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn snapshot_connection_is_reusable_after_success() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let chunk = 64 * 1024;
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, chunk, 2, IDLE, |_| {}).await;
    let mut peer = client(
        &server,
        snap_cfg(CLIENT_NODE, &recv_dir, chunk, 2, IDLE),
        Lane::Snapshot,
    )
    .await;
    for (i, len) in [0usize, 1, chunk, chunk + 1].into_iter().enumerate() {
        let src = make_file(td.path(), &format!("s{i}.bin"), len);
        peer.full_snapshot(snap_vote(), snapshot_of(src), never_cancelled(), opt())
            .await
            .unwrap_or_else(|e| panic!("snapshot of {len} bytes: {e:?}"));
    }
    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 4);
    assert_eq!(server.stats().accepted(Lane::Snapshot), 1, "one connection carried all four");
    server.shutdown().await;
}

#[tokio::test]
async fn snapshot_bad_offset_rejected() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, 64 * 1024, 4, IDLE, |_| {}).await;
    let stats = server.stats();
    let mut framed = raw_conn(&server, Lane::Snapshot).await;

    framed.send(begin_frame(1000)).await.unwrap();
    framed.send(chunk_frame(0, 100)).await.unwrap();
    eventually("first chunk accepted", || {
        stats.snapshot_chunks.load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(dir_entries(&recv_dir).len(), 1, "a receive file exists mid-transfer");

    // Overlaps the bytes already written.
    framed.send(chunk_frame(50, 100)).await.unwrap();
    expect_error_then_closed(&mut framed).await;
    wait_dir_empty(&recv_dir).await;
    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 0);
    assert_eq!(stats.snapshot_chunks.load(Ordering::Relaxed), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn snapshot_over_declared_size_rejected() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, 64 * 1024, 4, IDLE, |_| {}).await;

    // A chunk that runs past the declared total.
    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed.send(begin_frame(100)).await.unwrap();
    framed.send(chunk_frame(0, 101)).await.unwrap();
    expect_error_then_closed(&mut framed).await;
    wait_dir_empty(&recv_dir).await;

    // A second chunk that crosses the total after a valid first one.
    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed.send(begin_frame(100)).await.unwrap();
    framed.send(chunk_frame(0, 60)).await.unwrap();
    framed.send(chunk_frame(60, 41)).await.unwrap();
    expect_error_then_closed(&mut framed).await;
    wait_dir_empty(&recv_dir).await;

    // END with fewer bytes than declared.
    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed.send(begin_frame(100)).await.unwrap();
    framed.send(chunk_frame(0, 60)).await.unwrap();
    framed.send(end_frame()).await.unwrap();
    expect_error_then_closed(&mut framed).await;
    wait_dir_empty(&recv_dir).await;

    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn snapshot_begin_over_max_rejected() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, 64 * 1024, 4, IDLE, |cfg| {
        cfg.max_snapshot_bytes = 1000;
    })
    .await;

    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed.send(begin_frame(1001)).await.unwrap();
    expect_error_then_closed(&mut framed).await;
    assert!(dir_entries(&recv_dir).is_empty(), "no tmp file was created");

    // Exactly the maximum is fine.
    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed.send(begin_frame(1000)).await.unwrap();
    let stats = server.stats();
    framed.send(chunk_frame(0, 1000)).await.unwrap();
    framed.send(end_frame()).await.unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("response in time")
        .expect("open")
        .expect("frame");
    assert_eq!(resp.header.kind, SNAP_RESP);
    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 1);
    assert_eq!(stats.snapshot_chunks.load(Ordering::Relaxed), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn snapshot_second_begin_rejected() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, 64 * 1024, 4, IDLE, |_| {}).await;
    let stats = server.stats();
    let mut framed = raw_conn(&server, Lane::Snapshot).await;

    framed.send(begin_frame(1000)).await.unwrap();
    framed.send(chunk_frame(0, 100)).await.unwrap();
    eventually("chunk accepted", || {
        stats.snapshot_chunks.load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(dir_entries(&recv_dir).len(), 1);
    framed.send(begin_frame(1000)).await.unwrap();
    expect_closed(&mut framed).await;
    wait_dir_empty(&recv_dir).await;
    assert_eq!(stats.decode_errors.load(Ordering::Relaxed), 1);
    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn snapshot_early_close_removes_tmp() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, 64 * 1024, 4, IDLE, |_| {}).await;
    let stats = server.stats();
    let mut framed = raw_conn(&server, Lane::Snapshot).await;

    framed.send(begin_frame(1000)).await.unwrap();
    framed.send(chunk_frame(0, 100)).await.unwrap();
    eventually("chunk accepted", || {
        stats.snapshot_chunks.load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(dir_entries(&recv_dir).len(), 1);
    drop(framed);
    wait_dir_empty(&recv_dir).await;
    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn snapshot_server_shutdown_removes_tmp() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(stub.clone(), &recv_dir, 64 * 1024, 4, IDLE, |_| {}).await;
    let stats = server.stats();
    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed.send(begin_frame(1000)).await.unwrap();
    framed.send(chunk_frame(0, 100)).await.unwrap();
    eventually("chunk accepted", || {
        stats.snapshot_chunks.load(Ordering::Relaxed) == 1
    })
    .await;
    server.shutdown().await;
    wait_dir_empty(&recv_dir).await;
}

/// A receiver that never acknowledges (its window is far larger than the sender's) keeps a
/// partial file on the server and stalls the sender after its window.
struct Stalled {
    server: PeerServer,
    recv_dir: std::path::PathBuf,
    stub: Arc<StubHandler>,
    src: SnapshotFile,
    _td: tempfile::TempDir,
}

async fn stalled_receiver() -> Stalled {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let src = make_file(td.path(), "snap.bin", 1024 * 1024);
    let stub = Arc::new(StubHandler::default());
    // Server acks every 100 chunks; the 16-chunk snapshot never earns an ack.
    let server = start_snapshot_server(stub.clone(), &recv_dir, 64 * 1024, 100, IDLE, |_| {}).await;
    Stalled {
        server,
        recv_dir,
        stub,
        src,
        _td: td,
    }
}

#[tokio::test]
async fn snapshot_cancel_aborts_and_removes_tmp() {
    let s = stalled_receiver().await;
    let stats = s.server.stats();
    let mut peer = client(
        &s.server,
        snap_cfg(CLIENT_NODE, &s.recv_dir, 64 * 1024, 2, IDLE),
        Lane::Snapshot,
    )
    .await;

    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel = async move {
        let _ = cancel_rx.await;
        ReplicationClosed::new("test cancel")
    };
    let src = s.src.clone();
    let task = tokio::spawn(async move {
        peer.full_snapshot(snap_vote(), snapshot_of(src), cancel, opt())
            .await
    });

    // The sender sends its window of 2 chunks, then waits for an ack that never comes.
    eventually("two chunks received", || {
        stats.snapshot_chunks.load(Ordering::Relaxed) == 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished(), "sender is blocked on the ack window");
    assert_eq!(stats.snapshot_chunks.load(Ordering::Relaxed), 2, "window respected");
    assert_eq!(dir_entries(&s.recv_dir).len(), 1);

    let started = Instant::now();
    cancel_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cancel is prompt")
        .unwrap();
    assert!(
        matches!(result, Err(StreamingError::Closed(_))),
        "got {result:?}"
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    wait_dir_empty(&s.recv_dir).await;
    assert_eq!(s.stub.install_calls.load(Ordering::SeqCst), 0);
    s.server.shutdown().await;
}

#[tokio::test]
async fn snapshot_idle_timeout_cleans_up() {
    // Sender side: a receiver that stops acking makes the client fail with Timeout.
    let s = stalled_receiver().await;
    let mut peer = client(
        &s.server,
        snap_cfg(CLIENT_NODE, &s.recv_dir, 64 * 1024, 2, Duration::from_millis(200)),
        Lane::Snapshot,
    )
    .await;
    let started = Instant::now();
    let err = peer
        .full_snapshot(snap_vote(), snapshot_of(s.src.clone()), never_cancelled(), opt())
        .await
        .expect_err("no acks, no completion");
    assert!(matches!(err, StreamingError::Timeout(_)), "got {err:?}");
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
    // The failed client dropped its connection; the server removed the partial file.
    wait_dir_empty(&s.recv_dir).await;
    s.server.shutdown().await;

    // Receiver side: a sender that stalls mid-transfer is closed and cleaned up.
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let stub = Arc::new(StubHandler::default());
    let server = start_snapshot_server(
        stub.clone(),
        &recv_dir,
        64 * 1024,
        4,
        Duration::from_millis(200),
        |_| {},
    )
    .await;
    let stats = server.stats();
    let mut framed = raw_conn(&server, Lane::Snapshot).await;
    framed.send(begin_frame(1000)).await.unwrap();
    framed.send(chunk_frame(0, 100)).await.unwrap();
    eventually("chunk accepted", || {
        stats.snapshot_chunks.load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(dir_entries(&recv_dir).len(), 1);
    // Say nothing more; the server must give up on its own.
    expect_closed(&mut framed).await;
    wait_dir_empty(&recv_dir).await;
    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 0);
    server.shutdown().await;
}

/// Counters from the scripted receiver below.
#[derive(Default)]
struct FakeReceiver {
    chunks: AtomicUsize,
    max_unacked: AtomicUsize,
    /// A chunk arrived while the receiver was holding back an ack the sender was owed.
    overran_window: AtomicBool,
}

/// A receiver that acknowledges every `window` chunks only after `ack_delay`, and reports whether
/// any further chunk arrives during that delay (a sender honouring the window sends none).
async fn fake_receiver(
    window: usize,
    ack_delay: Duration,
) -> (std::net::SocketAddr, Arc<FakeReceiver>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let fake = Arc::new(FakeReceiver::default());
    let counters = fake.clone();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(socket, FrameCodec::new(4096));
        server_handshake(&mut framed, CLUSTER, SERVER_NODE, Duration::from_secs(2))
            .await
            .expect("hello");
        framed.codec_mut().set_max_frame_length(1 << 20);
        let (mut unacked, mut written) = (0usize, 0u64);
        let mut vote = snap_vote();
        let mut held: Option<Frame> = None;
        loop {
            let frame = match held.take() {
                Some(frame) => frame,
                None => match framed.next().await {
                    Some(Ok(frame)) => frame,
                    _ => return,
                },
            };
            let req_id = frame.header.req_id;
            match frame.header.kind {
                SNAP_BEGIN => vote = decode_body::<SnapBegin>(&frame.body).unwrap().vote,
                SNAP_CHUNK => {
                    let (_, bytes) = decode_snap_chunk(&frame.body).unwrap();
                    written += bytes.len() as u64;
                    unacked += 1;
                    counters.chunks.fetch_add(1, Ordering::SeqCst);
                    counters.max_unacked.fetch_max(unacked, Ordering::SeqCst);
                    if unacked == window {
                        match tokio::time::timeout(ack_delay, framed.next()).await {
                            Err(_) => {}
                            Ok(Some(Ok(next))) if next.header.kind == SNAP_END => {
                                // The final window needs no ack.
                                held = Some(next);
                                continue;
                            }
                            Ok(Some(Ok(_))) => {
                                counters.overran_window.store(true, Ordering::SeqCst);
                                return;
                            }
                            Ok(_) => return,
                        }
                        unacked = 0;
                        let ack = Frame::new(
                            SNAP_ACK,
                            Lane::Snapshot,
                            req_id,
                            encode_body(&SnapAck { written }).unwrap(),
                        );
                        framed.send(ack).await.unwrap();
                    }
                }
                SNAP_END => {
                    let resp = SnapshotResponse::<TypeConfig>::new(vote);
                    let reply = Frame::new(SNAP_RESP, Lane::Snapshot, req_id, encode_body(&resp).unwrap());
                    framed.send(reply).await.unwrap();
                }
                other => panic!("unexpected frame kind {other}"),
            }
        }
    });
    (addr, fake)
}

async fn client_to(addr: std::net::SocketAddr, cfg: TcpNetConfig) -> PeerClient {
    let node = BasicNode::new(addr.to_string());
    TcpNetworkFactory::new(cfg)
        .new_snapshot_client(SERVER_NODE, &node)
        .await
}

#[tokio::test]
async fn snapshot_ack_window_bounds_outstanding_chunks() {
    let td = tempfile::tempdir().unwrap();
    let src = make_file(td.path(), "snap.bin", 8 * 64 * 1024);
    let (addr, fake) = fake_receiver(2, Duration::from_millis(100)).await;
    let mut peer = client_to(addr, snap_cfg(CLIENT_NODE, td.path(), 64 * 1024, 2, IDLE)).await;

    peer.full_snapshot(snap_vote(), snapshot_of(src), never_cancelled(), opt())
        .await
        .expect("transfer completes");
    assert_eq!(fake.chunks.load(Ordering::SeqCst), 8);
    assert_eq!(fake.max_unacked.load(Ordering::SeqCst), 2, "never above the window");
    assert!(
        !fake.overran_window.load(Ordering::SeqCst),
        "no chunk was sent while an ack was owed"
    );
}

#[tokio::test]
async fn snapshot_idle_timeout_is_per_chunk_not_total() {
    let td = tempfile::tempdir().unwrap();
    let src = make_file(td.path(), "snap.bin", 8 * 64 * 1024);
    // Four acknowledgements, each 250 ms late: about a second in total against a 400 ms idle bound.
    let (addr, _fake) = fake_receiver(2, Duration::from_millis(250)).await;
    let idle = Duration::from_millis(400);
    let mut peer = client_to(addr, snap_cfg(CLIENT_NODE, td.path(), 64 * 1024, 2, idle)).await;

    let started = Instant::now();
    peer.full_snapshot(snap_vote(), snapshot_of(src), never_cancelled(), opt())
        .await
        .expect("each wait is within the idle bound");
    assert!(
        started.elapsed() > idle,
        "the whole transfer outlasted the idle bound ({:?})",
        started.elapsed()
    );
}

#[tokio::test]
async fn snapshot_handler_failure_removes_tmp_and_reports() {
    struct Failing;
    impl RaftHandler for Failing {
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

    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let src = make_file(td.path(), "snap.bin", 100 * 1024);
    let server = start_server(Arc::new(Failing), |cfg| {
        *cfg = snap_cfg(SERVER_NODE, &recv_dir, 64 * 1024, 4, IDLE);
    })
    .await;
    let mut peer = client(
        &server,
        snap_cfg(CLIENT_NODE, &recv_dir, 64 * 1024, 4, IDLE),
        Lane::Snapshot,
    )
    .await;
    let err = peer
        .full_snapshot(snap_vote(), snapshot_of(src), never_cancelled(), opt())
        .await
        .expect_err("handler stopped");
    assert!(matches!(err, StreamingError::Unreachable(_)), "got {err:?}");
    wait_dir_empty(&recv_dir).await;
    server.shutdown().await;
}

// ---- heartbeat lane --------------------------------------------------------------------------

#[tokio::test]
async fn heartbeat_lane_uses_own_connection() {
    let stub = Arc::new(StubHandler::default());
    let server = start_server(stub.clone(), |_| {}).await;
    let stats = server.stats();
    let node = BasicNode::new(server.local_addr().to_string());
    let mut factory = TcpNetworkFactory::new(config(CLUSTER, CLIENT_NODE));
    let mut replication = factory.new_client(SERVER_NODE, &node).await;
    let mut heartbeat = factory.new_heartbeat_client(SERVER_NODE, &node).await;
    assert_eq!(replication.lane(), Lane::Replication);
    assert_eq!(heartbeat.lane(), Lane::Heartbeat);

    assert!(replication.vote(vote_req(), opt()).await.unwrap().vote_granted);
    assert!(heartbeat.vote(vote_req(), opt()).await.unwrap().vote_granted);
    assert!(heartbeat.pre_vote(vote_req(), opt()).await.unwrap().vote_granted);
    assert!(replication.pre_vote(vote_req(), opt()).await.unwrap().vote_granted);

    // Exactly one connection per lane, however many calls each made.
    assert_eq!(stats.accepted(Lane::Replication), 1);
    assert_eq!(stats.accepted(Lane::Heartbeat), 1);
    assert_eq!(stats.accepted(Lane::Snapshot), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn heartbeat_lane_pipelines_like_replication() {
    let stub = Arc::new(StubHandler::with_appends(Duration::from_millis(10), None));
    let server = start_server(stub, |_| {}).await;
    let stats = server.stats();
    let mut peer = client(&server, config(CLUSTER, CLIENT_NODE), Lane::Heartbeat).await;

    let reqs: Vec<_> = (0..12).map(append_req).collect();
    let mut out = peer
        .stream_append(futures::stream::iter(reqs), opt())
        .await
        .expect("stream established");
    let mut seen = Vec::new();
    while let Some(item) = out.next().await {
        seen.push(echoed(item.expect("response")));
    }
    assert_eq!(seen, (0..12).collect::<Vec<u64>>(), "responses in request order");
    assert!(stats.max_inflight_observed.load(Ordering::Relaxed) >= 2, "pipelined");
    assert_eq!(stats.accepted(Lane::Heartbeat), 1);
    assert_eq!(stats.accepted(Lane::Replication), 0);
    server.shutdown().await;
}

#[tokio::test]
async fn slow_install_is_not_cut_off_by_the_per_frame_idle_timeout() {
    // The install itself (validation of the whole file) takes longer than the per-frame idle
    // bound; the sender must wait for the response under the install bound instead.
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let src = make_file(td.path(), "snap.bin", 64 * 1024);
    let stub = Arc::new(StubHandler {
        install_delay: Duration::from_millis(900),
        ..Default::default()
    });
    let server = start_server(stub.clone(), |cfg| {
        cfg.recv_dir = recv_dir.clone();
        cfg.snapshot_idle_timeout = Duration::from_millis(300);
        cfg.snapshot_chunk_size = 16 * 1024;
        cfg.snapshot_ack_window = 2;
    })
    .await;
    let mut cfg = snap_cfg(CLIENT_NODE, &recv_dir, 16 * 1024, 2, Duration::from_millis(300));
    cfg.snapshot_install_timeout = Duration::from_secs(10);
    let mut peer = client(&server, cfg, Lane::Snapshot).await;

    peer.full_snapshot(snap_vote(), snapshot_of(src), never_cancelled(), opt())
        .await
        .expect("a slow install within the install bound succeeds");
    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_does_not_starve_heartbeat() {
    let td = tempfile::tempdir().unwrap();
    let recv_dir = td.path().join("recv");
    let src = make_file(td.path(), "big.bin", 32 * 1024 * 1024);
    let src_crc = crc32c::crc32c(&std::fs::read(&src.path).unwrap());
    let stub = Arc::new(StubHandler {
        install_delay: Duration::from_millis(300),
        ..Default::default()
    });
    let server = start_server(stub.clone(), |cfg| cfg.recv_dir = recv_dir.clone()).await;
    let stats = server.stats();
    let mut cfg = config(CLUSTER, CLIENT_NODE);
    cfg.recv_dir = recv_dir.clone();

    let mut snapshot_peer = client(&server, cfg.clone(), Lane::Snapshot).await;
    let mut heartbeat_peer = client(&server, cfg, Lane::Heartbeat).await;

    // Heartbeat session: one request every 10 ms, round trip measured per request.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<AppendEntriesRequest<TypeConfig>>();
    let input = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|req| (req, rx))
    })
    .boxed();
    let mut out = heartbeat_peer
        .stream_append(input, opt())
        .await
        .expect("heartbeat stream");

    // Baseline round trip on an otherwise idle server, so the bound below scales with the
    // machine instead of being an absolute wall-clock number.
    let mut baseline = Vec::new();
    for index in 0..10u64 {
        let sent = Instant::now();
        tx.send(append_req(index)).unwrap();
        let item = out.next().await.expect("heartbeat response").expect("accepted");
        assert_eq!(echoed(item), index);
        baseline.push(sent.elapsed());
    }
    baseline.sort();
    let baseline = baseline[baseline.len() / 2];

    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();
    let snapshot_task = tokio::spawn(async move {
        let result = snapshot_peer
            .full_snapshot(snap_vote(), snapshot_of(src), never_cancelled(), opt())
            .await;
        done_flag.store(true, Ordering::SeqCst);
        result
    });

    eventually("snapshot connection to be accepted", || {
        stats.accepted(Lane::Snapshot) == 1
    })
    .await;
    const TOTAL_CHUNKS: u64 = 32; // 32 MiB in 1 MiB chunks
    let mut rtts = Vec::new();
    let mut during_chunks = 0usize;
    let mut index = 100u64;
    while !done.load(Ordering::SeqCst) {
        let sent = Instant::now();
        tx.send(append_req(index)).unwrap();
        let item = out.next().await.expect("heartbeat response").expect("accepted");
        assert_eq!(echoed(item), index);
        rtts.push(sent.elapsed());
        // Structural evidence of overlap: a heartbeat answered while chunks were still arriving.
        let chunks = stats.snapshot_chunks.load(Ordering::Relaxed);
        if chunks > 0 && chunks < TOTAL_CHUNKS {
            during_chunks += 1;
        }
        index += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    snapshot_task.await.unwrap().expect("snapshot installed");

    assert_eq!(stub.install_calls.load(Ordering::SeqCst), 1);
    {
        let installed = stub.installed.lock().unwrap();
        assert_eq!(installed[0].len, 32 * 1024 * 1024);
        assert_eq!(installed[0].crc, src_crc, "32 MiB arrived intact");
    }
    // Structural fact: the lanes used distinct connections.
    assert_eq!(stats.accepted(Lane::Heartbeat), 1);
    assert_eq!(stats.accepted(Lane::Snapshot), 1);
    assert_eq!(stats.accepted(Lane::Replication), 0);

    // Heartbeats overlapped the snapshot, and were not held up by it: the typical round trip
    // stays within a multiple of the idle baseline (with a floor for scheduler noise). A
    // heartbeat stuck behind the snapshot would wait out the whole 300 ms install on every sample.
    assert!(rtts.len() >= 10, "heartbeats overlapped the snapshot ({} samples)", rtts.len());
    assert!(
        during_chunks >= 1,
        "no heartbeat was answered while snapshot chunks were still arriving"
    );
    rtts.sort();
    let median = rtts[rtts.len() / 2];
    let max = *rtts.last().unwrap();
    let limit = (baseline * 20).max(Duration::from_millis(100));
    assert!(
        median < limit,
        "median heartbeat round trip {median:?} against idle baseline {baseline:?}"
    );
    assert!(max < Duration::from_secs(2), "max heartbeat round trip {max:?}");
    server.shutdown().await;
}
