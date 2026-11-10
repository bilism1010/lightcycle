//! TCP client side of the Raft peer transport (decisions D-05, D-07, D-12).
//!
//! [`TcpNetworkFactory`] hands openraft one [`PeerClient`] per lane and target (interpretation
//! A4): `new_client` is the replication lane, `new_heartbeat_client` the heartbeat lane and
//! `new_snapshot_client` the snapshot lane. Each `PeerClient` lazily dials its own TCP connection
//! on first use, so a bulk lane can never head-of-line block the heartbeat lane.
//!
//! Error mapping (matches the fake network): connect, EOF and socket errors drop the connection
//! and return `RPCError::Unreachable`; timeouts return `RPCError::Timeout`; codec, frame and
//! unexpected-kind errors drop the connection and return `RPCError::Network`; a handler failure
//! reported by the peer is `Network` (the connection survives) or, when the remote node has
//! stopped, `Unreachable`. After any error the connection is dropped and the next call
//! reconnects; no resynchronization is ever attempted.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, SplitSink, SplitStream};
use futures::{SinkExt, Stream, StreamExt};
use openraft::errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RPCTypes};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::{BasicNode, OptionalSend, RaftNetworkFactory, RaftNetworkV2};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::codec::Framed;

use crate::net::protocol::{
    decode_body, encode_body, AbortOnDrop, ProtocolError, RemoteFailure, WireAppendResult,
    APPEND_REQ, APPEND_RESP, PREVOTE_REQ, PREVOTE_RESP, VOTE_REQ, VOTE_RESP,
};
use crate::raft::types::{SnapshotFile, TypeConfig};
use crate::wire::frame::{Frame, FrameCodec, FrameError, Lane, HEADER_LEN, KIND_ERROR};
use crate::wire::hello::{client_handshake, HandshakeError, Hello};

/// Frame cap in force until the server's HelloAck has arrived.
const PRE_HELLO_FRAME_CAP: usize = 4096;
/// Slack added to the snapshot chunk size for the snapshot lane's frame cap.
const SNAPSHOT_FRAME_SLACK: usize = 64 * 1024;

/// Settings shared by the client factory and the server.
#[derive(Debug, Clone)]
pub struct TcpNetConfig {
    /// Cluster this node belongs to; mismatching Hellos are rejected.
    pub cluster_id: u64,
    /// This node's id (announced in the Hello).
    pub node_id: u64,
    /// Frame cap for the replication and heartbeat lanes (default 32 MiB).
    pub max_frame_len: usize,
    /// Time allowed for the Hello exchange (default 5 s).
    pub handshake_timeout: Duration,
    /// Time allowed for establishing the TCP connection (default 2 s).
    pub connect_timeout: Duration,
    /// Maximum AppendEntries requests in flight on one pipelined connection (default 32).
    pub inflight_requests: usize,
    /// Maximum request body bytes in flight on one pipelined connection (default 8 MiB).
    pub inflight_bytes: usize,
    /// Snapshot chunk size (default 1 MiB); used by plan 03-17.
    pub snapshot_chunk_size: usize,
    /// Unacknowledged snapshot chunks allowed in flight (default 4); used by plan 03-17.
    pub snapshot_ack_window: usize,
    /// Snapshot transfer idle timeout (default 30 s); used by plan 03-17.
    pub snapshot_idle_timeout: Duration,
    /// Largest snapshot a receiver accepts, in bytes; used by plan 03-17.
    pub max_snapshot_bytes: u64,
    /// Directory where received snapshot files are written; used by plan 03-17.
    pub recv_dir: PathBuf,
}

impl TcpNetConfig {
    /// A config with the documented defaults.
    pub fn new(cluster_id: u64, node_id: u64, recv_dir: PathBuf) -> Self {
        TcpNetConfig {
            cluster_id,
            node_id,
            max_frame_len: 32 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            inflight_requests: 32,
            inflight_bytes: 8 * 1024 * 1024,
            snapshot_chunk_size: 1024 * 1024,
            snapshot_ack_window: 4,
            snapshot_idle_timeout: Duration::from_secs(30),
            max_snapshot_bytes: 8 * 1024 * 1024 * 1024,
            recv_dir,
        }
    }

    /// Frame cap for an established connection on `lane`: replication and heartbeat use
    /// `max_frame_len`; the snapshot lane only ever carries one chunk plus bookkeeping.
    pub fn lane_frame_cap(&self, lane: Lane) -> usize {
        match lane {
            Lane::Replication | Lane::Heartbeat => self.max_frame_len,
            Lane::Snapshot => self.snapshot_chunk_size + SNAPSHOT_FRAME_SLACK,
        }
    }
}

/// openraft network factory over TCP.
#[derive(Debug, Clone)]
pub struct TcpNetworkFactory {
    cfg: Arc<TcpNetConfig>,
}

impl TcpNetworkFactory {
    /// A factory dialing peers with `cfg`.
    pub fn new(cfg: TcpNetConfig) -> Self {
        TcpNetworkFactory { cfg: Arc::new(cfg) }
    }

    fn client(&self, target: u64, node: &BasicNode, lane: Lane) -> PeerClient {
        PeerClient::new(self.cfg.clone(), target, node.addr.clone(), lane)
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = PeerClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.client(target, node, Lane::Replication)
    }

    async fn new_heartbeat_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.client(target, node, Lane::Heartbeat)
    }

    async fn new_snapshot_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.client(target, node, Lane::Snapshot)
    }
}

fn network_err<E: std::error::Error + 'static>(e: E) -> RPCError<TypeConfig> {
    RPCError::Network(NetworkError::new(&e))
}

fn unreachable_err(msg: impl ToString) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::from_string(msg))
}

/// Map a frame-level failure: socket errors are `Unreachable`, codec errors are `Network`.
fn frame_err(e: FrameError) -> RPCError<TypeConfig> {
    match e {
        FrameError::Io(io) => RPCError::Unreachable(Unreachable::new(&io)),
        other => network_err(other),
    }
}

/// A peer that sent a handler failure frame.
fn remote_failure_err(failure: &RemoteFailure) -> RPCError<TypeConfig> {
    if failure.fatal {
        unreachable_err(format!("remote node stopped: {}", failure.message))
    } else {
        network_err(std::io::Error::other(format!(
            "remote handler error: {}",
            failure.message
        )))
    }
}

/// One directed connection to a peer on one lane.
pub struct PeerClient {
    cfg: Arc<TcpNetConfig>,
    target: u64,
    addr: String,
    lane: Lane,
    conn: Option<Framed<TcpStream, FrameCodec>>,
    next_req_id: u64,
}

impl PeerClient {
    fn new(cfg: Arc<TcpNetConfig>, target: u64, addr: String, lane: Lane) -> Self {
        PeerClient {
            cfg,
            target,
            addr,
            lane,
            conn: None,
            next_req_id: 1,
        }
    }

    /// The lane this client dials.
    pub fn lane(&self) -> Lane {
        self.lane
    }

    fn timeout_err(&self, action: RPCTypes, timeout: Duration) -> RPCError<TypeConfig> {
        RPCError::Timeout(openraft::errors::Timeout {
            action,
            id: self.cfg.node_id,
            target: self.target,
            timeout,
        })
    }

    /// Establish the TCP connection and complete the Hello, if not already connected.
    async fn ensure_conn(&mut self, action: RPCTypes) -> Result<(), RPCError<TypeConfig>> {
        if self.conn.is_some() {
            return Ok(());
        }
        let stream = match tokio::time::timeout(
            self.cfg.connect_timeout,
            TcpStream::connect(self.addr.as_str()),
        )
        .await
        {
            Err(_) => return Err(self.timeout_err(action, self.cfg.connect_timeout)),
            Ok(Err(e)) => return Err(RPCError::Unreachable(Unreachable::new(&e))),
            Ok(Ok(stream)) => stream,
        };
        let _ = stream.set_nodelay(true);
        let mut framed = Framed::new(stream, FrameCodec::new(PRE_HELLO_FRAME_CAP));
        let hello = Hello::new(self.cfg.cluster_id, self.cfg.node_id, self.lane);
        match client_handshake(&mut framed, hello, self.cfg.handshake_timeout).await {
            Ok(_) => {}
            Err(HandshakeError::Timeout) => {
                return Err(self.timeout_err(action, self.cfg.handshake_timeout))
            }
            Err(HandshakeError::Frame(e)) => return Err(frame_err(e)),
            // Socket failures and peer rejections (wrong cluster, version): back off rather than
            // retry immediately, since retrying cannot change the outcome.
            Err(e) => return Err(RPCError::Unreachable(Unreachable::new(&e))),
        }
        framed
            .codec_mut()
            .set_max_frame_length(self.cfg.lane_frame_cap(self.lane));
        self.conn = Some(framed);
        Ok(())
    }

    /// One request/response exchange. The connection is taken out for the duration and only put
    /// back after a well-formed answer, so every error path drops it.
    async fn unary(
        &mut self,
        req_kind: u8,
        resp_kind: u8,
        body: Bytes,
        action: RPCTypes,
        option: &RPCOption,
    ) -> Result<Frame, RPCError<TypeConfig>> {
        self.ensure_conn(action).await?;
        let id = self.next_req_id;
        self.next_req_id = self.next_req_id.wrapping_add(1);
        let lane = self.lane;
        let mut conn = self.conn.take().expect("ensure_conn established a connection");
        let soft_ttl = option.soft_ttl();

        let exchange = async {
            conn.send(Frame::new(req_kind, lane, id, body))
                .await
                .map_err(frame_err)?;
            match conn.next().await {
                None => Err(unreachable_err("connection closed by peer")),
                Some(Err(e)) => Err(frame_err(e)),
                Some(Ok(frame)) => Ok(frame),
            }
        };
        let frame = match tokio::time::timeout(soft_ttl, exchange).await {
            Err(_) => return Err(self.timeout_err(action, soft_ttl)),
            Ok(Err(e)) => return Err(e),
            Ok(Ok(frame)) => frame,
        };

        if frame.header.req_id != id {
            return Err(network_err(std::io::Error::other(format!(
                "response req_id {} does not match request {}",
                frame.header.req_id, id
            ))));
        }
        match frame.header.kind {
            kind if kind == resp_kind => {
                self.conn = Some(conn);
                Ok(frame)
            }
            KIND_ERROR => {
                let failure = RemoteFailure::decode(&frame.body);
                if !failure.fatal {
                    self.conn = Some(conn);
                }
                Err(remote_failure_err(&failure))
            }
            other => Err(network_err(std::io::Error::other(format!(
                "unexpected frame kind {other} in response"
            )))),
        }
    }

    /// Typed vote/pre-vote call.
    async fn vote_call(
        &mut self,
        req_kind: u8,
        resp_kind: u8,
        rpc: VoteRequest<TypeConfig>,
        option: &RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let body = encode_body(&rpc).map_err(network_err::<ProtocolError>)?;
        let frame = self
            .unary(req_kind, resp_kind, body, RPCTypes::Vote, option)
            .await?;
        decode_body(&frame.body).map_err(|e| {
            self.conn = None;
            network_err(e)
        })
    }
}

impl RaftNetworkV2<TypeConfig> for PeerClient {
    type SnapshotData = SnapshotFile;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let body = encode_body(&rpc).map_err(network_err::<ProtocolError>)?;
        let frame = self
            .unary(APPEND_REQ, APPEND_RESP, body, RPCTypes::AppendEntries, &option)
            .await?;
        let wire: WireAppendResult = decode_body(&frame.body).map_err(|e| {
            self.conn = None;
            network_err(e)
        })?;
        Ok(wire.into())
    }

    /// Pipelined AppendEntries over this client's own connection (decision D-07).
    ///
    /// The connection is taken for the session: a writer task sends requests without waiting for
    /// answers, bounded by a request window and a byte window; the returned stream reads the
    /// responses, which arrive in request order on the single TCP connection, and checks each
    /// `req_id` against the oldest unanswered request. Any mismatch, wrong kind, decode failure,
    /// timeout or EOF yields one error item and ends the stream; the connection is dropped and
    /// the next call dials afresh.
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> BoxFuture<
        's,
        Result<
            BoxStream<'s, Result<AppendResult<TypeConfig>, RPCError<TypeConfig>>>,
            RPCError<TypeConfig>,
        >,
    >
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
    {
        Box::pin(async move {
            self.ensure_conn(RPCTypes::AppendEntries).await?;
            let framed = self
                .conn
                .take()
                .expect("ensure_conn established a connection");
            let (sink, stream) = framed.split();
            let byte_total = (self.cfg.inflight_bytes / 1024).max(1);
            let (pending_tx, pending_rx) = mpsc::unbounded_channel();
            let writer = tokio::spawn(append_writer(
                input,
                sink,
                pending_tx,
                Arc::new(Semaphore::new(self.cfg.inflight_requests.max(1))),
                Arc::new(Semaphore::new(byte_total)),
                byte_total,
                self.lane,
                self.cfg.lane_frame_cap(self.lane),
            ));
            let reader = ResponseReader {
                stream,
                pending: pending_rx,
                soft_ttl: option.soft_ttl(),
                me: self.cfg.node_id,
                target: self.target,
                _writer: AbortOnDrop(writer),
            };
            let out = futures::stream::unfold(Some(reader), |reader| async move {
                let mut reader = reader?;
                match reader.next_response().await {
                    Step::Response(result) => Some((Ok(result), Some(reader))),
                    Step::End => None,
                    Step::Failed(e) => Some((Err(e), None)),
                }
            })
            .boxed();
            Ok(out)
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.vote_call(VOTE_REQ, VOTE_RESP, rpc, &option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.vote_call(PREVOTE_REQ, PREVOTE_RESP, rpc, &option).await
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // The chunked snapshot lane is delivered by plan 03-17.
        Err(StreamingError::Unreachable(Unreachable::from_string(
            "snapshot transfer not implemented yet",
        )))
    }
}

/// A request handed to the wire (or a writer failure) as seen by the response reader.
enum Pending {
    /// Request `req_id` was sent; its window permits are released when its response arrives.
    Sent {
        req_id: u64,
        permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
    },
    /// The writer could not produce a request (for example it exceeds the frame cap).
    Failed(RPCError<TypeConfig>),
}

/// Send requests from `input` without waiting for answers, within the request and byte windows.
///
/// A request's id is queued for the reader before its frame is written, so a response can never
/// outrun its id. The task ends when `input` ends, a write fails (the reader then sees the dead
/// socket) or the owning stream is dropped (the task is aborted).
#[allow(clippy::too_many_arguments)]
async fn append_writer<S>(
    mut input: S,
    mut sink: SplitSink<Framed<TcpStream, FrameCodec>, Frame>,
    pending: mpsc::UnboundedSender<Pending>,
    req_sem: Arc<Semaphore>,
    byte_sem: Arc<Semaphore>,
    byte_total: usize,
    lane: Lane,
    frame_cap: usize,
) where
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Unpin,
{
    let mut next_id = 1u64;
    while let Some(req) = input.next().await {
        let body = match encode_body(&req) {
            Ok(body) => body,
            Err(e) => {
                let _ = pending.send(Pending::Failed(network_err(e)));
                return;
            }
        };
        if HEADER_LEN + body.len() > frame_cap {
            let too_large = FrameError::TooLarge {
                declared: HEADER_LEN + body.len(),
                max: frame_cap,
            };
            let _ = pending.send(Pending::Failed(network_err(too_large)));
            return;
        }
        // A body larger than the whole byte window is admitted alone.
        let kib = body.len().div_ceil(1024).clamp(1, byte_total);
        let Ok(req_permit) = req_sem.clone().acquire_owned().await else {
            return;
        };
        let Ok(byte_permit) = byte_sem.clone().acquire_many_owned(kib as u32).await else {
            return;
        };
        let req_id = next_id;
        next_id += 1;
        let sent = Pending::Sent {
            req_id,
            permits: (req_permit, byte_permit),
        };
        if pending.send(sent).is_err() {
            return;
        }
        if sink
            .send(Frame::new(APPEND_REQ, lane, req_id, body))
            .await
            .is_err()
        {
            return;
        }
    }
}

enum Step {
    Response(AppendResult<TypeConfig>),
    End,
    Failed(RPCError<TypeConfig>),
}

/// Read half of a pipelined session.
struct ResponseReader {
    stream: SplitStream<Framed<TcpStream, FrameCodec>>,
    pending: mpsc::UnboundedReceiver<Pending>,
    soft_ttl: Duration,
    me: u64,
    target: u64,
    /// Aborts the writer when the response stream is dropped.
    _writer: AbortOnDrop,
}

impl ResponseReader {
    /// The next in-order response, or the end of the session.
    async fn next_response(&mut self) -> Step {
        // Waiting for the writer to queue the next request is not timed: an idle session is
        // healthy. The stream ends when the writer is done and everything sent was answered.
        let (req_id, _permits) = match self.pending.recv().await {
            None => return Step::End,
            Some(Pending::Failed(e)) => return Step::Failed(e),
            Some(Pending::Sent { req_id, permits }) => (req_id, permits),
        };
        let frame = match tokio::time::timeout(self.soft_ttl, self.stream.next()).await {
            Err(_) => {
                return Step::Failed(RPCError::Timeout(openraft::errors::Timeout {
                    action: RPCTypes::AppendEntries,
                    id: self.me,
                    target: self.target,
                    timeout: self.soft_ttl,
                }))
            }
            Ok(None) => return Step::Failed(unreachable_err("connection closed by peer")),
            Ok(Some(Err(e))) => return Step::Failed(frame_err(e)),
            Ok(Some(Ok(frame))) => frame,
        };
        if frame.header.req_id != req_id {
            return Step::Failed(network_err(std::io::Error::other(format!(
                "response req_id {} does not match oldest unanswered request {}",
                frame.header.req_id, req_id
            ))));
        }
        match frame.header.kind {
            APPEND_RESP => match decode_body::<WireAppendResult>(&frame.body) {
                Ok(wire) => Step::Response(wire.into()),
                Err(e) => Step::Failed(network_err(e)),
            },
            KIND_ERROR => Step::Failed(remote_failure_err(&RemoteFailure::decode(&frame.body))),
            other => Step::Failed(network_err(std::io::Error::other(format!(
                "unexpected frame kind {other} in append response"
            )))),
        }
    }
}
