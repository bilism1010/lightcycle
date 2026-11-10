//! TCP server for the Raft peer transport (decisions D-05, D-06).
//!
//! [`PeerServer`] accepts connections, runs the Hello handshake under a tiny frame cap, widens the
//! cap for the lane the Hello announced, and dispatches decoded frames into an
//! `Arc<dyn RaftHandler>`. The listener is a separate port from the Phase 5 client RPC and has no
//! TLS or authentication (decision D-08, accepted).
//!
//! Defensive rules: any frame decode error, unknown kind, lane mismatch or oversized declared
//! length closes the connection (no resynchronization). All responses of one connection go
//! through a single writer task so concurrent handler futures never interleave bytes.

use std::fs::File;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{BoxStream, SplitSink};
use futures::{SinkExt, StreamExt};
use openraft::errors::{Fatal, RaftError};
use openraft::raft::{AppendEntriesRequest, AppendResult, VoteRequest};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::net::client::TcpNetConfig;
use crate::net::handler::RaftHandler;
use crate::net::protocol::{
    decode_body, decode_snap_chunk, encode_body, AbortOnDrop, RemoteFailure, SnapAck, SnapBegin,
    WireAppendResult, APPEND_REQ, APPEND_RESP, PREVOTE_REQ, PREVOTE_RESP, SNAP_ACK, SNAP_BEGIN,
    SNAP_CHUNK, SNAP_END, SNAP_RESP, VOTE_REQ, VOTE_RESP,
};
use crate::raft::types::{SnapshotFile, TypeConfig};
use crate::wire::frame::{Frame, FrameCodec, FrameError, Lane, KIND_ERROR};
use crate::wire::hello::server_handshake;

/// Frame cap in force until the Hello is accepted.
pub const PRE_HELLO_FRAME_CAP: usize = 4096;
/// Capacity of the per-connection outgoing frame queue.
const OUT_QUEUE: usize = 256;

type ConnSink = SplitSink<Framed<TcpStream, FrameCodec>, Frame>;

/// Server-side counters, readable while the server runs (all relaxed atomics).
#[derive(Debug, Default)]
pub struct ServerStats {
    /// Connections accepted past the handshake, indexed by `Lane as usize`.
    pub accepted_by_lane: [AtomicU64; 3],
    /// AppendEntries request frames received.
    pub append_req_frames: AtomicU64,
    /// AppendEntries response frames sent.
    pub append_resp_frames: AtomicU64,
    /// Largest number of AppendEntries requests received but not yet answered on one connection.
    pub max_inflight_observed: AtomicU64,
    /// Connections closed because of a frame decode error, bad body, unknown kind or lane
    /// violation.
    pub decode_errors: AtomicU64,
    /// Connections that failed the Hello (including silent ones that timed out).
    pub handshake_failures: AtomicU64,
    /// Snapshot chunks accepted (offset and size validated, bytes written).
    pub snapshot_chunks: AtomicU64,
    /// Connections closed on accept because `max_connections` were already open.
    pub connections_rejected: AtomicU64,
    /// Snapshot begins refused because `max_snapshot_receives` were already running.
    pub snapshot_receives_rejected: AtomicU64,
}

impl ServerStats {
    /// Connections accepted on `lane`.
    pub fn accepted(&self, lane: Lane) -> u64 {
        self.accepted_by_lane[lane as usize].load(Ordering::Relaxed)
    }
}

/// A running Raft peer listener.
pub struct PeerServer {
    local_addr: SocketAddr,
    stats: Arc<ServerStats>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl PeerServer {
    /// Bind `addr` (port 0 allowed) and start accepting connections.
    pub async fn bind<A: ToSocketAddrs>(
        addr: A,
        cfg: TcpNetConfig,
        handler: Arc<dyn RaftHandler>,
    ) -> io::Result<PeerServer> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let stats = Arc::new(ServerStats::default());
        let cancel = CancellationToken::new();
        let limits = Limits {
            connections: Arc::new(Semaphore::new(cfg.max_connections.max(1))),
            snapshot_receives: Arc::new(Semaphore::new(cfg.max_snapshot_receives.max(1))),
        };
        let task = tokio::spawn(accept_loop(
            listener,
            Arc::new(cfg),
            handler,
            stats.clone(),
            limits,
            cancel.clone(),
        ));
        Ok(PeerServer {
            local_addr,
            stats,
            cancel,
            task,
        })
    }

    /// The address the listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Shared counters.
    pub fn stats(&self) -> Arc<ServerStats> {
        self.stats.clone()
    }

    /// Stop accepting, close every connection and wait for the accept loop to finish.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        let _ = (&mut self.task).await;
    }
}

impl Drop for PeerServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Server-wide resource caps shared by every connection.
#[derive(Clone)]
struct Limits {
    connections: Arc<Semaphore>,
    snapshot_receives: Arc<Semaphore>,
}

async fn accept_loop(
    listener: TcpListener,
    cfg: Arc<TcpNetConfig>,
    handler: Arc<dyn RaftHandler>,
    stats: Arc<ServerStats>,
    limits: Limits,
    cancel: CancellationToken,
) {
    // Dropping the set on exit aborts every connection task.
    let mut conns: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((socket, _peer)) => {
                    let Ok(slot) = limits.connections.clone().try_acquire_owned() else {
                        // Over the cap: refuse by closing, before any buffer is allocated.
                        count(&stats.connections_rejected);
                        tracing::warn!("peer connection limit reached; dropping new connection");
                        drop(socket);
                        continue;
                    };
                    conns.spawn(serve_connection(
                        socket,
                        cfg.clone(),
                        handler.clone(),
                        stats.clone(),
                        limits.snapshot_receives.clone(),
                        slot,
                        cancel.child_token(),
                    ));
                }
                Err(e) => {
                    // e.g. EMFILE: back off instead of spinning.
                    tracing::warn!(error = %e, "peer accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

fn count(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Record a terminal protocol violation; the caller closes the connection.
fn protocol_violation(stats: &ServerStats, why: &str) {
    tracing::debug!(why, "peer protocol violation; closing connection");
    count(&stats.decode_errors);
}

async fn serve_connection(
    socket: TcpStream,
    cfg: Arc<TcpNetConfig>,
    handler: Arc<dyn RaftHandler>,
    stats: Arc<ServerStats>,
    snapshot_slots: Arc<Semaphore>,
    _connection_slot: OwnedSemaphorePermit,
    cancel: CancellationToken,
) {
    let _ = socket.set_nodelay(true);
    cfg.apply_keepalive(&socket);
    let mut framed = Framed::new(socket, FrameCodec::new(PRE_HELLO_FRAME_CAP));
    let hello = match server_handshake(
        &mut framed,
        cfg.cluster_id,
        cfg.node_id,
        cfg.handshake_timeout,
    )
    .await
    {
        Ok(hello) => hello,
        Err(e) => {
            count(&stats.handshake_failures);
            tracing::debug!(error = %e, "peer handshake failed");
            return;
        }
    };
    let lane = hello.lane;
    count(&stats.accepted_by_lane[lane as usize]);
    framed
        .codec_mut()
        .set_max_frame_length(cfg.lane_frame_cap(lane));
    if lane == Lane::Snapshot {
        run_snapshot_lane(framed, cfg, handler, stats, snapshot_slots, cancel).await;
    } else {
        run_lane(framed, lane, cfg, handler, stats, cancel).await;
    }
}

/// Drain the connection's outgoing queue into the socket; cancels the connection on exit.
///
/// Each frame must be accepted by the socket within `write_timeout`: a peer that stopped reading
/// would otherwise park this task, fill the queue and wedge the whole connection forever.
async fn writer_task(
    mut sink: ConnSink,
    mut rx: mpsc::Receiver<Frame>,
    cancel: CancellationToken,
    write_timeout: Duration,
) {
    let _guard = cancel.drop_guard();
    while let Some(frame) = rx.recv().await {
        match tokio::time::timeout(write_timeout, sink.send(frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => break,
            Err(_) => {
                tracing::debug!("peer stopped reading; closing connection");
                break;
            }
        }
    }
}

/// A vote or pre-vote handler failure. `RaftError` with the default `Infallible` API error can
/// only be `Fatal` (the node has stopped), so the peer is told to back off, as for AppendEntries.
fn vote_failure(e: RaftError<TypeConfig>) -> RemoteFailure {
    match e {
        RaftError::Fatal(f) => RemoteFailure::new(true, f.to_string()),
        RaftError::APIError(never) => match never {},
    }
}

fn error_frame(lane: Lane, req_id: u64, failure: &RemoteFailure) -> Frame {
    Frame::new(KIND_ERROR, lane, req_id, failure.encode())
}

/// An AppendEntries request accepted from the wire and not yet answered. Holding the permits is
/// what bounds the request and byte windows; they are released when the response is sent.
struct Inflight {
    req_id: u64,
    _permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
}

/// The handler-side pipeline of one replication or heartbeat connection, created lazily on the
/// first AppendEntries frame. Requests feed `handler.stream_append`; a responder task pairs each
/// yielded result with the queued request id and writes the response in order.
struct AppendSession {
    req_tx: mpsc::Sender<AppendEntriesRequest<TypeConfig>>,
    ids_tx: mpsc::Sender<Inflight>,
    req_sem: Arc<Semaphore>,
    byte_sem: Arc<Semaphore>,
    /// Size of the byte window in KiB permits.
    byte_total: usize,
    /// Requests received but not yet answered.
    inflight: Arc<AtomicU64>,
    _responder: AbortOnDrop,
}

impl AppendSession {
    fn start(
        cfg: &TcpNetConfig,
        lane: Lane,
        handler: &Arc<dyn RaftHandler>,
        stats: &Arc<ServerStats>,
        out: mpsc::Sender<Frame>,
        cancel: CancellationToken,
    ) -> Self {
        let window = cfg.inflight_requests.max(1);
        let byte_total = (cfg.inflight_bytes / 1024).max(1);
        let (req_tx, req_rx) = mpsc::channel(window);
        let (ids_tx, ids_rx) = mpsc::channel(window);
        let reqs = futures::stream::unfold(req_rx, |mut rx| async move {
            rx.recv().await.map(|req| (req, rx))
        })
        .boxed();
        let results = handler.stream_append(reqs);
        let inflight = Arc::new(AtomicU64::new(0));
        let responder = tokio::spawn(append_responder(
            results,
            ids_rx,
            out,
            lane,
            stats.clone(),
            inflight.clone(),
            cancel,
        ));
        AppendSession {
            req_tx,
            ids_tx,
            req_sem: Arc::new(Semaphore::new(window)),
            byte_sem: Arc::new(Semaphore::new(byte_total)),
            byte_total,
            inflight,
            _responder: AbortOnDrop(responder),
        }
    }
}

/// Write one APPEND_RESP per handler result, in request order. Ends (cancelling the connection)
/// when the handler stream ends or fails; a handler `Fatal` is reported to the peer first.
async fn append_responder(
    mut results: BoxStream<'static, Result<AppendResult<TypeConfig>, Fatal<TypeConfig>>>,
    mut ids: mpsc::Receiver<Inflight>,
    out: mpsc::Sender<Frame>,
    lane: Lane,
    stats: Arc<ServerStats>,
    inflight: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    let _guard = cancel.drop_guard();
    while let Some(item) = results.next().await {
        // The id is queued before the request itself, so it is always already there.
        let Some(entry) = ids.recv().await else { break };
        let frame = match item {
            Ok(result) => match encode_body(&WireAppendResult::from(result)) {
                Ok(body) => Frame::new(APPEND_RESP, lane, entry.req_id, body),
                Err(e) => {
                    let failure = RemoteFailure::new(false, e.to_string());
                    let _ = out.send(error_frame(lane, entry.req_id, &failure)).await;
                    break;
                }
            },
            Err(fatal) => {
                let failure = RemoteFailure::new(true, fatal.to_string());
                let _ = out.send(error_frame(lane, entry.req_id, &failure)).await;
                break;
            }
        };
        if out.send(frame).await.is_err() {
            break;
        }
        count(&stats.append_resp_frames);
        inflight.fetch_sub(1, Ordering::Relaxed);
        // `entry` drops here, releasing its window permits.
    }
}

async fn run_lane(
    framed: Framed<TcpStream, FrameCodec>,
    lane: Lane,
    cfg: Arc<TcpNetConfig>,
    handler: Arc<dyn RaftHandler>,
    stats: Arc<ServerStats>,
    cancel: CancellationToken,
) {
    let (sink, mut stream) = framed.split();
    let (out_tx, out_rx) = mpsc::channel::<Frame>(OUT_QUEUE);
    // Everything spawned for this connection stops when the connection token is cancelled, which
    // the guard below does on every exit path.
    let conn_cancel = cancel.child_token();
    let _conn_guard = conn_cancel.clone().drop_guard();
    let mut writer = AbortOnDrop(tokio::spawn(writer_task(
        sink,
        out_rx,
        conn_cancel.clone(),
        cfg.write_timeout,
    )));
    let unary_slots = Arc::new(Semaphore::new(cfg.inflight_requests.max(1)));
    let mut append: Option<AppendSession> = None;

    loop {
        let next = tokio::select! {
            biased;
            _ = conn_cancel.cancelled() => break,
            next = async {
                if lane == Lane::Heartbeat {
                    // A live peer heartbeats far more often than this; silence means the peer
                    // is gone without a FIN (the keepalive is the second line of defense).
                    tokio::time::timeout(cfg.heartbeat_idle_timeout, stream.next())
                        .await
                        .unwrap_or(None)
                } else {
                    stream.next().await
                }
            } => next,
        };
        let frame = match next {
            None => break,
            Some(Err(FrameError::Io(_))) => break,
            Some(Err(e)) => {
                protocol_violation(&stats, &format!("frame decode: {e}"));
                break;
            }
            Some(Ok(frame)) => frame,
        };
        if frame.header.lane != lane as u8 {
            protocol_violation(&stats, "lane mismatch");
            break;
        }
        let req_id = frame.header.req_id;
        match (frame.header.kind, lane) {
            (VOTE_REQ | PREVOTE_REQ, Lane::Replication | Lane::Heartbeat) => {
                let pre_vote = frame.header.kind == PREVOTE_REQ;
                let Ok(req) = decode_body::<VoteRequest<TypeConfig>>(&frame.body) else {
                    protocol_violation(&stats, "vote body");
                    break;
                };
                let permit = tokio::select! {
                    biased;
                    _ = conn_cancel.cancelled() => break,
                    permit = unary_slots.clone().acquire_owned() => match permit {
                        Ok(p) => p,
                        Err(_) => break,
                    },
                };
                let fut = if pre_vote {
                    handler.pre_vote(req)
                } else {
                    handler.vote(req)
                };
                let resp_kind = if pre_vote { PREVOTE_RESP } else { VOTE_RESP };
                let out = out_tx.clone();
                let task_cancel = conn_cancel.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let result = tokio::select! {
                        biased;
                        _ = task_cancel.cancelled() => return,
                        result = fut => result,
                    };
                    let reply = match result {
                        Ok(resp) => match encode_body(&resp) {
                            Ok(body) => Frame::new(resp_kind, lane, req_id, body),
                            Err(e) => error_frame(
                                lane,
                                req_id,
                                &RemoteFailure::new(false, e.to_string()),
                            ),
                        },
                        Err(e) => error_frame(lane, req_id, &vote_failure(e)),
                    };
                    let _ = out.send(reply).await;
                });
            }
            (APPEND_REQ, Lane::Replication | Lane::Heartbeat) => {
                let Ok(req) = decode_body::<AppendEntriesRequest<TypeConfig>>(&frame.body) else {
                    protocol_violation(&stats, "append body");
                    break;
                };
                let session = append.get_or_insert_with(|| {
                    AppendSession::start(
                        &cfg,
                        lane,
                        &handler,
                        &stats,
                        out_tx.clone(),
                        conn_cancel.clone(),
                    )
                });
                // Request window, then byte window: the reader stops reading the socket (TCP
                // back-pressure) while either is exhausted. A body larger than the whole byte
                // window is admitted alone.
                let kib = frame.body.len().div_ceil(1024).clamp(1, session.byte_total);
                let req_permit = tokio::select! {
                    biased;
                    _ = conn_cancel.cancelled() => break,
                    permit = session.req_sem.clone().acquire_owned() => match permit {
                        Ok(p) => p,
                        Err(_) => break,
                    },
                };
                let byte_permit = tokio::select! {
                    biased;
                    _ = conn_cancel.cancelled() => break,
                    permit = session.byte_sem.clone().acquire_many_owned(kib as u32) => match permit {
                        Ok(p) => p,
                        Err(_) => break,
                    },
                };
                count(&stats.append_req_frames);
                let now = session.inflight.fetch_add(1, Ordering::Relaxed) + 1;
                stats.max_inflight_observed.fetch_max(now, Ordering::Relaxed);
                let entry = Inflight {
                    req_id,
                    _permits: (req_permit, byte_permit),
                };
                // Id first, request second: the responder finds the id waiting for any result.
                let queued = tokio::select! {
                    biased;
                    _ = conn_cancel.cancelled() => false,
                    sent = session.ids_tx.send(entry) => sent.is_ok(),
                };
                if !queued {
                    break;
                }
                let delivered = tokio::select! {
                    biased;
                    _ = conn_cancel.cancelled() => false,
                    sent = session.req_tx.send(req) => sent.is_ok(),
                };
                if !delivered {
                    break;
                }
            }
            // Unknown kind, or a kind that is not valid on this lane (snapshot frames only
            // exist on the snapshot lane, see `run_snapshot_lane`): terminal.
            _ => {
                protocol_violation(&stats, "unknown kind");
                break;
            }
        }
    }

    // The connection is ending. The last frames queued for the peer (a Fatal reason, a Conflict
    // answer) must reach the socket before the writer is stopped, so stop every producer and let
    // the writer drain; aborting it outright makes delivery a scheduling race. A server shutdown
    // (the parent token) skips the courtesy. The wait is bounded for a peer that stopped reading.
    conn_cancel.cancel();
    drop(append);
    drop(out_tx);
    if !cancel.is_cancelled() {
        let _ = tokio::time::timeout(ERROR_SEND_TIMEOUT, &mut writer.0).await;
    }
}

// ---- snapshot lane ---------------------------------------------------------------------------

/// Time allowed for delivering a terminal error frame before the connection is dropped.
const ERROR_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Source of receive file numbers, shared by every server in the process. Receive files are named
/// `recv-<n>.tmp` from this counter only; no peer-supplied string ever forms part of a path.
static NEXT_RECV: AtomicU64 = AtomicU64::new(0);

/// Deletes the receive file when dropped, so every exit path (protocol error, close, idle
/// timeout, task abort, handler failure) removes it. After a successful hand-off the file is
/// normally already gone (the installer renamed it); if it is still there the installer skipped
/// the snapshot as stale or duplicate (a leader change can resend one), and removing it here is
/// what stops that file leaking. The handler only answers `Ok` once an accepted snapshot is
/// fully installed, so this never removes a file that is still needed.
struct TmpGuard {
    path: PathBuf,
}

impl Drop for TmpGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// An active snapshot receive: at most one per connection.
struct Receive {
    /// Held until the receive (including its install) ends.
    _slot: OwnedSemaphorePermit,
    guard: TmpGuard,
    file: Arc<File>,
    vote: openraft::type_config::alias::VoteOf<TypeConfig>,
    meta: openraft::type_config::alias::SnapshotMetaOf<TypeConfig>,
    total_len: u64,
    written: u64,
    chunks_since_ack: usize,
}

/// Create `dir/recv-<n>.tmp` exclusively with mode 0600.
fn create_recv_file(dir: &std::path::Path) -> io::Result<(PathBuf, File)> {
    std::fs::create_dir_all(dir)?;
    // A leftover file from an earlier process makes `create_new` fail; try the next number.
    for _ in 0..1024 {
        let n = NEXT_RECV.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("recv-{n}.tmp"));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "no free receive file name",
    ))
}

/// Write all of `buf` at `offset` without moving a shared file cursor.
fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0usize;
        while done < buf.len() {
            let n = file.seek_write(&buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            done += n;
        }
        Ok(())
    }
}

/// Deliver `frame` within `bound`; false when the peer cannot take it.
async fn send_bounded(
    framed: &mut Framed<TcpStream, FrameCodec>,
    frame: Frame,
    bound: Duration,
) -> bool {
    matches!(tokio::time::timeout(bound, framed.send(frame)).await, Ok(Ok(())))
}

/// Reject a snapshot request: count the violation and tell the peer why, best effort. The caller
/// closes the connection.
async fn snapshot_reject(
    framed: &mut Framed<TcpStream, FrameCodec>,
    stats: &ServerStats,
    req_id: u64,
    fatal: bool,
    why: &str,
) {
    protocol_violation(stats, why);
    let failure = RemoteFailure::new(fatal, why);
    let _ = send_bounded(
        framed,
        error_frame(Lane::Snapshot, req_id, &failure),
        ERROR_SEND_TIMEOUT,
    )
    .await;
}

/// Snapshot-lane state machine (decisions D-11, D-12), one connection, one active receive.
///
/// `SNAP_BEGIN` creates the receive file; every `SNAP_CHUNK` must continue exactly where the file
/// ends and stay within the declared length; `SNAP_END` requires the file to be complete, then
/// hands it to `RaftHandler::install_snapshot`. While a receive is active every wait for the next
/// frame is bounded by `snapshot_idle_timeout`. Any violation closes the connection; there is no
/// resynchronization.
async fn run_snapshot_lane(
    mut framed: Framed<TcpStream, FrameCodec>,
    cfg: Arc<TcpNetConfig>,
    handler: Arc<dyn RaftHandler>,
    stats: Arc<ServerStats>,
    snapshot_slots: Arc<Semaphore>,
    cancel: CancellationToken,
) {
    let window = cfg.snapshot_ack_window.max(1);
    let idle = cfg.snapshot_idle_timeout;
    let mut recv: Option<Receive> = None;

    loop {
        let active = recv.is_some();
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            next = async {
                if active {
                    tokio::time::timeout(idle, framed.next()).await.ok()
                } else {
                    Some(framed.next().await)
                }
            } => next,
        };
        let frame = match next {
            // An active receive that sent nothing for the idle bound.
            None => {
                tracing::debug!("snapshot sender idle; closing connection");
                break;
            }
            Some(None) => break,
            Some(Some(Err(FrameError::Io(_)))) => break,
            Some(Some(Err(e))) => {
                protocol_violation(&stats, &format!("frame decode: {e}"));
                break;
            }
            Some(Some(Ok(frame))) => frame,
        };
        if frame.header.lane != Lane::Snapshot as u8 {
            protocol_violation(&stats, "lane mismatch");
            break;
        }
        let req_id = frame.header.req_id;
        match frame.header.kind {
            SNAP_BEGIN => {
                if recv.is_some() {
                    protocol_violation(&stats, "second snapshot begin");
                    break;
                }
                let Ok(begin) = decode_body::<SnapBegin>(&frame.body) else {
                    protocol_violation(&stats, "snapshot begin body");
                    break;
                };
                if begin.total_len > cfg.max_snapshot_bytes {
                    snapshot_reject(&mut framed, &stats, req_id, false, "snapshot too large")
                        .await;
                    break;
                }
                // A receive claims up to `max_snapshot_bytes` of disk: bound how many run at once.
                let Ok(slot) = snapshot_slots.clone().try_acquire_owned() else {
                    count(&stats.snapshot_receives_rejected);
                    snapshot_reject(
                        &mut framed,
                        &stats,
                        req_id,
                        false,
                        "too many concurrent snapshot receives",
                    )
                    .await;
                    break;
                };
                let dir = cfg.recv_dir.clone();
                let created =
                    tokio::task::spawn_blocking(move || create_recv_file(&dir)).await;
                let Ok(Ok((path, file))) = created else {
                    snapshot_reject(
                        &mut framed,
                        &stats,
                        req_id,
                        false,
                        "cannot create snapshot receive file",
                    )
                    .await;
                    break;
                };
                recv = Some(Receive {
                    _slot: slot,
                    guard: TmpGuard { path },
                    file: Arc::new(file),
                    vote: begin.vote,
                    meta: begin.meta,
                    total_len: begin.total_len,
                    written: 0,
                    chunks_since_ack: 0,
                });
            }
            SNAP_CHUNK => {
                let Some(r) = recv.as_mut() else {
                    protocol_violation(&stats, "snapshot chunk without begin");
                    break;
                };
                let Ok((offset, bytes)) = decode_snap_chunk(&frame.body) else {
                    protocol_violation(&stats, "snapshot chunk body");
                    break;
                };
                let end = offset.checked_add(bytes.len() as u64);
                let within = end.is_some_and(|end| end <= r.total_len);
                if offset != r.written || bytes.is_empty() || !within {
                    snapshot_reject(&mut framed, &stats, req_id, false, "bad snapshot chunk")
                        .await;
                    break;
                }
                let file = r.file.clone();
                let write =
                    tokio::task::spawn_blocking(move || pwrite_all(&file, &bytes, offset)).await;
                if !matches!(write, Ok(Ok(()))) {
                    snapshot_reject(
                        &mut framed,
                        &stats,
                        req_id,
                        false,
                        "snapshot write failed",
                    )
                    .await;
                    break;
                }
                r.written = end.expect("checked above");
                r.chunks_since_ack += 1;
                count(&stats.snapshot_chunks);
                if r.chunks_since_ack >= window {
                    r.chunks_since_ack = 0;
                    let Ok(body) = encode_body(&SnapAck { written: r.written }) else {
                        break;
                    };
                    let ack = Frame::new(SNAP_ACK, Lane::Snapshot, req_id, body);
                    if !send_bounded(&mut framed, ack, idle).await {
                        break;
                    }
                }
            }
            SNAP_END => {
                let Some(r) = recv.take() else {
                    protocol_violation(&stats, "snapshot end without begin");
                    break;
                };
                if r.written != r.total_len {
                    snapshot_reject(&mut framed, &stats, req_id, false, "incomplete snapshot")
                        .await;
                    break;
                }
                let file = r.file.clone();
                let synced = tokio::task::spawn_blocking(move || file.sync_all()).await;
                if !matches!(synced, Ok(Ok(()))) {
                    snapshot_reject(&mut framed, &stats, req_id, false, "snapshot sync failed")
                        .await;
                    break;
                }
                let Receive {
                    guard,
                    vote,
                    meta,
                    total_len,
                    ..
                } = r;
                let installed = handler.install_snapshot(
                    vote,
                    meta,
                    SnapshotFile {
                        path: guard.path.clone(),
                        len: total_len,
                    },
                );
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    result = installed => result,
                };
                match result {
                    Ok(resp) => {
                        // Installed (file renamed away) or skipped as stale: either way the
                        // receive file is done with, and `guard` removes any remainder.
                        drop(guard);
                        let Ok(body) = encode_body(&resp) else { break };
                        let reply = Frame::new(SNAP_RESP, Lane::Snapshot, req_id, body);
                        if !send_bounded(&mut framed, reply, idle).await {
                            break;
                        }
                    }
                    Err(fatal) => {
                        // `guard` drops here and removes the file.
                        drop(guard);
                        let failure = RemoteFailure::new(true, fatal.to_string());
                        let _ = send_bounded(
                            &mut framed,
                            error_frame(Lane::Snapshot, req_id, &failure),
                            ERROR_SEND_TIMEOUT,
                        )
                        .await;
                        break;
                    }
                }
            }
            _ => {
                protocol_violation(&stats, "unknown kind");
                break;
            }
        }
    }
    // `recv`, if still set, drops here and removes its file.
}
