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

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use openraft::raft::VoteRequest;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::net::client::TcpNetConfig;
use crate::net::handler::RaftHandler;
use crate::net::protocol::{
    decode_body, encode_body, AbortOnDrop, RemoteFailure, PREVOTE_REQ, PREVOTE_RESP, VOTE_REQ,
    VOTE_RESP,
};
use crate::raft::types::TypeConfig;
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
    /// Snapshot chunks received (written by plan 03-17).
    pub snapshot_chunks: AtomicU64,
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
        let task = tokio::spawn(accept_loop(
            listener,
            Arc::new(cfg),
            handler,
            stats.clone(),
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

async fn accept_loop(
    listener: TcpListener,
    cfg: Arc<TcpNetConfig>,
    handler: Arc<dyn RaftHandler>,
    stats: Arc<ServerStats>,
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
                    conns.spawn(serve_connection(
                        socket,
                        cfg.clone(),
                        handler.clone(),
                        stats.clone(),
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

async fn serve_connection(
    socket: TcpStream,
    cfg: Arc<TcpNetConfig>,
    handler: Arc<dyn RaftHandler>,
    stats: Arc<ServerStats>,
    cancel: CancellationToken,
) {
    let _ = socket.set_nodelay(true);
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
    run_lane(framed, lane, cfg, handler, stats, cancel).await;
}

/// Drain the connection's outgoing queue into the socket; cancels the connection on exit.
async fn writer_task(mut sink: ConnSink, mut rx: mpsc::Receiver<Frame>, cancel: CancellationToken) {
    let _guard = cancel.drop_guard();
    while let Some(frame) = rx.recv().await {
        if sink.send(frame).await.is_err() {
            break;
        }
    }
}

fn error_frame(lane: Lane, req_id: u64, failure: &RemoteFailure) -> Frame {
    Frame::new(KIND_ERROR, lane, req_id, failure.encode())
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
    let _writer = AbortOnDrop(tokio::spawn(writer_task(sink, out_rx, conn_cancel.clone())));
    let unary_slots = Arc::new(Semaphore::new(cfg.inflight_requests.max(1)));

    loop {
        let next = tokio::select! {
            biased;
            _ = conn_cancel.cancelled() => break,
            next = stream.next() => next,
        };
        let frame = match next {
            None => break,
            Some(Err(FrameError::Io(_))) => break,
            Some(Err(e)) => {
                tracing::debug!(error = %e, "peer frame decode failed; closing");
                count(&stats.decode_errors);
                break;
            }
            Some(Ok(frame)) => frame,
        };
        if frame.header.lane != lane as u8 {
            count(&stats.decode_errors);
            break;
        }
        let req_id = frame.header.req_id;
        match (frame.header.kind, lane) {
            (VOTE_REQ | PREVOTE_REQ, Lane::Replication | Lane::Heartbeat) => {
                let pre_vote = frame.header.kind == PREVOTE_REQ;
                let Ok(req) = decode_body::<VoteRequest<TypeConfig>>(&frame.body) else {
                    count(&stats.decode_errors);
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
                        Err(e) => error_frame(
                            lane,
                            req_id,
                            &RemoteFailure::new(false, e.to_string()),
                        ),
                    };
                    let _ = out.send(reply).await;
                });
            }
            // Unknown kind, or a kind that is not valid on this lane (snapshot frames are
            // handled by plan 03-17): terminal.
            _ => {
                count(&stats.decode_errors);
                break;
            }
        }
    }
}
