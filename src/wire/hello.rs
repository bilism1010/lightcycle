//! Connection handshake: every connection begins with a fixed-layout [`Hello`].
//!
//! The Hello is encoded by hand, deliberately not through serde, so version negotiation never
//! depends on a serialization library. Layout (23 bytes):
//!
//! ```text
//! magic "LCYC" (4) | wire_version u16 BE | cluster_id u64 BE | node_id u64 BE | lane u8
//! ```
//!
//! The `cluster_id` check only blocks accidental cross-cluster connections. It is NOT a security
//! control: decision D-08 accepts no TLS or authentication in this phase.
//!
//! Callers should cap the connection's frame length at a few KiB until the server handshake
//! succeeds (see `FrameCodec::set_max_frame_length`) and close the connection on any error.

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use super::frame::{
    Frame, FrameCodec, FrameError, Lane, KIND_ERROR, KIND_HELLO, KIND_HELLO_ACK,
};

/// Magic bytes opening every Hello.
pub const MAGIC: [u8; 4] = *b"LCYC";
/// Current wire protocol version.
pub const WIRE_VERSION: u16 = 1;
/// Exact encoded size of a Hello.
pub const HELLO_LEN: usize = 23;
/// Default time allowed for a peer to complete the handshake.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The first message of every connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hello {
    /// Protocol version of the sender.
    pub wire_version: u16,
    /// Cluster the sender belongs to.
    pub cluster_id: u64,
    /// Sender's node id.
    pub node_id: u64,
    /// Lane this connection will carry.
    pub lane: Lane,
}

/// Errors from the connection handshake. The caller must close the connection on any of them.
#[derive(Debug, Error)]
pub enum HandshakeError {
    /// The peer did not complete the handshake in time.
    #[error("handshake timed out")]
    Timeout,
    /// The Hello did not start with the expected magic (or was too short to contain it).
    #[error("bad handshake magic")]
    BadMagic,
    /// The peer speaks a different wire version.
    #[error("wire version mismatch: theirs {theirs}, ours {ours}")]
    VersionMismatch {
        /// Version the peer announced.
        theirs: u16,
        /// Our version.
        ours: u16,
    },
    /// The peer belongs to a different cluster.
    #[error("cluster id mismatch: theirs {theirs}, ours {ours}")]
    ClusterMismatch {
        /// Cluster id the peer announced.
        theirs: u64,
        /// Our cluster id.
        ours: u64,
    },
    /// The peer is a different node than the one dialed (a stale or wrong membership address).
    #[error("node id mismatch: peer identified as node {theirs}, expected node {expected}")]
    NodeMismatch {
        /// Node id the peer announced.
        theirs: u64,
        /// Node id the caller meant to reach.
        expected: u64,
    },
    /// The peer announced this node's own id: a self-dial or a mis-wired membership.
    #[error("peer announced this node's own id {0}")]
    SelfDial(u64),
    /// The first frame was not the expected handshake frame.
    #[error("unexpected frame during handshake")]
    UnexpectedFrame,
    /// The remote end rejected our Hello with the given reason.
    #[error("remote rejected handshake: {0}")]
    Remote(String),
    /// Transport I/O failure (including the peer closing early).
    #[error("handshake io error: {0}")]
    Io(#[from] std::io::Error),
    /// Frame-level decode or encode failure.
    #[error("handshake frame error: {0}")]
    Frame(#[from] FrameError),
}

impl Hello {
    /// Build a Hello with the current [`WIRE_VERSION`].
    pub fn new(cluster_id: u64, node_id: u64, lane: Lane) -> Self {
        Hello {
            wire_version: WIRE_VERSION,
            cluster_id,
            node_id,
            lane,
        }
    }

    /// Encode to the fixed 23-byte layout.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HELLO_LEN);
        buf.put_slice(&MAGIC);
        buf.put_u16(self.wire_version);
        buf.put_u64(self.cluster_id);
        buf.put_u64(self.node_id);
        buf.put_u8(self.lane as u8);
        buf.freeze()
    }

    /// Decode a Hello, checking magic and wire version before the rest of the layout so a
    /// future version with a different layout is reported as a version mismatch.
    pub fn decode(body: &[u8]) -> Result<Hello, HandshakeError> {
        if body.len() < MAGIC.len() + 2 || body[..4] != MAGIC {
            return Err(HandshakeError::BadMagic);
        }
        let wire_version = u16::from_be_bytes([body[4], body[5]]);
        if wire_version != WIRE_VERSION {
            return Err(HandshakeError::VersionMismatch {
                theirs: wire_version,
                ours: WIRE_VERSION,
            });
        }
        if body.len() != HELLO_LEN {
            return Err(HandshakeError::Frame(FrameError::Truncated));
        }
        let cluster_id = u64::from_be_bytes(body[6..14].try_into().expect("8 bytes"));
        let node_id = u64::from_be_bytes(body[14..22].try_into().expect("8 bytes"));
        let lane = Lane::try_from(body[22])?;
        Ok(Hello {
            wire_version,
            cluster_id,
            node_id,
            lane,
        })
    }
}

fn eof() -> HandshakeError {
    HandshakeError::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "connection closed during handshake",
    ))
}

fn reason_of(err: &HandshakeError) -> &'static str {
    match err {
        HandshakeError::BadMagic => "bad magic",
        HandshakeError::VersionMismatch { .. } => "wire version mismatch",
        HandshakeError::ClusterMismatch { .. } => "cluster id mismatch",
        HandshakeError::SelfDial(_) => "node id is the server's own",
        HandshakeError::UnexpectedFrame => "unexpected frame",
        _ => "handshake rejected",
    }
}

/// Send one best-effort KIND_ERROR frame; the original rejection is what the caller sees.
async fn send_rejection<T>(framed: &mut Framed<T, FrameCodec>, lane: Lane, err: &HandshakeError)
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let frame = Frame::new(KIND_ERROR, lane, 0, Bytes::from(reason_of(err)));
    let _ = framed.send(frame).await;
}

/// Server side: read the first frame within `timeout`, validate it, and reply with a HelloAck
/// carrying our own Hello (lane echoed). On any rejection one KIND_ERROR frame is sent and the
/// error returned; the caller must then close the connection.
pub async fn server_handshake<T>(
    framed: &mut Framed<T, FrameCodec>,
    expected_cluster_id: u64,
    my_node_id: u64,
    timeout: Duration,
) -> Result<Hello, HandshakeError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let first = match tokio::time::timeout(timeout, framed.next()).await {
        Err(_) => return Err(HandshakeError::Timeout),
        Ok(None) => return Err(eof()),
        Ok(Some(Err(e))) => return Err(e.into()),
        Ok(Some(Ok(frame))) => frame,
    };

    let validated = if first.header.kind != KIND_HELLO {
        Err(HandshakeError::UnexpectedFrame)
    } else {
        Hello::decode(&first.body).and_then(|hello| {
            if hello.cluster_id != expected_cluster_id {
                Err(HandshakeError::ClusterMismatch {
                    theirs: hello.cluster_id,
                    ours: expected_cluster_id,
                })
            } else if hello.node_id == my_node_id {
                Err(HandshakeError::SelfDial(hello.node_id))
            } else {
                Ok(hello)
            }
        })
    };

    let hello = match validated {
        Ok(h) => h,
        Err(err) => {
            let lane = Lane::try_from(first.header.lane).unwrap_or(Lane::Replication);
            send_rejection(framed, lane, &err).await;
            return Err(err);
        }
    };

    let ours = Hello::new(expected_cluster_id, my_node_id, hello.lane);
    let ack = Frame::new(KIND_HELLO_ACK, hello.lane, first.header.req_id, ours.encode());
    framed.send(ack).await?;
    Ok(hello)
}

/// Client side: send `hello`, then wait under `timeout` for a HelloAck (returning the server's
/// Hello) or a KIND_ERROR (surfaced as [`HandshakeError::Remote`]).
pub async fn client_handshake<T>(
    framed: &mut Framed<T, FrameCodec>,
    hello: Hello,
    timeout: Duration,
) -> Result<Hello, HandshakeError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    client_handshake_expecting(framed, hello, None, timeout).await
}

/// [`client_handshake`] that also rejects a server whose Hello names a node other than
/// `expected_node_id` or a lane other than the one requested. Dialing the wrong node of the same
/// cluster would otherwise attribute its votes and acknowledgements to the intended target.
pub async fn client_handshake_expecting<T>(
    framed: &mut Framed<T, FrameCodec>,
    hello: Hello,
    expected_node_id: Option<u64>,
    timeout: Duration,
) -> Result<Hello, HandshakeError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let exchange = async {
        framed
            .send(Frame::new(KIND_HELLO, hello.lane, 0, hello.encode()))
            .await?;
        let reply = match framed.next().await {
            None => return Err(eof()),
            Some(Err(e)) => return Err(HandshakeError::from(e)),
            Some(Ok(frame)) => frame,
        };
        match reply.header.kind {
            KIND_HELLO_ACK => {
                let theirs = Hello::decode(&reply.body)?;
                if theirs.cluster_id != hello.cluster_id {
                    return Err(HandshakeError::ClusterMismatch {
                        theirs: theirs.cluster_id,
                        ours: hello.cluster_id,
                    });
                }
                if let Some(expected) = expected_node_id {
                    if theirs.node_id != expected {
                        return Err(HandshakeError::NodeMismatch {
                            theirs: theirs.node_id,
                            expected,
                        });
                    }
                }
                if expected_node_id.is_some() && theirs.lane != hello.lane {
                    return Err(HandshakeError::UnexpectedFrame);
                }
                Ok(theirs)
            }
            KIND_ERROR => Err(HandshakeError::Remote(
                String::from_utf8_lossy(&reply.body).into_owned(),
            )),
            _ => Err(HandshakeError::UnexpectedFrame),
        }
    };
    match tokio::time::timeout(timeout, exchange).await {
        Ok(result) => result,
        Err(_) => Err(HandshakeError::Timeout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, DuplexStream};

    const T: Duration = Duration::from_secs(2);

    fn pair() -> (
        Framed<DuplexStream, FrameCodec>,
        Framed<DuplexStream, FrameCodec>,
    ) {
        let (a, b) = duplex(4096);
        (
            Framed::new(a, FrameCodec::new(4096)),
            Framed::new(b, FrameCodec::new(4096)),
        )
    }

    #[tokio::test]
    async fn handshake_ok_exchanges_node_ids_and_lane() {
        let (mut c, mut s) = pair();
        let hello = Hello::new(42, 7, Lane::Snapshot);
        let (cr, sr) = tokio::join!(
            client_handshake(&mut c, hello, T),
            server_handshake(&mut s, 42, 9, T)
        );
        let server_view = cr.unwrap();
        let client_view = sr.unwrap();
        assert_eq!(client_view, hello);
        assert_eq!(server_view.node_id, 9);
        assert_eq!(server_view.cluster_id, 42);
        assert_eq!(server_view.lane, Lane::Snapshot);
        assert_eq!(server_view.wire_version, WIRE_VERSION);
    }

    #[tokio::test]
    async fn client_rejects_a_server_that_is_not_the_expected_node() {
        let (mut c, mut s) = pair();
        let hello = Hello::new(42, 7, Lane::Replication);
        let (cr, _sr) = tokio::join!(
            client_handshake_expecting(&mut c, hello, Some(3), T),
            server_handshake(&mut s, 42, 9, T)
        );
        assert!(
            matches!(cr, Err(HandshakeError::NodeMismatch { theirs: 9, expected: 3 })),
            "{cr:?}"
        );

        let (mut c, mut s) = pair();
        let (cr, _sr) = tokio::join!(
            client_handshake_expecting(&mut c, hello, Some(9), T),
            server_handshake(&mut s, 42, 9, T)
        );
        assert_eq!(cr.unwrap().node_id, 9);

        // Without an expectation any node of the cluster is accepted (the plain entry point).
        let (mut c, mut s) = pair();
        let (cr, _sr) = tokio::join!(
            client_handshake(&mut c, hello, T),
            server_handshake(&mut s, 42, 9, T)
        );
        assert_eq!(cr.unwrap().node_id, 9);
    }

    #[tokio::test]
    async fn rejects_bad_magic() {
        let (mut c, mut s) = pair();
        let mut body = BytesMut::from(&Hello::new(1, 1, Lane::Replication).encode()[..]);
        body[0] = b'X';
        c.send(Frame::new(KIND_HELLO, Lane::Replication, 0, body.freeze()))
            .await
            .unwrap();
        let err = server_handshake(&mut s, 1, 2, T).await.unwrap_err();
        assert!(matches!(err, HandshakeError::BadMagic), "{err:?}");
        let reply = c.next().await.unwrap().unwrap();
        assert_eq!(reply.header.kind, KIND_ERROR);
    }

    #[tokio::test]
    async fn rejects_cluster_mismatch() {
        let (mut c, mut s) = pair();
        let hello = Hello::new(1, 7, Lane::Replication);
        let (cr, sr) = tokio::join!(
            client_handshake(&mut c, hello, T),
            server_handshake(&mut s, 2, 9, T)
        );
        match sr.unwrap_err() {
            HandshakeError::ClusterMismatch { theirs, ours } => {
                assert_eq!((theirs, ours), (1, 2));
            }
            other => panic!("expected ClusterMismatch, got {other:?}"),
        }
        match cr.unwrap_err() {
            HandshakeError::Remote(reason) => assert_eq!(reason, "cluster id mismatch"),
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_a_peer_that_announces_the_servers_own_id() {
        let (mut c, mut s) = pair();
        let hello = Hello::new(1, 9, Lane::Replication);
        let (cr, sr) = tokio::join!(
            client_handshake(&mut c, hello, T),
            server_handshake(&mut s, 1, 9, T)
        );
        assert!(matches!(sr.unwrap_err(), HandshakeError::SelfDial(9)));
        match cr.unwrap_err() {
            HandshakeError::Remote(reason) => assert_eq!(reason, "node id is the server's own"),
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_future_wire_version() {
        let (mut c, mut s) = pair();
        let mut hello = Hello::new(1, 7, Lane::Heartbeat);
        hello.wire_version = WIRE_VERSION + 1;
        let (cr, sr) = tokio::join!(
            client_handshake(&mut c, hello, T),
            server_handshake(&mut s, 1, 9, T)
        );
        match sr.unwrap_err() {
            HandshakeError::VersionMismatch { theirs, ours } => {
                assert_eq!((theirs, ours), (WIRE_VERSION + 1, WIRE_VERSION));
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
        assert!(matches!(cr.unwrap_err(), HandshakeError::Remote(_)));
    }

    #[tokio::test]
    async fn rejects_non_hello_first_frame() {
        let (mut c, mut s) = pair();
        c.send(Frame::new(16, Lane::Replication, 0, Bytes::new()))
            .await
            .unwrap();
        let err = server_handshake(&mut s, 1, 2, T).await.unwrap_err();
        assert!(matches!(err, HandshakeError::UnexpectedFrame), "{err:?}");
    }

    #[tokio::test]
    async fn times_out_idle_connection() {
        let (_c, mut s) = pair();
        let err = server_handshake(&mut s, 1, 2, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(matches!(err, HandshakeError::Timeout), "{err:?}");
    }

    #[tokio::test]
    async fn client_times_out_when_server_silent() {
        let (mut c, _s) = pair();
        let err = client_handshake(
            &mut c,
            Hello::new(1, 1, Lane::Replication),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, HandshakeError::Timeout), "{err:?}");
    }

    #[test]
    fn hello_roundtrip_fixed_layout() {
        let hello = Hello::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718, Lane::Heartbeat);
        let bytes = hello.encode();
        assert_eq!(bytes.len(), 23);
        assert_eq!(&bytes[..4], b"LCYC");
        assert_eq!(&bytes[4..6], &[0, 1]);
        assert_eq!(bytes[22], 1);
        assert_eq!(Hello::decode(&bytes).unwrap(), hello);
    }

    #[test]
    fn hello_decode_rejects_truncated_and_short() {
        assert!(matches!(Hello::decode(b"LC"), Err(HandshakeError::BadMagic)));
        let bytes = Hello::new(1, 1, Lane::Replication).encode();
        assert!(matches!(
            Hello::decode(&bytes[..20]),
            Err(HandshakeError::Frame(FrameError::Truncated))
        ));
    }
}
