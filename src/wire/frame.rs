//! Length-prefixed frame codec shared by the peer transport, snapshot files and (later) client RPC.
//!
//! Wire layout of one frame:
//!
//! ```text
//! +----------------+-------+-------+-------+----------------+----------------+
//! | length: u32 BE | kind  | flags | lane  | req_id: u64 BE | body ...       |
//! |   (4 bytes)    | (1)   | (1)   | (1)   |   (8 bytes)    | (length - 11)  |
//! +----------------+-------+-------+-------+----------------+----------------+
//! ```
//!
//! `length` counts the 11-byte header plus the body (not the prefix itself). It is capped by
//! `max_frame_length`; a declared length above the cap is rejected from the 4-byte prefix alone,
//! before any body byte is buffered.
//!
//! Kinds 0-15 are reserved by the wire module ([`KIND_HELLO`], [`KIND_HELLO_ACK`],
//! [`KIND_ERROR`]); application kinds start at [`KIND_APP_BASE`].
//!
//! A decode error is terminal for the connection: there is no resynchronization, so callers must
//! close the connection on any [`FrameError`].

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// Size of the length prefix in bytes.
pub const LENGTH_PREFIX_LEN: usize = 4;
/// Size of the fixed frame header in bytes (kind, flags, lane, req_id).
pub const HEADER_LEN: usize = 11;

/// Handshake request frame kind.
pub const KIND_HELLO: u8 = 1;
/// Handshake acknowledgement frame kind.
pub const KIND_HELLO_ACK: u8 = 2;
/// Error frame kind (body is a short ASCII reason).
pub const KIND_ERROR: u8 = 3;
/// First frame kind available to applications; 0-15 are reserved by the wire module.
pub const KIND_APP_BASE: u8 = 16;

/// Logical lane a connection or frame belongs to.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    /// Log replication traffic.
    Replication = 0,
    /// Heartbeats and votes.
    Heartbeat = 1,
    /// Snapshot transfer.
    Snapshot = 2,
}

impl TryFrom<u8> for Lane {
    type Error = FrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Lane::Replication),
            1 => Ok(Lane::Heartbeat),
            2 => Ok(Lane::Snapshot),
            other => Err(FrameError::UnknownLane(other)),
        }
    }
}

/// Errors from encoding or decoding frames. Any of these is terminal for the connection.
#[derive(Debug, Error)]
pub enum FrameError {
    /// Underlying I/O failure.
    #[error("frame io error: {0}")]
    Io(#[from] std::io::Error),
    /// Declared frame length exceeds the configured cap.
    #[error("frame too large: declared {declared} bytes, max {max}")]
    TooLarge {
        /// Length declared by the peer (or attempted by the encoder).
        declared: usize,
        /// Configured cap.
        max: usize,
    },
    /// The frame payload is shorter than the fixed header.
    #[error("frame truncated: payload shorter than the {HEADER_LEN}-byte header")]
    Truncated,
    /// A lane byte did not name a known lane.
    #[error("unknown lane byte {0}")]
    UnknownLane(u8),
}

/// The fixed 11-byte header preceding every frame body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Frame kind; 0-15 reserved, application kinds start at 16.
    pub kind: u8,
    /// Kind-specific flag bits.
    pub flags: u8,
    /// Lane byte (see [`Lane`]).
    pub lane: u8,
    /// Request correlation id.
    pub req_id: u64,
}

/// One decoded frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Fixed header.
    pub header: FrameHeader,
    /// Opaque body.
    pub body: Bytes,
}

impl Frame {
    /// Build a frame with zero flags.
    pub fn new(kind: u8, lane: Lane, req_id: u64, body: Bytes) -> Self {
        Frame {
            header: FrameHeader {
                kind,
                flags: 0,
                lane: lane as u8,
                req_id,
            },
            body,
        }
    }
}

/// Codec for [`Frame`]s: a `LengthDelimitedCodec` (4-byte big-endian prefix, explicit cap) plus
/// the 11-byte header. Usable on a `BytesMut` directly or through `tokio_util::codec::Framed`.
#[derive(Debug)]
pub struct FrameCodec {
    inner: LengthDelimitedCodec,
    max_frame_length: usize,
    /// True once the inner codec has consumed a length prefix but not yet produced its frame, so
    /// `src` no longer starts with a prefix and the cap check must not be repeated.
    mid_frame: bool,
}

impl FrameCodec {
    /// Create a codec capping header-plus-body payloads at `max_frame_length` bytes.
    pub fn new(max_frame_length: usize) -> Self {
        let inner = LengthDelimitedCodec::builder()
            .length_field_length(LENGTH_PREFIX_LEN)
            .big_endian()
            .max_frame_length(max_frame_length)
            .new_codec();
        FrameCodec {
            inner,
            max_frame_length,
            mid_frame: false,
        }
    }

    /// Change the cap, e.g. widen it per lane once a Hello is accepted.
    pub fn set_max_frame_length(&mut self, max_frame_length: usize) {
        self.max_frame_length = max_frame_length;
        self.inner.set_max_frame_length(max_frame_length);
    }

    /// Current cap on header-plus-body payload length.
    pub fn max_frame_length(&self) -> usize {
        self.max_frame_length
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        // Check the declared length ourselves so the error carries the declared size and fires
        // from the prefix alone, before the inner codec reserves any body capacity.
        // The inner codec consumes the prefix as soon as it has 4 bytes and then waits for the body,
        // so on a retry with a partial body `src` starts with body bytes, not a prefix.
        let starts_with_prefix = !self.mid_frame && src.len() >= LENGTH_PREFIX_LEN;
        if starts_with_prefix {
            let declared =
                u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
            if declared > self.max_frame_length {
                return Err(FrameError::TooLarge {
                    declared,
                    max: self.max_frame_length,
                });
            }
        }
        let Some(mut payload) = self.inner.decode(src)? else {
            self.mid_frame = self.mid_frame || starts_with_prefix;
            return Ok(None);
        };
        self.mid_frame = false;
        if payload.len() < HEADER_LEN {
            return Err(FrameError::Truncated);
        }
        let kind = payload.get_u8();
        let flags = payload.get_u8();
        let lane = payload.get_u8();
        let req_id = payload.get_u64();
        Ok(Some(Frame {
            header: FrameHeader {
                kind,
                flags,
                lane,
                req_id,
            },
            body: payload.freeze(),
        }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = FrameError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), FrameError> {
        let payload_len = HEADER_LEN + item.body.len();
        if payload_len > self.max_frame_length || payload_len > u32::MAX as usize {
            return Err(FrameError::TooLarge {
                declared: payload_len,
                max: self.max_frame_length,
            });
        }
        // Write prefix, header and body straight into the output buffer: the body is copied once.
        dst.reserve(LENGTH_PREFIX_LEN + payload_len);
        dst.put_u32(payload_len as u32);
        dst.put_u8(item.header.kind);
        dst.put_u8(item.header.flags);
        dst.put_u8(item.header.lane);
        dst.put_u64(item.header.req_id);
        dst.put_slice(&item.body);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use tokio_util::codec::Framed;

    fn sample(kind: u8, req_id: u64, body: &'static [u8]) -> Frame {
        Frame {
            header: FrameHeader {
                kind,
                flags: 0x5a,
                lane: Lane::Heartbeat as u8,
                req_id,
            },
            body: Bytes::from_static(body),
        }
    }

    #[test]
    fn frame_roundtrip_sync() {
        let mut codec = FrameCodec::new(1024);
        let frame = sample(KIND_APP_BASE, 0x0102_0304_0506_0708, b"hello body");
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        assert_eq!(buf.len(), LENGTH_PREFIX_LEN + HEADER_LEN + 10);
        // Length prefix is big-endian and counts header + body.
        assert_eq!(&buf[..4], &21u32.to_be_bytes());
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert!(buf.is_empty());
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[tokio::test]
    async fn frame_roundtrip_over_duplex() {
        let (a, b) = tokio::io::duplex(4096);
        let mut tx = Framed::new(a, FrameCodec::new(1024));
        let mut rx = Framed::new(b, FrameCodec::new(1024));
        let frames = vec![
            sample(KIND_HELLO, 1, b"one"),
            sample(KIND_APP_BASE, 2, b""),
            sample(KIND_APP_BASE + 1, 3, b"three three three"),
        ];
        for f in &frames {
            tx.send(f.clone()).await.unwrap();
        }
        for f in &frames {
            let got = rx.next().await.unwrap().unwrap();
            assert_eq!(&got, f);
        }
    }

    #[test]
    fn frame_split_across_partial_reads_decodes() {
        // Regression: after the prefix was consumed, a retry with a partial body must not
        // re-interpret body bytes as a length prefix.
        let mut enc = FrameCodec::new(1 << 20);
        let frames: Vec<Frame> = (0..4u64)
            .map(|i| Frame::new(KIND_APP_BASE, Lane::Replication, i, Bytes::from(vec![0xFFu8; 300 * (i as usize + 1)])))
            .collect();
        let mut wire = BytesMut::new();
        for f in &frames {
            enc.encode(f.clone(), &mut wire).unwrap();
        }
        for step in [1usize, 3, 7, 100] {
            let mut dec = FrameCodec::new(1 << 20);
            let mut buf = BytesMut::new();
            let mut got = Vec::new();
            for chunk in wire.chunks(step) {
                buf.extend_from_slice(chunk);
                while let Some(f) = dec.decode(&mut buf).unwrap() {
                    got.push(f);
                }
            }
            assert_eq!(got, frames, "step {step}");
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn declared_length_over_cap_rejected_without_buffering() {
        let mut codec = FrameCodec::new(1024 * 1024);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(1u32 << 30).to_be_bytes());
        let cap_before = buf.capacity();
        match codec.decode(&mut buf) {
            Err(FrameError::TooLarge { declared, max }) => {
                assert_eq!(declared, 1 << 30);
                assert_eq!(max, 1024 * 1024);
            }
            other => panic!("expected TooLarge, got {other:?}"),
        }
        assert_eq!(buf.capacity(), cap_before, "buffer must not grow");
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn encode_over_cap_rejected() {
        let mut codec = FrameCodec::new(16);
        let mut buf = BytesMut::new();
        let err = codec
            .encode(sample(KIND_APP_BASE, 1, b"too long body"), &mut buf)
            .unwrap_err();
        assert!(matches!(err, FrameError::TooLarge { .. }));
        assert!(buf.is_empty());
    }

    #[test]
    fn set_max_frame_length_widens_cap() {
        let mut codec = FrameCodec::new(16);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&100u32.to_be_bytes());
        assert!(matches!(
            codec.decode(&mut buf),
            Err(FrameError::TooLarge { .. })
        ));
        codec.set_max_frame_length(1024);
        assert_eq!(codec.max_frame_length(), 1024);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn frame_shorter_than_header_is_truncated() {
        let mut codec = FrameCodec::new(1024);
        let mut buf = BytesMut::new();
        buf.put_u32(5);
        buf.extend_from_slice(&[1, 2, 3, 4, 5]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn lane_try_from_rejects_unknown() {
        assert_eq!(Lane::try_from(0).unwrap(), Lane::Replication);
        assert_eq!(Lane::try_from(1).unwrap(), Lane::Heartbeat);
        assert_eq!(Lane::try_from(2).unwrap(), Lane::Snapshot);
        assert!(matches!(
            Lane::try_from(3),
            Err(FrameError::UnknownLane(3))
        ));
    }
}
