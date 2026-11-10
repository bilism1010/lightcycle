//! Application frame kinds and message bodies of the Raft peer transport (decision D-05).
//!
//! Frame kinds below [`KIND_APP_BASE`](crate::wire::frame::KIND_APP_BASE) (16) belong to the wire
//! module (Hello, HelloAck, Error); everything here starts at 16. Bodies are postcard encodings of
//! the openraft request and response types (openraft's `serde` feature), or the small structs
//! defined below. `req_id` in the frame header pairs a response with its request.
//!
//! The kinds and bodies defined here are the cluster-compatibility contract: changing them
//! requires a `wire_version` bump in the Hello (see `wire::hello`).
//!
//! openraft's `AppendResult` does not implement serde (a transport defines its own wire format),
//! so [`WireAppendResult`] is that format and carries the conflict hint.
//!
//! Failures of the remote handler travel in a `KIND_ERROR` frame with the request's `req_id`, the
//! body being a [`RemoteFailure`].

use bytes::{BufMut, Bytes, BytesMut};
use openraft::raft::{AppendError, AppendResult, ConflictHint};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, VoteOf};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinHandle;

use crate::raft::types::TypeConfig;

/// AppendEntries request (postcard `AppendEntriesRequest`).
pub const APPEND_REQ: u8 = 16;
/// AppendEntries response (postcard [`WireAppendResult`]).
pub const APPEND_RESP: u8 = 17;
/// Vote request (postcard `VoteRequest`).
pub const VOTE_REQ: u8 = 18;
/// Vote response (postcard `VoteResponse`).
pub const VOTE_RESP: u8 = 19;
/// Pre-Vote request (postcard `VoteRequest`).
pub const PREVOTE_REQ: u8 = 20;
/// Pre-Vote response (postcard `VoteResponse`).
pub const PREVOTE_RESP: u8 = 21;
/// Snapshot transfer start (postcard [`SnapBegin`]); handled by plan 03-17.
pub const SNAP_BEGIN: u8 = 22;
/// Snapshot chunk ([`encode_snap_chunk`]); handled by plan 03-17.
pub const SNAP_CHUNK: u8 = 23;
/// Snapshot chunk acknowledgement (postcard [`SnapAck`]); handled by plan 03-17.
pub const SNAP_ACK: u8 = 24;
/// Snapshot transfer end (empty body); handled by plan 03-17.
pub const SNAP_END: u8 = 25;
/// Snapshot install response (postcard `SnapshotResponse`); handled by plan 03-17.
pub const SNAP_RESP: u8 = 26;

/// Longest remote error message carried in a [`RemoteFailure`].
pub const MAX_REMOTE_MESSAGE: usize = 256;

/// Errors from encoding or decoding a message body.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// postcard could not encode or decode the body.
    #[error("body codec error: {0}")]
    Codec(#[from] postcard::Error),
    /// Bytes were left over after a complete message was decoded.
    #[error("{0} trailing bytes after message body")]
    Trailing(usize),
    /// A fixed-layout body was shorter than its layout.
    #[error("message body truncated")]
    Truncated,
}

/// Encode `value` as a postcard frame body.
pub fn encode_body<T: Serialize>(value: &T) -> Result<Bytes, ProtocolError> {
    Ok(Bytes::from(postcard::to_stdvec(value)?))
}

/// Decode a postcard frame body, rejecting trailing bytes.
pub fn decode_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, ProtocolError> {
    let (value, rest) = postcard::take_from_bytes(body)?;
    if !rest.is_empty() {
        return Err(ProtocolError::Trailing(rest.len()));
    }
    Ok(value)
}

/// Wire form of an AppendEntries outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireAppendResult {
    /// The follower accepted entries up to this log id.
    Accepted(Option<LogIdOf<TypeConfig>>),
    /// The follower's log does not hold the request's `prev_log_id`.
    Conflict {
        /// `prev_log_id` of the rejected request.
        log_id: LogIdOf<TypeConfig>,
        /// The follower's conflict hint, if it produced one.
        hint: Option<Vec<LogIdOf<TypeConfig>>>,
    },
    /// The follower holds a higher vote.
    HigherVote(VoteOf<TypeConfig>),
}

impl From<AppendResult<TypeConfig>> for WireAppendResult {
    fn from(result: AppendResult<TypeConfig>) -> Self {
        match result {
            Ok(log_id) => WireAppendResult::Accepted(log_id),
            Err(AppendError::Conflict { log_id, hint }) => WireAppendResult::Conflict {
                log_id,
                hint: hint.map(|h| h.log_ids),
            },
            Err(AppendError::HigherVote(vote)) => WireAppendResult::HigherVote(vote),
        }
    }
}

impl From<WireAppendResult> for AppendResult<TypeConfig> {
    fn from(wire: WireAppendResult) -> Self {
        match wire {
            WireAppendResult::Accepted(log_id) => Ok(log_id),
            WireAppendResult::Conflict { log_id, hint } => Err(AppendError::Conflict {
                log_id,
                hint: hint.map(ConflictHint::new),
            }),
            WireAppendResult::HigherVote(vote) => Err(AppendError::HigherVote(vote)),
        }
    }
}

/// Body of a `KIND_ERROR` response: the remote handler failed.
///
/// Layout: one byte (1 when the remote node is fatally stopped, else 0) followed by a UTF-8
/// message of at most [`MAX_REMOTE_MESSAGE`] bytes. Decoding never fails; a malformed message is
/// replaced lossily.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFailure {
    /// The remote node has stopped (openraft `Fatal`); the caller should back off.
    pub fatal: bool,
    /// Human-readable reason, truncated to [`MAX_REMOTE_MESSAGE`] bytes.
    pub message: String,
}

impl RemoteFailure {
    /// A failure with the message truncated at a character boundary.
    pub fn new(fatal: bool, message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > MAX_REMOTE_MESSAGE {
            let mut end = MAX_REMOTE_MESSAGE;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        RemoteFailure { fatal, message }
    }

    /// Encode to the fixed layout.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + self.message.len());
        buf.put_u8(u8::from(self.fatal));
        buf.put_slice(self.message.as_bytes());
        buf.freeze()
    }

    /// Decode, tolerating any byte sequence.
    pub fn decode(body: &[u8]) -> Self {
        let fatal = body.first().is_some_and(|b| *b != 0);
        let text = body.get(1..).unwrap_or(&[]);
        let text = &text[..text.len().min(MAX_REMOTE_MESSAGE)];
        RemoteFailure {
            fatal,
            message: String::from_utf8_lossy(text).into_owned(),
        }
    }
}

/// Opens a snapshot transfer (`SNAP_BEGIN`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapBegin {
    /// The leader's vote.
    pub vote: VoteOf<TypeConfig>,
    /// Metadata of the snapshot being sent.
    pub meta: SnapshotMetaOf<TypeConfig>,
    /// Total snapshot length in bytes.
    pub total_len: u64,
}

/// Acknowledges received snapshot bytes (`SNAP_ACK`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapAck {
    /// Total bytes written by the receiver so far.
    pub written: u64,
}

/// Encode a `SNAP_CHUNK` body: an 8-byte big-endian offset followed by the raw chunk bytes
/// (the bytes are not re-encoded).
pub fn encode_snap_chunk(offset: u64, bytes: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(8 + bytes.len());
    buf.put_u64(offset);
    buf.put_slice(bytes);
    buf.freeze()
}

/// Split a `SNAP_CHUNK` body into its offset and the raw chunk bytes.
pub fn decode_snap_chunk(body: &Bytes) -> Result<(u64, Bytes), ProtocolError> {
    if body.len() < 8 {
        return Err(ProtocolError::Truncated);
    }
    let offset = u64::from_be_bytes(body[..8].try_into().expect("8 bytes"));
    Ok((offset, body.slice(8..)))
}

/// Aborts the wrapped task when dropped, so connection helper tasks never outlive the connection
/// object that owns them.
pub(crate) struct AbortOnDrop(pub(crate) JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::raft::{AppendEntriesRequest, VoteRequest, VoteResponse};

    fn lid(index: u64) -> LogIdOf<TypeConfig> {
        openraft::LogId::new_term_index(1, index)
    }

    #[test]
    fn kinds_are_application_range_and_distinct() {
        let kinds = [
            APPEND_REQ,
            APPEND_RESP,
            VOTE_REQ,
            VOTE_RESP,
            PREVOTE_REQ,
            PREVOTE_RESP,
            SNAP_BEGIN,
            SNAP_CHUNK,
            SNAP_ACK,
            SNAP_END,
            SNAP_RESP,
        ];
        for (i, k) in kinds.iter().enumerate() {
            assert_eq!(*k as usize, 16 + i);
            assert!(*k >= crate::wire::frame::KIND_APP_BASE);
        }
    }

    #[test]
    fn append_result_roundtrips_every_variant() {
        let cases: Vec<AppendResult<TypeConfig>> = vec![
            Ok(None),
            Ok(Some(lid(7))),
            Err(AppendError::Conflict {
                log_id: lid(3),
                hint: None,
            }),
            Err(AppendError::Conflict {
                log_id: lid(3),
                hint: Some(ConflictHint::new(vec![lid(1), lid(2)])),
            }),
            Err(AppendError::HigherVote(VoteOf::<TypeConfig>::new(9, 2))),
        ];
        for case in cases {
            let body = encode_body(&WireAppendResult::from(case.clone())).unwrap();
            let wire: WireAppendResult = decode_body(&body).unwrap();
            assert_eq!(AppendResult::<TypeConfig>::from(wire), case);
        }
    }

    #[test]
    fn append_request_and_vote_bodies_roundtrip() {
        let req = AppendEntriesRequest::<TypeConfig> {
            vote: VoteOf::<TypeConfig>::new_committed(1, 1),
            prev_log_id: Some(lid(4)),
            entries: Vec::new(),
            leader_commit: Some(lid(3)),
        };
        let body = encode_body(&req).unwrap();
        let back: AppendEntriesRequest<TypeConfig> = decode_body(&body).unwrap();
        assert_eq!(back.prev_log_id, req.prev_log_id);
        assert_eq!(back.leader_commit, req.leader_commit);
        assert_eq!(back.vote, req.vote);

        let vreq = VoteRequest::<TypeConfig>::new(VoteOf::<TypeConfig>::new(2, 1), Some(lid(5)));
        let body = encode_body(&vreq).unwrap();
        let back: VoteRequest<TypeConfig> = decode_body(&body).unwrap();
        assert_eq!(back.vote, vreq.vote);
        assert_eq!(back.last_log_id, vreq.last_log_id);

        let vresp = VoteResponse::<TypeConfig>::new(VoteOf::<TypeConfig>::new(2, 1), None, true);
        let body = encode_body(&vresp).unwrap();
        let back: VoteResponse<TypeConfig> = decode_body(&body).unwrap();
        assert!(back.vote_granted);
    }

    #[test]
    fn decode_body_rejects_trailing_and_garbage() {
        let mut body = encode_body(&SnapAck { written: 5 }).unwrap().to_vec();
        body.push(0);
        assert!(matches!(
            decode_body::<SnapAck>(&body),
            Err(ProtocolError::Trailing(1))
        ));
        assert!(decode_body::<WireAppendResult>(&[0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn snap_chunk_layout_roundtrips() {
        let body = encode_snap_chunk(4096, b"payload");
        assert_eq!(body.len(), 8 + 7);
        let (offset, bytes) = decode_snap_chunk(&body).unwrap();
        assert_eq!(offset, 4096);
        assert_eq!(&bytes[..], b"payload");
        assert!(matches!(
            decode_snap_chunk(&Bytes::from_static(b"short")),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn remote_failure_roundtrips_and_truncates() {
        let f = RemoteFailure::new(true, "node stopped");
        assert_eq!(RemoteFailure::decode(&f.encode()), f);
        let long = RemoteFailure::new(false, "é".repeat(400));
        assert!(long.message.len() <= MAX_REMOTE_MESSAGE);
        assert!(!RemoteFailure::decode(&long.encode()).fatal);
        assert_eq!(RemoteFailure::decode(&[]).message, "");
    }

    #[test]
    fn snap_begin_roundtrips() {
        let begin = SnapBegin {
            vote: VoteOf::<TypeConfig>::new_committed(1, 1),
            meta: openraft::SnapshotMeta {
                last_log_id: Some(lid(5)),
                last_membership: Default::default(),
            },
            total_len: 1234,
        };
        let back: SnapBegin = decode_body(&encode_body(&begin).unwrap()).unwrap();
        assert_eq!(back.total_len, 1234);
        assert_eq!(back.meta.last_log_id, Some(lid(5)));
    }
}
