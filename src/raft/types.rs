//! Shared Raft-facing types: the replicated command/response enums and the openraft
//! `TypeConfig`.
//!
//! Leader identity (interpretation A2): `TypeConfig` uses
//! `openraft::impls::leader_id_std::LeaderId<u64, u64>`, i.e. standard Raft with a single leader
//! per term. The leader id is serialized into every log id, and therefore into every WAL record
//! (plan 03-08 onward); changing it later requires a log format bump.

use std::path::PathBuf;

use bytes::Bytes;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

/// A replicated key-value command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvRequest {
    /// Unconditionally set `key` to `value`.
    Set { key: Bytes, value: Bytes },
    /// Remove `key`.
    Delete { key: Bytes },
    /// Compare-and-swap: set `key` to `desired` iff its current value equals `expected`
    /// (`None` means absent; a `None` desired deletes).
    Cas {
        key: Bytes,
        expected: Option<Bytes>,
        desired: Option<Bytes>,
    },
}

impl KvRequest {
    /// Bytes of user data (keys and values) the command carries.
    pub fn payload_len(&self) -> usize {
        match self {
            KvRequest::Set { key, value } => key.len() + value.len(),
            KvRequest::Delete { key } => key.len(),
            KvRequest::Cas { key, expected, desired } => {
                key.len()
                    + expected.as_ref().map_or(0, |b| b.len())
                    + desired.as_ref().map_or(0, |b| b.len())
            }
        }
    }
}

impl std::fmt::Display for KvRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Result of applying a [`KvRequest`] to the state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResponse {
    /// The command was applied.
    Ok,
    /// A value read (reserved for read-style responses).
    Value(Option<Bytes>),
    /// A CAS precondition did not hold; carries the current value.
    CasFailed { current: Option<Bytes> },
}

openraft::declare_raft_types!(
    /// openraft type configuration for Lightcycle.
    pub TypeConfig:
        D = KvRequest,
        R = KvResponse,
        Node = BasicNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, u64>,
);

/// The state machine's `SnapshotData`: a plain handle to an immutable snapshot file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFile {
    /// Path of the immutable snapshot file.
    pub path: PathBuf,
    /// Length of the file in bytes.
    pub len: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::entry::EntryPayload;
    use openraft::type_config::alias::{EntryOf, LogIdOf};

    fn b(s: &'static str) -> Bytes {
        Bytes::from_static(s.as_bytes())
    }

    #[test]
    fn kv_request_postcard_roundtrip() {
        let cases = vec![
            KvRequest::Set { key: b("k"), value: b("v") },
            KvRequest::Delete { key: b("k") },
            KvRequest::Cas { key: b("k"), expected: Some(b("a")), desired: None },
            KvRequest::Cas { key: b("k"), expected: None, desired: Some(b("b")) },
        ];
        for c in cases {
            let enc = postcard::to_stdvec(&c).unwrap();
            let dec: KvRequest = postcard::from_bytes(&enc).unwrap();
            assert_eq!(c, dec);
        }
    }

    #[test]
    fn entry_postcard_roundtrip() {
        let log_id: LogIdOf<TypeConfig> = openraft::LogId::new_term_index(3, 7);
        let entry: EntryOf<TypeConfig> = openraft::Entry {
            log_id,
            payload: EntryPayload::Normal(KvRequest::Set { key: b("k"), value: b("v") }),
        };
        let enc = postcard::to_stdvec(&entry).unwrap();
        let dec: EntryOf<TypeConfig> = postcard::from_bytes(&enc).unwrap();
        assert_eq!(entry, dec);
    }
}
