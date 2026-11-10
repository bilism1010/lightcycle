//! Segment layout and the WAL types shared by the writer, recovery, cache and store plans.
//!
//! Every segment starts with a [`RecordType::Preamble`](super::record::RecordType::Preamble)
//! record carrying a postcard-encoded [`Preamble`]: the format version, the segment sequence, the
//! first log index the segment may hold, and the durable state (purge point, vote, committed log
//! id) that must survive deletion of older segments. Segment file names are derived only from
//! numeric ids so peer or user supplied strings can never form a path.

use serde::{Deserialize, Serialize};
use std::io;

/// Current on-disk format version stored in every segment preamble.
pub const FORMAT_VERSION: u16 = 1;

/// Default segment size: 64 MiB.
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// File name for a segment: 16 lowercase hex digits plus `.wal`.
pub fn segment_file_name(seq: u64) -> String {
    format!("{seq:016x}.wal")
}

/// Strict inverse of [`segment_file_name`]. Returns `None` for anything that is not exactly 16
/// lowercase hex digits followed by `.wal` (path separators, `..`, `.tmp` suffixes, uppercase hex
/// and wrong lengths are all rejected).
pub fn parse_segment_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".wal")?;
    if stem.len() != 16 || !stem.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return None;
    }
    u64::from_str_radix(stem, 16).ok()
}

/// Whether a record of `record_len` bytes fits in a segment of `segment_size` at `offset`.
pub fn fits(offset: u64, record_len: usize, segment_size: u64) -> bool {
    offset
        .checked_add(record_len as u64)
        .is_some_and(|end| end <= segment_size)
}

/// Durable state carried forward into every new segment's preamble.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreambleState {
    /// Purge point: the last purged log index and the encoded log id at that point.
    pub purged: Option<(u64, Vec<u8>)>,
    /// Encoded vote.
    pub vote: Option<Vec<u8>>,
    /// Encoded committed log id.
    pub committed: Option<Vec<u8>>,
}

/// Contents of the first record of every segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Preamble {
    /// Must equal [`FORMAT_VERSION`].
    pub format_version: u16,
    /// Sequence number of this segment.
    pub segment_seq: u64,
    /// First log index this segment may contain.
    pub first_index: u64,
    /// State carried forward from older segments.
    pub state: PreambleState,
}

/// Encodes a preamble with postcard.
pub fn encode_preamble(preamble: &Preamble) -> Vec<u8> {
    postcard::to_stdvec(preamble).expect("postcard encoding of a Preamble cannot fail")
}

/// Decodes a preamble, rejecting any `format_version` other than [`FORMAT_VERSION`].
pub fn decode_preamble(bytes: &[u8]) -> Result<Preamble, WalError> {
    let preamble: Preamble = postcard::from_bytes(bytes).map_err(|e| WalError::Corrupt {
        seq: 0,
        offset: 0,
        reason: format!("preamble decode failed: {e}"),
    })?;
    if preamble.format_version != FORMAT_VERSION {
        return Err(WalError::Corrupt {
            seq: preamble.segment_seq,
            offset: 0,
            reason: format!(
                "unsupported format version {} (expected {FORMAT_VERSION})",
                preamble.format_version
            ),
        });
    }
    Ok(preamble)
}

/// What recovery learned about one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSummary {
    /// Segment sequence number.
    pub seq: u64,
    /// First entry index physically present, if any.
    pub first_index: Option<u64>,
    /// Last entry index physically present, if any.
    pub last_index: Option<u64>,
}

/// Result of recovery, handed to the writer so it can resume appending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalStart {
    /// Sequence of the segment to append to (0 = no segment yet).
    pub active_seq: u64,
    /// Offset in the active segment where the next record goes.
    pub tail_offset: u64,
    /// Recovered durable state.
    pub state: PreambleState,
    /// Surviving segments, ascending.
    pub segments: Vec<SegmentSummary>,
}

impl WalStart {
    /// The start state of an empty WAL directory.
    pub fn fresh() -> WalStart {
        WalStart {
            active_seq: 0,
            tail_offset: 0,
            state: PreambleState::default(),
            segments: Vec::new(),
        }
    }
}

/// Where an entry's record lives on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryLocation {
    /// Log index.
    pub index: u64,
    /// Segment sequence.
    pub seq: u64,
    /// Byte offset of the record within the segment.
    pub offset: u64,
    /// Total record length in bytes, header included.
    pub len: u32,
}

/// Callbacks from the writer thread that keep the index and cache consistent with the log.
pub trait WalObserver: Send + 'static {
    /// Entries were appended (not necessarily durable yet).
    fn on_entries_written(&mut self, locs: &[EntryLocation]);
    /// The log was truncated after `keep_through` (`None` = everything dropped).
    fn on_truncate(&mut self, keep_through: Option<u64>);
    /// The log was purged up to and including `upto`.
    fn on_purge(&mut self, upto: u64);
    /// Everything up to and including `upto_index` is durable.
    fn on_flushed(&mut self, upto_index: u64);
}

/// An observer that ignores every callback.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

impl WalObserver for NoopObserver {
    fn on_entries_written(&mut self, _locs: &[EntryLocation]) {}
    fn on_truncate(&mut self, _keep_through: Option<u64>) {}
    fn on_purge(&mut self, _upto: u64) {}
    fn on_flushed(&mut self, _upto_index: u64) {}
}

/// When the writer calls fsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fsync {
    /// Group commit: every acknowledged write is durable first (default).
    Always,
    /// Labelled-unsafe benchmarking relaxation: sync at most every `interval_ms`.
    Periodic {
        /// Milliseconds between syncs.
        interval_ms: u64,
    },
    /// Labelled-unsafe benchmarking relaxation: never sync.
    Never,
}

impl Fsync {
    /// True for every mode that can lose acknowledged writes on power loss (decision D-04).
    pub fn is_unsafe(&self) -> bool {
        !matches!(self, Fsync::Always)
    }
}

/// Tunables for the WAL.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Size of each preallocated segment in bytes. **Treat it as a cluster-wide setting.** It
    /// bounds the largest log entry a node's WAL accepts (see
    /// [`max_entry_bytes`](crate::wal::store::max_entry_bytes)), and an entry the leader
    /// accepted is replicated as is: a follower with a smaller `segment_size` would reject it
    /// in `append`, which openraft treats as fatal for that follower, and fail the same way on
    /// every restart. Nothing negotiates or checks this between nodes, so keep it identical on
    /// every member (the command line offers no flag for it; only library users can differ).
    pub segment_size: u64,
    /// Fsync policy.
    pub fsync: Fsync,
    /// Largest group-commit batch in bytes.
    pub max_batch_bytes: usize,
    /// Entry cache budget in bytes.
    pub cache_budget_bytes: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            segment_size: DEFAULT_SEGMENT_SIZE,
            fsync: Fsync::Always,
            max_batch_bytes: 4 * 1024 * 1024,
            cache_budget_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Errors surfaced by the WAL.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// An underlying I/O call failed.
    #[error("wal io error: {0}")]
    Io(#[from] io::Error),
    /// Mid-log corruption or an unreadable preamble.
    #[error("wal corrupt in segment {seq} at offset {offset}: {reason}")]
    Corrupt {
        /// Segment sequence.
        seq: u64,
        /// Byte offset of the bad record.
        offset: u64,
        /// What was wrong.
        reason: String,
    },
    /// Invalid configuration.
    #[error("invalid wal config: {0}")]
    Config(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_roundtrip_and_rejects_traversal() {
        for seq in [0u64, 1, 255, u64::MAX] {
            let name = segment_file_name(seq);
            assert_eq!(name.len(), 20);
            assert_eq!(parse_segment_name(&name), Some(seq));
        }
        assert_eq!(segment_file_name(1), "0000000000000001.wal");
        for bad in [
            "../0000000000000001.wal",
            "0000000000000001.wal.tmp",
            "000000000000000A.wal",
            "1.wal",
            "",
            "0000000000000001",
            ".wal",
            "/0000000000000001.wal",
            "0000000000000001.wal/",
            "000000000000000g.wal",
        ] {
            assert_eq!(parse_segment_name(bad), None, "{bad:?} must be rejected");
        }
    }

    fn sample() -> Preamble {
        Preamble {
            format_version: FORMAT_VERSION,
            segment_seq: 5,
            first_index: 100,
            state: PreambleState::default(),
        }
    }

    #[test]
    fn preamble_roundtrip() {
        let p = sample();
        assert_eq!(decode_preamble(&encode_preamble(&p)).unwrap(), p);
    }

    #[test]
    fn preamble_rejects_future_format_version() {
        let mut p = sample();
        p.format_version = FORMAT_VERSION + 1;
        let err = decode_preamble(&encode_preamble(&p)).unwrap_err();
        assert!(matches!(err, WalError::Corrupt { .. }), "got {err:?}");
        assert!(matches!(decode_preamble(&[0xff, 0xff]), Err(WalError::Corrupt { .. })));
    }

    #[test]
    fn preamble_state_carries_vote_purged_committed() {
        let mut p = sample();
        p.state = PreambleState {
            purged: Some((42, vec![1, 2, 3])),
            vote: Some(vec![9, 9]),
            committed: Some(vec![7]),
        };
        let back = decode_preamble(&encode_preamble(&p)).unwrap();
        assert_eq!(back.state.purged, Some((42, vec![1, 2, 3])));
        assert_eq!(back.state.vote, Some(vec![9, 9]));
        assert_eq!(back.state.committed, Some(vec![7]));
    }

    #[test]
    fn fits_boundaries() {
        assert!(fits(0, 100, 100));
        assert!(fits(90, 10, 100));
        assert!(!fits(90, 11, 100));
        assert!(!fits(u64::MAX, 1, u64::MAX));
        assert!(fits(100, 0, 100));
    }

    #[test]
    fn fsync_unsafe_flags() {
        assert!(!Fsync::Always.is_unsafe());
        assert!(Fsync::Periodic { interval_ms: 5 }.is_unsafe());
        assert!(Fsync::Never.is_unsafe());
        let cfg = WalConfig::default();
        assert_eq!(cfg.segment_size, DEFAULT_SEGMENT_SIZE);
        assert_eq!(cfg.fsync, Fsync::Always);
        assert_eq!(cfg.max_batch_bytes, 4 * 1024 * 1024);
        assert_eq!(cfg.cache_budget_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn wal_start_fresh_is_empty() {
        let s = WalStart::fresh();
        assert_eq!(s.active_seq, 0);
        assert_eq!(s.tail_offset, 0);
        assert!(s.segments.is_empty());
        assert_eq!(s.state, PreambleState::default());
    }
}
