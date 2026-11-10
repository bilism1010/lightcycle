//! Startup recovery scan of WAL segments (decisions D-01 and D-16).
//!
//! Recovery rebuilds the logical log by walking every segment in ascending order and applying its
//! records in order: `Entry` adds a location, `Vote` and `Committed` replace their blobs,
//! `TruncateAfter` drops a suffix, `Purge` drops a prefix and records the purge point. The first
//! record of every segment is a `Preamble` that re-emits the durable state, so the vote, purge
//! point and committed id survive deletion of older segments.

use std::collections::BTreeMap;
use std::io;

use super::io::SegmentIo;
use super::record::{decode_record, Decoded, RecordType};
use super::segment::{
    decode_preamble, EntryLocation, PreambleState, SegmentSummary, WalConfig, WalError, WalStart,
};

/// Observable outcome of a recovery, used by restart tests to prove a torn tail was really
/// discarded rather than passing vacuously.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryStats {
    /// Non-zero bytes found beyond the last valid record of the last segment and zeroed.
    pub torn_bytes_discarded: u64,
    /// Segments walked by the scan.
    pub segments_scanned: u32,
    /// Entries in the recovered logical log.
    pub entries_recovered: u64,
}

/// The logical log reconstructed from disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredLog {
    /// Where the writer resumes, plus the recovered vote, purge point and committed id.
    pub start: WalStart,
    /// Ascending, contiguous entry locations after truncate and purge markers are applied.
    pub locations: Vec<EntryLocation>,
    /// Non-zero bytes discarded from the last segment's torn tail.
    pub torn_bytes_discarded: u64,
}

impl RecoveredLog {
    /// Counters describing this recovery.
    pub fn stats(&self) -> RecoveryStats {
        RecoveryStats {
            torn_bytes_discarded: self.torn_bytes_discarded,
            segments_scanned: u32::try_from(self.start.segments.len()).unwrap_or(u32::MAX),
            entries_recovered: self.locations.len() as u64,
        }
    }
}

fn corrupt(seq: u64, offset: usize, reason: impl Into<String>) -> WalError {
    WalError::Corrupt {
        seq,
        offset: offset as u64,
        reason: reason.into(),
    }
}

/// Reads a segment image (at most `cap` bytes) in full.
fn read_image(io: &dyn SegmentIo, seq: u64, cap: u64) -> Result<Vec<u8>, WalError> {
    let len = io.len(seq)?.min(cap);
    let len = usize::try_from(len).map_err(|_| io::Error::other("segment too large to read"))?;
    let mut buf = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        let n = io.read_at(seq, filled as u64, &mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(buf)
}

/// Running state of the scan.
struct Scanner {
    state: PreambleState,
    locations: Vec<EntryLocation>,
}

impl Scanner {
    fn new() -> Scanner {
        Scanner {
            state: PreambleState::default(),
            locations: Vec::new(),
        }
    }

    /// Drops locations at or below `upto`.
    fn drop_through(&mut self, upto: u64) {
        let n = self.locations.partition_point(|l| l.index <= upto);
        self.locations.drain(..n);
    }

    /// Drops locations above `keep_through` (`None` drops everything).
    fn drop_after(&mut self, keep_through: Option<u64>) {
        match keep_through {
            None => self.locations.clear(),
            Some(k) => {
                let n = self.locations.partition_point(|l| l.index <= k);
                self.locations.truncate(n);
            }
        }
    }

    /// Walks one segment image and returns the offset where scanning stopped.
    fn scan(&mut self, seq: u64, image: &[u8]) -> Result<usize, WalError> {
        let mut off = match decode_record(image) {
            Decoded::Record {
                ty: RecordType::Preamble,
                payload,
                consumed,
            } => {
                let preamble = decode_preamble(payload).map_err(|e| match e {
                    WalError::Corrupt { reason, .. } => corrupt(seq, 0, reason),
                    other => other,
                })?;
                if preamble.segment_seq != seq {
                    return Err(corrupt(
                        seq,
                        0,
                        format!("preamble names segment {} but the file is segment {seq}", preamble.segment_seq),
                    ));
                }
                // The preamble re-emits the state at roll time: it replaces the running state.
                self.state = preamble.state;
                if let Some((purged, _)) = &self.state.purged {
                    let purged = *purged;
                    self.drop_through(purged);
                }
                consumed
            }
            _ => return Err(corrupt(seq, 0, "segment does not start with a valid preamble")),
        };

        loop {
            match decode_record(&image[off..]) {
                Decoded::End | Decoded::Invalid => break,
                Decoded::Record { ty, payload, consumed } => {
                    self.apply(seq, off, ty, payload, consumed)?;
                    off += consumed;
                }
            }
        }
        Ok(off)
    }

    fn apply(&mut self, seq: u64, off: usize, ty: RecordType, payload: &[u8], consumed: usize) -> Result<(), WalError> {
        match ty {
            RecordType::Preamble => return Err(corrupt(seq, off, "preamble record is not first in its segment")),
            RecordType::Entry => {
                let Some(head) = payload.first_chunk::<8>() else {
                    return Err(corrupt(seq, off, "entry record shorter than its index"));
                };
                let index = u64::from_le_bytes(*head);
                if matches!(&self.state.purged, Some((purged, _)) if index <= *purged) {
                    // Physically present but already purged (the segment outlived the purge).
                    return Ok(());
                }
                if let Some(last) = self.locations.last() {
                    if last.index.checked_add(1) != Some(index) {
                        return Err(corrupt(
                            seq,
                            off,
                            format!("entry index {index} does not follow {}", last.index),
                        ));
                    }
                }
                let len = u32::try_from(consumed).map_err(|_| corrupt(seq, off, "record length overflow"))?;
                self.locations.push(EntryLocation {
                    index,
                    seq,
                    offset: off as u64,
                    len,
                });
            }
            RecordType::Vote => self.state.vote = Some(payload.to_vec()),
            RecordType::Committed => self.state.committed = Some(payload.to_vec()),
            RecordType::TruncateAfter => match payload {
                [0] => self.drop_after(None),
                [1, rest @ ..] if rest.len() == 8 => {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(rest);
                    self.drop_after(Some(u64::from_le_bytes(b)));
                }
                _ => return Err(corrupt(seq, off, "malformed truncate marker")),
            },
            RecordType::Purge => {
                let Some(head) = payload.first_chunk::<8>() else {
                    return Err(corrupt(seq, off, "purge record shorter than its index"));
                };
                let upto = u64::from_le_bytes(*head);
                self.drop_through(upto);
                self.state.purged = Some((upto, payload[8..].to_vec()));
            }
        }
        Ok(())
    }
}

/// Rebuilds the logical log from the segments in `io`.
pub fn recover(io: &mut dyn SegmentIo, cfg: &WalConfig) -> Result<RecoveredLog, WalError> {
    let mut seqs = io.list_segments()?;
    seqs.sort_unstable();
    seqs.dedup();
    if seqs.is_empty() {
        return Ok(RecoveredLog {
            start: WalStart::fresh(),
            locations: Vec::new(),
            torn_bytes_discarded: 0,
        });
    }
    for pair in seqs.windows(2) {
        if pair[1] != pair[0] + 1 {
            return Err(corrupt(pair[0] + 1, 0, "segment sequence has a gap"));
        }
    }

    let mut scanner = Scanner::new();
    let mut tail_offset = 0usize;
    for &seq in &seqs {
        let image = read_image(&*io, seq, cfg.segment_size)?;
        tail_offset = scanner.scan(seq, &image)?;
    }

    let mut spans: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    for loc in &scanner.locations {
        spans
            .entry(loc.seq)
            .and_modify(|s| s.1 = loc.index)
            .or_insert((loc.index, loc.index));
    }
    let segments = seqs
        .iter()
        .map(|&seq| SegmentSummary {
            seq,
            first_index: spans.get(&seq).map(|s| s.0),
            last_index: spans.get(&seq).map(|s| s.1),
        })
        .collect();

    Ok(RecoveredLog {
        start: WalStart {
            active_seq: seqs.last().copied().unwrap_or(0),
            tail_offset: tail_offset as u64,
            state: scanner.state,
            segments,
        },
        locations: scanner.locations,
        torn_bytes_discarded: 0,
    })
}

/// Declarative log builder shared by the recovery and cache tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::super::io::{FakeIo, SegmentIo};
    use super::super::record::{encode_record, RecordType};
    use super::super::segment::{encode_preamble, EntryLocation, Preamble, PreambleState, WalConfig, FORMAT_VERSION};

    /// Writes segments and records through [`SegmentIo`] on a [`FakeIo`].
    pub(crate) struct LogBuilder {
        pub(crate) io: FakeIo,
        pub(crate) segment_size: u64,
        /// Sequence of the segment currently being appended to (0 = none yet).
        pub(crate) seq: u64,
        /// Offset of the next record in the current segment.
        pub(crate) offset: u64,
        /// Every entry record written, in write order.
        pub(crate) entries: Vec<EntryLocation>,
        /// Start offset of every record (preamble included) written, per segment, in write order.
        pub(crate) records: Vec<(u64, u64)>,
    }

    impl LogBuilder {
        pub(crate) fn new(segment_size: u64) -> LogBuilder {
            LogBuilder {
                io: FakeIo::new(),
                segment_size,
                seq: 0,
                offset: 0,
                entries: Vec::new(),
                records: Vec::new(),
            }
        }

        pub(crate) fn cfg(&self) -> WalConfig {
            WalConfig {
                segment_size: self.segment_size,
                ..WalConfig::default()
            }
        }

        /// Starts the next segment (sequence = previous + 1, or 1) with a well-formed preamble.
        pub(crate) fn segment(&mut self, first_index: u64, state: PreambleState) -> &mut LogBuilder {
            let seq = self.seq + 1;
            self.segment_at(
                seq,
                Preamble {
                    format_version: FORMAT_VERSION,
                    segment_seq: seq,
                    first_index,
                    state,
                },
            )
        }

        /// Starts a segment whose file sequence is `file_seq` with an arbitrary preamble.
        pub(crate) fn segment_at(&mut self, file_seq: u64, preamble: Preamble) -> &mut LogBuilder {
            self.io.create_segment(file_seq, self.segment_size).expect("create segment");
            self.seq = file_seq;
            self.offset = 0;
            let mut buf = Vec::new();
            encode_record(&mut buf, RecordType::Preamble, &encode_preamble(&preamble));
            self.write(&buf);
            self
        }

        fn write(&mut self, buf: &[u8]) {
            assert!(
                self.offset + buf.len() as u64 <= self.segment_size,
                "test log overflows its segment"
            );
            self.io.write_at(self.seq, self.offset, buf).expect("write record");
            self.records.push((self.seq, self.offset));
            self.offset += buf.len() as u64;
        }

        /// Appends one framed record and returns its (offset, length).
        pub(crate) fn raw(&mut self, ty: RecordType, payload: &[u8]) -> (u64, u32) {
            let mut buf = Vec::new();
            let n = encode_record(&mut buf, ty, payload);
            let start = self.offset;
            self.write(&buf);
            (start, n as u32)
        }

        pub(crate) fn entry(&mut self, index: u64, body: &[u8]) -> EntryLocation {
            let mut payload = index.to_le_bytes().to_vec();
            payload.extend_from_slice(body);
            let (offset, len) = self.raw(RecordType::Entry, &payload);
            let loc = EntryLocation {
                index,
                seq: self.seq,
                offset,
                len,
            };
            self.entries.push(loc);
            loc
        }

        pub(crate) fn vote(&mut self, blob: &[u8]) -> &mut LogBuilder {
            self.raw(RecordType::Vote, blob);
            self
        }

        pub(crate) fn committed(&mut self, blob: &[u8]) -> &mut LogBuilder {
            self.raw(RecordType::Committed, blob);
            self
        }

        pub(crate) fn truncate_after(&mut self, keep_through: Option<u64>) -> &mut LogBuilder {
            let payload = match keep_through {
                None => vec![0u8],
                Some(k) => {
                    let mut p = vec![1u8];
                    p.extend_from_slice(&k.to_le_bytes());
                    p
                }
            };
            self.raw(RecordType::TruncateAfter, &payload);
            self
        }

        pub(crate) fn purge(&mut self, upto: u64, blob: &[u8]) -> &mut LogBuilder {
            let mut payload = upto.to_le_bytes().to_vec();
            payload.extend_from_slice(blob);
            self.raw(RecordType::Purge, &payload);
            self
        }

        /// Makes everything written so far durable (segment data and the directory).
        pub(crate) fn sync(&mut self) -> &mut LogBuilder {
            for seq in self.io.list_segments().expect("list") {
                self.io.sync_segment(seq).expect("sync segment");
            }
            self.io.sync_dir().expect("sync dir");
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::LogBuilder;
    use super::*;
    use crate::wal::io::SegmentIo;

    const SEG: u64 = 4096;

    fn read_record(io: &impl SegmentIo, loc: &EntryLocation) -> (u64, Vec<u8>) {
        let mut buf = vec![0u8; loc.len as usize];
        assert_eq!(io.read_at(loc.seq, loc.offset, &mut buf).unwrap(), buf.len());
        match decode_record(&buf) {
            Decoded::Record {
                ty: RecordType::Entry,
                payload,
                consumed,
            } => {
                assert_eq!(consumed, loc.len as usize);
                let index = u64::from_le_bytes(payload[..8].try_into().unwrap());
                (index, payload[8..].to_vec())
            }
            other => panic!("location {loc:?} does not hold an entry: {other:?}"),
        }
    }

    #[test]
    fn recovers_clean_log_into_locations_and_state() {
        let mut b = LogBuilder::new(SEG);
        b.segment(1, PreambleState::default());
        let mut expected = Vec::new();
        for i in 1..=3u64 {
            expected.push(b.entry(i, format!("body-{i}").as_bytes()));
        }
        b.vote(b"vote-1");
        b.segment(
            4,
            PreambleState {
                purged: None,
                vote: Some(b"vote-1".to_vec()),
                committed: None,
            },
        );
        for i in 4..=5u64 {
            expected.push(b.entry(i, format!("body-{i}").as_bytes()));
        }
        b.committed(b"commit-5");
        let end = b.offset;
        b.sync();

        let mut io = b.io.clone();
        let got = recover(&mut io, &b.cfg()).unwrap();

        assert_eq!(got.locations, expected);
        assert_eq!(got.locations.len(), 5);
        for (loc, i) in got.locations.iter().zip(1u64..) {
            let (index, body) = read_record(&io, loc);
            assert_eq!(index, i);
            assert_eq!(body, format!("body-{i}").into_bytes());
        }
        assert_eq!(got.start.active_seq, 2);
        assert_eq!(got.start.tail_offset, end);
        assert_eq!(got.start.state.vote.as_deref(), Some(&b"vote-1"[..]));
        assert_eq!(got.start.state.committed.as_deref(), Some(&b"commit-5"[..]));
        assert_eq!(got.start.state.purged, None);
        assert_eq!(got.torn_bytes_discarded, 0);
        assert_eq!(
            got.start.segments,
            vec![
                SegmentSummary {
                    seq: 1,
                    first_index: Some(1),
                    last_index: Some(3)
                },
                SegmentSummary {
                    seq: 2,
                    first_index: Some(4),
                    last_index: Some(5)
                },
            ]
        );
        let stats = got.stats();
        assert_eq!(stats.segments_scanned, 2);
        assert_eq!(stats.entries_recovered, 5);
        assert_eq!(stats.torn_bytes_discarded, 0);
    }

    #[test]
    fn empty_directory_recovers_fresh() {
        let mut io = crate::wal::io::FakeIo::new();
        let got = recover(&mut io, &WalConfig::default()).unwrap();
        assert_eq!(got.start, WalStart::fresh());
        assert!(got.locations.is_empty());
        assert_eq!(got.stats(), RecoveryStats::default());
    }
}
