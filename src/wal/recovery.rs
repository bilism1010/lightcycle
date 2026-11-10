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
use super::record::{decode_record, Decoded, RecordType, RECORD_HEADER_LEN};
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

/// Whether any CRC-valid record starts at or after `from` in `image`.
///
/// A torn write damages only a suffix of the unsynced data, so nothing valid can follow a torn
/// tail. A valid record after an invalid one is therefore evidence of media corruption (a bit
/// flip or a lost or misdirected sector write) that would otherwise be mistaken for a torn tail
/// and silently discard every acknowledged record behind it.
fn valid_record_follows(image: &[u8], from: usize) -> bool {
    (from..image.len().saturating_sub(RECORD_HEADER_LEN - 1))
        .any(|p| matches!(decode_record(&image[p..]), Decoded::Record { .. }))
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
    ///
    /// In the last segment an invalid record is the torn tail and just ends the scan, provided
    /// nothing valid follows it. A valid record behind an invalid one, or behind a zero header,
    /// means acknowledged data lies beyond the damage, so it is mid-log corruption and not a tear.
    /// Anywhere else an invalid record, or data after the end marker, is mid-log corruption
    /// (T-03-17).
    fn scan(&mut self, seq: u64, image: &[u8], is_last: bool) -> Result<usize, WalError> {
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
                        format!(
                            "preamble names segment {} but the file is segment {seq}",
                            preamble.segment_seq
                        ),
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
            _ => {
                return Err(corrupt(
                    seq,
                    0,
                    "segment does not start with a valid preamble",
                ))
            }
        };

        loop {
            match decode_record(&image[off..]) {
                Decoded::End | Decoded::Invalid
                    if is_last && valid_record_follows(image, off + 1) =>
                {
                    return Err(corrupt(
                        seq,
                        off,
                        "valid records follow an invalid record in the last segment: \
                         corruption, not a torn tail",
                    ));
                }
                Decoded::End => break,
                Decoded::Invalid if is_last => break,
                Decoded::Invalid => {
                    return Err(corrupt(
                        seq,
                        off,
                        "invalid record before the end of a non-final segment",
                    ))
                }
                Decoded::Record {
                    ty,
                    payload,
                    consumed,
                } => {
                    self.apply(seq, off, ty, payload, consumed)?;
                    off += consumed;
                }
            }
        }
        if !is_last {
            if let Some(pos) = image[off..].iter().position(|b| *b != 0) {
                return Err(corrupt(
                    seq,
                    off + pos,
                    "non-zero data after the end of a non-final segment",
                ));
            }
        }
        Ok(off)
    }

    fn apply(
        &mut self,
        seq: u64,
        off: usize,
        ty: RecordType,
        payload: &[u8],
        consumed: usize,
    ) -> Result<(), WalError> {
        match ty {
            RecordType::Preamble => {
                return Err(corrupt(
                    seq,
                    off,
                    "preamble record is not first in its segment",
                ))
            }
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
                let len = u32::try_from(consumed)
                    .map_err(|_| corrupt(seq, off, "record length overflow"))?;
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

/// Whether the last segment never received a valid preamble: all zero (crash right after
/// creation) or a torn preamble record. A valid record of another type, or data after a zero
/// header, is not a creation crash and is reported as corruption instead. So is a damaged
/// preamble with valid records behind it: that segment held acknowledged data.
fn head_is_unwritten(seq: u64, image: &[u8]) -> Result<bool, WalError> {
    match decode_record(image) {
        Decoded::Invalid if valid_record_follows(image, 1) => Err(corrupt(
            seq,
            0,
            "damaged preamble with valid records behind it",
        )),
        Decoded::Invalid => Ok(true),
        Decoded::End => match image.iter().position(|b| *b != 0) {
            None => Ok(true),
            Some(pos) => Err(corrupt(seq, pos, "data after a zero preamble header")),
        },
        Decoded::Record { .. } => Ok(false),
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

    // The last segment decides whether a crash left a half-created file behind, so read it first.
    let mut last_image = None;
    if let Some(&last_seq) = seqs.last() {
        let image = read_image(&*io, last_seq, cfg.segment_size)?;
        if head_is_unwritten(last_seq, &image)? {
            // Crash right after creation (or mid-preamble): nothing in it was ever acknowledged
            // because the preceding segment holds every durable record.
            io.delete_segment(last_seq)?;
            io.sync_dir()?;
            seqs.pop();
        } else {
            last_image = Some(image);
        }
    }
    if seqs.is_empty() {
        return Ok(RecoveredLog {
            start: WalStart::fresh(),
            locations: Vec::new(),
            torn_bytes_discarded: 0,
        });
    }

    let mut scanner = Scanner::new();
    let mut tail_offset = 0usize;
    let mut torn_bytes_discarded = 0u64;
    let count = seqs.len();
    for (i, &seq) in seqs.iter().enumerate() {
        let is_last = i + 1 == count;
        let image = match last_image.take() {
            Some(image) if is_last => image,
            _ => read_image(&*io, seq, cfg.segment_size)?,
        };
        tail_offset = scanner.scan(seq, &image, is_last)?;
        if is_last {
            // Everything past the last valid record must be zero before any append, otherwise a
            // stale valid-looking record could be resurrected later (T-03-18).
            let torn = image[tail_offset..].iter().filter(|b| **b != 0).count() as u64;
            if torn > 0 {
                io.set_len(seq, tail_offset as u64)?;
                io.sync_segment(seq)?;
                io.set_len(seq, cfg.segment_size)?;
                io.sync_segment(seq)?;
                io.sync_dir()?;
                torn_bytes_discarded = torn;
            }
        }
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
        torn_bytes_discarded,
    })
}

/// Declarative log builder shared by the recovery and cache tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::super::io::{FakeIo, SegmentIo};
    use super::super::record::{encode_record, RecordType};
    use super::super::segment::{
        encode_preamble, EntryLocation, Preamble, PreambleState, WalConfig, FORMAT_VERSION,
    };

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
        pub(crate) fn segment(
            &mut self,
            first_index: u64,
            state: PreambleState,
        ) -> &mut LogBuilder {
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
            self.io
                .create_segment(file_seq, self.segment_size)
                .expect("create segment");
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
            self.io
                .write_at(self.seq, self.offset, buf)
                .expect("write record");
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
        assert_eq!(
            io.read_at(loc.seq, loc.offset, &mut buf).unwrap(),
            buf.len()
        );
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

    // ---- Task 2: torn tails, corruption, markers, deleted segments ----

    use crate::wal::io::{FakeIo, IoEvent};
    use crate::wal::segment::{Preamble, FORMAT_VERSION};
    use proptest::prelude::*;

    const BODY: [u8; 40] = [0xAB; 40];

    /// Segment 1 holds entries 1-3 and a vote, all synced.
    fn synced_base() -> LogBuilder {
        let mut b = LogBuilder::new(SEG);
        b.segment(1, PreambleState::default());
        for i in 1..=3 {
            b.entry(i, &BODY);
        }
        b.vote(b"v1");
        b.sync();
        b
    }

    fn preamble(version: u16, declared_seq: u64) -> Preamble {
        Preamble {
            format_version: version,
            segment_seq: declared_seq,
            first_index: 1,
            state: PreambleState::default(),
        }
    }

    fn assert_contiguous(locs: &[EntryLocation], first: u64) {
        for (loc, i) in locs.iter().zip(first..) {
            assert_eq!(loc.index, i);
        }
    }

    fn indices(got: &RecoveredLog) -> Vec<u64> {
        got.locations.iter().map(|l| l.index).collect()
    }

    fn valid_prefix_end(image: &[u8]) -> usize {
        let mut off = 0;
        while let Decoded::Record { consumed, .. } = decode_record(&image[off..]) {
            off += consumed;
        }
        off
    }

    #[test]
    fn torn_tail_every_byte_offset() {
        let build = || {
            let mut b = synced_base();
            for i in 4..=8 {
                b.entry(i, &BODY);
            }
            b
        };
        let total = build().io.unsynced_bytes();
        assert!(
            total > 200,
            "the unsynced batch should span many bytes, got {total}"
        );

        let mut prev = 0usize;
        let mut saw_torn = false;
        for keep in 0..=total {
            let b = build();
            let mut io = b.io.clone();
            io.power_loss(keep);
            let got = recover(&mut io, &b.cfg()).unwrap();

            let n = got.locations.len();
            assert!((3..=8).contains(&n), "keep {keep}: {n} entries");
            assert!(
                n >= prev,
                "keep {keep}: recovered prefix shrank from {prev} to {n}"
            );
            prev = n;
            assert_contiguous(&got.locations, 1);
            saw_torn |= got.torn_bytes_discarded > 0;

            let tail = got.start.tail_offset as usize;
            let volatile = io.segment_bytes(1);
            assert_eq!(volatile.len() as u64, SEG);
            assert!(
                volatile[tail..].iter().all(|b| *b == 0),
                "keep {keep}: tail not zero"
            );
            assert_eq!(
                io.durable_bytes(1),
                volatile,
                "keep {keep}: repair not durable"
            );

            let again = recover(&mut io, &b.cfg()).unwrap();
            assert_eq!(again.torn_bytes_discarded, 0);
            assert_eq!(again.start, got.start);
            assert_eq!(again.locations, got.locations);
        }
        assert!(saw_torn, "the sweep never exercised a torn tail");
        assert_eq!(prev, 8, "keeping every byte recovers the whole batch");
        let b = build();
        let mut io = b.io.clone();
        io.power_loss(0);
        assert_eq!(recover(&mut io, &b.cfg()).unwrap().locations.len(), 3);
    }

    #[test]
    fn corrupt_middle_segment_is_hard_error() {
        let mut b = LogBuilder::new(SEG);
        b.segment(1, PreambleState::default());
        for i in 1..=3 {
            b.entry(i, &BODY);
        }
        b.vote(b"v1").committed(b"c1");
        b.segment(4, PreambleState::default());
        b.entry(4, &BODY);
        b.entry(5, &BODY);
        b.sync();

        let seg1: Vec<u64> = b.records.iter().filter(|r| r.0 == 1).map(|r| r.1).collect();
        assert!(seg1.len() >= 6);
        let mut io = b.io.clone();
        assert!(recover(&mut io, &b.cfg()).is_ok());
        for off in seg1 {
            for delta in [4u64, 8] {
                let pos = off + delta;
                let orig = io.segment_bytes(1)[pos as usize];
                io.write_at(1, pos, &[orig ^ 0xFF]).unwrap();
                match recover(&mut io, &b.cfg()) {
                    Err(WalError::Corrupt { seq: 1, .. }) => {}
                    other => panic!("flip at {pos}: expected Corrupt in segment 1, got {other:?}"),
                }
                io.write_at(1, pos, &[orig]).unwrap();
            }
        }
        assert!(recover(&mut io, &b.cfg()).is_ok());
    }

    #[test]
    fn bit_flip_in_last_segment_with_records_behind_it_is_corrupt() {
        // Entries 4..=8 are synced behind the flipped record, so they are acknowledged data and
        // must never be zeroed as a "torn tail".
        let mut b = synced_base();
        let mut flipped = Vec::new();
        for i in 4..=8 {
            flipped.push(b.entry(i, &BODY));
        }
        b.sync();
        let mut io = b.io.clone();
        let target = &flipped[1];
        let pos = target.offset + RECORD_HEADER_LEN as u64 + 2;
        let orig = io.segment_bytes(1)[pos as usize];
        io.write_at(1, pos, &[orig ^ 0xFF]).unwrap();
        io.sync_segment(1).unwrap();
        let before = io.segment_bytes(1);
        assert!(matches!(
            recover(&mut io, &b.cfg()),
            Err(WalError::Corrupt { seq: 1, .. })
        ));
        assert_eq!(io.segment_bytes(1), before, "recovery must not repair corruption");

        // The same flip in the very last record is indistinguishable from a tear and is repaired.
        io.write_at(1, pos, &[orig]).unwrap();
        let last = &flipped[4];
        let pos = last.offset + RECORD_HEADER_LEN as u64 + 2;
        let orig = io.segment_bytes(1)[pos as usize];
        io.write_at(1, pos, &[orig ^ 0xFF]).unwrap();
        let got = recover(&mut io, &b.cfg()).unwrap();
        assert_eq!(got.locations.len(), 7);
        assert!(got.torn_bytes_discarded > 0);
    }

    #[test]
    fn damaged_preamble_in_last_segment_with_records_behind_it_is_corrupt() {
        let mut b = synced_base();
        b.segment(4, PreambleState::default());
        b.entry(4, &BODY);
        b.entry(5, &BODY);
        b.sync();
        let mut io = b.io.clone();
        let orig = io.segment_bytes(2)[10];
        io.write_at(2, 10, &[orig ^ 0xFF]).unwrap();
        io.sync_segment(2).unwrap();
        assert!(matches!(
            recover(&mut io, &b.cfg()),
            Err(WalError::Corrupt { seq: 2, .. })
        ));
        assert_eq!(io.list_segments().unwrap(), vec![1, 2], "segment must not be deleted");
    }

    #[test]
    fn data_after_end_marker_in_non_last_segment_is_corrupt() {
        let mut b = synced_base();
        b.segment(4, PreambleState::default());
        b.entry(4, &BODY);
        b.sync();
        let mut io = b.io.clone();
        io.write_at(1, SEG - 1, &[1]).unwrap();
        assert!(matches!(
            recover(&mut io, &b.cfg()),
            Err(WalError::Corrupt { seq: 1, .. })
        ));
    }

    #[test]
    fn zero_filled_tail_is_end() {
        let b = synced_base();
        let mut io = b.io.clone();
        io.record_events(true);
        let got = recover(&mut io, &b.cfg()).unwrap();
        assert_eq!(got.torn_bytes_discarded, 0);
        assert_eq!(got.locations.len(), 3);
        assert_eq!(got.start.tail_offset, b.offset);
        assert!(
            !io.events()
                .iter()
                .any(|e| matches!(e, IoEvent::SetLen { .. } | IoEvent::Delete { .. })),
            "a clean log must not be repaired: {:?}",
            io.events()
        );
    }

    #[test]
    fn truncate_marker_applied_in_order() {
        let mut b = LogBuilder::new(SEG);
        b.segment(1, PreambleState::default());
        let old: Vec<_> = (1..=5).map(|i| b.entry(i, b"old")).collect();
        b.truncate_after(Some(3));
        let new: Vec<_> = (4..=6).map(|i| b.entry(i, b"new")).collect();
        b.sync();
        let io = b.io.clone();
        let got = recover(&mut io.clone(), &b.cfg()).unwrap();
        assert_eq!(got.locations.len(), 6);
        assert_contiguous(&got.locations, 1);
        assert_eq!(&got.locations[..3], &old[..3]);
        assert_eq!(&got.locations[3..], &new[..]);
        assert!(new[0].offset > old[4].offset);
        assert_eq!(read_record(&io, &got.locations[3]).1, b"new");
    }

    #[test]
    fn truncate_all_then_rewrite_from_any_index() {
        let mut b = LogBuilder::new(SEG);
        b.segment(1, PreambleState::default());
        for i in 1..=3 {
            b.entry(i, b"x");
        }
        b.truncate_after(None);
        b.entry(10, b"y");
        b.entry(11, b"y");
        b.sync();
        let got = recover(&mut b.io.clone(), &b.cfg()).unwrap();
        assert_eq!(indices(&got), vec![10, 11]);
        assert_eq!(got.start.segments[0].first_index, Some(10));
    }

    #[test]
    fn purge_marker_drops_prefix() {
        let mut b = LogBuilder::new(SEG);
        b.segment(1, PreambleState::default());
        for i in 1..=5 {
            b.entry(i, b"x");
        }
        b.purge(2, b"log-id-2");
        b.sync();
        let got = recover(&mut b.io.clone(), &b.cfg()).unwrap();
        assert_eq!(indices(&got), vec![3, 4, 5]);
        assert_eq!(got.start.state.purged, Some((2, b"log-id-2".to_vec())));
        assert_eq!(got.start.segments[0].first_index, Some(3));
    }

    #[test]
    fn preamble_state_survives_deleted_segments() {
        let build = || {
            let mut b = LogBuilder::new(SEG);
            b.segment(1, PreambleState::default());
            for i in 1..=3 {
                b.entry(i, b"x");
            }
            b.vote(b"V1").committed(b"C1");
            b.segment(
                4,
                PreambleState {
                    purged: None,
                    vote: Some(b"V1".to_vec()),
                    committed: Some(b"C1".to_vec()),
                },
            );
            b.entry(4, b"x");
            b.entry(5, b"x");
            b.purge(5, b"P5").vote(b"V2");
            b.segment(
                6,
                PreambleState {
                    purged: Some((5, b"P5".to_vec())),
                    vote: Some(b"V2".to_vec()),
                    committed: Some(b"C1".to_vec()),
                },
            );
            b.entry(6, b"x");
            b.committed(b"C2");
            b.sync();
            b
        };

        let b = build();
        let full = recover(&mut b.io.clone(), &b.cfg()).unwrap();
        assert_eq!(indices(&full), vec![6]);
        assert_eq!(full.start.state.vote.as_deref(), Some(&b"V2"[..]));
        assert_eq!(full.start.state.committed.as_deref(), Some(&b"C2"[..]));
        assert_eq!(full.start.state.purged, Some((5, b"P5".to_vec())));

        for gone in [vec![1], vec![1, 2]] {
            let b = build();
            let mut io = b.io.clone();
            for seq in &gone {
                io.delete_segment(*seq).unwrap();
            }
            io.sync_dir().unwrap();
            let got = recover(&mut io, &b.cfg()).unwrap();
            assert_eq!(got.start.state, full.start.state, "after deleting {gone:?}");
            assert_eq!(got.locations, full.locations);
        }
    }

    #[test]
    fn entries_at_or_below_the_purge_point_in_surviving_segments_are_ignored() {
        // The purge marker lived in a deleted segment; only the preamble remembers it.
        let mut b = LogBuilder::new(SEG);
        b.segment(
            1,
            PreambleState {
                purged: Some((2, b"P2".to_vec())),
                vote: None,
                committed: None,
            },
        );
        for i in 1..=5 {
            b.entry(i, b"x");
        }
        b.sync();
        let got = recover(&mut b.io.clone(), &b.cfg()).unwrap();
        assert_eq!(indices(&got), vec![3, 4, 5]);
    }

    #[test]
    fn rejects_preamble_seq_mismatch() {
        let mut b = LogBuilder::new(SEG);
        b.segment_at(2, preamble(FORMAT_VERSION, 7));
        b.sync();
        assert!(matches!(
            recover(&mut b.io.clone(), &b.cfg()),
            Err(WalError::Corrupt {
                seq: 2,
                offset: 0,
                ..
            })
        ));
    }

    #[test]
    fn rejects_unsupported_format_version() {
        let mut b = LogBuilder::new(SEG);
        b.segment_at(1, preamble(FORMAT_VERSION + 1, 1));
        b.sync();
        assert!(matches!(
            recover(&mut b.io.clone(), &b.cfg()),
            Err(WalError::Corrupt { seq: 1, .. })
        ));
    }

    #[test]
    fn missing_middle_segment_is_error() {
        let mut b = LogBuilder::new(SEG);
        for i in 1..=3 {
            b.segment(i, PreambleState::default());
            b.entry(i, b"x");
        }
        b.sync();
        let mut io = b.io.clone();
        io.delete_segment(2).unwrap();
        assert!(matches!(
            recover(&mut io, &b.cfg()),
            Err(WalError::Corrupt { seq: 2, .. })
        ));
    }

    #[test]
    fn entry_index_gap_is_corrupt() {
        let mut b = LogBuilder::new(SEG);
        b.segment(1, PreambleState::default());
        b.entry(1, b"x");
        b.entry(2, b"x");
        b.entry(4, b"x");
        b.sync();
        assert!(matches!(
            recover(&mut b.io.clone(), &b.cfg()),
            Err(WalError::Corrupt { seq: 1, .. })
        ));
    }

    #[test]
    fn malformed_marker_payloads_are_corrupt() {
        for (ty, payload) in [
            (RecordType::Entry, vec![1u8, 2, 3]),
            (RecordType::Purge, vec![1u8]),
            (RecordType::TruncateAfter, vec![2u8]),
            (RecordType::TruncateAfter, vec![1u8, 0, 0]),
        ] {
            let mut b = LogBuilder::new(SEG);
            b.segment(1, PreambleState::default());
            b.raw(ty, &payload);
            b.sync();
            assert!(
                matches!(
                    recover(&mut b.io.clone(), &b.cfg()),
                    Err(WalError::Corrupt { .. })
                ),
                "{ty:?} {payload:?}"
            );
        }
    }

    #[test]
    fn unsynced_last_segment_without_preamble_is_removed() {
        let b = synced_base();
        let mut io = b.io.clone();
        io.create_segment(2, SEG).unwrap();
        io.sync_dir().unwrap();
        assert_eq!(io.list_segments().unwrap(), vec![1, 2]);

        let got = recover(&mut io, &b.cfg()).unwrap();
        assert_eq!(io.list_segments().unwrap(), vec![1]);
        assert_eq!(got.start.active_seq, 1);
        assert_eq!(got.start.tail_offset, b.offset);
        assert_eq!(got.locations.len(), 3);
        // The deletion was made durable.
        io.power_loss(0);
        assert_eq!(io.list_segments().unwrap(), vec![1]);
    }

    #[test]
    fn torn_preamble_in_last_segment_is_removed() {
        let b = synced_base();
        let mut io = b.io.clone();
        let mut other = LogBuilder::new(SEG);
        other.segment(2, PreambleState::default());
        let half = other.io.segment_bytes(1)[..6].to_vec();
        io.create_segment(2, SEG).unwrap();
        io.write_at(2, 0, &half).unwrap();
        io.sync_segment(2).unwrap();
        io.sync_dir().unwrap();

        let got = recover(&mut io, &b.cfg()).unwrap();
        assert_eq!(io.list_segments().unwrap(), vec![1]);
        assert_eq!(got.start.active_seq, 1);
    }

    #[test]
    fn only_segment_all_zero_recovers_fresh() {
        let mut io = FakeIo::new();
        io.create_segment(1, SEG).unwrap();
        io.sync_dir().unwrap();
        let cfg = WalConfig {
            segment_size: SEG,
            ..WalConfig::default()
        };
        let got = recover(&mut io, &cfg).unwrap();
        assert_eq!(got.start, WalStart::fresh());
        assert!(io.list_segments().unwrap().is_empty());
    }

    #[test]
    fn non_last_segment_without_preamble_is_corrupt() {
        let mut b = LogBuilder::new(SEG);
        b.io.create_segment(1, SEG).unwrap();
        b.segment_at(2, preamble(FORMAT_VERSION, 2));
        b.sync();
        assert!(matches!(
            recover(&mut b.io.clone(), &b.cfg()),
            Err(WalError::Corrupt { seq: 1, .. })
        ));
    }

    #[test]
    fn recovery_stats_report_torn_bytes() {
        let clean = synced_base();
        let stats = recover(&mut clean.io.clone(), &clean.cfg())
            .unwrap()
            .stats();
        assert_eq!(stats.torn_bytes_discarded, 0);
        assert_eq!(stats.entries_recovered, 3);
        assert_eq!(stats.segments_scanned, 1);

        for keep in [7usize, 301] {
            let mut b = synced_base();
            for i in 4..=8 {
                b.entry(i, &[0xCD; 100]);
            }
            let mut io = b.io.clone();
            io.power_loss(keep);
            let pre = io.segment_bytes(1);
            let repair_at = valid_prefix_end(&pre);
            let expected = pre[repair_at..].iter().filter(|b| **b != 0).count() as u64;

            let got = recover(&mut io, &b.cfg()).unwrap();
            let stats = got.stats();
            assert!(expected > 0, "keep {keep}: the cut must leave torn bytes");
            assert_eq!(stats.torn_bytes_discarded, expected, "keep {keep}");
            assert_eq!(got.start.tail_offset as usize, repair_at);
            assert_eq!(stats.entries_recovered, got.locations.len() as u64);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        #[test]
        fn random_corruption_never_panics(
            flips in prop::collection::vec((0usize..4096, 1u8..=255), 0..6),
            truncate in prop::option::of(0u64..=4096),
        ) {
            let mut b = LogBuilder::new(SEG);
            b.segment(1, PreambleState::default());
            b.entry(1, &BODY);
            b.entry(2, &BODY);
            b.vote(b"v");
            b.segment(2, PreambleState::default());
            for i in 3..=6 {
                b.entry(i, &BODY);
            }
            b.sync();
            let used = b.offset as usize;
            let mut io = b.io.clone();
            for (pos, mask) in flips {
                let pos = (pos % used) as u64;
                let orig = io.segment_bytes(2)[pos as usize];
                io.write_at(2, pos, &[orig ^ mask]).unwrap();
            }
            if let Some(len) = truncate {
                io.set_len(2, len).unwrap();
            }
            if let Ok(got) = recover(&mut io, &b.cfg()) {
                if let Some(first) = got.locations.first() {
                    assert_contiguous(&got.locations, first.index);
                }
                let again = recover(&mut io, &b.cfg()).unwrap();
                prop_assert_eq!(again.torn_bytes_discarded, 0);
                prop_assert_eq!(again.start, got.start);
                prop_assert_eq!(again.locations, got.locations);
            }
        }
    }
}
