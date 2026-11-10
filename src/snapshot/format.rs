//! Snapshot file layout (decisions D-09, D-11): header frame, CRC-protected record blocks, footer.
//!
//! Every part of the file is a frame of the shared [`FrameCodec`] (lane 0, flags 0, `req_id` equal
//! to the frame's sequence number starting at 0), so a byte range of the file can be shipped as an
//! `InstallSnapshot` chunk with no re-encoding. The reader validates kind, flags, lane and
//! sequence number of every frame, so the frame headers are covered as well as the bodies.
//!
//! ```text
//! header body : "LCSNAP" | format_version u16 | created_unix_secs u64 | postcard(meta) | crc32c u32
//! block body  : count u32 | { klen u32 | vlen u32 | stamp u64 | flags u8 | key | value }* | crc32c u32
//! footer body : total_records u64 | total_payload_bytes u64 | rolling_crc32c u32 | crc32c u32
//! ```
//!
//! All integers are big-endian. `flags` bit 0 marks a tombstone (`vlen` is then 0). Each CRC32C
//! covers every byte of its body that precedes it. `total_payload_bytes` is the sum of key and
//! value lengths over all records. The rolling CRC is `crc32c_append` across the complete block
//! bodies (CRC trailer included) in file order. Any deviation, including truncation at a frame
//! boundary and trailing bytes after the footer, is a [`SnapshotError`].

use std::io::{self, Read, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use openraft::type_config::alias::SnapshotMetaOf;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use crate::raft::types::TypeConfig;
use crate::storage::VersionedEntry;
use crate::wire::frame::{Frame, FrameCodec, FrameError, Lane};

/// Metadata carried in the header frame.
pub type Meta = SnapshotMetaOf<TypeConfig>;

/// Frame kind of the header frame.
pub const SNAP_HEADER: u8 = 32;
/// Frame kind of a record block frame.
pub const SNAP_BLOCK: u8 = 33;
/// Frame kind of the footer frame.
pub const SNAP_FOOTER: u8 = 34;

/// File magic at the start of the header body.
pub const MAGIC: [u8; 6] = *b"LCSNAP";
/// Current on-disk format version.
pub const FORMAT_VERSION: u16 = 1;
/// Target size of a record block (a single larger record gets its own block).
pub const TARGET_BLOCK_SIZE: usize = 64 * 1024;
/// Frame payload cap used when writing and reading snapshot files: 16 MiB plus slack.
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024 + 4096;
/// Read size used by the streaming reader.
pub const READ_CHUNK: usize = 256 * 1024;

/// Per-record fixed header: klen u32, vlen u32, stamp u64, flags u8.
const RECORD_HEADER_LEN: usize = 4 + 4 + 8 + 1;
const FLAG_TOMBSTONE: u8 = 0x01;
/// Header body bytes before the postcard meta: magic, version, created.
const HEADER_FIXED_LEN: usize = MAGIC.len() + 2 + 8;
/// Footer body: records u64, bytes u64, rolling crc u32, own crc u32.
const FOOTER_LEN: usize = 8 + 8 + 4 + 4;

/// Errors from writing, reading or storing snapshot files.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// Underlying I/O failure.
    #[error("snapshot io error: {0}")]
    Io(#[from] io::Error),
    /// The header does not start with the snapshot magic.
    #[error("bad snapshot magic")]
    BadMagic,
    /// The header names a format version this build cannot read. The file may be perfectly
    /// healthy (for example written by a newer build before a downgrade): do not wipe the node.
    #[error(
        "snapshot format version {0} is not supported by this build (a newer build may have \
         written it): run a build that supports it instead of wiping the node"
    )]
    UnsupportedVersion(u16),
    /// The file ended before the footer was complete.
    #[error("snapshot truncated")]
    Truncated,
    /// A frame was malformed: bad kind, flags, lane, sequence number or structure.
    #[error("snapshot frame corrupt")]
    FrameCorrupt,
    /// A record block failed its CRC.
    #[error("snapshot block {block} crc mismatch")]
    BlockCrc {
        /// Zero-based index of the block.
        block: u64,
    },
    /// The header failed its CRC.
    #[error("snapshot header crc mismatch")]
    HeaderCrc,
    /// The footer failed its CRC or disagrees with what was read.
    #[error("snapshot footer mismatch")]
    FooterMismatch,
    /// Bytes follow the footer.
    #[error("trailing bytes after snapshot footer")]
    Trailing,
    /// The newest snapshot is unusable; the log below it was purged, so the node must be wiped
    /// and rejoin.
    #[error("latest snapshot is corrupt: wipe the node and rejoin")]
    CorruptNeedsWipe,
    /// A single record does not fit in one frame.
    #[error("record of {0} bytes exceeds the snapshot frame cap")]
    RecordTooLarge(usize),
    /// A received snapshot's header does not match the metadata it was announced with.
    #[error("snapshot header does not match the expected metadata")]
    MetaMismatch,
}

impl From<FrameError> for SnapshotError {
    fn from(e: FrameError) -> Self {
        match e {
            FrameError::Io(e) => SnapshotError::Io(e),
            _ => SnapshotError::FrameCorrupt,
        }
    }
}

fn new_codec() -> FrameCodec {
    FrameCodec::new(MAX_FRAME_LEN)
}

/// Encode a footer body (including its own CRC). Shared by the writer and by tests that need a
/// footer with deliberately wrong counts but a valid CRC.
pub(crate) fn encode_footer_body(records: u64, payload_bytes: u64, rolling_crc: u32) -> Bytes {
    let mut b = BytesMut::with_capacity(FOOTER_LEN);
    b.put_u64(records);
    b.put_u64(payload_bytes);
    b.put_u32(rolling_crc);
    let crc = crc32c::crc32c(&b);
    b.put_u32(crc);
    b.freeze()
}

/// Streaming snapshot writer over any [`Write`].
pub struct SnapshotFileWriter<W: Write> {
    w: W,
    codec: FrameCodec,
    out: BytesMut,
    seq: u64,
    state: WState,
    /// Block under construction: 4 bytes reserved for the count, then records.
    pending: BytesMut,
    pending_count: u32,
    records: u64,
    payload_bytes: u64,
    rolling_crc: u32,
}

#[derive(PartialEq, Eq)]
enum WState {
    Fresh,
    Body,
}

impl<W: Write> SnapshotFileWriter<W> {
    /// Wrap `w`. Nothing is written until [`write_header`](Self::write_header).
    pub fn new(w: W) -> Self {
        let mut pending = BytesMut::with_capacity(TARGET_BLOCK_SIZE + 1024);
        pending.put_u32(0);
        SnapshotFileWriter {
            w,
            codec: new_codec(),
            out: BytesMut::new(),
            seq: 0,
            state: WState::Fresh,
            pending,
            pending_count: 0,
            records: 0,
            payload_bytes: 0,
            rolling_crc: 0,
        }
    }

    fn emit(&mut self, kind: u8, body: Bytes) -> Result<(), SnapshotError> {
        let frame = Frame::new(kind, Lane::Replication, self.seq, body);
        self.codec.encode(frame, &mut self.out)?;
        self.seq += 1;
        self.w.write_all(&self.out)?;
        self.out.clear();
        Ok(())
    }

    /// Write the header frame. Must be called exactly once, first.
    pub fn write_header(&mut self, meta: &Meta, created_unix_secs: u64) -> Result<(), SnapshotError> {
        if self.state != WState::Fresh {
            return Err(SnapshotError::FrameCorrupt);
        }
        let meta_bytes = postcard::to_stdvec(meta)
            .map_err(|e| SnapshotError::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let mut b = BytesMut::with_capacity(HEADER_FIXED_LEN + meta_bytes.len() + 4);
        b.put_slice(&MAGIC);
        b.put_u16(FORMAT_VERSION);
        b.put_u64(created_unix_secs);
        b.put_slice(&meta_bytes);
        let crc = crc32c::crc32c(&b);
        b.put_u32(crc);
        self.emit(SNAP_HEADER, b.freeze())?;
        self.state = WState::Body;
        Ok(())
    }

    /// Append one record, emitting a block frame whenever the block reaches the target size.
    pub fn push(&mut self, e: &VersionedEntry) -> Result<(), SnapshotError> {
        if self.state != WState::Body {
            return Err(SnapshotError::FrameCorrupt);
        }
        let vlen = e.value.as_ref().map_or(0, |v| v.len());
        let rec_len = RECORD_HEADER_LEN + e.key.len() + vlen;
        // Count (4) + record + crc (4) must fit in one frame body.
        if rec_len + 8 + crate::wire::frame::HEADER_LEN > MAX_FRAME_LEN || e.key.len() > u32::MAX as usize {
            return Err(SnapshotError::RecordTooLarge(rec_len));
        }
        if self.pending_count > 0 && self.pending.len() + rec_len > TARGET_BLOCK_SIZE {
            self.flush_block()?;
        }
        self.pending.put_u32(e.key.len() as u32);
        self.pending.put_u32(vlen as u32);
        self.pending.put_u64(e.stamp);
        self.pending.put_u8(if e.value.is_none() { FLAG_TOMBSTONE } else { 0 });
        self.pending.put_slice(&e.key);
        if let Some(v) = &e.value {
            self.pending.put_slice(v);
        }
        self.pending_count += 1;
        self.records += 1;
        self.payload_bytes += (e.key.len() + vlen) as u64;
        if self.pending.len() >= TARGET_BLOCK_SIZE {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), SnapshotError> {
        if self.pending_count == 0 {
            return Ok(());
        }
        let count = self.pending_count;
        self.pending[..4].copy_from_slice(&count.to_be_bytes());
        let crc = crc32c::crc32c(&self.pending);
        self.pending.put_u32(crc);
        let body = self.pending.split().freeze();
        self.rolling_crc = crc32c::crc32c_append(self.rolling_crc, &body);
        self.pending.reserve(TARGET_BLOCK_SIZE + 1024);
        self.pending.put_u32(0);
        self.pending_count = 0;
        self.emit(SNAP_BLOCK, body)
    }

    /// Flush the last block, write the footer and flush the underlying writer. Returns it.
    pub fn finish(mut self) -> Result<W, SnapshotError> {
        if self.state != WState::Body {
            return Err(SnapshotError::FrameCorrupt);
        }
        self.flush_block()?;
        let footer = encode_footer_body(self.records, self.payload_bytes, self.rolling_crc);
        self.emit(SNAP_FOOTER, footer)?;
        self.w.flush()?;
        Ok(self.w)
    }
}

/// One item yielded by [`SnapshotFileReader::next`].
#[derive(Debug)]
pub enum SnapshotItem {
    /// The validated header.
    Header {
        /// Snapshot metadata.
        meta: Meta,
        /// Creation time, seconds since the Unix epoch.
        created_unix_secs: u64,
    },
    /// A validated block of records.
    Block(Vec<VersionedEntry>),
    /// The validated footer; the whole file has been checked and no byte follows it.
    Footer {
        /// Total record count.
        records: u64,
        /// Total key plus value bytes.
        payload_bytes: u64,
    },
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum RState {
    ExpectHeader,
    Blocks,
    Done,
}

/// Streaming snapshot reader over any [`Read`] with bounded memory (one block at a time).
pub struct SnapshotFileReader<R: Read> {
    src: R,
    codec: FrameCodec,
    buf: BytesMut,
    scratch: Vec<u8>,
    seq: u64,
    state: RState,
    blocks: u64,
    records: u64,
    payload_bytes: u64,
    rolling_crc: u32,
}

impl<R: Read> SnapshotFileReader<R> {
    /// Wrap `src`.
    pub fn new(src: R) -> Self {
        SnapshotFileReader {
            src,
            codec: new_codec(),
            buf: BytesMut::new(),
            scratch: vec![0u8; READ_CHUNK],
            seq: 0,
            state: RState::ExpectHeader,
            blocks: 0,
            records: 0,
            payload_bytes: 0,
            rolling_crc: 0,
        }
    }

    /// Read more bytes into the frame buffer; returns the number read (0 at end of input).
    fn fill(&mut self) -> Result<usize, SnapshotError> {
        loop {
            match self.src.read(&mut self.scratch) {
                Ok(n) => {
                    self.buf.extend_from_slice(&self.scratch[..n]);
                    return Ok(n);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn next_frame(&mut self) -> Result<Frame, SnapshotError> {
        loop {
            if let Some(f) = self.codec.decode(&mut self.buf)? {
                return Ok(f);
            }
            if self.fill()? == 0 {
                return Err(SnapshotError::Truncated);
            }
        }
    }

    /// Yield the next validated item. After [`SnapshotItem::Footer`] further calls fail.
    pub fn next(&mut self) -> Result<SnapshotItem, SnapshotError> {
        if self.state == RState::Done {
            return Err(SnapshotError::FrameCorrupt);
        }
        let frame = self.next_frame()?;
        let h = frame.header;
        if h.flags != 0 || h.lane != Lane::Replication as u8 || h.req_id != self.seq {
            return Err(SnapshotError::FrameCorrupt);
        }
        self.seq += 1;
        match (self.state, h.kind) {
            (RState::ExpectHeader, SNAP_HEADER) => {
                let item = parse_header(&frame.body)?;
                self.state = RState::Blocks;
                Ok(item)
            }
            (RState::Blocks, SNAP_BLOCK) => {
                let block = self.blocks;
                let (records, payload) = parse_block(&frame.body, block)?;
                self.blocks += 1;
                self.records += records.len() as u64;
                self.payload_bytes += payload;
                self.rolling_crc = crc32c::crc32c_append(self.rolling_crc, &frame.body);
                Ok(SnapshotItem::Block(records))
            }
            (RState::Blocks, SNAP_FOOTER) => {
                self.check_footer(&frame.body)?;
                self.check_no_trailing()?;
                self.state = RState::Done;
                Ok(SnapshotItem::Footer {
                    records: self.records,
                    payload_bytes: self.payload_bytes,
                })
            }
            _ => Err(SnapshotError::FrameCorrupt),
        }
    }

    fn check_footer(&self, body: &[u8]) -> Result<(), SnapshotError> {
        if body.len() != FOOTER_LEN {
            return Err(SnapshotError::FooterMismatch);
        }
        let (fields, crc) = body.split_at(FOOTER_LEN - 4);
        if crc32c::crc32c(fields) != u32::from_be_bytes(crc.try_into().expect("4 bytes")) {
            return Err(SnapshotError::FooterMismatch);
        }
        let records = u64::from_be_bytes(fields[0..8].try_into().expect("8 bytes"));
        let payload = u64::from_be_bytes(fields[8..16].try_into().expect("8 bytes"));
        let rolling = u32::from_be_bytes(fields[16..20].try_into().expect("4 bytes"));
        if records != self.records || payload != self.payload_bytes || rolling != self.rolling_crc {
            return Err(SnapshotError::FooterMismatch);
        }
        Ok(())
    }

    fn check_no_trailing(&mut self) -> Result<(), SnapshotError> {
        if !self.buf.is_empty() || self.fill()? != 0 {
            return Err(SnapshotError::Trailing);
        }
        Ok(())
    }
}

fn parse_header(body: &[u8]) -> Result<SnapshotItem, SnapshotError> {
    if body.len() < HEADER_FIXED_LEN + 4 {
        return Err(SnapshotError::FrameCorrupt);
    }
    if body[..MAGIC.len()] != MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let version = u16::from_be_bytes([body[6], body[7]]);
    if version != FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }
    let (covered, crc) = body.split_at(body.len() - 4);
    if crc32c::crc32c(covered) != u32::from_be_bytes(crc.try_into().expect("4 bytes")) {
        return Err(SnapshotError::HeaderCrc);
    }
    let created = u64::from_be_bytes(body[8..16].try_into().expect("8 bytes"));
    let (meta, rest): (Meta, &[u8]) =
        postcard::take_from_bytes(&covered[HEADER_FIXED_LEN..]).map_err(|_| SnapshotError::FrameCorrupt)?;
    if !rest.is_empty() {
        return Err(SnapshotError::FrameCorrupt);
    }
    Ok(SnapshotItem::Header { meta, created_unix_secs: created })
}

/// Validate a block body's CRC and structure; returns its records and key plus value byte total.
fn parse_block(body: &Bytes, block: u64) -> Result<(Vec<VersionedEntry>, u64), SnapshotError> {
    if body.len() < 4 + 4 {
        return Err(SnapshotError::FrameCorrupt);
    }
    let (covered, crc) = body.split_at(body.len() - 4);
    if crc32c::crc32c(covered) != u32::from_be_bytes(crc.try_into().expect("4 bytes")) {
        return Err(SnapshotError::BlockCrc { block });
    }
    let count = u32::from_be_bytes(covered[..4].try_into().expect("4 bytes")) as usize;
    if count == 0 {
        return Err(SnapshotError::FrameCorrupt);
    }
    let end = covered.len();
    let mut off = 4usize;
    // Every record is at least RECORD_HEADER_LEN bytes, which bounds the allocation.
    let mut records = Vec::with_capacity(count.min(end / RECORD_HEADER_LEN));
    let mut payload = 0u64;
    for _ in 0..count {
        if end - off < RECORD_HEADER_LEN {
            return Err(SnapshotError::FrameCorrupt);
        }
        let mut hdr = &covered[off..off + RECORD_HEADER_LEN];
        let klen = hdr.get_u32() as usize;
        let vlen = hdr.get_u32() as usize;
        let stamp = hdr.get_u64();
        let flags = hdr.get_u8();
        off += RECORD_HEADER_LEN;
        let tombstone = match flags {
            0 => false,
            FLAG_TOMBSTONE => true,
            _ => return Err(SnapshotError::FrameCorrupt),
        };
        if (tombstone && vlen != 0) || klen > end - off || vlen > end - off - klen {
            return Err(SnapshotError::FrameCorrupt);
        }
        let key = body.slice(off..off + klen);
        let value = if tombstone { None } else { Some(body.slice(off + klen..off + klen + vlen)) };
        off += klen + vlen;
        payload += (klen + vlen) as u64;
        records.push(VersionedEntry { key, value, stamp });
    }
    if off != end {
        return Err(SnapshotError::FrameCorrupt);
    }
    Ok((records, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::reader::validate_file;
    use openraft::LogId;

    fn meta(index: u64) -> Meta {
        openraft::SnapshotMeta {
            last_log_id: Some(LogId::new_term_index(2, index)),
            last_membership: Default::default(),
        }
    }

    fn rec(i: u32, vlen: usize, stamp: u64) -> VersionedEntry {
        VersionedEntry {
            key: Bytes::from(format!("key-{i:05}")),
            value: Some(Bytes::from(vec![(i % 251) as u8; vlen])),
            stamp,
        }
    }

    fn tomb(i: u32, stamp: u64) -> VersionedEntry {
        VersionedEntry { key: Bytes::from(format!("key-{i:05}")), value: None, stamp }
    }

    /// Small multi-block file: two large records (one block each), then small ones and a tombstone.
    fn sample_records() -> Vec<VersionedEntry> {
        vec![
            rec(1, 40_000, 5),
            rec(2, 40_000, 6),
            rec(3, 10, 7),
            tomb(4, 9),
            rec(5, 0, 11),
        ]
    }

    fn encode(records: &[VersionedEntry]) -> Vec<u8> {
        let mut w = SnapshotFileWriter::new(Vec::new());
        w.write_header(&meta(7), 1_700_000_000).unwrap();
        for r in records {
            w.push(r).unwrap();
        }
        w.finish().unwrap()
    }

    fn read_all(bytes: &[u8]) -> Result<(Meta, Vec<Vec<VersionedEntry>>, u64), SnapshotError> {
        let mut r = SnapshotFileReader::new(bytes);
        let SnapshotItem::Header { meta, created_unix_secs } = r.next()? else {
            return Err(SnapshotError::FrameCorrupt);
        };
        assert_eq!(created_unix_secs, 1_700_000_000);
        let mut blocks = Vec::new();
        loop {
            match r.next()? {
                SnapshotItem::Block(b) => blocks.push(b),
                SnapshotItem::Footer { records, .. } => return Ok((meta, blocks, records)),
                SnapshotItem::Header { .. } => unreachable!(),
            }
        }
    }

    #[test]
    fn header_block_footer_roundtrip() {
        let recs = sample_records();
        let bytes = encode(&recs);
        let (m, blocks, total) = read_all(&bytes).unwrap();
        assert_eq!(m, meta(7));
        assert_eq!(total, recs.len() as u64);
        assert!(blocks.len() >= 2, "expected several blocks, got {}", blocks.len());
        let flat: Vec<_> = blocks.into_iter().flatten().collect();
        assert_eq!(flat, recs);
    }

    #[test]
    fn block_packing_and_oversize_record_own_block() {
        // 40 KB + 40 KB do not fit one 64 KiB block: the second starts a new block, which the
        // small tail records then share.
        let (_, blocks, _) = read_all(&encode(&sample_records())).unwrap();
        assert_eq!(blocks.iter().map(Vec::len).collect::<Vec<_>>(), vec![1, 4]);

        // A record above the target size sits alone: small before and after it are not merged in.
        let recs = vec![rec(1, 10, 1), rec(2, 100_000, 2), rec(3, 10, 3)];
        let (_, blocks, _) = read_all(&encode(&recs)).unwrap();
        assert_eq!(blocks.iter().map(Vec::len).collect::<Vec<_>>(), vec![1, 1, 1]);
    }

    #[test]
    fn empty_snapshot_roundtrips() {
        let bytes = encode(&[]);
        let (_, blocks, total) = read_all(&bytes).unwrap();
        assert!(blocks.is_empty());
        assert_eq!(total, 0);
    }

    #[test]
    fn oversized_record_rejected() {
        let mut w = SnapshotFileWriter::new(Vec::new());
        w.write_header(&meta(1), 0).unwrap();
        let big = VersionedEntry {
            key: Bytes::from_static(b"k"),
            value: Some(Bytes::from(vec![0u8; MAX_FRAME_LEN])),
            stamp: 1,
        };
        assert!(matches!(w.push(&big), Err(SnapshotError::RecordTooLarge(_))));
    }

    fn write_tmp(dir: &tempfile::TempDir, bytes: &[u8]) -> std::path::PathBuf {
        let p = dir.path().join("f.snap");
        std::fs::write(&p, bytes).unwrap();
        p
    }

    /// A ~2 KiB snapshot: header, one block of records, a tombstone and the footer.
    fn tiny_file() -> Vec<u8> {
        let mut recs: Vec<_> = (0..12).map(|i| rec(i, 100, 3 + i as u64)).collect();
        recs.push(tomb(99, 40));
        encode(&recs)
    }

    #[test]
    fn bit_flip_detected() {
        let good = tiny_file();
        assert!(good.len() < 4096, "keep exhaustive flipping fast: {}", good.len());
        let dir = tempfile::tempdir().unwrap();
        let p = write_tmp(&dir, &good);
        validate_file(&p).expect("pristine file validates");
        for byte in 0..good.len() {
            for bit in 0..8 {
                let mut bad = good.clone();
                bad[byte] ^= 1 << bit;
                std::fs::write(&p, &bad).unwrap();
                assert!(
                    validate_file(&p).is_err(),
                    "flip of byte {byte} bit {bit} went undetected"
                );
            }
        }
    }

    #[test]
    fn truncation_detected() {
        let good = tiny_file();
        let dir = tempfile::tempdir().unwrap();
        let p = write_tmp(&dir, &good);
        for len in 0..good.len() {
            std::fs::write(&p, &good[..len]).unwrap();
            assert!(validate_file(&p).is_err(), "truncation to {len} bytes went undetected");
        }
        // Frame boundaries explicitly: every cut that lands exactly between frames is Truncated.
        let mut codec = new_codec();
        let mut buf = BytesMut::from(&good[..]);
        let mut boundaries = Vec::new();
        while codec.decode(&mut buf).unwrap().is_some() {
            boundaries.push(good.len() - buf.len());
        }
        assert_eq!(boundaries.last().copied(), Some(good.len()));
        assert!(boundaries.len() >= 3);
        for b in &boundaries[..boundaries.len() - 1] {
            assert!(matches!(
                SnapshotFileReader::new(&good[..*b]).next_to_end(),
                Err(SnapshotError::Truncated)
            ));
        }
    }

    impl<R: Read> SnapshotFileReader<R> {
        /// Test helper: drive the reader to the footer.
        fn next_to_end(&mut self) -> Result<(), SnapshotError> {
            loop {
                if let SnapshotItem::Footer { .. } = self.next()? {
                    return Ok(());
                }
            }
        }
    }

    #[test]
    fn footer_count_mismatch_detected() {
        let recs = sample_records();
        let mut bytes = encode(&recs);
        // The footer is the last frame: prefix + header + 24-byte body.
        let footer_frame_len = 4 + crate::wire::frame::HEADER_LEN + FOOTER_LEN;
        let start = bytes.len() - footer_frame_len;
        let seq = u64::from_be_bytes(bytes[start + 4 + 3..start + 4 + 11].try_into().unwrap());
        let payload: u64 = recs
            .iter()
            .map(|r| (r.key.len() + r.value.as_ref().map_or(0, |v| v.len())) as u64)
            .sum();
        // Read the true rolling crc out of the old footer so only the count is wrong.
        let rolling = u32::from_be_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
        let tampered = encode_footer_body(recs.len() as u64 + 1, payload, rolling);
        let mut out = BytesMut::new();
        new_codec()
            .encode(Frame::new(SNAP_FOOTER, Lane::Replication, seq, tampered), &mut out)
            .unwrap();
        bytes.truncate(start);
        bytes.extend_from_slice(&out);
        assert!(matches!(read_all(&bytes), Err(SnapshotError::FooterMismatch)));

        // Sanity: the same rewrite with the true count is accepted.
        let honest = encode_footer_body(recs.len() as u64, payload, rolling);
        let mut out = BytesMut::new();
        new_codec()
            .encode(Frame::new(SNAP_FOOTER, Lane::Replication, seq, honest), &mut out)
            .unwrap();
        bytes.truncate(start);
        bytes.extend_from_slice(&out);
        read_all(&bytes).expect("honest footer accepted");
    }

    #[test]
    fn trailing_bytes_detected() {
        let mut bytes = encode(&sample_records());
        bytes.push(0);
        assert!(matches!(read_all(&bytes), Err(SnapshotError::Trailing)));
        // A whole extra frame after the footer is trailing data as well.
        let mut bytes = encode(&sample_records());
        let again = encode(&[]);
        bytes.extend_from_slice(&again);
        assert!(matches!(read_all(&bytes), Err(SnapshotError::Trailing)));
    }

    #[test]
    fn bad_magic_and_version() {
        let good = tiny_file();
        // Header body starts after prefix(4) + header(11).
        let body = 4 + crate::wire::frame::HEADER_LEN;
        let mut bad = good.clone();
        bad[body] = b'X';
        assert!(matches!(read_all(&bad), Err(SnapshotError::BadMagic)));
        let mut bad = good;
        bad[body + 7] = 9;
        assert!(matches!(read_all(&bad), Err(SnapshotError::UnsupportedVersion(_))));
    }
}
