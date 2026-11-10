//! WAL record framing with CRC32C (decision D-01).
//!
//! Layout: `[len u32 LE][crc32c u32 LE][type u8][payload]`, where `len = 1 + payload.len()` (the
//! type byte plus the payload) and the CRC covers the four `len` bytes, the type byte and then the
//! payload. A header that is entirely zero marks unwritten preallocated space.
//!
//! Entry payload = `index u64 LE || body bytes`. TruncateAfter payload = a flag byte (0 = keep
//! nothing, 1 = keep through) followed by a `u64 LE` index when the flag is 1.

/// Bytes in the fixed record header: length, CRC and type.
pub const RECORD_HEADER_LEN: usize = 9;

/// Largest accepted `len` field (type byte plus payload).
pub const MAX_RECORD_LEN: u32 = 16 * 1024 * 1024;

/// Kind of a WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// First record of every segment; carries the segment preamble.
    Preamble = 1,
    /// A Raft log entry.
    Entry = 2,
    /// The persisted vote.
    Vote = 3,
    /// Truncation marker: entries after the kept index are discarded.
    TruncateAfter = 4,
    /// Purge marker: entries up to and including the purge point are discarded.
    Purge = 5,
    /// The persisted committed log id.
    Committed = 6,
}

impl RecordType {
    /// Parses the on-disk type byte.
    pub fn from_u8(byte: u8) -> Option<RecordType> {
        match byte {
            1 => Some(RecordType::Preamble),
            2 => Some(RecordType::Entry),
            3 => Some(RecordType::Vote),
            4 => Some(RecordType::TruncateAfter),
            5 => Some(RecordType::Purge),
            6 => Some(RecordType::Committed),
            _ => None,
        }
    }
}

/// Result of decoding one record from the front of a buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded<'a> {
    /// A valid record.
    Record {
        /// Record kind.
        ty: RecordType,
        /// Payload bytes (borrowed from the input buffer).
        payload: &'a [u8],
        /// Total bytes the record occupies, header included.
        consumed: usize,
    },
    /// Unwritten space: the header is all zero (or fewer than a header's worth of bytes remain).
    End,
    /// Bad length, short body, CRC mismatch or unknown type.
    Invalid,
}

fn record_crc(len_bytes: &[u8; 4], ty: u8, payload: &[u8]) -> u32 {
    let crc = crc32c::crc32c(len_bytes);
    let crc = crc32c::crc32c_append(crc, &[ty]);
    crc32c::crc32c_append(crc, payload)
}

/// Appends one framed record to `out` and returns the number of bytes appended.
///
/// The caller is responsible for keeping the payload within [`MAX_RECORD_LEN`]; the writer rejects
/// oversize records with `WalError::RecordTooLarge` before framing.
pub fn encode_record(out: &mut Vec<u8>, ty: RecordType, payload: &[u8]) -> usize {
    let len = (1 + payload.len()) as u32;
    let len_bytes = len.to_le_bytes();
    let crc = record_crc(&len_bytes, ty as u8, payload);
    let start = out.len();
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(&crc.to_le_bytes());
    out.push(ty as u8);
    out.extend_from_slice(payload);
    out.len() - start
}

/// Decodes the record at the front of `buf`. Never panics on any input.
pub fn decode_record(buf: &[u8]) -> Decoded<'_> {
    if buf.len() < RECORD_HEADER_LEN {
        return Decoded::End;
    }
    let header = &buf[..RECORD_HEADER_LEN];
    if header.iter().all(|b| *b == 0) {
        return Decoded::End;
    }
    let len_bytes: [u8; 4] = [header[0], header[1], header[2], header[3]];
    let len = u32::from_le_bytes(len_bytes);
    if len == 0 || len > MAX_RECORD_LEN {
        return Decoded::Invalid;
    }
    let crc_stored = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let total = 8usize + len as usize;
    if buf.len() < total {
        return Decoded::Invalid;
    }
    let ty_byte = header[8];
    let payload = &buf[RECORD_HEADER_LEN..total];
    if record_crc(&len_bytes, ty_byte, payload) != crc_stored {
        return Decoded::Invalid;
    }
    match RecordType::from_u8(ty_byte) {
        Some(ty) => Decoded::Record {
            ty,
            payload,
            consumed: total,
        },
        None => Decoded::Invalid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [RecordType; 6] = [
        RecordType::Preamble,
        RecordType::Entry,
        RecordType::Vote,
        RecordType::TruncateAfter,
        RecordType::Purge,
        RecordType::Committed,
    ];

    #[test]
    fn roundtrip_all_record_types() {
        for ty in ALL {
            for payload in [&b""[..], &b"x"[..], &[7u8; 1000][..]] {
                let mut buf = Vec::new();
                let n = encode_record(&mut buf, ty, payload);
                assert_eq!(n, buf.len());
                assert_eq!(n, RECORD_HEADER_LEN + payload.len());
                match decode_record(&buf) {
                    Decoded::Record {
                        ty: t,
                        payload: p,
                        consumed,
                    } => {
                        assert_eq!(t, ty);
                        assert_eq!(p, payload);
                        assert_eq!(consumed, n);
                    }
                    other => panic!("expected record, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn two_records_back_to_back_then_end() {
        let mut buf = Vec::new();
        let a = encode_record(&mut buf, RecordType::Entry, b"first");
        encode_record(&mut buf, RecordType::Vote, b"second");
        buf.extend_from_slice(&[0u8; 32]);
        let Decoded::Record { consumed, .. } = decode_record(&buf) else {
            panic!("first record");
        };
        assert_eq!(consumed, a);
        let Decoded::Record { ty, payload, consumed: c2 } = decode_record(&buf[a..]) else {
            panic!("second record");
        };
        assert_eq!(ty, RecordType::Vote);
        assert_eq!(payload, b"second");
        assert_eq!(decode_record(&buf[a + c2..]), Decoded::End);
    }

    #[test]
    fn crc_known_vector() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn crc_detects_every_single_bit_flip() {
        let payload: Vec<u8> = (0..64u8).collect();
        let mut buf = Vec::new();
        encode_record(&mut buf, RecordType::Entry, &payload);
        for byte in 0..buf.len() {
            for bit in 0..8 {
                let mut bad = buf.clone();
                bad[byte] ^= 1 << bit;
                if let Decoded::Record { payload: p, .. } = decode_record(&bad) {
                    panic!("flip of byte {byte} bit {bit} accepted (payload equal: {})", p == payload);
                }
            }
        }
    }

    #[test]
    fn zero_header_is_end() {
        assert_eq!(decode_record(&[0u8; RECORD_HEADER_LEN]), Decoded::End);
        assert_eq!(decode_record(&[0u8; 64]), Decoded::End);
        assert_eq!(decode_record(&[]), Decoded::End);
        assert_eq!(decode_record(&[1, 2, 3]), Decoded::End);
    }

    #[test]
    fn zero_len_with_nonzero_header_is_invalid() {
        let mut buf = [0u8; RECORD_HEADER_LEN];
        buf[4] = 1;
        assert_eq!(decode_record(&buf), Decoded::Invalid);
    }

    #[test]
    fn oversize_len_is_invalid() {
        let mut buf = vec![0u8; RECORD_HEADER_LEN + 8];
        buf[..4].copy_from_slice(&(MAX_RECORD_LEN + 1).to_le_bytes());
        buf[8] = RecordType::Entry as u8;
        assert_eq!(decode_record(&buf), Decoded::Invalid);
        buf[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode_record(&buf), Decoded::Invalid);
    }

    #[test]
    fn short_body_is_invalid() {
        let mut buf = Vec::new();
        encode_record(&mut buf, RecordType::Entry, &[9u8; 40]);
        buf.truncate(buf.len() - 1);
        assert_eq!(decode_record(&buf), Decoded::Invalid);
    }

    #[test]
    fn unknown_type_is_invalid() {
        let mut buf = Vec::new();
        encode_record(&mut buf, RecordType::Entry, b"abc");
        // Re-frame with an unknown type byte and a matching CRC so only the type is wrong.
        let len_bytes: [u8; 4] = buf[..4].try_into().unwrap();
        let crc = record_crc(&len_bytes, 99, b"abc");
        buf[4..8].copy_from_slice(&crc.to_le_bytes());
        buf[8] = 99;
        assert_eq!(decode_record(&buf), Decoded::Invalid);
    }
}
