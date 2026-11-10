//! Proves `FakeIo` (gated behind the `test-util` feature) is importable from integration tests.

use lightcycle::wal::io::{FakeIo, SegmentIo};
use lightcycle::wal::record::{decode_record, encode_record, Decoded, RecordType};

#[test]
fn fake_io_reachable_from_integration_tests() {
    let mut io = FakeIo::new();
    io.create_segment(1, 256).unwrap();
    io.sync_dir().unwrap();

    let mut rec = Vec::new();
    let n = encode_record(&mut rec, RecordType::Entry, b"reachable");
    io.write_at(1, 0, &rec).unwrap();
    io.sync_segment(1).unwrap();
    io.power_loss(0);

    let mut buf = vec![0u8; n];
    assert_eq!(io.read_at(1, 0, &mut buf).unwrap(), n);
    match decode_record(&buf) {
        Decoded::Record { ty, payload, consumed } => {
            assert_eq!(ty, RecordType::Entry);
            assert_eq!(payload, b"reachable");
            assert_eq!(consumed, n);
        }
        other => panic!("expected record, got {other:?}"),
    }
}
