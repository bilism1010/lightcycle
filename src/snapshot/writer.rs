//! Fuzzy snapshot writer: epoch-pinned scan of a live engine into a snapshot file (D-09, D-11).

use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::LocalEngine;
use crate::snapshot::format::{Meta, SnapshotError, SnapshotFileWriter};

/// Buffer between the frame writer and the file: a few blocks per `write(2)`.
const FILE_BUF: usize = 256 * 1024;

/// Scan `engine` into a new snapshot file at `path` and return its length after `sync_all`.
///
/// Blocking; run it on a blocking thread. The file is created with `create_new` and mode 0600.
/// The scan threshold is the snapshot start index (`meta.last_log_id` index, or 0): live keys of
/// any stamp are kept, tombstones with a stamp above it are kept (dropping them is unsound), and
/// tombstones at or below it are dropped (R-2). The scan holds no epoch pin across keys, and the
/// callback does nothing but a buffered write.
pub fn build_snapshot_file(
    engine: &LocalEngine,
    meta: &Meta,
    path: &Path,
) -> Result<u64, SnapshotError> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(path)?;

    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let threshold = meta.last_log_id.as_ref().map_or(0, |l| l.index());

    let mut w = SnapshotFileWriter::new(BufWriter::with_capacity(FILE_BUF, file));
    w.write_header(meta, created)?;
    let mut err: Option<SnapshotError> = None;
    engine.scan_versioned(threshold, |e| {
        if err.is_none() {
            if let Err(e) = w.push(&e) {
                err = Some(e);
            }
        }
    });
    if let Some(e) = err {
        return Err(e);
    }
    let buffered = w.finish()?;
    let file = buffered.into_inner().map_err(|e| SnapshotError::Io(e.into_error()))?;
    file.sync_all()?;
    Ok(file.metadata()?.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::reader::{load_snapshot, validate_file};
    use openraft::LogId;

    fn meta(index: u64) -> Meta {
        openraft::SnapshotMeta {
            last_log_id: Some(LogId::new_term_index(1, index)),
            last_membership: Default::default(),
        }
    }

    #[test]
    fn roundtrip_three_keys_with_tombstone_rule() {
        let live = LocalEngine::new();
        live.set_at(2, b"a", b"value-a");
        live.set_at(1, b"b", b"value-b");
        live.delete_at(3, b"b"); // tombstone stamp 3 <= 5: dropped
        live.set_at(6, b"c", b"value-c");
        live.delete_at(7, b"c"); // tombstone stamp 7 > 5: kept

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.snap");
        let len = build_snapshot_file(&live, &meta(5), &path).unwrap();
        assert_eq!(len, std::fs::metadata(&path).unwrap().len());
        assert_eq!(validate_file(&path).unwrap(), meta(5));

        let (m, restored) = load_snapshot(&path).unwrap();
        assert_eq!(m, meta(5));
        assert_eq!(restored.get(b"a").unwrap(), &b"value-a"[..]);
        assert_eq!(restored.index().stamp_of(b"a"), Some(2));
        assert_eq!(restored.get(b"b"), None);
        assert_eq!(restored.index().stamp_of(b"b"), None, "stale tombstone must be dropped");
        assert_eq!(restored.get(b"c"), None);
        assert_eq!(restored.index().stamp_of(b"c"), Some(7), "newer tombstone must be kept");
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_file_is_0600_and_create_new() {
        use std::os::unix::fs::PermissionsExt;
        let live = LocalEngine::new();
        live.set_at(1, b"k", b"v");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.snap");
        build_snapshot_file(&live, &meta(1), &path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert!(matches!(
            build_snapshot_file(&live, &meta(1), &path),
            Err(SnapshotError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists
        ));
    }

    #[test]
    fn large_engine_roundtrips_across_many_blocks() {
        let live = LocalEngine::new();
        for i in 0..5_000u64 {
            live.set_at(i + 1, format!("key-{i:06}").as_bytes(), &[(i % 200) as u8; 100]);
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.snap");
        build_snapshot_file(&live, &meta(5_000), &path).unwrap();
        let (_, restored) = load_snapshot(&path).unwrap();
        for i in 0..5_000u64 {
            let k = format!("key-{i:06}");
            assert_eq!(restored.get(k.as_bytes()), live.get(k.as_bytes()), "{k}");
            assert_eq!(restored.index().stamp_of(k.as_bytes()), Some(i + 1));
        }
    }
}
