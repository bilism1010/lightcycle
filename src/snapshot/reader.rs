//! Snapshot readers: streaming restore into a fresh engine, full validation, header-only peek.

use std::fs::File;
use std::path::Path;

use crate::engine::LocalEngine;
use crate::snapshot::format::{Meta, SnapshotError, SnapshotFileReader, SnapshotItem};
use crate::storage::VersionedEntry;

fn open(path: &Path) -> Result<SnapshotFileReader<File>, SnapshotError> {
    Ok(SnapshotFileReader::new(File::open(path)?))
}

fn expect_header(r: &mut SnapshotFileReader<File>) -> Result<Meta, SnapshotError> {
    match r.next()? {
        SnapshotItem::Header { meta, .. } => Ok(meta),
        _ => Err(SnapshotError::FrameCorrupt),
    }
}

/// Stream the whole file through `on_block`, validating every frame, the footer counts and the
/// rolling CRC. Memory is bounded by one block.
fn walk(
    path: &Path,
    mut on_block: impl FnMut(Vec<VersionedEntry>),
) -> Result<Meta, SnapshotError> {
    let mut r = open(path)?;
    let meta = expect_header(&mut r)?;
    loop {
        match r.next()? {
            SnapshotItem::Block(b) => on_block(b),
            SnapshotItem::Footer { .. } => return Ok(meta),
            SnapshotItem::Header { .. } => return Err(SnapshotError::FrameCorrupt),
        }
    }
}

/// Restore the snapshot at `path` into a fresh engine. Tombstones restore as stamped slots.
///
/// The file is fully validated; any error leaves nothing to clean up because the engine is only
/// returned on success.
pub fn load_snapshot(path: &Path) -> Result<(Meta, LocalEngine), SnapshotError> {
    let engine = LocalEngine::new();
    let meta = walk(path, |block| {
        for e in block {
            engine.load_record(&e.key, e.value.as_deref(), e.stamp);
        }
    })?;
    Ok((meta, engine))
}

/// Validate the whole file (every frame, block CRC, footer) without building an engine.
pub fn validate_file(path: &Path) -> Result<Meta, SnapshotError> {
    walk(path, drop)
}

/// Validate only the header frame and return its metadata.
pub fn read_header(path: &Path) -> Result<Meta, SnapshotError> {
    expect_header(&mut open(path)?)
}
