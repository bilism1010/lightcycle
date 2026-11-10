//! On-disk snapshot store: exactly one valid snapshot, durable before any older one is deleted.
//!
//! Files are named `snap-<index:020>-<term:020>.snap` (`.tmp` while being written). Names are
//! derived only from the numeric log index and term and are parsed strictly: anything else in the
//! directory is ignored, so a peer-supplied string can never reach a path.
//!
//! Durability order for a new snapshot (pitfall 3, the log is purged right after): write the
//! `.tmp` and `sync_all` it, rename to `.snap`, fsync the directory, and only then delete older
//! snapshots. A corrupt latest snapshot is a hard error ([`SnapshotError::CorruptNeedsWipe`]):
//! there is no fallback to an older file because the log below the newest one has been purged.

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::engine::LocalEngine;
use crate::raft::types::SnapshotFile;
use crate::snapshot::format::{Meta, SnapshotError};
use crate::snapshot::reader::{read_header, validate_file};
use crate::snapshot::writer::build_snapshot_file;

/// Steps of publishing a snapshot; a hook error after a stage simulates a crash right there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// The `.tmp` file is fully written.
    TmpWritten,
    /// The `.tmp` file is synced (the writer syncs before returning).
    TmpSynced,
    /// The `.tmp` file was renamed to `.snap`.
    Renamed,
    /// The directory entry is durable.
    DirSynced,
    /// Older snapshots were deleted.
    OldDeleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ext {
    Snap,
    Tmp,
}

/// Re-lists attempted by [`SnapshotStore::latest`] when the newest file vanishes under it.
const LATEST_RETRIES: usize = 3;

const DIGITS: usize = 20;
/// `snap-` + 20 digits + `-` + 20 digits.
const STEM_LEN: usize = 5 + DIGITS + 1 + DIGITS;

fn file_name(index: u64, term: u64, ext: Ext) -> String {
    let ext = match ext {
        Ext::Snap => "snap",
        Ext::Tmp => "tmp",
    };
    format!("snap-{index:020}-{term:020}.{ext}")
}

/// Strictly parse `snap-<20 digits>-<20 digits>.(snap|tmp)`; anything else is `None`.
fn parse_name(name: &str) -> Option<(u64, u64, Ext)> {
    let (stem, ext) = if let Some(s) = name.strip_suffix(".snap") {
        (s, Ext::Snap)
    } else if let Some(s) = name.strip_suffix(".tmp") {
        (s, Ext::Tmp)
    } else {
        return None;
    };
    if stem.len() != STEM_LEN || !stem.is_ascii() {
        return None;
    }
    let b = stem.as_bytes();
    if &b[..5] != b"snap-" || b[5 + DIGITS] != b'-' {
        return None;
    }
    let (idx, term) = (&stem[5..5 + DIGITS], &stem[6 + DIGITS..]);
    if !idx.bytes().all(|c| c.is_ascii_digit()) || !term.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some((idx.parse().ok()?, term.parse().ok()?, ext))
}

/// `(index, term)` a snapshot's metadata is filed under; `(0, 0)` when it covers no log entry.
fn key_of(meta: &Meta) -> (u64, u64) {
    meta.last_log_id
        .as_ref()
        .map_or((0, 0), |l| (l.index(), l.leader_id.term))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Handle to the snapshot directory.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    dir: PathBuf,
}

impl SnapshotStore {
    /// Open (creating with mode 0700) the snapshot directory, remove leftover `.tmp` files from
    /// an interrupted build or install, and delete all but the highest-index `.snap`.
    pub fn open(dir: impl Into<PathBuf>) -> Result<SnapshotStore, SnapshotError> {
        let dir = dir.into();
        let mut b = fs::DirBuilder::new();
        b.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            b.mode(0o700);
        }
        b.create(&dir)?;
        let store = SnapshotStore { dir };
        for (_, _, ext, path) in store.entries()? {
            if ext == Ext::Tmp {
                fs::remove_file(path)?;
            }
        }
        let snaps = store.snaps()?;
        if let Some(newest) = snaps.last().map(|s| (s.0, s.1)) {
            store.delete_snaps_where(|k| k != newest)?;
        }
        Ok(store)
    }

    /// Directory this store manages.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Strictly named regular files in the directory (symlinks and other names are ignored).
    fn entries(&self) -> io::Result<Vec<(u64, u64, Ext, PathBuf)>> {
        let mut out = Vec::new();
        for ent in fs::read_dir(&self.dir)? {
            let ent = ent?;
            if !ent.file_type()?.is_file() {
                continue;
            }
            let Some(name) = ent.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if let Some((index, term, ext)) = parse_name(&name) {
                out.push((index, term, ext, ent.path()));
            }
        }
        Ok(out)
    }

    /// Published snapshots, sorted by `(index, term)` ascending.
    fn snaps(&self) -> io::Result<Vec<(u64, u64, PathBuf)>> {
        let mut v: Vec<_> = self
            .entries()?
            .into_iter()
            .filter(|e| e.2 == Ext::Snap)
            .map(|(i, t, _, p)| (i, t, p))
            .collect();
        v.sort();
        Ok(v)
    }

    /// Removes the published snapshots whose key satisfies `pred`. A file that is already gone
    /// (a concurrent build or install removed it between the listing and the unlink) is the
    /// desired end state, not an error.
    fn delete_snaps_where(&self, pred: impl Fn((u64, u64)) -> bool) -> io::Result<()> {
        for (i, t, p) in self.snaps()? {
            if pred((i, t)) {
                match fs::remove_file(p) {
                    Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// The newest snapshot, if any. Its header must parse, validate and agree with its file name;
    /// otherwise [`SnapshotError::CorruptNeedsWipe`] (no fallback to an older file). Block-level
    /// integrity is checked when the snapshot is loaded or installed.
    pub fn latest(&self) -> Result<Option<(Meta, SnapshotFile)>, SnapshotError> {
        // A concurrent build or install can delete the file between listing and reading it. That
        // is a lost race, not a failure: list again, which then finds the newer file.
        for _ in 0..LATEST_RETRIES {
            match self.latest_once() {
                Err(SnapshotError::Io(e)) if e.kind() == io::ErrorKind::NotFound => continue,
                other => return other,
            }
        }
        self.latest_once()
    }

    fn latest_once(&self) -> Result<Option<(Meta, SnapshotFile)>, SnapshotError> {
        let Some((index, term, path)) = self.snaps()?.pop() else {
            return Ok(None);
        };
        let meta = match read_header(&path) {
            Ok(m) => m,
            Err(SnapshotError::Io(e)) => return Err(SnapshotError::Io(e)),
            Err(_) => return Err(SnapshotError::CorruptNeedsWipe),
        };
        if key_of(&meta) != (index, term) {
            return Err(SnapshotError::CorruptNeedsWipe);
        }
        let len = fs::metadata(&path)?.len();
        Ok(Some((meta, SnapshotFile { path, len })))
    }

    /// Snapshot `engine` as `meta` and publish it; see [`build_with_hook`](Self::build_with_hook).
    pub fn build(&self, engine: &LocalEngine, meta: &Meta) -> Result<SnapshotFile, SnapshotError> {
        self.build_with_hook(engine, meta, |_| Ok(()))
    }

    /// Build and publish a snapshot, calling `hook` after each [`Stage`]. A hook error aborts at
    /// that stage, leaving the directory exactly as a crash there would (a failed write, by
    /// contrast, removes its partial `.tmp`). Returns only when the new file and its directory
    /// entry are durable and older snapshots are gone.
    pub fn build_with_hook(
        &self,
        engine: &LocalEngine,
        meta: &Meta,
        mut hook: impl FnMut(Stage) -> io::Result<()>,
    ) -> Result<SnapshotFile, SnapshotError> {
        let (index, term) = key_of(meta);
        let tmp = self.dir.join(file_name(index, term, Ext::Tmp));
        let fin = self.dir.join(file_name(index, term, Ext::Snap));
        match fs::remove_file(&tmp) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        let len = match build_snapshot_file(engine, meta, &tmp) {
            Ok(len) => len,
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                return Err(e);
            }
        };
        hook(Stage::TmpWritten)?;
        hook(Stage::TmpSynced)?;
        fs::rename(&tmp, &fin)?;
        hook(Stage::Renamed)?;
        sync_dir(&self.dir)?;
        hook(Stage::DirSynced)?;
        self.delete_snaps_where(|k| k < (index, term))?;
        sync_dir(&self.dir)?;
        hook(Stage::OldDeleted)?;
        Ok(SnapshotFile { path: fin, len })
    }

    /// Adopt a snapshot received from a peer at `tmp`: the whole file must validate and its
    /// header must equal `expected`; then it is renamed into place, the directory synced and
    /// older snapshots removed. On any rejection `tmp` is left unrenamed.
    pub fn install_received(
        &self,
        tmp: &Path,
        expected: &Meta,
    ) -> Result<SnapshotFile, SnapshotError> {
        let meta = validate_file(tmp)?;
        if &meta != expected {
            return Err(SnapshotError::MetaMismatch);
        }
        let (index, term) = key_of(expected);
        let fin = self.dir.join(file_name(index, term, Ext::Snap));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(tmp, fs::Permissions::from_mode(0o600))?;
        }
        let len = fs::metadata(tmp)?.len();
        fs::rename(tmp, &fin)?;
        sync_dir(&self.dir)?;
        self.delete_snaps_where(|k| k < (index, term))?;
        sync_dir(&self.dir)?;
        Ok(SnapshotFile { path: fin, len })
    }

    /// Path a received snapshot for `meta` may be staged at before [`install_received`](Self::install_received).
    pub fn staging_path(&self, meta: &Meta) -> PathBuf {
        let (index, term) = key_of(meta);
        self.dir.join(file_name(index, term, Ext::Tmp))
    }

    /// Read up to `len` bytes of `path` starting at `offset` (fewer at end of file, none past it).
    pub fn read_chunk(path: &Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut f = File::open(path)?;
        f.seek(SeekFrom::Start(offset))?;
        let mut out = Vec::new();
        f.take(len as u64).read_to_end(&mut out)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::LogId;

    fn meta(index: u64, term: u64) -> Meta {
        openraft::SnapshotMeta {
            last_log_id: Some(LogId::new_term_index(term, index)),
            last_membership: Default::default(),
        }
    }

    fn engine(n: u64) -> LocalEngine {
        let e = LocalEngine::new();
        for i in 0..n {
            e.set_at(i + 1, format!("k{i}").as_bytes(), format!("v{i}").as_bytes());
        }
        e
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        v.sort();
        v
    }

    fn crash_at(stage: Stage) -> impl FnMut(Stage) -> io::Result<()> {
        move |s| {
            if s == stage {
                Err(io::Error::other("simulated crash"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn name_parsing_is_strict() {
        let good = file_name(7, 3, Ext::Snap);
        assert_eq!(parse_name(&good), Some((7, 3, Ext::Snap)));
        assert_eq!(parse_name(&file_name(u64::MAX, 1, Ext::Tmp)), Some((u64::MAX, 1, Ext::Tmp)));
        for bad in [
            "snap-../x.snap",
            "snap-abc.snap",
            "snap-1-2.snap",
            "../snap-00000000000000000007-00000000000000000003.snap",
            "snap-00000000000000000007-00000000000000000003.snap.bak",
            "snap-00000000000000000007_00000000000000000003.snap",
            "snap-0000000000000000000x-00000000000000000003.snap",
            "snap-+0000000000000000007-00000000000000000003.snap",
            "snap-99999999999999999999-00000000000000000003.snap", // > u64::MAX
            "snap-00000000000000000007-00000000000000000003",
            "",
        ] {
            assert_eq!(parse_name(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn numeric_names_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().join("s")).unwrap();
        for junk in ["snap-abc.snap", "x.snap", "snap-1-2.snap", "notes.txt"] {
            fs::write(store.dir().join(junk), b"junk").unwrap();
        }
        assert!(store.latest().unwrap().is_none());
        let reopened = SnapshotStore::open(store.dir()).unwrap();
        assert!(reopened.latest().unwrap().is_none());
        assert_eq!(names(store.dir()).len(), 4, "foreign files are ignored, not deleted");
        #[cfg(unix)]
        {
            // A symlink with a valid name is not a regular file and is ignored.
            let target = dir.path().join("elsewhere");
            fs::write(&target, b"x").unwrap();
            std::os::unix::fs::symlink(&target, store.dir().join(file_name(9, 1, Ext::Snap))).unwrap();
            assert!(store.latest().unwrap().is_none());
        }
    }

    #[cfg(unix)]
    #[test]
    fn directory_is_0700_and_files_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().join("snaps")).unwrap();
        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(store.dir()), 0o700);
        let f = store.build(&engine(3), &meta(3, 1)).unwrap();
        assert_eq!(mode(&f.path), 0o600);
    }

    #[test]
    fn build_and_latest() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        assert!(store.latest().unwrap().is_none());
        let f = store.build(&engine(10), &meta(10, 2)).unwrap();
        assert_eq!(f.path.file_name().unwrap().to_str().unwrap(), file_name(10, 2, Ext::Snap));
        assert_eq!(f.len, fs::metadata(&f.path).unwrap().len());
        let (m, got) = store.latest().unwrap().unwrap();
        assert_eq!(m, meta(10, 2));
        assert_eq!(got, f);
    }

    #[test]
    fn retention_keeps_exactly_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        store.build(&engine(5), &meta(5, 1)).unwrap();
        let old = file_name(5, 1, Ext::Snap);
        let new = file_name(9, 1, Ext::Snap);
        let d = store.dir().to_path_buf();
        let (o, n) = (old.clone(), new.clone());
        store
            .build_with_hook(&engine(9), &meta(9, 1), move |stage| {
                let has = |name: &str| d.join(name).exists();
                match stage {
                    Stage::TmpWritten | Stage::TmpSynced => assert!(has(&o) && !has(&n)),
                    // The older snapshot must outlive the rename and the directory sync.
                    Stage::Renamed | Stage::DirSynced => assert!(has(&o) && has(&n)),
                    Stage::OldDeleted => assert!(!has(&o) && has(&n)),
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(names(dir.path()), vec![new]);
        assert_eq!(store.latest().unwrap().unwrap().0, meta(9, 1));
    }

    #[test]
    fn crash_before_rename_leaves_old_snapshot() {
        for stage in [Stage::TmpWritten, Stage::TmpSynced] {
            let dir = tempfile::tempdir().unwrap();
            let store = SnapshotStore::open(dir.path()).unwrap();
            store.build(&engine(5), &meta(5, 1)).unwrap();
            let err = store.build_with_hook(&engine(9), &meta(9, 1), crash_at(stage)).unwrap_err();
            assert!(matches!(err, SnapshotError::Io(_)), "{stage:?}");
            // The old snapshot is still the latest and still fully valid.
            let (m, f) = store.latest().unwrap().unwrap();
            assert_eq!(m, meta(5, 1));
            validate_file(&f.path).unwrap();
            assert!(names(dir.path()).contains(&file_name(9, 1, Ext::Tmp)), "{stage:?}");
            // Restart: the leftover tmp is removed.
            let store = SnapshotStore::open(dir.path()).unwrap();
            assert_eq!(names(dir.path()), vec![file_name(5, 1, Ext::Snap)]);
            assert_eq!(store.latest().unwrap().unwrap().0, meta(5, 1));
        }
    }

    #[test]
    fn crash_after_rename_before_old_deleted() {
        for stage in [Stage::Renamed, Stage::DirSynced] {
            let dir = tempfile::tempdir().unwrap();
            let store = SnapshotStore::open(dir.path()).unwrap();
            store.build(&engine(5), &meta(5, 1)).unwrap();
            store.build_with_hook(&engine(9), &meta(9, 1), crash_at(stage)).unwrap_err();
            assert_eq!(names(dir.path()).len(), 2, "{stage:?}: both exist");
            assert_eq!(store.latest().unwrap().unwrap().0, meta(9, 1));
            SnapshotStore::open(dir.path()).unwrap();
            assert_eq!(names(dir.path()), vec![file_name(9, 1, Ext::Snap)]);
        }
    }

    #[test]
    fn corrupt_latest_is_hard_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        store.build(&engine(5), &meta(5, 1)).unwrap();
        // Leave both snapshots on disk, then corrupt the newer one.
        store.build_with_hook(&engine(9), &meta(9, 1), crash_at(Stage::DirSynced)).unwrap_err();
        let newest = dir.path().join(file_name(9, 1, Ext::Snap));
        let mut bytes = fs::read(&newest).unwrap();
        bytes[20] ^= 0xff; // inside the header body
        fs::write(&newest, &bytes).unwrap();
        assert!(matches!(store.latest(), Err(SnapshotError::CorruptNeedsWipe)));
        // Truncated to nothing is just as fatal, and the older file is never used instead.
        fs::write(&newest, b"").unwrap();
        assert!(matches!(store.latest(), Err(SnapshotError::CorruptNeedsWipe)));
    }

    #[test]
    fn header_disagreeing_with_name_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let f = store.build(&engine(5), &meta(5, 1)).unwrap();
        fs::rename(&f.path, dir.path().join(file_name(6, 1, Ext::Snap))).unwrap();
        assert!(matches!(store.latest(), Err(SnapshotError::CorruptNeedsWipe)));
    }

    #[test]
    fn failed_build_removes_partial_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        // A directory squatting on the final name makes the rename fail after a good write.
        fs::create_dir(dir.path().join(file_name(3, 1, Ext::Snap))).unwrap();
        assert!(store.build(&engine(3), &meta(3, 1)).is_err());
    }

    #[test]
    fn chunked_copy_reassembles_identical_bytes() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let src = SnapshotStore::open(src_dir.path()).unwrap();
        let dst = SnapshotStore::open(dst_dir.path()).unwrap();
        let m = meta(500, 4);
        let f = src.build(&engine(500), &m).unwrap();

        let staged = dst.staging_path(&m);
        let mut bytes = Vec::new();
        let mut off = 0u64;
        loop {
            let chunk = SnapshotStore::read_chunk(&f.path, off, 1000).unwrap();
            if chunk.is_empty() {
                break;
            }
            off += chunk.len() as u64;
            bytes.extend_from_slice(&chunk);
        }
        assert_eq!(bytes, fs::read(&f.path).unwrap());
        assert_eq!(off, f.len);
        assert!(SnapshotStore::read_chunk(&f.path, f.len + 10, 1000).unwrap().is_empty());
        fs::write(&staged, &bytes).unwrap();

        let installed = dst.install_received(&staged, &m).unwrap();
        assert_eq!(installed.len, f.len);
        assert!(!staged.exists());
        assert_eq!(dst.latest().unwrap().unwrap().0, m);
        assert_eq!(fs::read(&installed.path).unwrap(), bytes);
    }

    #[test]
    fn install_received_rejects_meta_mismatch_and_corruption() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let src = SnapshotStore::open(src_dir.path()).unwrap();
        let dst = SnapshotStore::open(dst_dir.path()).unwrap();
        let m = meta(50, 2);
        let f = src.build(&engine(50), &m).unwrap();
        let good = fs::read(&f.path).unwrap();
        let staged = dst.staging_path(&m);

        // Meta announced differently from what the file carries.
        fs::write(&staged, &good).unwrap();
        assert!(matches!(
            dst.install_received(&staged, &meta(51, 2)),
            Err(SnapshotError::MetaMismatch)
        ));
        assert!(staged.exists());

        // Corrupted body (flip a byte near the end, inside the last block or footer).
        let mut bad = good.clone();
        let at = bad.len() - 30;
        bad[at] ^= 0x01;
        fs::write(&staged, &bad).unwrap();
        assert!(dst.install_received(&staged, &m).is_err());
        // Truncated.
        fs::write(&staged, &good[..good.len() - 1]).unwrap();
        assert!(dst.install_received(&staged, &m).is_err());

        assert!(staged.exists(), "rejected tmp stays unrenamed");
        assert!(dst.latest().unwrap().is_none());
        assert_eq!(names(dst_dir.path()), vec![file_name(50, 2, Ext::Tmp)]);
    }

    #[test]
    fn install_removes_older_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let donor = SnapshotStore::open(other.path()).unwrap();
        store.build(&engine(5), &meta(5, 1)).unwrap();
        let m = meta(40, 2);
        let f = donor.build(&engine(40), &m).unwrap();
        let staged = store.staging_path(&m);
        fs::copy(&f.path, &staged).unwrap();
        store.install_received(&staged, &m).unwrap();
        assert_eq!(names(dir.path()), vec![file_name(40, 2, Ext::Snap)]);
    }
}
