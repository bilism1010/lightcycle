//! Segment I/O seam (decision D-03).
//!
//! All WAL file access goes through [`SegmentIo`]. [`StdFsIo`] is the production backend: plain
//! `std::fs` calls on the caller's thread (never `tokio::fs`) so an io_uring backend can replace it
//! later without touching the writer. [`FakeIo`] is a deterministic in-memory backend used by every
//! durability test in this phase; it is gated behind `cfg(any(test, feature = "test-util"))`.
//!
//! The std backend uses Unix-only APIs (`FileExt`, file modes), matching the supported targets.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

/// Positional read access to segments, shareable across threads (used by the entry cache and the
/// log reader so reads never go through the writer thread).
pub trait SegmentRead: Send + Sync {
    /// Reads up to `buf.len()` bytes at `offset`; returns the number of bytes read (0 at EOF).
    fn read_at(&self, seq: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
}

/// Mutating segment operations, owned by the single WAL writer thread.
pub trait SegmentIo: Send + 'static {
    /// Segment sequence numbers present in the directory, ascending.
    fn list_segments(&self) -> io::Result<Vec<u64>>;
    /// Exclusively creates a segment (mode 0600) of `size` zero-filled bytes.
    fn create_segment(&mut self, seq: u64, size: u64) -> io::Result<()>;
    /// Writes all of `data` at `offset`.
    fn write_at(&mut self, seq: u64, offset: u64, data: &[u8]) -> io::Result<()>;
    /// Makes the segment's data durable (a data sync).
    fn sync_segment(&mut self, seq: u64) -> io::Result<()>;
    /// Makes directory entries (segment creations and deletions) durable.
    fn sync_dir(&mut self) -> io::Result<()>;
    /// Truncates or extends the segment to `len` bytes.
    fn set_len(&mut self, seq: u64, len: u64) -> io::Result<()>;
    /// Deletes a segment file.
    fn delete_segment(&mut self, seq: u64) -> io::Result<()>;
    /// Reads up to `buf.len()` bytes at `offset`; returns the number of bytes read.
    fn read_at(&self, seq: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    /// Current length of the segment in bytes.
    fn len(&self, seq: u64) -> io::Result<u64>;
    /// A thread-safe read handle sharing this backend's state.
    fn reader(&self) -> Arc<dyn SegmentRead>;
}

// Private numeric-only name helpers for Task 1; replaced by the shared ones in `segment.rs`.
fn file_name(seq: u64) -> String {
    format!("{seq:016x}.wal")
}

fn parse_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".wal")?;
    if stem.len() != 16 || !stem.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return None;
    }
    u64::from_str_radix(stem, 16).ok()
}

/// Open-handle cache shared between the writer-side backend and its readers.
struct Handles {
    dir: PathBuf,
    files: Mutex<HashMap<u64, Arc<File>>>,
}

impl Handles {
    fn path(&self, seq: u64) -> PathBuf {
        self.dir.join(file_name(seq))
    }

    fn get(&self, seq: u64) -> io::Result<Arc<File>> {
        let mut files = self.files.lock();
        if let Some(file) = files.get(&seq) {
            return Ok(Arc::clone(file));
        }
        let file = Arc::new(OpenOptions::new().read(true).write(true).open(self.path(seq))?);
        files.insert(seq, Arc::clone(&file));
        Ok(file)
    }
}

/// Production backend over `std::fs`.
///
/// On Apple targets `File::sync_data` issues `F_FULLFSYNC` (about 4 ms p50 measured), so latency
/// numbers taken on a Mac are correctness-only, not representative of Linux fdatasync cost.
pub struct StdFsIo {
    handles: Arc<Handles>,
}

impl StdFsIo {
    /// Opens (creating with mode 0700 if missing) the WAL directory. Errors if the path exists but
    /// is not a directory.
    pub fn new(dir: impl AsRef<Path>) -> io::Result<StdFsIo> {
        let dir = dir.as_ref().to_path_buf();
        match std::fs::metadata(&dir) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{} exists and is not a directory", dir.display()),
                ))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
            }
            Err(e) => return Err(e),
        }
        Ok(StdFsIo {
            handles: Arc::new(Handles {
                dir,
                files: Mutex::new(HashMap::new()),
            }),
        })
    }
}

struct StdFsReader(Arc<Handles>);

impl SegmentRead for StdFsReader {
    fn read_at(&self, seq: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.0.get(seq)?.read_at(buf, offset)
    }
}

impl SegmentIo for StdFsIo {
    fn list_segments(&self) -> io::Result<Vec<u64>> {
        let mut seqs = Vec::new();
        for entry in std::fs::read_dir(&self.handles.dir)? {
            let entry = entry?;
            if let Some(seq) = entry.file_name().to_str().and_then(parse_name) {
                seqs.push(seq);
            }
        }
        seqs.sort_unstable();
        Ok(seqs)
    }

    fn create_segment(&mut self, seq: u64, size: u64) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.handles.path(seq))?;
        file.set_len(size)?;
        self.handles.files.lock().insert(seq, Arc::new(file));
        Ok(())
    }

    fn write_at(&mut self, seq: u64, offset: u64, data: &[u8]) -> io::Result<()> {
        self.handles.get(seq)?.write_all_at(data, offset)
    }

    fn sync_segment(&mut self, seq: u64) -> io::Result<()> {
        self.handles.get(seq)?.sync_data()
    }

    fn sync_dir(&mut self) -> io::Result<()> {
        File::open(&self.handles.dir)?.sync_all()
    }

    fn set_len(&mut self, seq: u64, len: u64) -> io::Result<()> {
        self.handles.get(seq)?.set_len(len)
    }

    fn delete_segment(&mut self, seq: u64) -> io::Result<()> {
        std::fs::remove_file(self.handles.path(seq))?;
        self.handles.files.lock().remove(&seq);
        Ok(())
    }

    fn read_at(&self, seq: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.handles.get(seq)?.read_at(buf, offset)
    }

    fn len(&self, seq: u64) -> io::Result<u64> {
        Ok(self.handles.get(seq)?.metadata()?.len())
    }

    fn reader(&self) -> Arc<dyn SegmentRead> {
        Arc::new(StdFsReader(Arc::clone(&self.handles)))
    }
}

#[cfg(any(test, feature = "test-util"))]
mod fake {
    use super::{SegmentIo, SegmentRead};
    use parking_lot::Mutex;
    use std::collections::{BTreeMap, BTreeSet};
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    /// Counters exposed by [`FakeIo::stats`].
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct FakeStats {
        /// Successful segment data syncs.
        pub syncs: u64,
        /// Successful directory syncs.
        pub dir_syncs: u64,
        /// Successful `write_at` calls.
        pub writes: u64,
        /// Total bytes passed to successful `write_at` calls.
        pub bytes_written: u64,
    }

    /// One successful mutating call, recorded in call order when event recording is on.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum IoEvent {
        /// `create_segment`.
        Create { seq: u64 },
        /// `write_at`.
        Write { seq: u64, offset: u64, len: usize },
        /// `sync_segment`.
        SyncSegment { seq: u64 },
        /// `sync_dir`.
        SyncDir,
        /// `set_len`.
        SetLen { seq: u64, len: u64 },
        /// `delete_segment`.
        Delete { seq: u64 },
    }

    #[derive(Default)]
    struct State {
        /// Volatile images of segments in the volatile directory listing.
        files: BTreeMap<u64, Vec<u8>>,
        /// Durable images; kept for deleted segments until the deletion is dir-synced.
        durable_files: BTreeMap<u64, Vec<u8>>,
        /// Segments that exist after a power loss.
        durable_dir: BTreeSet<u64>,
        stats: FakeStats,
        sync_delay: Duration,
        fail_sync: bool,
        fail_dir_sync: bool,
        fail_write: bool,
        crashed: bool,
        delete_countdown: Option<usize>,
        record_events: bool,
        events: Vec<IoEvent>,
    }

    fn crashed_err() -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, "fake io crashed")
    }

    fn injected_err() -> io::Error {
        io::Error::other("injected")
    }

    fn not_found(seq: u64) -> io::Error {
        io::Error::new(io::ErrorKind::NotFound, format!("fake segment {seq} not found"))
    }

    impl State {
        fn check_alive(&self) -> io::Result<()> {
            if self.crashed {
                Err(crashed_err())
            } else {
                Ok(())
            }
        }

        fn event(&mut self, ev: IoEvent) {
            if self.record_events {
                self.events.push(ev);
            }
        }

        fn power_loss(&mut self, keep: usize) {
            let mut budget = keep;
            let mut files = BTreeMap::new();
            for seq in self.durable_dir.iter().copied() {
                let durable = self.durable_files.get(&seq).cloned().unwrap_or_default();
                let mut image = durable.clone();
                // A torn write: retain the first `budget` differing bytes of a still-live segment.
                if let Some(volatile) = self.files.get(&seq) {
                    for (i, vb) in volatile.iter().enumerate() {
                        if budget == 0 {
                            break;
                        }
                        if durable.get(i) != Some(vb) {
                            if i >= image.len() {
                                image.resize(i + 1, 0);
                            }
                            image[i] = *vb;
                            budget -= 1;
                        }
                    }
                }
                files.insert(seq, image);
            }
            self.durable_files.retain(|seq, _| self.durable_dir.contains(seq));
            // The surviving images are what is now on disk: they become the durable state too.
            for (seq, image) in &files {
                self.durable_files.insert(*seq, image.clone());
            }
            self.files = files;
        }

        fn crash(&mut self, keep: usize) {
            self.power_loss(keep);
            self.crashed = true;
        }
    }

    /// Deterministic in-memory [`SegmentIo`] with fault injection and a power-loss model.
    ///
    /// Cloning yields another handle over the same shared state, so a test can keep a handle while
    /// the WAL writer owns another.
    #[derive(Clone, Default)]
    pub struct FakeIo {
        state: Arc<Mutex<State>>,
    }

    impl FakeIo {
        /// A new empty fake directory.
        pub fn new() -> FakeIo {
            FakeIo::default()
        }

        /// Snapshot of the call counters.
        pub fn stats(&self) -> FakeStats {
            self.state.lock().stats
        }

        /// Sleep this long (without holding the state lock) inside every `sync_segment`.
        pub fn set_sync_delay(&self, delay: Duration) {
            self.state.lock().sync_delay = delay;
        }

        /// The next `sync_segment` fails with `io::Error::other("injected")`.
        pub fn fail_next_sync(&self) {
            self.state.lock().fail_sync = true;
        }

        /// The next `sync_dir` fails with `io::Error::other("injected")` and publishes nothing.
        pub fn fail_next_dir_sync(&self) {
            self.state.lock().fail_dir_sync = true;
        }

        /// The next `write_at` fails with `io::Error::other("injected")`.
        pub fn fail_next_write(&self) {
            self.state.lock().fail_write = true;
        }

        /// Length of the last synced image of `seq` (0 if it has none).
        pub fn durable_len(&self, seq: u64) -> u64 {
            self.state.lock().durable_files.get(&seq).map_or(0, |v| v.len() as u64)
        }

        /// The volatile image of `seq` (empty if the segment does not exist).
        pub fn segment_bytes(&self, seq: u64) -> Vec<u8> {
            self.state.lock().files.get(&seq).cloned().unwrap_or_default()
        }

        /// The last synced image of `seq` (empty if it has none).
        pub fn durable_bytes(&self, seq: u64) -> Vec<u8> {
            self.state.lock().durable_files.get(&seq).cloned().unwrap_or_default()
        }

        /// Number of volatile bytes, across all live segments, that differ from the durable image.
        pub fn unsynced_bytes(&self) -> usize {
            let st = self.state.lock();
            let mut total = 0;
            for (seq, volatile) in &st.files {
                let durable: &[u8] = st.durable_files.get(seq).map_or(&[], |v| v.as_slice());
                let n = volatile.len().max(durable.len());
                total += (0..n).filter(|i| volatile.get(*i) != durable.get(*i)).count();
            }
            total
        }

        /// Simulates power loss: reverts to the durable directory listing and durable images,
        /// except that the first `keep_unsynced_bytes` differing bytes (in segment then offset
        /// order) of still-live segments survive as a torn write.
        pub fn power_loss(&self, keep_unsynced_bytes: usize) {
            self.state.lock().power_loss(keep_unsynced_bytes);
        }

        /// [`FakeIo::power_loss`] plus: every later mutating call fails with `BrokenPipe` until
        /// [`FakeIo::revive`]. Models a dead process whose writer thread is still running.
        pub fn crash(&self, keep_unsynced_bytes: usize) {
            self.state.lock().crash(keep_unsynced_bytes);
        }

        /// Clears the crashed state so a restarted node can reuse this fake.
        pub fn revive(&self) {
            self.state.lock().crashed = false;
        }

        /// The `n`th (1-based) later `delete_segment` call performs `crash(0)` and returns
        /// `BrokenPipe` instead of deleting: power loss between a durable purge marker and the
        /// segment deletions.
        pub fn crash_before_nth_delete(&self, n: usize) {
            assert!(n >= 1, "n is 1-based");
            self.state.lock().delete_countdown = Some(n);
        }

        /// Turns the ordered [`IoEvent`] log on or off (off by default to bound memory).
        pub fn record_events(&self, on: bool) {
            self.state.lock().record_events = on;
        }

        /// The recorded events, in call order.
        pub fn events(&self) -> Vec<IoEvent> {
            self.state.lock().events.clone()
        }

        /// Drops all recorded events.
        pub fn clear_events(&self) {
            self.state.lock().events.clear();
        }

        fn read_impl(&self, seq: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            let st = self.state.lock();
            let image = st.files.get(&seq).ok_or_else(|| not_found(seq))?;
            let Ok(start) = usize::try_from(offset) else {
                return Ok(0);
            };
            if start >= image.len() {
                return Ok(0);
            }
            let n = buf.len().min(image.len() - start);
            buf[..n].copy_from_slice(&image[start..start + n]);
            Ok(n)
        }
    }

    struct FakeReader(FakeIo);

    impl SegmentRead for FakeReader {
        fn read_at(&self, seq: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read_impl(seq, offset, buf)
        }
    }

    impl SegmentIo for FakeIo {
        fn list_segments(&self) -> io::Result<Vec<u64>> {
            Ok(self.state.lock().files.keys().copied().collect())
        }

        fn create_segment(&mut self, seq: u64, size: u64) -> io::Result<()> {
            let mut st = self.state.lock();
            st.check_alive()?;
            if st.files.contains_key(&seq) {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, "segment exists"));
            }
            let size = usize::try_from(size).map_err(|_| io::Error::other("size too large"))?;
            st.files.insert(seq, vec![0; size]);
            st.durable_files.insert(seq, vec![0; size]);
            st.event(IoEvent::Create { seq });
            Ok(())
        }

        fn write_at(&mut self, seq: u64, offset: u64, data: &[u8]) -> io::Result<()> {
            let mut st = self.state.lock();
            st.check_alive()?;
            if std::mem::take(&mut st.fail_write) {
                return Err(injected_err());
            }
            let image = st.files.get_mut(&seq).ok_or_else(|| not_found(seq))?;
            let start = usize::try_from(offset).map_err(|_| io::Error::other("offset too large"))?;
            let end = start + data.len();
            if image.len() < end {
                image.resize(end, 0);
            }
            image[start..end].copy_from_slice(data);
            st.stats.writes += 1;
            st.stats.bytes_written += data.len() as u64;
            st.event(IoEvent::Write {
                seq,
                offset,
                len: data.len(),
            });
            Ok(())
        }

        fn sync_segment(&mut self, seq: u64) -> io::Result<()> {
            let delay = {
                let mut st = self.state.lock();
                st.check_alive()?;
                if std::mem::take(&mut st.fail_sync) {
                    return Err(injected_err());
                }
                if !st.files.contains_key(&seq) {
                    return Err(not_found(seq));
                }
                st.sync_delay
            };
            // Sleep without the lock so the test thread can inspect state or crash meanwhile.
            if !delay.is_zero() {
                std::thread::sleep(delay);
            }
            let mut st = self.state.lock();
            // A crash during the sleep means the sync never completed.
            st.check_alive()?;
            let image = st.files.get(&seq).ok_or_else(|| not_found(seq))?.clone();
            st.durable_files.insert(seq, image);
            st.stats.syncs += 1;
            st.event(IoEvent::SyncSegment { seq });
            Ok(())
        }

        fn sync_dir(&mut self) -> io::Result<()> {
            let mut st = self.state.lock();
            st.check_alive()?;
            if std::mem::take(&mut st.fail_dir_sync) {
                return Err(injected_err());
            }
            st.durable_dir = st.files.keys().copied().collect();
            let State {
                durable_files,
                durable_dir,
                ..
            } = &mut *st;
            durable_files.retain(|seq, _| durable_dir.contains(seq));
            st.stats.dir_syncs += 1;
            st.event(IoEvent::SyncDir);
            Ok(())
        }

        fn set_len(&mut self, seq: u64, len: u64) -> io::Result<()> {
            let mut st = self.state.lock();
            st.check_alive()?;
            let new_len = usize::try_from(len).map_err(|_| io::Error::other("len too large"))?;
            st.files.get_mut(&seq).ok_or_else(|| not_found(seq))?.resize(new_len, 0);
            st.event(IoEvent::SetLen { seq, len });
            Ok(())
        }

        fn delete_segment(&mut self, seq: u64) -> io::Result<()> {
            let mut st = self.state.lock();
            st.check_alive()?;
            if let Some(remaining) = st.delete_countdown {
                if remaining <= 1 {
                    st.delete_countdown = None;
                    st.crash(0);
                    return Err(crashed_err());
                }
                st.delete_countdown = Some(remaining - 1);
            }
            if st.files.remove(&seq).is_none() {
                return Err(not_found(seq));
            }
            st.event(IoEvent::Delete { seq });
            Ok(())
        }

        fn read_at(&self, seq: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.read_impl(seq, offset, buf)
        }

        fn len(&self, seq: u64) -> io::Result<u64> {
            let st = self.state.lock();
            st.files.get(&seq).map(|v| v.len() as u64).ok_or_else(|| not_found(seq))
        }

        fn reader(&self) -> Arc<dyn SegmentRead> {
            Arc::new(FakeReader(self.clone()))
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
pub use fake::{FakeIo, FakeStats, IoEvent};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::record::{decode_record, encode_record, Decoded, RecordType};
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};

    #[test]
    fn std_fs_create_write_sync_read() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("wal");
        let mut io = StdFsIo::new(&dir).unwrap();
        assert_eq!(std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777, 0o700);

        io.create_segment(1, 4096).unwrap();
        let file_mode = std::fs::metadata(dir.join(file_name(1))).unwrap().permissions().mode();
        assert_eq!(file_mode & 0o777, 0o600);
        assert_eq!(io.len(1).unwrap(), 4096);

        let mut rec = Vec::new();
        let n = encode_record(&mut rec, RecordType::Entry, b"hello wal");
        io.write_at(1, 100, &rec).unwrap();
        io.sync_segment(1).unwrap();
        io.sync_dir().unwrap();

        // Read through both the trait method and the shared reader handle.
        let mut buf = vec![0u8; n];
        assert_eq!(io.read_at(1, 100, &mut buf).unwrap(), n);
        match decode_record(&buf) {
            Decoded::Record { ty, payload, consumed } => {
                assert_eq!(ty, RecordType::Entry);
                assert_eq!(payload, b"hello wal");
                assert_eq!(consumed, n);
            }
            other => panic!("expected record, got {other:?}"),
        }
        let reader = io.reader();
        let mut buf2 = vec![0u8; n];
        assert_eq!(reader.read_at(1, 100, &mut buf2).unwrap(), n);
        assert_eq!(buf, buf2);

        // Zero filled before the record, and reading past EOF returns 0.
        let mut head = [1u8; 8];
        assert_eq!(io.read_at(1, 0, &mut head).unwrap(), 8);
        assert_eq!(head, [0u8; 8]);
        assert_eq!(io.read_at(1, 4096, &mut head).unwrap(), 0);

        io.set_len(1, 200).unwrap();
        assert_eq!(io.len(1).unwrap(), 200);
        assert_eq!(io.list_segments().unwrap(), vec![1]);
        io.delete_segment(1).unwrap();
        assert!(io.list_segments().unwrap().is_empty());
    }

    #[test]
    fn std_fs_create_is_exclusive() {
        let tmp = tempfile::tempdir().unwrap();
        let mut io = StdFsIo::new(tmp.path().join("wal")).unwrap();
        io.create_segment(7, 64).unwrap();
        let err = io.create_segment(7, 64).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn std_fs_list_ignores_foreign_files_and_sorts() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("wal");
        let mut io = StdFsIo::new(&dir).unwrap();
        io.create_segment(10, 16).unwrap();
        io.create_segment(2, 16).unwrap();
        std::fs::write(dir.join("notes.txt"), b"x").unwrap();
        std::fs::write(dir.join("0000000000000003.wal.tmp"), b"x").unwrap();
        assert_eq!(io.list_segments().unwrap(), vec![2, 10]);
    }

    #[test]
    fn std_fs_rejects_non_directory_path() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("plain");
        std::fs::write(&file, b"x").unwrap();
        assert!(StdFsIo::new(&file).is_err());
    }

    #[test]
    fn fake_io_counts_syncs() {
        let mut io = FakeIo::new();
        io.create_segment(1, 128).unwrap();
        io.write_at(1, 0, b"abc").unwrap();
        io.sync_segment(1).unwrap();
        io.sync_segment(1).unwrap();
        io.sync_dir().unwrap();
        let s = io.stats();
        assert_eq!(s.syncs, 2);
        assert_eq!(s.dir_syncs, 1);
        assert_eq!(s.writes, 1);
        assert_eq!(s.bytes_written, 3);

        io.fail_next_sync();
        assert_eq!(io.sync_segment(1).unwrap_err().to_string(), "injected");
        assert_eq!(io.stats().syncs, 2);
        io.sync_segment(1).unwrap();
        io.fail_next_write();
        assert_eq!(io.write_at(1, 0, b"z").unwrap_err().to_string(), "injected");
        io.write_at(1, 0, b"z").unwrap();

        io.set_sync_delay(Duration::from_millis(30));
        let t = Instant::now();
        io.sync_segment(1).unwrap();
        assert!(t.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn fake_io_power_loss_drops_unsynced_bytes() {
        let mut io = FakeIo::new();
        io.create_segment(1, 64).unwrap();
        io.sync_dir().unwrap();
        io.write_at(1, 0, &[1, 2, 3, 4]).unwrap();
        io.sync_segment(1).unwrap();
        io.write_at(1, 4, &[5, 6, 7, 8]).unwrap();
        assert_eq!(io.unsynced_bytes(), 4);
        io.power_loss(0);
        assert_eq!(&io.segment_bytes(1)[..8], &[1, 2, 3, 4, 0, 0, 0, 0]);
        assert_eq!(io.unsynced_bytes(), 0);

        // A torn write keeps the first `keep` differing bytes in offset order.
        io.write_at(1, 4, &[5, 6, 7, 8]).unwrap();
        io.power_loss(2);
        assert_eq!(&io.segment_bytes(1)[..8], &[1, 2, 3, 4, 5, 6, 0, 0]);
    }

    #[test]
    fn fake_io_crash_fails_later_calls_until_revive() {
        let mut io = FakeIo::new();
        io.create_segment(1, 32).unwrap();
        io.sync_dir().unwrap();
        io.crash(0);
        let broken = |e: io::Error| e.kind() == io::ErrorKind::BrokenPipe;
        assert!(broken(io.write_at(1, 0, b"x").unwrap_err()));
        assert!(broken(io.sync_segment(1).unwrap_err()));
        assert!(broken(io.sync_dir().unwrap_err()));
        assert!(broken(io.create_segment(2, 8).unwrap_err()));
        assert!(broken(io.set_len(1, 8).unwrap_err()));
        assert!(broken(io.delete_segment(1).unwrap_err()));
        // Reads still work on the post-crash state.
        assert_eq!(io.list_segments().unwrap(), vec![1]);
        io.revive();
        io.write_at(1, 0, b"x").unwrap();
    }

    #[test]
    fn fake_io_created_segment_lost_on_power_loss_without_sync_dir() {
        let mut io = FakeIo::new();
        io.create_segment(1, 64).unwrap();
        io.write_at(1, 0, b"data").unwrap();
        io.sync_segment(1).unwrap();
        io.power_loss(0);
        assert!(io.list_segments().unwrap().is_empty());
        assert!(io.segment_bytes(1).is_empty());

        io.create_segment(1, 64).unwrap();
        io.write_at(1, 0, b"data").unwrap();
        io.sync_segment(1).unwrap();
        io.sync_dir().unwrap();
        io.power_loss(0);
        assert_eq!(io.list_segments().unwrap(), vec![1]);
        assert_eq!(&io.segment_bytes(1)[..4], b"data");
    }

    #[test]
    fn fake_io_deleted_segment_resurrects_without_sync_dir() {
        let mut io = FakeIo::new();
        io.create_segment(1, 64).unwrap();
        io.write_at(1, 0, b"keep").unwrap();
        io.sync_segment(1).unwrap();
        io.sync_dir().unwrap();

        io.delete_segment(1).unwrap();
        assert!(io.list_segments().unwrap().is_empty());
        io.power_loss(0);
        assert_eq!(io.list_segments().unwrap(), vec![1]);
        assert_eq!(&io.segment_bytes(1)[..4], b"keep");

        io.delete_segment(1).unwrap();
        io.sync_dir().unwrap();
        io.power_loss(0);
        assert!(io.list_segments().unwrap().is_empty());
        assert_eq!(io.durable_len(1), 0);
    }

    #[test]
    fn fake_io_event_log_records_call_order() {
        let mut io = FakeIo::new();
        io.create_segment(9, 16).unwrap();
        assert!(io.events().is_empty(), "recording is off by default");

        io.record_events(true);
        io.create_segment(1, 16).unwrap();
        io.sync_dir().unwrap();
        io.write_at(1, 4, b"ab").unwrap();
        io.sync_segment(1).unwrap();
        io.set_len(1, 8).unwrap();
        io.delete_segment(1).unwrap();
        // A failed call records nothing.
        io.fail_next_sync();
        let _ = io.sync_segment(9);
        assert_eq!(
            io.events(),
            vec![
                IoEvent::Create { seq: 1 },
                IoEvent::SyncDir,
                IoEvent::Write { seq: 1, offset: 4, len: 2 },
                IoEvent::SyncSegment { seq: 1 },
                IoEvent::SetLen { seq: 1, len: 8 },
                IoEvent::Delete { seq: 1 },
            ]
        );
        io.clear_events();
        assert!(io.events().is_empty());
        io.record_events(false);
        io.sync_dir().unwrap();
        assert!(io.events().is_empty());
    }

    #[test]
    fn fake_io_crash_before_nth_delete() {
        let mut io = FakeIo::new();
        io.create_segment(1, 16).unwrap();
        io.create_segment(2, 16).unwrap();
        io.sync_dir().unwrap();
        io.crash_before_nth_delete(2);
        io.delete_segment(1).unwrap();
        let err = io.delete_segment(2).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        // Crashed: nothing mutates; the un-dir-synced deletion of segment 1 was lost to power loss.
        assert!(io.create_segment(3, 8).is_err());
        io.revive();
        assert_eq!(io.list_segments().unwrap(), vec![1, 2]);
        io.delete_segment(1).unwrap();
        io.delete_segment(2).unwrap();
    }

    #[test]
    fn fake_io_crash_during_delayed_sync_leaves_it_incomplete() {
        let mut io = FakeIo::new();
        io.create_segment(1, 16).unwrap();
        io.sync_dir().unwrap();
        io.write_at(1, 0, b"late").unwrap();
        io.set_sync_delay(Duration::from_millis(200));
        let before = io.durable_bytes(1);

        let mut worker = io.clone();
        let handle = std::thread::spawn(move || worker.sync_segment(1));
        std::thread::sleep(Duration::from_millis(50));
        // The fake stays inspectable while the sync sleeps.
        assert_eq!(io.unsynced_bytes(), 4);
        io.crash(0);
        let result = handle.join().unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(io.durable_bytes(1), before);
        assert_eq!(io.stats().syncs, 0);
    }

    #[test]
    fn fake_io_unsynced_bytes_and_fail_next_dir_sync() {
        let mut io = FakeIo::new();
        io.create_segment(1, 32).unwrap();
        assert_eq!(io.unsynced_bytes(), 0, "zero-filled creation differs from nothing");
        io.write_at(1, 0, &[9; 10]).unwrap();
        io.write_at(1, 20, &[7; 2]).unwrap();
        assert_eq!(io.unsynced_bytes(), 12);
        io.sync_segment(1).unwrap();
        assert_eq!(io.unsynced_bytes(), 0);

        io.fail_next_dir_sync();
        assert_eq!(io.sync_dir().unwrap_err().to_string(), "injected");
        assert_eq!(io.stats().dir_syncs, 0);
        io.power_loss(0);
        assert!(io.list_segments().unwrap().is_empty(), "failed dir sync published nothing");

        io.create_segment(1, 32).unwrap();
        io.sync_dir().unwrap();
        assert_eq!(io.stats().dir_syncs, 1);
    }

    #[test]
    fn fake_io_reader_reads_volatile_image() {
        let mut io = FakeIo::new();
        io.create_segment(1, 16).unwrap();
        io.write_at(1, 2, b"xyz").unwrap();
        let reader = io.reader();
        let mut buf = [0u8; 3];
        assert_eq!(reader.read_at(1, 2, &mut buf).unwrap(), 3);
        assert_eq!(&buf, b"xyz");
        assert_eq!(reader.read_at(1, 100, &mut buf).unwrap(), 0);
        assert_eq!(reader.read_at(5, 0, &mut buf).unwrap_err().kind(), io::ErrorKind::NotFound);
        assert_eq!(io.len(1).unwrap(), 16);
    }
}
