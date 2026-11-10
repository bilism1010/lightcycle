//! openraft log storage over the WAL (decisions D-01, D-02, D-03, D-04; plan 03-11).
//!
//! [`WalLogStore`] is the only place that depends on both openraft and the WAL. It owns the
//! non-`Clone` ring producer of the writer thread (openraft moves the store into `Raft::new` and
//! calls it through `&mut self`, which is what makes the SPSC ring safe by construction), keeps the
//! in-memory entry cache and the live bounds consistent with writer order, and exposes the log to
//! replication tasks through the cheap, read-only [`WalLogReader`].
//!
//! Ordering rules the store relies on:
//!
//! * `append` inserts into the cache itself before returning (the observer only indexes and
//!   evicts), so entries are readable the moment `append` returns, long before the covering fsync.
//! * `truncate_after` and `purge` await the writer's ack. The writer reports the truncation or
//!   purge to the observer before that ack, so once either method returns, the offset index and
//!   the cache agree with the marker and a later re-append of the same indexes is contiguous.
//! * A full ring never blocks a Tokio worker thread: submission yields and retries.

use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::entry::EntryPayload;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use openraft::{LogState, OptionalSend, RaftLogReader};
use parking_lot::RwLock;

use super::cache::{read_entry, LogIndex, LogShared, SharedObserver};
use super::io::{SegmentIo, SegmentRead, StdFsIo};
use super::recovery::{recover, RecoveryStats};
use super::record::{MAX_RECORD_LEN, RECORD_HEADER_LEN};
use super::segment::{WalConfig, WalError};
use super::writer::{Ack, WalHandle, WalOp, WalPayload, WalStats};
use crate::raft::types::{KvRequest, TypeConfig};

type Entry = EntryOf<TypeConfig>;
type LogId = LogIdOf<TypeConfig>;
type Vote = VoteOf<TypeConfig>;
type Shared = Arc<RwLock<LogShared<Entry>>>;

/// Fixed per-entry overhead added to the payload size estimate used for cache accounting.
const ENTRY_OVERHEAD_BYTES: usize = 32;
/// Space kept free in a segment for its preamble (header plus vote, purge point and committed id)
/// when deciding whether an entry can ever fit.
const PREAMBLE_RESERVE_BYTES: usize = 512;
/// Byte budget for the entries of one replication read (see
/// [`WalLogReader::limited_get_log_entries`](RaftLogReader::limited_get_log_entries)). A batch
/// holds the entries up to this budget, or a single entry when that alone is larger (at most
/// about `MAX_RECORD_LEN`, 16 MiB), so a request stays under half of the transport's default
/// 32 MiB frame cap. A transport configured with a frame cap below twice this budget can still
/// refuse a batch of large entries.
pub const REPLICATION_BATCH_BYTES: usize = 16 * 1024 * 1024;
/// Failed submissions that only yield before the submit loop starts sleeping briefly.
const YIELD_ATTEMPTS: u32 = 64;

/// Entry body encoded on the writer thread: the payload adapter between openraft's entry type and
/// the opaque-record WAL writer.
pub struct EntryBody(Entry);

impl WalPayload for EntryBody {
    fn encode_into(&self, out: &mut Vec<u8>) {
        let buf = std::mem::take(out);
        // Encoding an `Entry<TypeConfig>` cannot fail (no maps with non-string keys, no custom
        // fallible serializers); a panic here is caught by the writer and becomes a fail-stop.
        *out = postcard::to_extend(&self.0, buf).expect("postcard encoding of an Entry cannot fail");
    }
}

fn decode_entry(bytes: &[u8]) -> Option<Entry> {
    postcard::from_bytes(bytes).ok()
}

fn estimate_size(entry: &Entry) -> usize {
    let payload = match &entry.payload {
        EntryPayload::Normal(KvRequest::Set { key, value }) => key.len() + value.len(),
        EntryPayload::Normal(KvRequest::Delete { key }) => key.len(),
        EntryPayload::Normal(KvRequest::Cas { key, expected, desired }) => {
            key.len()
                + expected.as_ref().map_or(0, |b| b.len())
                + desired.as_ref().map_or(0, |b| b.len())
        }
        _ => 0,
    };
    payload + ENTRY_OVERHEAD_BYTES
}

/// Largest postcard-encoded entry the writer can frame under `cfg`: it must fit one record
/// (`MAX_RECORD_LEN` covers the type byte and the 8-byte index prefix) and an otherwise empty
/// segment. Entries above it are rejected before they touch any state, because the writer cannot
/// store them.
pub fn max_entry_bytes(cfg: &WalConfig) -> usize {
    let by_record = MAX_RECORD_LEN as usize - 1 - 8;
    let segment = usize::try_from(cfg.segment_size).unwrap_or(usize::MAX);
    let by_segment = segment.saturating_sub(PREAMBLE_RESERVE_BYTES + RECORD_HEADER_LEN + 8);
    by_record.min(by_segment)
}

fn encode<T: serde::Serialize>(value: &T) -> io::Result<Vec<u8>> {
    postcard::to_stdvec(value).map_err(io::Error::other)
}

fn wal_to_io(e: WalError) -> io::Error {
    match e {
        WalError::Io(e) => e,
        other => io::Error::other(other),
    }
}

fn writer_failed() -> io::Error {
    io::Error::other("wal writer has failed")
}

fn corrupt_blob(what: &str, e: postcard::Error) -> WalError {
    WalError::Corrupt {
        seq: 0,
        offset: 0,
        reason: format!("recovered {what} failed to decode: {e}"),
    }
}

/// Live log bounds and the vote, shared with every [`WalLogReader`].
#[derive(Debug, Clone)]
struct Meta {
    vote: Option<Vote>,
    /// First live index.
    first: u64,
    /// One past the last live index (`first == next` means no live entries).
    next: u64,
}

/// Completes `callback` with `result` exactly once (used for ops that never reach the writer).
fn fail_callback(callback: IOFlushed<TypeConfig>, err: &io::Error) {
    callback.io_completed(Err(io::Error::new(err.kind(), err.to_string())));
}

/// openraft log storage backed by the group-commit WAL.
pub struct WalLogStore {
    handle: Option<WalHandle<EntryBody>>,
    shared: Shared,
    reader: Arc<dyn SegmentRead>,
    meta: Arc<RwLock<Meta>>,
    purged: Option<LogId>,
    committed: Option<LogId>,
    last_log_id: Option<LogId>,
    recovery: RecoveryStats,
    stats: Arc<WalStats>,
    /// Largest encoded entry the writer accepts (see [`max_entry_bytes`]).
    max_entry: usize,
}

impl WalLogStore {
    /// Opens (recovering if needed) the WAL in `dir` on the real filesystem.
    pub fn open(dir: impl Into<PathBuf>, cfg: WalConfig) -> Result<WalLogStore, WalError> {
        let io = StdFsIo::new(dir.into())?;
        WalLogStore::open_with_io(Box::new(io), cfg)
    }

    /// Opens the WAL over an arbitrary backend (tests inject `FakeIo`).
    pub fn open_with_io(
        mut io: Box<dyn SegmentIo>,
        cfg: WalConfig,
    ) -> Result<WalLogStore, WalError> {
        let recovered = recover(&mut *io, &cfg)?;
        let max_entry = max_entry_bytes(&cfg);
        let recovery = recovered.stats();
        let reader = io.reader();

        let vote: Option<Vote> = recovered
            .start
            .state
            .vote
            .as_deref()
            .map(postcard::from_bytes)
            .transpose()
            .map_err(|e| corrupt_blob("vote", e))?;
        let purged: Option<LogId> = recovered
            .start
            .state
            .purged
            .as_ref()
            .map(|(_, blob)| postcard::from_bytes(blob))
            .transpose()
            .map_err(|e| corrupt_blob("purged log id", e))?;
        let committed: Option<LogId> = match recovered.start.state.committed.as_deref() {
            None | Some([]) => None,
            Some(blob) => {
                Some(postcard::from_bytes(blob).map_err(|e| corrupt_blob("committed log id", e))?)
            }
        };

        let first_loc = recovered.locations.first().map(|l| l.index);
        let last_loc = recovered.locations.last().map(|l| l.index);
        let purged_next = purged.as_ref().map_or(0, |p| p.index() + 1);
        let first = first_loc.unwrap_or(purged_next);
        let next = last_loc.map_or(first, |l| l + 1);

        let shared: Shared = Arc::new(RwLock::new(LogShared::new(
            LogIndex::from_recovered(recovered.locations),
            cfg.cache_budget_bytes,
        )));

        let last_log_id = match last_loc {
            None => purged,
            Some(last) => match read_entry(&shared, &*reader, last, decode_entry)? {
                Some(entry) => Some(entry.log_id),
                None => {
                    return Err(WalError::Corrupt {
                        seq: 0,
                        offset: 0,
                        reason: format!("recovered last entry {last} could not be read"),
                    })
                }
            },
        };

        let handle = WalHandle::spawn(
            io,
            cfg,
            recovered.start,
            Box::new(SharedObserver(Arc::clone(&shared))),
        )?;
        let stats = handle.stats();
        Ok(WalLogStore {
            handle: Some(handle),
            shared,
            reader,
            meta: Arc::new(RwLock::new(Meta { vote, first, next })),
            purged,
            committed,
            last_log_id,
            recovery,
            stats,
            max_entry,
        })
    }

    /// Writer counters (batches, syncs, fsync histogram).
    pub fn wal_stats(&self) -> Arc<WalStats> {
        Arc::clone(&self.stats)
    }

    /// What recovery found when this store was opened. Read it before the store is moved into
    /// `Raft::new`.
    pub fn recovery_stats(&self) -> RecoveryStats {
        self.recovery
    }

    /// Drains the writer (every pending ack is completed) and joins its thread, off the async
    /// worker.
    pub async fn shutdown(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = tokio::task::spawn_blocking(move || handle.shutdown()).await;
        }
    }

    fn handle_mut(&mut self) -> io::Result<&mut WalHandle<EntryBody>> {
        match self.handle.as_mut() {
            Some(h) if !h.is_failed() => Ok(h),
            Some(_) => Err(writer_failed()),
            None => Err(io::Error::other("wal store is shut down")),
        }
    }

    /// Publishes `op`, yielding (never blocking the worker thread) while the ring is full.
    async fn submit(&mut self, mut op: WalOp<EntryBody>) -> io::Result<()> {
        let handle = self.handle_mut()?;
        let mut attempts = 0u32;
        loop {
            match handle.try_submit(op) {
                Ok(()) => return Ok(()),
                Err(back) => op = back,
            }
            attempts += 1;
            if attempts < YIELD_ATTEMPTS {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }
    }

    /// Submits an op built around a oneshot ack and waits for the covering fsync.
    async fn submit_and_wait(
        &mut self,
        make: impl FnOnce(Option<Ack>) -> WalOp<EntryBody>,
    ) -> io::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ack: Ack = Box::new(move |r| {
            let _ = tx.send(r);
        });
        self.submit(make(Some(ack))).await?;
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("wal writer dropped an ack")),
        }
    }
}

impl RaftLogStorage<TypeConfig> for WalLogStore {
    type LogReader = WalLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        Ok(LogState {
            last_purged_log_id: self.purged,
            last_log_id: self.last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        WalLogReader {
            shared: Arc::clone(&self.shared),
            reader: Arc::clone(&self.reader),
            meta: Arc::clone(&self.meta),
        }
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let blob = encode(vote)?;
        self.submit_and_wait(|ack| WalOp::Vote { blob, ack }).await?;
        self.meta.write().vote = Some(*vote);
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        let blob = match &committed {
            Some(id) => encode(id)?,
            None => Vec::new(),
        };
        self.submit(WalOp::Committed { blob, ack: None }).await?;
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        if let Err(e) = self.handle_mut() {
            fail_callback(callback, &e);
            return Err(e);
        }
        let entries: Vec<Entry> = entries.into_iter().collect();
        if entries.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        // Reject the whole call before any state changes: an entry the writer cannot store must
        // never reach the cache, the live bounds or the writer, or the log would gain a hole.
        let rejected = entries.iter().find_map(|entry| {
            match postcard::experimental::serialized_size(entry) {
                Ok(n) if n <= self.max_entry => None,
                Ok(n) => Some(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "log entry {} encodes to {n} bytes, above the {} byte limit",
                        entry.log_id.index(),
                        self.max_entry
                    ),
                )),
                Err(e) => Some(io::Error::other(e)),
            }
        });
        if let Some(err) = rejected {
            fail_callback(callback, &err);
            return Err(err);
        }

        // Readable the moment this method returns: cache first, then publish.
        let mut callback = Some(callback);
        let count = entries.len();
        for (i, entry) in entries.into_iter().enumerate() {
            let index = entry.log_id.index();
            let log_id = entry.log_id;
            {
                let size = estimate_size(&entry);
                self.shared.write().cache.insert(index, entry.clone(), size);
            }
            self.last_log_id = Some(log_id);
            {
                let mut meta = self.meta.write();
                if index < meta.first {
                    meta.first = index;
                }
                meta.next = index + 1;
            }
            // The writer can fail between two entries of a batch. Check before the callback moves
            // into the final op's ack, so the error path can still complete it exactly once. (A
            // failure in the instant between this check and the publish drops the ack, which the
            // caller sees as the `Err` returned here; openraft treats that as fatal anyway.)
            if let Err(e) = self.handle_mut() {
                if let Some(cb) = callback.take() {
                    fail_callback(cb, &e);
                }
                return Err(e);
            }
            let ack: Option<Ack> = if i + 1 == count {
                callback.take().map(|cb| -> Ack { Box::new(move |r| cb.io_completed(r)) })
            } else {
                None
            };
            self.submit(WalOp::Entry { index, payload: EntryBody(entry), ack }).await?;
        }
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let keep_through = last_log_id.as_ref().map(|id| id.index());
        self.handle_mut()?;
        self.shared.write().cache.truncate(keep_through);
        self.last_log_id = match last_log_id {
            Some(id) => Some(id),
            None => self.purged,
        };
        {
            let mut meta = self.meta.write();
            meta.next = match keep_through {
                Some(k) => k + 1,
                None => meta.first,
            };
        }
        self.submit_and_wait(|ack| WalOp::TruncateAfter { keep_through, ack }).await
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let upto = log_id.index();
        let blob = encode(&log_id)?;
        self.submit_and_wait(|ack| WalOp::Purge { upto, blob, ack }).await?;
        self.purged = Some(log_id);
        if self.last_log_id.as_ref().is_none_or(|l| l.index() < upto) {
            self.last_log_id = Some(log_id);
        }
        {
            let mut meta = self.meta.write();
            meta.first = upto + 1;
            meta.next = meta.next.max(meta.first);
        }
        self.shared.write().cache.purge(upto);
        Ok(())
    }
}

/// Read-only view of the log for replication tasks. Cheap to clone; never touches the writer.
#[derive(Clone)]
pub struct WalLogReader {
    shared: Shared,
    reader: Arc<dyn SegmentRead>,
    meta: Arc<RwLock<Meta>>,
}

impl WalLogReader {
    /// Reads the live entries of `[start, end)`, stopping at a hole. With a `budget`, it also
    /// stops before an entry that would push the estimated total past it; the first entry is
    /// always returned, so a non-empty range never yields an empty result.
    fn read_bounded(
        &self,
        start: u64,
        end: u64,
        budget: Option<usize>,
    ) -> Result<Vec<Entry>, io::Error> {
        let (first, next) = {
            let meta = self.meta.read();
            (meta.first, meta.next)
        };
        let lo = start.max(first);
        let hi = end.min(next);

        let mut out = Vec::new();
        let mut total = 0usize;
        for index in lo..hi {
            match read_entry(&self.shared, &*self.reader, index, decode_entry).map_err(wal_to_io)? {
                Some(entry) => {
                    if let Some(budget) = budget {
                        let size = estimate_size(&entry);
                        if !out.is_empty() && total.saturating_add(size) > budget {
                            break;
                        }
                        total = total.saturating_add(size);
                    }
                    out.push(entry);
                }
                // Purged or truncated while we read: stop at the hole, never skip it.
                None => break,
            }
        }
        Ok(out)
    }
}

impl RaftLogReader<TypeConfig> for WalLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let start = match range.start_bound() {
            Bound::Included(s) => *s,
            Bound::Excluded(s) => s.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(e) => e.saturating_add(1),
            Bound::Excluded(e) => *e,
            Bound::Unbounded => u64::MAX,
        };
        self.read_bounded(start, end, None)
    }

    /// The replication read: at most [`REPLICATION_BATCH_BYTES`] of entries (always at least
    /// one), so an AppendEntries request built from it stays under the transport's frame cap
    /// however large the individual entries are. openraft caps batches by entry count only.
    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> Result<Vec<Entry>, io::Error> {
        self.read_bounded(start, end, Some(REPLICATION_BATCH_BYTES))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.meta.read().vote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::io::{FakeIo, IoEvent, SegmentIo};
    use bytes::Bytes;
    use openraft::type_config::TypeConfigExt;

    fn cfg(segment_size: u64) -> WalConfig {
        WalConfig {
            segment_size,
            ..WalConfig::default()
        }
    }

    fn log_id(term: u64, index: u64) -> LogId {
        openraft::LogId::new_term_index(term, index)
    }

    fn entry(term: u64, index: u64, value_len: usize) -> Entry {
        openraft::Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(KvRequest::Set {
                key: Bytes::from(format!("key-{index}")),
                value: Bytes::from(vec![(index % 251) as u8; value_len]),
            }),
        }
    }

    fn vote(term: u64, node: u64) -> Vote {
        openraft::impls::Vote::new(term, node)
    }

    /// A signalling callback and a future that resolves with its single completion.
    fn signal() -> (IOFlushed<TypeConfig>, impl std::future::Future<Output = io::Result<()>>) {
        let (tx, rx) = TypeConfig::oneshot::<io::Result<()>>();
        (IOFlushed::signal(tx), async move {
            rx.await.unwrap_or_else(|_| Err(io::Error::other("callback dropped uncompleted")))
        })
    }

    fn open(io: &FakeIo, segment_size: u64) -> WalLogStore {
        WalLogStore::open_with_io(Box::new(io.clone()), cfg(segment_size)).unwrap()
    }

    /// Appends `entries` and waits for the covering fsync.
    async fn append_sync(store: &mut WalLogStore, entries: Vec<Entry>) {
        let (cb, done) = signal();
        store.append(entries, cb).await.unwrap();
        done.await.unwrap();
    }

    #[tokio::test]
    async fn append_vote_restart() {
        let io = FakeIo::new();
        let mut store = open(&io, 1 << 16);
        assert_eq!(store.recovery_stats(), RecoveryStats::default());
        let v = vote(2, 7);
        store.save_vote(&v).await.unwrap();
        let entries = vec![entry(1, 1, 16), entry(1, 2, 16)];
        append_sync(&mut store, entries.clone()).await;
        store.shutdown().await;

        let mut store = open(&io, 1 << 16);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 2)));
        assert_eq!(state.last_purged_log_id, None);
        let mut reader = store.get_log_reader().await;
        assert_eq!(reader.try_get_log_entries(1..=2).await.unwrap(), entries);
        assert_eq!(reader.read_vote().await.unwrap(), Some(v));
        let stats = store.recovery_stats();
        assert_eq!(stats.entries_recovered, 2);
        assert_eq!(stats.torn_bytes_discarded, 0);
        store.shutdown().await;
    }

    fn entries(lo: u64, hi: u64, term: u64, len: usize) -> Vec<Entry> {
        (lo..=hi).map(|i| entry(term, i, len)).collect()
    }

    async fn read_all(store: &mut WalLogStore, lo: u64, hi: u64) -> Vec<Entry> {
        store.get_log_reader().await.try_get_log_entries(lo..=hi).await.unwrap()
    }

    fn seg_list(io: &FakeIo) -> Vec<u64> {
        SegmentIo::list_segments(io).unwrap()
    }

    #[tokio::test]
    async fn oversized_entry_is_rejected_whole_and_leaves_no_index_gap() {
        let io = FakeIo::new();
        let mut store = open(&io, 4096);
        append_sync(&mut store, entries(1, 2, 1, 16)).await;

        // A non-final oversized entry used to be dropped silently while its successor was acked.
        let mut batch = entries(3, 4, 1, 16);
        batch[0] = entry(1, 3, 5000);
        let (cb, done) = signal();
        let err = store.append(batch, cb).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(done.await.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        // Nothing of the rejected call is visible, and the log still continues at index 3.
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, Some(log_id(1, 2)));
        assert_eq!(read_all(&mut store, 1, 10).await, entries(1, 2, 1, 16));
        append_sync(&mut store, entries(3, 4, 1, 16)).await;
        store.shutdown().await;

        let mut store = open(&io, 4096);
        assert_eq!(read_all(&mut store, 1, 10).await, entries(1, 4, 1, 16));
        store.shutdown().await;
    }

    #[test]
    fn entry_limit_is_bounded_by_record_and_segment() {
        let big = max_entry_bytes(&cfg(1 << 40));
        assert_eq!(big, MAX_RECORD_LEN as usize - 9);
        let small = max_entry_bytes(&cfg(4096));
        assert!(small < 4096 && small > 4096 - 1024);
        assert_eq!(max_entry_bytes(&cfg(16)), 0);
    }

    #[tokio::test]
    async fn replication_read_is_bounded_by_bytes_not_only_entry_count() {
        let io = FakeIo::new();
        let mut store = open(&io, 64 << 20);
        // Three 12 MiB entries are 36 MiB: more than one 32 MiB frame holds, and each one is
        // below the per-entry limit, so only the read side can keep a request under the cap.
        append_sync(&mut store, entries(1, 3, 1, 12 << 20)).await;
        append_sync(&mut store, entries(4, 8, 1, 1 << 20)).await;
        let mut reader = store.get_log_reader().await;

        // The 12 MiB entries are served one per read (a second would pass the 16 MiB budget)...
        for index in 1..=3u64 {
            let got = reader.limited_get_log_entries(index, 4).await.unwrap();
            assert_eq!(got.len(), 1, "from {index}");
            assert_eq!(got[0].log_id.index(), index);
        }
        // ...while small entries still go in one read, and a read always makes progress.
        let small = reader.limited_get_log_entries(4, 9).await.unwrap();
        assert_eq!(small, entries(4, 8, 1, 1 << 20));
        let total: usize = reader
            .limited_get_log_entries(1, 9)
            .await
            .unwrap()
            .iter()
            .map(estimate_size)
            .sum();
        assert!(total <= REPLICATION_BATCH_BYTES, "{total}");

        // The unlimited read (apply path, truncation checks) must still return everything.
        assert_eq!(reader.try_get_log_entries(1..=8).await.unwrap().len(), 8);
        store.shutdown().await;
    }

    #[tokio::test]
    async fn truncate_marker() {
        let io = FakeIo::new();
        let mut store = open(&io, 1 << 16);
        append_sync(&mut store, entries(1, 5, 1, 16)).await;
        let before = io.durable_bytes(1);

        store.truncate_after(Some(log_id(1, 3))).await.unwrap();
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, Some(log_id(1, 3)));
        assert_eq!(read_all(&mut store, 1, 6).await, entries(1, 3, 1, 16));
        append_sync(&mut store, entries(4, 6, 2, 16)).await;

        let mut want = entries(1, 3, 1, 16);
        want.extend(entries(4, 6, 2, 16));
        assert_eq!(read_all(&mut store, 1, 6).await, want);
        let after = io.durable_bytes(1);
        // Segments are preallocated zero-filled: only the written prefix must be unchanged.
        let used = before.iter().rposition(|b| *b != 0).unwrap() + 1;
        assert_eq!(&after[..used], &before[..used], "truncation is a marker, not an edit");
        store.shutdown().await;

        let mut store = open(&io, 1 << 16);
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, Some(log_id(2, 6)));
        assert_eq!(read_all(&mut store, 1, 6).await, want);
        store.shutdown().await;
    }

    #[tokio::test]
    async fn truncate_everything_then_reappend() {
        let io = FakeIo::new();
        let mut store = open(&io, 1 << 16);
        append_sync(&mut store, entries(0, 3, 1, 8)).await;
        store.truncate_after(None).await.unwrap();
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);
        assert!(read_all(&mut store, 0, 10).await.is_empty());
        append_sync(&mut store, entries(0, 1, 2, 8)).await;
        store.shutdown().await;
        let mut store = open(&io, 1 << 16);
        assert_eq!(read_all(&mut store, 0, 10).await, entries(0, 1, 2, 8));
        store.shutdown().await;
    }

    #[tokio::test]
    async fn preamble_survives_purge() {
        let io = FakeIo::new();
        let mut store = open(&io, 4096);
        let v = vote(3, 9);
        // The vote record lives in segment 1, which the purge deletes.
        store.save_vote(&v).await.unwrap();
        for chunk in 0..6 {
            let lo = chunk * 10 + 1;
            append_sync(&mut store, entries(lo, lo + 9, 1, 200)).await;
        }
        assert!(seg_list(&io).len() >= 4, "several segments rolled: {:?}", seg_list(&io));
        store.purge(log_id(1, 50)).await.unwrap();
        assert!(seg_list(&io)[0] > 1, "old segments were deleted: {:?}", seg_list(&io));
        assert_eq!(store.get_log_state().await.unwrap().last_purged_log_id, Some(log_id(1, 50)));
        store.shutdown().await;

        let mut store = open(&io, 4096);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 50)));
        assert_eq!(state.last_log_id, Some(log_id(1, 60)));
        let mut reader = store.get_log_reader().await;
        assert_eq!(reader.read_vote().await.unwrap(), Some(v));
        assert_eq!(read_all(&mut store, 51, 60).await, entries(51, 60, 1, 200));
        assert!(read_all(&mut store, 1, 50).await.is_empty(), "purged entries are gone");
        store.shutdown().await;
    }

    #[tokio::test]
    async fn purge_everything_keeps_last_log_id() {
        let io = FakeIo::new();
        let mut store = open(&io, 1 << 16);
        append_sync(&mut store, entries(1, 4, 1, 8)).await;
        store.purge(log_id(1, 4)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
        assert_eq!(state.last_log_id, Some(log_id(1, 4)));
        store.shutdown().await;
        let mut store = open(&io, 1 << 16);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
        assert_eq!(state.last_log_id, Some(log_id(1, 4)));
        // The next append continues contiguously after the purge point.
        append_sync(&mut store, entries(5, 6, 1, 8)).await;
        assert_eq!(read_all(&mut store, 1, 6).await, entries(5, 6, 1, 8));
        store.shutdown().await;
    }

    #[tokio::test]
    async fn append_visible_before_flush() {
        let io = FakeIo::new();
        io.set_sync_delay(Duration::from_millis(300));
        let mut store = open(&io, 1 << 16);
        let (cb, done) = signal();
        let want = entries(1, 3, 1, 16);
        store.append(want.clone(), cb).await.unwrap();
        // The covering fsync is still sleeping inside FakeIo.
        assert_eq!(io.stats().syncs, 0);
        assert_eq!(read_all(&mut store, 1, 3).await, want);
        done.await.unwrap();
        assert!(io.stats().syncs >= 1);
        store.shutdown().await;
    }

    #[tokio::test]
    async fn append_empty_completes_callback() {
        let io = FakeIo::new();
        let mut store = open(&io, 1 << 16);
        let (cb, done) = signal();
        store.append(Vec::new(), cb).await.unwrap();
        done.await.unwrap();
        store.shutdown().await;
    }

    #[tokio::test]
    async fn read_committed_roundtrip() {
        let io = FakeIo::new();
        let mut store = open(&io, 1 << 16);
        assert_eq!(store.read_committed().await.unwrap(), None);
        store.save_committed(Some(log_id(1, 7))).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(1, 7)));
        store.shutdown().await;
        let mut store = open(&io, 1 << 16);
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(1, 7)));
        store.shutdown().await;
    }

    #[tokio::test]
    async fn log_reader_serves_evicted_entries_from_disk() {
        let io = FakeIo::new();
        let config = WalConfig {
            segment_size: 1 << 16,
            cache_budget_bytes: 400,
            ..WalConfig::default()
        };
        let mut store = WalLogStore::open_with_io(Box::new(io.clone()), config).unwrap();
        let want = entries(1, 20, 1, 100);
        append_sync(&mut store, want.clone()).await;
        let cached = store.shared.read().cache.len();
        assert!(cached < 20, "tiny budget must have evicted flushed entries, cached {cached}");
        assert_eq!(read_all(&mut store, 1, 20).await, want);
        store.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ring_full_backpressure_yields() {
        let io = FakeIo::new();
        io.set_sync_delay(Duration::from_millis(300));
        let mut store = open(&io, 1 << 20);
        let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let t = Arc::clone(&ticks);
        let ticker = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                t.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
        // More single-entry appends than the ring (8192) plus the writer's pending queue can hold
        // while the first fsync sleeps.
        let total = 12_000u64;
        let mut last = None;
        for i in 1..=total {
            let cb = if i == total {
                let (cb, done) = signal();
                last = Some(done);
                cb
            } else {
                IOFlushed::noop()
            };
            store.append(vec![entry(1, i, 8)], cb).await.unwrap();
        }
        last.unwrap().await.unwrap();
        ticker.abort();
        let n = ticks.load(std::sync::atomic::Ordering::Relaxed);
        assert!(n >= 10, "the runtime kept running while the ring was full (ticks {n})");
        store.shutdown().await;
    }

    #[tokio::test]
    async fn failed_store_rejects_ops_and_completes_callbacks_once() {
        let io = FakeIo::new();
        io.set_sync_delay(Duration::from_millis(20));
        let mut store = open(&io, 1 << 16);
        io.fail_next_sync();
        let mut dones = Vec::new();
        for i in 1..=3u64 {
            let (cb, done) = signal();
            store.append(vec![entry(1, i, 8)], cb).await.unwrap();
            dones.push(done);
        }
        for done in dones {
            assert!(done.await.is_err(), "every pending callback receives the failure");
        }
        let (cb, done) = signal();
        assert!(store.append(vec![entry(1, 9, 8)], cb).await.is_err());
        assert!(done.await.is_err(), "a rejected append still completes its callback");
        assert!(store.save_vote(&vote(1, 1)).await.is_err());
        assert!(store.truncate_after(Some(log_id(1, 1))).await.is_err());
        assert!(store.purge(log_id(1, 1)).await.is_err());
        store.shutdown().await;
    }

    #[tokio::test]
    async fn acked_append_in_fresh_and_rolled_segments_survives_crash() {
        // (a) one entry in a freshly created segment.
        let io = FakeIo::new();
        let mut store = open(&io, 4096);
        append_sync(&mut store, entries(1, 1, 1, 16)).await;
        io.crash(0);
        drop(store);
        io.revive();
        let mut store = open(&io, 4096);
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, Some(log_id(1, 1)));
        assert_eq!(read_all(&mut store, 1, 1).await, entries(1, 1, 1, 16));
        store.shutdown().await;

        // (b) the last acked entry lives in a rolled segment.
        let io = FakeIo::new();
        let mut store = open(&io, 4096);
        let v = vote(4, 2);
        store.save_vote(&v).await.unwrap();
        append_sync(&mut store, entries(1, 100, 1, 100)).await;
        assert!(seg_list(&io).len() >= 3, "rolled at least twice: {:?}", seg_list(&io));
        io.crash(0);
        drop(store);
        io.revive();
        let mut store = open(&io, 4096);
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, Some(log_id(1, 100)));
        assert_eq!(read_all(&mut store, 1, 100).await, entries(1, 100, 1, 100));
        assert_eq!(store.get_log_reader().await.read_vote().await.unwrap(), Some(v));
        store.shutdown().await;
    }

    #[tokio::test]
    async fn purge_crash_between_marker_and_deletions_reopens_cleanly() {
        for n in [1usize, 2] {
            let io = FakeIo::new();
            let mut store = open(&io, 4096);
            let v = vote(5, 3);
            store.save_vote(&v).await.unwrap();
            append_sync(&mut store, entries(1, 100, 1, 100)).await;
            assert!(seg_list(&io).len() >= 4, "{:?}", seg_list(&io));

            io.crash_before_nth_delete(n);
            let _ = store.purge(log_id(1, 70)).await;
            drop(store);
            io.revive();

            let mut store = open(&io, 4096);
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id, Some(log_id(1, 70)), "n={n}");
            assert_eq!(state.last_log_id, Some(log_id(1, 100)), "n={n}");
            assert_eq!(store.get_log_reader().await.read_vote().await.unwrap(), Some(v));
            assert_eq!(read_all(&mut store, 71, 100).await, entries(71, 100, 1, 100), "n={n}");
            assert_eq!(store.recovery_stats().torn_bytes_discarded, 0, "n={n}");
            store.shutdown().await;
        }
    }

    #[tokio::test]
    async fn purge_marker_synced_before_first_delete_through_store() {
        let io = FakeIo::new();
        let mut store = open(&io, 4096);
        append_sync(&mut store, entries(1, 100, 1, 100)).await;
        let active = *seg_list(&io).last().unwrap();
        io.record_events(true);
        io.clear_events();
        store.purge(log_id(1, 80)).await.unwrap();
        let events = io.events();

        let marker = events
            .iter()
            .position(|e| matches!(e, IoEvent::Write { seq, .. } if *seq == active))
            .expect("purge marker written to the active segment");
        let first_delete = events
            .iter()
            .position(|e| matches!(e, IoEvent::Delete { .. }))
            .expect("purge deleted at least one segment");
        let last_delete = events.iter().rposition(|e| matches!(e, IoEvent::Delete { .. })).unwrap();
        let sync = events
            .iter()
            .enumerate()
            .skip(marker)
            .find(|(_, e)| matches!(e, IoEvent::SyncSegment { seq } if *seq == active))
            .map(|(i, _)| i)
            .expect("active segment synced after the marker");
        assert!(marker < sync && sync < first_delete, "{events:?}");
        assert!(
            events.iter().skip(last_delete).any(|e| matches!(e, IoEvent::SyncDir)),
            "directory synced after the last delete: {events:?}"
        );
        store.shutdown().await;
    }
}
