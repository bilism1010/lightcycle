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
use super::segment::{WalConfig, WalError};
use super::writer::{Ack, WalHandle, WalOp, WalPayload, WalStats};
use crate::raft::types::{KvRequest, TypeConfig};

type Entry = EntryOf<TypeConfig>;
type LogId = LogIdOf<TypeConfig>;
type Vote = VoteOf<TypeConfig>;
type Shared = Arc<RwLock<LogShared<Entry>>>;

/// Fixed per-entry overhead added to the payload size estimate used for cache accounting.
const ENTRY_OVERHEAD_BYTES: usize = 32;
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
            let ack: Option<Ack> = if i + 1 == count {
                callback.take().map(|cb| -> Ack { Box::new(move |r| cb.io_completed(r)) })
            } else {
                None
            };
            if let Err(e) = self.submit(WalOp::Entry { index, payload: EntryBody(entry), ack }).await
            {
                // Only reachable before the final op was handed over; otherwise the ack owns it.
                return Err(e);
            }
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

impl RaftLogReader<TypeConfig> for WalLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let (first, next) = {
            let meta = self.meta.read();
            (meta.first, meta.next)
        };
        let lo = match range.start_bound() {
            Bound::Included(s) => *s,
            Bound::Excluded(s) => s.saturating_add(1),
            Bound::Unbounded => 0,
        }
        .max(first);
        let hi = match range.end_bound() {
            Bound::Included(e) => e.saturating_add(1),
            Bound::Excluded(e) => *e,
            Bound::Unbounded => u64::MAX,
        }
        .min(next);

        let mut out = Vec::new();
        for index in lo..hi {
            match read_entry(&self.shared, &*self.reader, index, decode_entry).map_err(wal_to_io)? {
                Some(entry) => out.push(entry),
                // Purged or truncated while we read: stop at the hole, never skip it.
                None => break,
            }
        }
        Ok(out)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.meta.read().vote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::io::FakeIo;
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
}
