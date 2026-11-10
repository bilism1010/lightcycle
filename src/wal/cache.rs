//! Bounded in-memory entry cache and on-disk offset index (NFR-3).
//!
//! [`LogIndex`] remembers where every live entry's record sits on disk (about 20 bytes per entry),
//! so any entry can be fetched with a single positional read that never queues behind the writer
//! thread. [`EntryCache`] keeps decoded entries in memory under a byte budget, but never evicts an
//! entry above the flushed watermark, so the index always knows where an evicted entry lives and
//! memory is bounded by bytes rather than by the snapshot interval.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::RwLock;

use super::io::SegmentRead;
use super::record::{decode_record, Decoded, RecordType};
use super::segment::{EntryLocation, WalError, WalObserver};

fn index_gap(expected: u64, got: u64) -> WalError {
    WalError::Corrupt {
        seq: 0,
        offset: 0,
        reason: format!("log index push is not contiguous: expected {expected}, got {got}"),
    }
}

/// Offset index of the live logical log: entry index to record location.
#[derive(Debug, Default, Clone)]
pub struct LogIndex {
    /// Index of `locs[0]`; meaningless while `locs` is empty.
    first: u64,
    locs: VecDeque<EntryLocation>,
}

impl LogIndex {
    /// An empty index.
    pub fn new() -> LogIndex {
        LogIndex::default()
    }

    /// Builds the index from recovered locations (ascending and contiguous, as recovery yields).
    pub fn from_recovered(locations: Vec<EntryLocation>) -> LogIndex {
        let first = locations.first().map_or(0, |l| l.index);
        debug_assert!(
            locations.iter().zip(first..).all(|(l, i)| l.index == i),
            "recovered locations must be contiguous"
        );
        LogIndex {
            first,
            locs: locations.into(),
        }
    }

    /// Appends locations. They must continue the log contiguously (any start is accepted while the
    /// index is empty). A violation is a programmer error: a debug assertion in debug builds and a
    /// returned error in release builds; the index is left unchanged either way.
    pub fn push(&mut self, locs: &[EntryLocation]) -> Result<(), WalError> {
        let mut expected = self.last_index().map(|l| l + 1);
        for loc in locs {
            if let Some(e) = expected {
                if loc.index != e {
                    debug_assert!(
                        false,
                        "non-contiguous log index push: expected {e}, got {}",
                        loc.index
                    );
                    return Err(index_gap(e, loc.index));
                }
            }
            expected = Some(loc.index + 1);
        }
        for loc in locs {
            if self.locs.is_empty() {
                self.first = loc.index;
            }
            self.locs.push_back(*loc);
        }
        Ok(())
    }

    /// Drops every entry above `keep_through` (`None` drops everything).
    pub fn truncate(&mut self, keep_through: Option<u64>) {
        match keep_through {
            None => self.locs.clear(),
            Some(k) => {
                while self.locs.back().is_some_and(|l| l.index > k) {
                    self.locs.pop_back();
                }
            }
        }
    }

    /// Drops every entry at or below `upto`.
    pub fn purge(&mut self, upto: u64) {
        while self.locs.front().is_some_and(|l| l.index <= upto) {
            self.locs.pop_front();
            self.first += 1;
        }
    }

    /// Location of the entry at `index`, if it is in the live log.
    pub fn get(&self, index: u64) -> Option<EntryLocation> {
        let off = index.checked_sub(self.first)?;
        self.locs.get(usize::try_from(off).ok()?).copied()
    }

    /// First live index.
    pub fn first_index(&self) -> Option<u64> {
        self.locs.front().map(|l| l.index)
    }

    /// Last live index.
    pub fn last_index(&self) -> Option<u64> {
        self.locs.back().map(|l| l.index)
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.locs.len()
    }

    /// Whether the log has no live entries.
    pub fn is_empty(&self) -> bool {
        self.locs.is_empty()
    }
}

/// Decoded entries held in memory under a byte budget.
///
/// Entries are kept in ascending index order. Eviction removes the oldest entries first and only
/// ever removes entries at or below the flushed watermark.
#[derive(Debug, Clone)]
pub struct EntryCache<E: Clone> {
    entries: VecDeque<(u64, E, usize)>,
    bytes: usize,
    budget: usize,
    flushed: Option<u64>,
}

impl<E: Clone> EntryCache<E> {
    /// An empty cache that tries to stay within `budget_bytes`.
    pub fn new(budget_bytes: usize) -> EntryCache<E> {
        EntryCache {
            entries: VecDeque::new(),
            bytes: 0,
            budget: budget_bytes,
            flushed: None,
        }
    }

    /// Caches `entry` (accounted as `size_bytes`). An index at or below the newest cached one
    /// replaces that suffix, which keeps the deque ascending.
    pub fn insert(&mut self, index: u64, entry: E, size_bytes: usize) {
        if self.entries.back().is_some_and(|(i, _, _)| *i >= index) {
            self.truncate(index.checked_sub(1));
        }
        self.bytes += size_bytes;
        self.entries.push_back((index, entry, size_bytes));
        self.evict();
    }

    /// Records that everything up to and including `upto` is durable, then evicts oldest-first
    /// while over budget.
    pub fn set_flushed(&mut self, upto: u64) {
        self.flushed = Some(self.flushed.map_or(upto, |f| f.max(upto)));
        self.evict();
    }

    fn evict(&mut self) {
        while self.bytes > self.budget {
            let evictable = match (self.entries.front(), self.flushed) {
                (Some((i, _, _)), Some(f)) => *i <= f,
                _ => false,
            };
            if !evictable {
                break;
            }
            if let Some((_, _, size)) = self.entries.pop_front() {
                self.bytes -= size;
            }
        }
    }

    /// The cached entry at `index`, if present.
    pub fn get(&self, index: u64) -> Option<E> {
        let pos = self
            .entries
            .binary_search_by_key(&index, |(i, _, _)| *i)
            .ok()?;
        self.entries.get(pos).map(|(_, e, _)| e.clone())
    }

    /// Drops every cached entry above `keep_through` (`None` drops everything). The flushed
    /// watermark is clamped, because entries rewritten after a truncation are not yet durable.
    pub fn truncate(&mut self, keep_through: Option<u64>) {
        match keep_through {
            None => {
                self.entries.clear();
                self.bytes = 0;
                self.flushed = None;
            }
            Some(k) => {
                while self.entries.back().is_some_and(|(i, _, _)| *i > k) {
                    if let Some((_, _, size)) = self.entries.pop_back() {
                        self.bytes -= size;
                    }
                }
                self.flushed = self.flushed.map(|f| f.min(k));
            }
        }
    }

    /// Drops every cached entry at or below `upto`.
    pub fn purge(&mut self, upto: u64) {
        while self.entries.front().is_some_and(|(i, _, _)| *i <= upto) {
            if let Some((_, _, size)) = self.entries.pop_front() {
                self.bytes -= size;
            }
        }
    }

    /// Bytes currently accounted to cached entries.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is cached.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// State shared between the WAL writer thread (through [`SharedObserver`]) and log readers.
#[derive(Debug, Clone)]
pub struct LogShared<E: Clone> {
    /// Where each live entry's record lives.
    pub index: LogIndex,
    /// Decoded recent entries.
    pub cache: EntryCache<E>,
}

impl<E: Clone> LogShared<E> {
    /// Shared state over `index` with an empty cache of `budget_bytes`.
    pub fn new(index: LogIndex, budget_bytes: usize) -> LogShared<E> {
        LogShared {
            index,
            cache: EntryCache::new(budget_bytes),
        }
    }
}

/// [`WalObserver`] that keeps a shared [`LogShared`] consistent with the writer's ordered
/// callbacks. Each callback takes the write lock only briefly.
pub struct SharedObserver<E: Clone>(pub Arc<RwLock<LogShared<E>>>);

impl<E: Clone + Send + Sync + 'static> WalObserver for SharedObserver<E> {
    fn on_entries_written(&mut self, locs: &[EntryLocation]) {
        if let Err(e) = self.0.write().index.push(locs) {
            // Only reachable in release builds (debug builds assert): the index is unchanged.
            tracing::error!(error = %e, "wal observer received non-contiguous entries");
        }
    }

    fn on_truncate(&mut self, keep_through: Option<u64>) {
        let mut shared = self.0.write();
        shared.index.truncate(keep_through);
        shared.cache.truncate(keep_through);
    }

    fn on_purge(&mut self, upto: u64) {
        let mut shared = self.0.write();
        shared.index.purge(upto);
        shared.cache.purge(upto);
    }

    fn on_flushed(&mut self, upto_index: u64) {
        self.0.write().cache.set_flushed(upto_index);
    }
}

fn read_exact_at(reader: &dyn SegmentRead, loc: &EntryLocation) -> Result<Vec<u8>, WalError> {
    let mut buf = vec![0u8; loc.len as usize];
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read_at(loc.seq, loc.offset + filled as u64, &mut buf[filled..])?;
        if n == 0 {
            return Err(WalError::Corrupt {
                seq: loc.seq,
                offset: loc.offset,
                reason: "short read of an indexed entry".into(),
            });
        }
        filled += n;
    }
    Ok(buf)
}

fn decode_indexed<E>(
    buf: &[u8],
    loc: &EntryLocation,
    decode: &impl Fn(&[u8]) -> Option<E>,
) -> Result<E, WalError> {
    let corrupt = |reason: &str| WalError::Corrupt {
        seq: loc.seq,
        offset: loc.offset,
        reason: reason.into(),
    };
    let Decoded::Record {
        ty: RecordType::Entry,
        payload,
        consumed,
    } = decode_record(buf)
    else {
        return Err(corrupt("indexed location does not hold an entry record"));
    };
    let Some(head) = payload.first_chunk::<8>() else {
        return Err(corrupt("entry record shorter than its index"));
    };
    if consumed != buf.len() || u64::from_le_bytes(*head) != loc.index {
        return Err(corrupt("indexed location holds a different entry"));
    }
    decode(&payload[8..]).ok_or_else(|| corrupt("entry body failed to decode"))
}

/// Reads the entry at `index`: from the cache when present, otherwise from disk through the offset
/// index with one positional read, decoding the record body with `decode`.
///
/// Returns `Ok(None)` when the index is not in the live log, including when it was purged or
/// truncated while this read was in flight.
pub fn read_entry<E: Clone>(
    shared: &RwLock<LogShared<E>>,
    reader: &dyn SegmentRead,
    index: u64,
    decode: impl Fn(&[u8]) -> Option<E>,
) -> Result<Option<E>, WalError> {
    let loc = {
        let guard = shared.read();
        if let Some(entry) = guard.cache.get(index) {
            return Ok(Some(entry));
        }
        match guard.index.get(index) {
            Some(loc) => loc,
            None => return Ok(None),
        }
    };
    let result = read_exact_at(reader, &loc).and_then(|buf| decode_indexed(&buf, &loc, &decode));
    match result {
        Ok(entry) => Ok(Some(entry)),
        Err(e) => {
            // A purge or truncation racing with this read can delete or rewrite the record under
            // us; that is "not found", not corruption.
            if shared.read().index.get(index) == Some(loc) {
                Err(e)
            } else {
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::io::SegmentIo;
    use crate::wal::recovery::test_support::LogBuilder;
    use crate::wal::segment::PreambleState;

    fn loc(index: u64) -> EntryLocation {
        EntryLocation {
            index,
            seq: 1,
            offset: index * 100,
            len: 50,
        }
    }

    fn locs(range: std::ops::RangeInclusive<u64>) -> Vec<EntryLocation> {
        range.map(loc).collect()
    }

    fn cache_with(budget: usize, n: u64) -> EntryCache<u64> {
        let mut c = EntryCache::new(budget);
        for i in 1..=n {
            c.insert(i, i * 10, 50);
        }
        c
    }

    #[test]
    fn cache_never_evicts_unflushed() {
        let mut c = cache_with(100, 5);
        assert_eq!(c.bytes(), 250, "nothing is flushed, so nothing is evicted");
        assert_eq!(c.len(), 5);

        c.set_flushed(2);
        assert_eq!(c.get(1), None);
        assert_eq!(c.get(2), None);
        assert_eq!(
            c.get(3),
            Some(30),
            "above the watermark stays even over budget"
        );
        assert_eq!(c.bytes(), 150);

        c.set_flushed(5);
        assert_eq!(c.bytes(), 100, "evicts oldest flushed until within budget");
        assert_eq!(c.get(3), None);
        assert_eq!(c.get(4), Some(40));
        assert_eq!(c.get(5), Some(50));
    }

    #[test]
    fn budget_evicts_oldest_flushed() {
        let mut c = cache_with(120, 4);
        c.set_flushed(4);
        // 200 bytes over a 120 budget: drops 1 and 2 (oldest first), keeps 3 and 4.
        assert_eq!(c.bytes(), 100);
        assert_eq!(
            (c.get(1), c.get(2), c.get(3), c.get(4)),
            (None, None, Some(30), Some(40))
        );

        // New flushed inserts are evicted on arrival once over budget.
        c.insert(5, 50, 50);
        assert_eq!(c.bytes(), 100);
        assert_eq!(c.get(3), None, "oldest flushed entry makes room");
        assert_eq!(c.get(5), Some(50));
    }

    #[test]
    fn flushed_watermark_only_moves_forward() {
        let mut c = cache_with(0, 3);
        c.set_flushed(2);
        c.set_flushed(1);
        assert_eq!(c.len(), 1, "a lower watermark must not un-flush entries");
        assert_eq!(c.get(3), Some(30));
    }

    #[test]
    fn truncate_drops_suffix_from_index_and_cache() {
        let mut idx = LogIndex::from_recovered(locs(1..=5));
        idx.truncate(Some(3));
        assert_eq!(
            (idx.first_index(), idx.last_index(), idx.len()),
            (Some(1), Some(3), 3)
        );
        assert_eq!(idx.get(4), None);
        assert_eq!(idx.get(3), Some(loc(3)));

        let mut c = cache_with(1000, 5);
        c.set_flushed(5);
        c.truncate(Some(3));
        assert_eq!((c.get(3), c.get(4), c.get(5)), (Some(30), None, None));
        assert_eq!(c.bytes(), 150);

        idx.truncate(None);
        assert!(idx.is_empty());
        assert_eq!(idx.first_index(), None);
        c.truncate(None);
        assert_eq!((c.len(), c.bytes()), (0, 0));
    }

    #[test]
    fn truncate_clamps_the_flushed_watermark() {
        let mut c = cache_with(100, 3);
        c.set_flushed(3);
        assert_eq!((c.get(1), c.get(2), c.get(3)), (None, Some(20), Some(30)));
        c.truncate(Some(2));
        // Entries 3 and 4 are rewritten after the truncation, so they are not durable.
        c.insert(3, 300, 50);
        c.insert(4, 400, 50);
        c.insert(5, 500, 50);
        assert_eq!(
            c.get(3),
            Some(300),
            "an unflushed rewrite must not be evicted"
        );
        assert_eq!(c.get(2), None, "the flushed survivor is evicted instead");
        assert_eq!(c.bytes(), 150);
    }

    #[test]
    fn purge_drops_prefix_from_index_and_cache() {
        let mut idx = LogIndex::from_recovered(locs(1..=5));
        idx.purge(2);
        assert_eq!(
            (idx.first_index(), idx.last_index(), idx.len()),
            (Some(3), Some(5), 3)
        );
        assert_eq!(idx.get(2), None);
        assert_eq!(idx.get(3), Some(loc(3)));
        assert_eq!(idx.get(5), Some(loc(5)));
        assert_eq!(idx.get(6), None);
        idx.purge(100);
        assert!(idx.is_empty());

        let mut c = cache_with(1000, 5);
        c.purge(2);
        assert_eq!((c.get(2), c.get(3)), (None, Some(30)));
        assert_eq!(c.bytes(), 150);
    }

    #[test]
    fn log_index_push_appends_and_restarts_when_empty() {
        let mut idx = LogIndex::new();
        idx.push(&locs(7..=9)).unwrap();
        idx.push(&locs(10..=10)).unwrap();
        assert_eq!((idx.first_index(), idx.last_index()), (Some(7), Some(10)));
        assert_eq!(idx.get(8), Some(loc(8)));
        idx.truncate(None);
        idx.push(&locs(3..=4)).unwrap();
        assert_eq!((idx.first_index(), idx.last_index()), (Some(3), Some(4)));
        assert_eq!(idx.get(4), Some(loc(4)));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "non-contiguous")]
    fn log_index_push_requires_contiguous_indices() {
        let mut idx = LogIndex::from_recovered(locs(1..=3));
        let _ = idx.push(&locs(5..=5));
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn log_index_push_requires_contiguous_indices() {
        let mut idx = LogIndex::from_recovered(locs(1..=3));
        assert!(idx.push(&locs(5..=5)).is_err());
        assert_eq!(idx.last_index(), Some(3));
    }

    #[test]
    fn shared_observer_applies_callbacks_in_order() {
        let shared = Arc::new(RwLock::new(LogShared::<u64>::new(LogIndex::new(), 100)));
        let mut obs = SharedObserver(Arc::clone(&shared));

        obs.on_entries_written(&locs(1..=5));
        for i in 1..=5 {
            shared.write().cache.insert(i, i * 10, 50);
        }
        assert_eq!(shared.read().index.last_index(), Some(5));
        assert_eq!(shared.read().cache.bytes(), 250);

        obs.on_truncate(Some(3));
        {
            let g = shared.read();
            assert_eq!(g.index.last_index(), Some(3));
            assert_eq!(g.cache.get(4), None);
            assert_eq!(g.cache.bytes(), 150);
        }
        // Idempotent when the store already truncated the cache.
        obs.on_truncate(Some(3));
        assert_eq!(shared.read().cache.bytes(), 150);

        obs.on_entries_written(&locs(4..=6));
        for i in 4..=6 {
            shared.write().cache.insert(i, i * 100, 50);
        }
        assert_eq!(shared.read().index.last_index(), Some(6));
        assert_eq!(shared.read().cache.get(4), Some(400));

        obs.on_purge(2);
        {
            let g = shared.read();
            assert_eq!(g.index.first_index(), Some(3));
            assert_eq!(g.cache.get(2), None);
            assert_eq!(g.cache.get(3), Some(30));
        }

        obs.on_flushed(4);
        {
            let g = shared.read();
            // 4 entries (3..=6) = 200 bytes over budget 100; flushed through 4 evicts 3 and 4.
            assert_eq!(g.cache.bytes(), 100);
            assert_eq!(g.cache.get(3), None);
            assert_eq!(g.cache.get(4), None);
            assert_eq!(g.cache.get(5), Some(500));
            assert_eq!(
                g.index.get(4).map(|l| l.index),
                Some(4),
                "evicted entries stay indexed"
            );
        }
    }

    fn body_of(i: u64) -> Vec<u8> {
        format!("entry-body-{i}").into_bytes()
    }

    #[test]
    fn miss_reads_from_disk_via_location() {
        let mut b = LogBuilder::new(4096);
        b.segment(1, PreambleState::default());
        for i in 1..=4 {
            b.entry(i, &body_of(i));
        }
        b.sync();
        let reader = b.io.reader();

        let shared = RwLock::new(LogShared::<Vec<u8>>::new(
            LogIndex::from_recovered(b.entries.clone()),
            10,
        ));
        for i in 1..=4 {
            shared.write().cache.insert(i, body_of(i), 20);
        }
        let decode = |bytes: &[u8]| Some(bytes.to_vec());

        // Cache hit.
        let hit = read_entry(&shared, &*reader, 3, decode).unwrap();
        assert_eq!(hit, Some(body_of(3)));

        // Evict everything flushed, then the same read is served from disk.
        shared.write().cache.set_flushed(4);
        assert!(shared.read().cache.is_empty());
        for i in 1..=4 {
            assert_eq!(
                read_entry(&shared, &*reader, i, decode).unwrap(),
                Some(body_of(i))
            );
        }

        // Outside the live log.
        assert_eq!(read_entry(&shared, &*reader, 5, decode).unwrap(), None);
        assert_eq!(read_entry(&shared, &*reader, 0, decode).unwrap(), None);

        // A body the caller cannot decode is corruption, not a miss.
        let err = read_entry(&shared, &*reader, 2, |_: &[u8]| None::<Vec<u8>>).unwrap_err();
        assert!(matches!(err, WalError::Corrupt { seq: 1, .. }), "{err:?}");
    }

    #[test]
    fn disk_read_detects_wrong_record_and_tolerates_concurrent_purge() {
        let mut b = LogBuilder::new(4096);
        b.segment(1, PreambleState::default());
        for i in 1..=2 {
            b.entry(i, &body_of(i));
        }
        b.sync();
        let reader = b.io.reader();
        let decode = |bytes: &[u8]| Some(bytes.to_vec());

        // An index pointing at the wrong record is reported as corruption.
        let mut bad = b.entries.clone();
        bad[1].offset = bad[0].offset;
        bad[1].len = bad[0].len;
        let shared = RwLock::new(LogShared::<Vec<u8>>::new(LogIndex::from_recovered(bad), 0));
        assert!(matches!(
            read_entry(&shared, &*reader, 2, decode),
            Err(WalError::Corrupt { .. })
        ));

        // A segment deleted after a purge raced with the read is "not found".
        let shared = RwLock::new(LogShared::<Vec<u8>>::new(
            LogIndex::from_recovered(b.entries.clone()),
            0,
        ));
        let mut io = b.io.clone();
        io.delete_segment(1).unwrap();
        assert!(read_entry(&shared, &*reader, 1, decode).is_err());
        shared.write().index.purge(2);
        assert_eq!(read_entry(&shared, &*reader, 1, decode).unwrap(), None);
    }
}
