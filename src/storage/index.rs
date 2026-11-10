//! Lock-free ordered KV index: `crossbeam-skiplist` for the key space and
//! `crossbeam-epoch` for per-key cell reclamation.
//!
//! Each key maps to a `Slot` holding an epoch-managed atomic pointer to a
//! `Versioned` cell: the key's value *and* the Raft apply-index stamp of the
//! entry that produced it, in one allocation. The pair is swapped by a single
//! atomic pointer operation, so a reader (or the snapshot scan) can never
//! observe a torn `(value, stamp)` pair. A null pointer means "never written";
//! a deleted key is a non-null cell whose value is `None` (a tombstone) and
//! whose index is the stamp of the delete. Replaced cells are retired through
//! the epoch collector and freed only once no reader can still reference them.
//!
//! Two API families share the cell:
//!
//! * The legacy `put`/`get`/`delete`/`cas`/`len` methods keep their Phase 2
//!   signatures and semantics and write with stamp `0`. They exist for Phase 2
//!   callers and the benches.
//! * The `*_at` methods (`put_at`, `delete_at`, `cas_at`) are the Raft apply
//!   path. Each skips an entry whose index is at or below the key's stamp, so
//!   replaying a log suffix over a fuzzy snapshot is idempotent and correct
//!   for single-key operations including CAS. The Raft apply path must use
//!   only the `*_at` methods. `scan_versioned` / `load_versioned` produce and
//!   consume the stamped snapshot stream.
//!
//! Tombstones stay in the skiplist (they carry stamps); a snapshot scan drops
//! only tombstones whose stamp is at or below the snapshot start index.
//! Multi-key transactions are out of scope: single-key stamping does not
//! cover them.

use bytes::Bytes;
use crossbeam_epoch::{self as epoch, Atomic, Owned};
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

/// Failure of a compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasError {
    /// Current value did not match `expected`; carries the observed value.
    Mismatch { current: Option<Bytes> },
}

/// Outcome of a stamped (`*_at`) write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtStatus {
    /// The entry was applied (and stamped).
    Applied,
    /// The key's stamp was already at or above the entry's index.
    Skipped,
}

/// One entry of a versioned scan. `value == None` is a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedEntry {
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub stamp: u64,
}

/// A value and the apply-index stamp that produced it, swapped as one pointer.
struct Versioned {
    index: u64,
    value: Option<Bytes>,
}

struct Slot {
    cell: Atomic<Versioned>,
}

impl Slot {
    fn empty() -> Self {
        Self { cell: Atomic::null() }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        // SAFETY: we have exclusive access (`&mut self`), so no guard is needed.
        unsafe {
            let p = self.cell.load(Relaxed, epoch::unprotected());
            if !p.is_null() {
                drop(p.into_owned());
            }
        }
    }
}

/// Ordered, lock-free key-value index.
pub struct KvIndex {
    map: SkipMap<Bytes, Slot>,
}

impl Default for KvIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl KvIndex {
    pub fn new() -> Self {
        Self { map: SkipMap::new() }
    }

    /// Read the current value for `key`.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let entry = self.map.get(key)?;
        let guard = epoch::pin();
        let p = entry.value().cell.load(Acquire, &guard);
        // SAFETY: pointer is protected by `guard` for this scope.
        unsafe { p.as_ref() }.and_then(|v| v.value.clone())
    }

    /// Apply-index stamp of `key`'s cell (value or tombstone), if it has one.
    pub fn stamp_of(&self, key: &[u8]) -> Option<u64> {
        let entry = self.map.get(key)?;
        let guard = epoch::pin();
        let p = entry.value().cell.load(Acquire, &guard);
        // SAFETY: pointer is protected by `guard` for this scope.
        unsafe { p.as_ref() }.map(|v| v.index)
    }

    /// Insert or overwrite `key` (stamp 0).
    pub fn put(&self, key: &[u8], value: Bytes) {
        let entry = self.map.get_or_insert_with(Bytes::copy_from_slice(key), Slot::empty);
        let guard = epoch::pin();
        let old = entry
            .value()
            .cell
            .swap(Owned::new(Versioned { index: 0, value: Some(value) }), AcqRel, &guard);
        if !old.is_null() {
            // SAFETY: `old` was unlinked by the swap; no new reader can reach it.
            unsafe { guard.defer_destroy(old) };
        }
    }

    /// Insert or overwrite `key` as Raft entry `index`, unless the key's
    /// stamp is already at or above `index` (then nothing is written).
    pub fn put_at(&self, key: &[u8], value: Bytes, index: u64) -> AtStatus {
        let entry = self.map.get_or_insert_with(Bytes::copy_from_slice(key), Slot::empty);
        let slot = &entry.value().cell;
        let guard = epoch::pin();
        let mut new = Owned::new(Versioned { index, value: Some(value) });
        loop {
            let cur = slot.load(Acquire, &guard);
            // SAFETY: protected by `guard`.
            if let Some(c) = unsafe { cur.as_ref() } {
                if c.index >= index {
                    return AtStatus::Skipped;
                }
            }
            match slot.compare_exchange(cur, new, AcqRel, Acquire, &guard) {
                Ok(_) => {
                    if !cur.is_null() {
                        // SAFETY: `cur` was unlinked by our successful CAS.
                        unsafe { guard.defer_destroy(cur) };
                    }
                    return AtStatus::Applied;
                }
                // Lost a race: reclaim our allocation and re-check the stamp.
                Err(e) => new = e.new,
            }
        }
    }

    /// Remove `key` (stamp 0). Returns `true` if a value was present.
    pub fn delete(&self, key: &[u8]) -> bool {
        let Some(entry) = self.map.get(key) else {
            return false;
        };
        let slot = &entry.value().cell;
        let guard = epoch::pin();
        let mut new = Owned::new(Versioned { index: 0, value: None });
        loop {
            let cur = slot.load(Acquire, &guard);
            // SAFETY: protected by `guard`.
            if unsafe { cur.as_ref() }.is_none_or(|c| c.value.is_none()) {
                return false;
            }
            match slot.compare_exchange(cur, new, AcqRel, Acquire, &guard) {
                Ok(_) => {
                    // SAFETY: `cur` was unlinked by our successful CAS.
                    unsafe { guard.defer_destroy(cur) };
                    return true;
                }
                Err(e) => new = e.new,
            }
        }
    }

    /// Delete `key` as Raft entry `index`: swaps in a tombstone stamped
    /// `index` (even over an existing tombstone), unless the key's stamp is
    /// already at or above `index`. A key with no slot is a no-op (no slot is
    /// created) and counts as `Applied`.
    pub fn delete_at(&self, key: &[u8], index: u64) -> AtStatus {
        let Some(entry) = self.map.get(key) else {
            return AtStatus::Applied;
        };
        let slot = &entry.value().cell;
        let guard = epoch::pin();
        let mut new = Owned::new(Versioned { index, value: None });
        loop {
            let cur = slot.load(Acquire, &guard);
            // SAFETY: protected by `guard`.
            if let Some(c) = unsafe { cur.as_ref() } {
                if c.index >= index {
                    return AtStatus::Skipped;
                }
            }
            match slot.compare_exchange(cur, new, AcqRel, Acquire, &guard) {
                Ok(_) => {
                    if !cur.is_null() {
                        // SAFETY: `cur` was unlinked by our successful CAS.
                        unsafe { guard.defer_destroy(cur) };
                    }
                    return AtStatus::Applied;
                }
                Err(e) => new = e.new,
            }
        }
    }

    /// Compare-and-swap as Raft entry `index`. Returns `Skipped` (before
    /// evaluating `expected`) if the key's stamp is at or above `index`. A
    /// mismatch returns `Err` and leaves value and stamp untouched; success
    /// stamps `index` (`desired = None` writes a stamped tombstone).
    pub fn cas_at(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        desired: Option<Bytes>,
        index: u64,
    ) -> Result<AtStatus, CasError> {
        self.cas_inner(key, expected, desired, index, true)
    }

    /// Visit every key's `(value, stamp)` for a snapshot. Tombstones whose
    /// stamp is at or below `drop_tombstones_at_or_below` are omitted; newer
    /// tombstones are yielded (`value == None`). Never-written slots are
    /// skipped.
    ///
    /// Each key is read under its own short epoch pin that is dropped before
    /// the callback runs, so no pin is held across the scan or inside `f`
    /// (NFR-4): writes, including from inside `f`, are allowed and the scan
    /// is a fuzzy (non-atomic) view, which the stamps make safe to replay.
    pub fn scan_versioned(&self, drop_tombstones_at_or_below: u64, mut f: impl FnMut(VersionedEntry)) {
        for entry in self.map.iter() {
            let (value, stamp) = {
                let guard = epoch::pin();
                let p = entry.value().cell.load(Acquire, &guard);
                // SAFETY: protected by `guard`, which ends with this block.
                match unsafe { p.as_ref() } {
                    None => continue,
                    Some(c) => (c.value.clone(), c.index),
                }
            };
            if value.is_none() && stamp <= drop_tombstones_at_or_below {
                continue;
            }
            f(VersionedEntry { key: entry.key().clone(), value, stamp });
        }
    }

    /// Install `(value, stamp)` for `key` unconditionally (`None` = tombstone).
    /// For single-threaded restore of a snapshot into a fresh index.
    pub fn load_versioned(&self, key: &[u8], value: Option<Bytes>, stamp: u64) {
        let entry = self.map.get_or_insert_with(Bytes::copy_from_slice(key), Slot::empty);
        let guard = epoch::pin();
        let old = entry
            .value()
            .cell
            .swap(Owned::new(Versioned { index: stamp, value }), AcqRel, &guard);
        if !old.is_null() {
            // SAFETY: `old` was unlinked by the swap; no new reader can reach it.
            unsafe { guard.defer_destroy(old) };
        }
    }

    /// Atomically replace the value of `key` with `desired` if its current
    /// value equals `expected` (`None` = absent). `desired = None` deletes.
    pub fn cas(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        desired: Option<Bytes>,
    ) -> Result<(), CasError> {
        self.cas_inner(key, expected, desired, 0, false).map(|_| ())
    }

    /// Shared CAS loop. When `gated`, the entry is skipped (before the
    /// expected-value check) if the cell's stamp is at or above `index`. A
    /// failed CAS never stamps. Success stamps `index`.
    fn cas_inner(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        desired: Option<Bytes>,
        index: u64,
        gated: bool,
    ) -> Result<AtStatus, CasError> {
        let entry = match self.map.get(key) {
            Some(e) => e,
            None if expected.is_some() => return Err(CasError::Mismatch { current: None }),
            None if desired.is_none() => return Ok(AtStatus::Applied),
            None => self.map.get_or_insert_with(Bytes::copy_from_slice(key), Slot::empty),
        };
        let slot = &entry.value().cell;
        let guard = epoch::pin();
        let mut new = Owned::new(Versioned { index, value: desired });
        loop {
            let cur = slot.load(Acquire, &guard);
            // SAFETY: protected by `guard`.
            let cur_cell = unsafe { cur.as_ref() };
            if gated && cur_cell.is_some_and(|c| c.index >= index) {
                return Ok(AtStatus::Skipped);
            }
            let cur_val = cur_cell.and_then(|c| c.value.as_ref());
            if cur_val.map(|b| &b[..]) != expected {
                return Err(CasError::Mismatch { current: cur_val.cloned() });
            }
            match slot.compare_exchange(cur, new, AcqRel, Acquire, &guard) {
                Ok(_) => {
                    if !cur.is_null() {
                        // SAFETY: `cur` was unlinked by our successful CAS.
                        unsafe { guard.defer_destroy(cur) };
                    }
                    return Ok(AtStatus::Applied);
                }
                // Lost a race: reclaim our allocation and re-validate.
                Err(e) => new = e.new,
            }
        }
    }

    /// Number of keys that currently hold a value (O(n)).
    ///
    /// A point-in-time convenience that holds one epoch pin for its whole
    /// iteration, so it is unsuitable for snapshots (NFR-4); use
    /// [`scan_versioned`](Self::scan_versioned) for those.
    pub fn len(&self) -> usize {
        let guard = epoch::pin();
        self.map
            .iter()
            .filter(|e| {
                let p = e.value().cell.load(Acquire, &guard);
                // SAFETY: protected by `guard`.
                unsafe { p.as_ref() }.is_some_and(|c| c.value.is_some())
            })
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_basic_put_get_delete() {
        let idx = KvIndex::new();
        assert_eq!(idx.get(b"a"), None);
        idx.put(b"a", Bytes::from_static(b"1"));
        assert_eq!(idx.get(b"a"), Some(Bytes::from_static(b"1")));
        idx.put(b"a", Bytes::from_static(b"2"));
        assert_eq!(idx.get(b"a"), Some(Bytes::from_static(b"2")));
        assert!(idx.delete(b"a"));
        assert!(!idx.delete(b"a"));
        assert_eq!(idx.get(b"a"), None);
        assert!(!idx.delete(b"never"));
        assert!(idx.is_empty());
    }

    #[test]
    fn test_cas_semantics() {
        let idx = KvIndex::new();
        // insert-if-absent
        assert!(idx.cas(b"k", None, Some(Bytes::from_static(b"v1"))).is_ok());
        assert_eq!(
            idx.cas(b"k", None, Some(Bytes::from_static(b"x"))),
            Err(CasError::Mismatch { current: Some(Bytes::from_static(b"v1")) })
        );
        assert!(idx.cas(b"k", Some(b"v1"), Some(Bytes::from_static(b"v2"))).is_ok());
        assert_eq!(
            idx.cas(b"k", Some(b"v1"), None),
            Err(CasError::Mismatch { current: Some(Bytes::from_static(b"v2")) })
        );
        assert_eq!(
            idx.cas(b"missing", Some(b"v"), None),
            Err(CasError::Mismatch { current: None })
        );
        assert!(idx.cas(b"missing", None, None).is_ok());
        // delete via CAS, then re-insert
        assert!(idx.cas(b"k", Some(b"v2"), None).is_ok());
        assert_eq!(idx.get(b"k"), None);
        assert!(idx.cas(b"k", None, Some(Bytes::from_static(b"v3"))).is_ok());
        assert_eq!(idx.get(b"k"), Some(Bytes::from_static(b"v3")));
    }

    #[test]
    fn test_ordered_len() {
        let idx = KvIndex::new();
        for i in 0..10u8 {
            idx.put(&[i], Bytes::from(vec![i]));
        }
        idx.delete(&[3]);
        assert_eq!(idx.len(), 9);
    }

    #[test]
    fn test_lock_free_index_concurrent_ops() {
        const THREADS: usize = 16;
        const TOTAL_OPS: usize = 100_000;
        const OPS_PER_THREAD: usize = TOTAL_OPS / THREADS;
        const KEYSPACE: usize = 64;

        let idx = Arc::new(KvIndex::new());
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let idx = Arc::clone(&idx);
                std::thread::spawn(move || {
                    // xorshift: deterministic per-thread op mix
                    let mut s = 0x9E37_79B9_7F4A_7C15u64 ^ ((t as u64 + 1) * 0x1000_0001);
                    for _ in 0..OPS_PER_THREAD {
                        s ^= s << 13;
                        s ^= s >> 7;
                        s ^= s << 17;
                        let k = ((s >> 8) as usize % KEYSPACE) as u32;
                        let key = k.to_be_bytes();
                        match s % 4 {
                            0 | 1 => {
                                // Values encode their key: readers verify integrity.
                                let mut v = key.to_vec();
                                v.extend_from_slice(&(t as u32).to_be_bytes());
                                idx.put(&key, Bytes::from(v));
                            }
                            2 => {
                                if let Some(v) = idx.get(&key) {
                                    assert_eq!(&v[..4], &key, "torn or misplaced value");
                                }
                            }
                            _ => {
                                idx.delete(&key);
                            }
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker panicked");
        }
        // Surviving values are intact.
        for k in 0..KEYSPACE as u32 {
            if let Some(v) = idx.get(&k.to_be_bytes()) {
                assert_eq!(&v[..4], &k.to_be_bytes());
            }
        }
    }

    #[test]
    fn test_concurrent_cas_counter_is_exact() {
        const THREADS: usize = 16;
        const INCS: usize = 2_000;
        let idx = Arc::new(KvIndex::new());
        idx.put(b"ctr", Bytes::copy_from_slice(&0u64.to_be_bytes()));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let idx = Arc::clone(&idx);
                std::thread::spawn(move || {
                    for _ in 0..INCS {
                        loop {
                            let cur = idx.get(b"ctr").unwrap();
                            let n = u64::from_be_bytes(cur[..8].try_into().unwrap()) + 1;
                            let new = Bytes::copy_from_slice(&n.to_be_bytes());
                            if idx.cas(b"ctr", Some(&cur), Some(new)).is_ok() {
                                break;
                            }
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let v = idx.get(b"ctr").unwrap();
        assert_eq!(u64::from_be_bytes(v[..8].try_into().unwrap()), (THREADS * INCS) as u64);
    }

    #[test]
    fn test_put_at_skips_stale_index() {
        let idx = KvIndex::new();
        assert_eq!(idx.put_at(b"k", Bytes::from_static(b"a"), 5), AtStatus::Applied);
        assert_eq!(idx.put_at(b"k", Bytes::from_static(b"b"), 5), AtStatus::Skipped);
        assert_eq!(idx.put_at(b"k", Bytes::from_static(b"c"), 3), AtStatus::Skipped);
        assert_eq!(idx.get(b"k"), Some(Bytes::from_static(b"a")));
        assert_eq!(idx.put_at(b"k", Bytes::from_static(b"d"), 6), AtStatus::Applied);
        assert_eq!(idx.get(b"k"), Some(Bytes::from_static(b"d")));
    }

    fn b(s: &'static [u8]) -> Bytes {
        Bytes::from_static(s)
    }

    fn scan_all(idx: &KvIndex, threshold: u64) -> Vec<VersionedEntry> {
        let mut out = Vec::new();
        idx.scan_versioned(threshold, |e| out.push(e));
        out
    }

    #[test]
    fn test_delete_at_leaves_stamped_tombstone() {
        let idx = KvIndex::new();
        idx.put_at(b"k", b(b"v"), 3);
        assert_eq!(idx.delete_at(b"k", 3), AtStatus::Skipped);
        assert_eq!(idx.get(b"k"), Some(b(b"v")));
        assert_eq!(idx.delete_at(b"k", 4), AtStatus::Applied);
        assert_eq!(idx.get(b"k"), None);
        assert_eq!(idx.stamp_of(b"k"), Some(4));
        // Re-deleting a tombstone at a higher index re-stamps it.
        assert_eq!(idx.delete_at(b"k", 9), AtStatus::Applied);
        assert_eq!(idx.stamp_of(b"k"), Some(9));
        // A replayed older put cannot resurrect the key.
        assert_eq!(idx.put_at(b"k", b(b"old"), 5), AtStatus::Skipped);
        assert_eq!(idx.get(b"k"), None);
    }

    #[test]
    fn test_delete_at_absent_key_creates_no_slot() {
        let idx = KvIndex::new();
        assert_eq!(idx.delete_at(b"ghost", 8), AtStatus::Applied);
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.stamp_of(b"ghost"), None);
        assert!(scan_all(&idx, 0).is_empty());
    }

    #[test]
    fn test_cas_at_failed_does_not_stamp() {
        let idx = KvIndex::new();
        idx.put_at(b"k", b(b"v1"), 2);
        assert_eq!(
            idx.cas_at(b"k", Some(b"wrong"), Some(b(b"v2")), 6),
            Err(CasError::Mismatch { current: Some(b(b"v1")) })
        );
        assert_eq!(idx.stamp_of(b"k"), Some(2));
        assert_eq!(idx.get(b"k"), Some(b(b"v1")));
        assert_eq!(idx.cas_at(b"k", Some(b"v1"), Some(b(b"v2")), 6), Ok(AtStatus::Applied));
        assert_eq!(idx.stamp_of(b"k"), Some(6));
        assert_eq!(idx.get(b"k"), Some(b(b"v2")));
    }

    #[test]
    fn test_cas_at_skips_when_stamp_ge_index() {
        let idx = KvIndex::new();
        idx.put_at(b"k", b(b"v1"), 5);
        // Skipped even though `expected` would not match: the gate runs first.
        assert_eq!(idx.cas_at(b"k", Some(b"nope"), Some(b(b"x")), 5), Ok(AtStatus::Skipped));
        assert_eq!(idx.cas_at(b"k", Some(b"v1"), Some(b(b"x")), 4), Ok(AtStatus::Skipped));
        assert_eq!(idx.get(b"k"), Some(b(b"v1")));
        assert_eq!(idx.stamp_of(b"k"), Some(5));
    }

    #[test]
    fn test_cas_at_delete_writes_stamped_tombstone() {
        let idx = KvIndex::new();
        idx.put_at(b"k", b(b"v1"), 1);
        assert_eq!(idx.cas_at(b"k", Some(b"v1"), None, 7), Ok(AtStatus::Applied));
        assert_eq!(idx.get(b"k"), None);
        assert_eq!(idx.stamp_of(b"k"), Some(7));
        // Absent key, expected None, desired None: Ok, no slot created.
        assert_eq!(idx.cas_at(b"fresh", None, None, 3), Ok(AtStatus::Applied));
        assert_eq!(idx.stamp_of(b"fresh"), None);
        // Insert-if-absent over the tombstone works and stamps.
        assert_eq!(idx.cas_at(b"k", None, Some(b(b"v2")), 8), Ok(AtStatus::Applied));
        assert_eq!(idx.stamp_of(b"k"), Some(8));
    }

    #[test]
    fn test_scan_drops_old_tombstones_keeps_newer() {
        let idx = KvIndex::new();
        idx.put_at(b"live", b(b"v"), 2);
        idx.put_at(b"dead5", b(b"v"), 1);
        idx.delete_at(b"dead5", 5);
        idx.put_at(b"dead6", b(b"v"), 1);
        idx.delete_at(b"dead6", 6);
        let got = scan_all(&idx, 5);
        let keys: Vec<_> = got.iter().map(|e| e.key.clone()).collect();
        assert_eq!(keys, vec![b(b"dead6"), b(b"live")]);
        assert_eq!(got[0].value, None);
        assert_eq!(got[0].stamp, 6);
        assert_eq!(got[1].value, Some(b(b"v")));
        assert_eq!(got[1].stamp, 2);
    }

    #[test]
    fn test_scan_callback_runs_without_epoch_pin() {
        let idx = KvIndex::new();
        for i in 0..8u8 {
            idx.put_at(&[i], Bytes::from(vec![i]), 1);
        }
        let mut seen = 0;
        idx.scan_versioned(0, |e| {
            assert!(!crossbeam_epoch::is_pinned(), "scan callback ran under an epoch pin");
            // Writes during a scan are allowed.
            idx.put_at(&e.key, Bytes::from_static(b"rewritten"), 2);
            seen += 1;
        });
        assert_eq!(seen, 8);
        assert_eq!(idx.get(&[3]), Some(b(b"rewritten")));
    }

    #[test]
    fn test_load_versioned_roundtrips_scan() {
        let src = KvIndex::new();
        src.put_at(b"a", b(b"1"), 3);
        src.put_at(b"b", b(b"2"), 4);
        src.put_at(b"c", b(b"3"), 1);
        src.delete_at(b"c", 9); // retained tombstone (threshold 5)
        src.put_at(b"d", b(b"4"), 1);
        src.delete_at(b"d", 2); // dropped tombstone

        let dst = KvIndex::new();
        let mut loaded = Vec::new();
        src.scan_versioned(5, |e| {
            dst.load_versioned(&e.key, e.value.clone(), e.stamp);
            loaded.push(e.key);
        });
        assert_eq!(loaded, vec![b(b"a"), b(b"b"), b(b"c")]);
        for k in [&b"a"[..], b"b", b"c"] {
            assert_eq!(dst.get(k), src.get(k));
            assert_eq!(dst.stamp_of(k), src.stamp_of(k));
        }
        assert_eq!(dst.get(b"c"), None);
        assert_eq!(dst.stamp_of(b"c"), Some(9));
        assert_eq!(dst.stamp_of(b"d"), None);
        // The restored tombstone still blocks a replayed older put.
        assert_eq!(dst.put_at(b"c", b(b"3"), 6), AtStatus::Skipped);
    }

    /// T-03-03: the (value, stamp) pair is never torn. Every value encodes
    /// the index that wrote it, so a concurrent scan must always see them agree.
    #[test]
    fn test_scan_never_sees_torn_value_stamp_pair() {
        const KEYS: u8 = 8;
        const WRITES: u64 = 20_000;
        let idx = Arc::new(KvIndex::new());
        let writers: Vec<_> = (0..4u64)
            .map(|t| {
                let idx = Arc::clone(&idx);
                std::thread::spawn(move || {
                    for i in 1..=WRITES {
                        let index = i * 4 + t;
                        let key = [(i % KEYS as u64) as u8];
                        let v = Bytes::copy_from_slice(&index.to_be_bytes());
                        if index % 5 == 0 {
                            idx.delete_at(&key, index);
                        } else {
                            idx.put_at(&key, v, index);
                        }
                    }
                })
            })
            .collect();
        let reader = {
            let idx = Arc::clone(&idx);
            std::thread::spawn(move || {
                for _ in 0..200 {
                    idx.scan_versioned(0, |e| {
                        if let Some(v) = e.value {
                            assert_eq!(u64::from_be_bytes(v[..8].try_into().unwrap()), e.stamp);
                        }
                    });
                }
            })
        };
        for w in writers {
            w.join().unwrap();
        }
        reader.join().unwrap();
    }

    #[test]
    fn test_put_at_stamp_visible_via_stamp_of() {
        let idx = KvIndex::new();
        assert_eq!(idx.stamp_of(b"k"), None);
        idx.put_at(b"k", Bytes::from_static(b"a"), 7);
        assert_eq!(idx.stamp_of(b"k"), Some(7));
        idx.put_at(b"k", Bytes::from_static(b"b"), 2);
        assert_eq!(idx.stamp_of(b"k"), Some(7));
        idx.put(b"legacy", Bytes::from_static(b"x"));
        assert_eq!(idx.stamp_of(b"legacy"), Some(0));
    }
}
