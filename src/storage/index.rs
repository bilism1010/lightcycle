//! Lock-free ordered KV index: `crossbeam-skiplist` for the key space and
//! `crossbeam-epoch` for per-key value reclamation.
//!
//! Each key maps to a `Slot` holding an epoch-managed atomic pointer to the
//! current value (null = absent). Value updates (`put`, `delete`, `cas`) are
//! single atomic pointer operations on the slot, so they are linearizable;
//! replaced values are retired through the epoch collector and only freed
//! once no reader can still hold a reference.
//!
//! Deleted keys remain in the skiplist as empty slots (logical tombstones).
//! This keeps `delete` and `cas` race-free against concurrent re-insertion;
//! physical compaction of tombstones is deferred to a later phase.

use bytes::Bytes;
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

/// Failure of a compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasError {
    /// Current value did not match `expected`; carries the observed value.
    Mismatch { current: Option<Bytes> },
}

struct Slot {
    value: Atomic<Bytes>,
}

impl Slot {
    fn empty() -> Self {
        Self { value: Atomic::null() }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        // SAFETY: we have exclusive access (`&mut self`), so no guard is needed.
        unsafe {
            let p = self.value.load(Relaxed, epoch::unprotected());
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
        let p = entry.value().value.load(Acquire, &guard);
        // SAFETY: pointer is protected by `guard` for this scope.
        unsafe { p.as_ref() }.cloned()
    }

    /// Insert or overwrite `key`.
    pub fn put(&self, key: &[u8], value: Bytes) {
        let entry = self.map.get_or_insert_with(Bytes::copy_from_slice(key), Slot::empty);
        let guard = epoch::pin();
        let old = entry.value().value.swap(Owned::new(value), AcqRel, &guard);
        if !old.is_null() {
            // SAFETY: `old` was unlinked by the swap; no new reader can reach it.
            unsafe { guard.defer_destroy(old) };
        }
    }

    /// Remove `key`. Returns `true` if a value was present.
    pub fn delete(&self, key: &[u8]) -> bool {
        let Some(entry) = self.map.get(key) else {
            return false;
        };
        let guard = epoch::pin();
        let old = entry.value().value.swap(Shared::null(), AcqRel, &guard);
        if old.is_null() {
            return false;
        }
        // SAFETY: `old` was unlinked by the swap.
        unsafe { guard.defer_destroy(old) };
        true
    }

    /// Atomically replace the value of `key` with `desired` if its current
    /// value equals `expected` (`None` = absent). `desired = None` deletes.
    pub fn cas(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        desired: Option<Bytes>,
    ) -> Result<(), CasError> {
        let entry = match self.map.get(key) {
            Some(e) => e,
            None if expected.is_some() => return Err(CasError::Mismatch { current: None }),
            None if desired.is_none() => return Ok(()),
            None => self.map.get_or_insert_with(Bytes::copy_from_slice(key), Slot::empty),
        };
        let slot = &entry.value().value;
        let guard = epoch::pin();
        let mut new = desired.map(Owned::new);
        loop {
            let cur = slot.load(Acquire, &guard);
            // SAFETY: protected by `guard`.
            let cur_val = unsafe { cur.as_ref() };
            if cur_val.map(|b| &b[..]) != expected {
                return Err(CasError::Mismatch { current: cur_val.cloned() });
            }
            let result = match new.take() {
                Some(owned) => slot
                    .compare_exchange(cur, owned, AcqRel, Acquire, &guard)
                    .map_err(|e| Some(e.new)),
                None => slot
                    .compare_exchange(cur, Shared::null(), AcqRel, Acquire, &guard)
                    .map_err(|_| None),
            };
            match result {
                Ok(_) => {
                    if !cur.is_null() {
                        // SAFETY: `cur` was unlinked by our successful CAS.
                        unsafe { guard.defer_destroy(cur) };
                    }
                    return Ok(());
                }
                // Lost a race: reclaim our allocation and re-validate.
                Err(back) => new = back,
            }
        }
    }

    /// Number of keys that currently hold a value (O(n)).
    pub fn len(&self) -> usize {
        let guard = epoch::pin();
        self.map
            .iter()
            .filter(|e| !e.value().value.load(Acquire, &guard).is_null())
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
}
