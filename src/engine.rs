//! `LocalEngine`: assembles the slab arena, lock-free KV index and ring-buffer
//! command intake into the node-local execution tier.
//!
//! Values up to [`MAX_BLOCK_SIZE`](crate::arena::MAX_BLOCK_SIZE) are copied
//! into cacheline-aligned slab blocks that are handed to the index as
//! zero-copy `Bytes`; when the last reference is dropped (after the epoch
//! collector retires the old value) the block returns to the pool. Larger
//! values fall back to a plain heap copy.

use crate::arena::{SlabBlock, SlabPool, MAX_BLOCK_SIZE};
use crate::ipc::Consumer;
use crate::storage::{AtStatus, CasError, KvIndex, VersionedEntry};
use bytes::Bytes;

/// Slab-backed value storage that returns its block to the pool on drop.
struct SlabValue {
    block: Option<SlabBlock>,
    pool: SlabPool,
    len: usize,
}

impl AsRef<[u8]> for SlabValue {
    fn as_ref(&self) -> &[u8] {
        match &self.block {
            Some(b) => &b.as_slice()[..self.len],
            None => &[],
        }
    }
}

impl Drop for SlabValue {
    fn drop(&mut self) {
        if let Some(block) = self.block.take() {
            self.pool.deallocate(block);
        }
    }
}

/// A mutation dispatched to the engine through a ring buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

/// Result of a stamped (`*_at`) apply on the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The entry was applied and the key stamped.
    Applied,
    /// The key's stamp was already at or above the entry's index (replay).
    Skipped,
    /// A CAS did not match; carries the observed value. Nothing was stamped.
    CasFailed { current: Option<Bytes> },
}

impl From<AtStatus> for ApplyOutcome {
    fn from(s: AtStatus) -> Self {
        match s {
            AtStatus::Applied => ApplyOutcome::Applied,
            AtStatus::Skipped => ApplyOutcome::Skipped,
        }
    }
}

/// Node-local lock-free cache engine.
#[derive(Default)]
pub struct LocalEngine {
    index: KvIndex,
    slab: SlabPool,
}

impl LocalEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Copy `value` into a slab block (or the heap if oversized).
    fn materialize(&self, value: &[u8]) -> Bytes {
        if value.len() <= MAX_BLOCK_SIZE {
            if let Some(mut block) = self.slab.allocate(value.len()) {
                block.as_mut_slice()[..value.len()].copy_from_slice(value);
                return Bytes::from_owner(SlabValue {
                    block: Some(block),
                    pool: self.slab.clone(),
                    len: value.len(),
                });
            }
        }
        Bytes::copy_from_slice(value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.index.get(key)
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        self.index.put(key, self.materialize(value));
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.index.delete(key)
    }

    /// Apply a `set` as Raft entry `index`; skipped if already applied.
    pub fn set_at(&self, index: u64, key: &[u8], value: &[u8]) -> ApplyOutcome {
        self.index.put_at(key, self.materialize(value), index).into()
    }

    /// Apply a `delete` as Raft entry `index`; writes a stamped tombstone.
    pub fn delete_at(&self, index: u64, key: &[u8]) -> ApplyOutcome {
        self.index.delete_at(key, index).into()
    }

    /// Apply a compare-and-swap as Raft entry `index`.
    pub fn cas_at(
        &self,
        index: u64,
        key: &[u8],
        expected: Option<&[u8]>,
        desired: Option<&[u8]>,
    ) -> ApplyOutcome {
        match self.index.cas_at(key, expected, desired.map(|v| self.materialize(v)), index) {
            Ok(status) => status.into(),
            Err(CasError::Mismatch { current }) => ApplyOutcome::CasFailed { current },
        }
    }

    /// Fuzzy stamped scan for snapshots; see [`KvIndex::scan_versioned`].
    pub fn scan_versioned(
        &self,
        drop_tombstones_at_or_below: u64,
        f: impl FnMut(VersionedEntry),
    ) {
        self.index.scan_versioned(drop_tombstones_at_or_below, f);
    }

    /// Restore one scanned record into a fresh engine (single-threaded).
    pub fn load_record(&self, key: &[u8], value: Option<&[u8]>, stamp: u64) {
        self.index.load_versioned(key, value.map(|v| self.materialize(v)), stamp);
    }

    pub fn cas(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        desired: Option<&[u8]>,
    ) -> Result<(), CasError> {
        self.index.cas(key, expected, desired.map(|v| self.materialize(v)))
    }

    /// Apply up to `max` queued commands from a ring-buffer consumer.
    /// Returns the number applied.
    pub fn apply_batch<const N: usize>(&self, rx: &mut Consumer<Command, N>, max: usize) -> usize {
        rx.consume_batch(max, |cmd| match cmd {
            Command::Set { key, value } => self.set(&key, &value),
            Command::Delete { key } => {
                self.delete(&key);
            }
        })
    }

    /// Access to the slab pool (for stats).
    pub fn slab(&self) -> &SlabPool {
        &self.slab
    }

    pub fn index(&self) -> &KvIndex {
        &self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::RingBuffer;
    use std::sync::Arc;

    #[test]
    fn test_engine_set_get_delete_cas() {
        let e = LocalEngine::new();
        e.set(b"k", b"hello");
        assert_eq!(e.get(b"k").unwrap(), &b"hello"[..]);
        assert!(e.cas(b"k", Some(b"hello"), Some(b"world")).is_ok());
        assert_eq!(e.get(b"k").unwrap(), &b"world"[..]);
        assert!(e.cas(b"k", Some(b"nope"), None).is_err());
        assert!(e.delete(b"k"));
        assert!(e.get(b"k").is_none());
    }

    #[test]
    fn test_engine_oversize_value_falls_back_to_heap() {
        let e = LocalEngine::new();
        let big = vec![7u8; MAX_BLOCK_SIZE + 1];
        e.set(b"big", &big);
        assert_eq!(e.get(b"big").unwrap(), &big[..]);
        assert_eq!(e.slab().live_blocks(), 0);
    }

    #[test]
    fn test_engine_slab_blocks_are_recycled() {
        let e = LocalEngine::new();
        for i in 0..10_000u32 {
            e.set(b"hot", &i.to_be_bytes());
        }
        // Epoch advancement can be delayed by pinned threads in other tests
        // sharing the global collector, so poll until retired blocks drain.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while e.slab().live_blocks() > 1 && std::time::Instant::now() < deadline {
            crossbeam_epoch::pin().flush();
            std::thread::yield_now();
        }
        // Only the current value's block is still live: every retired value
        // was reclaimed by EBR and its slab block returned to the pool.
        assert_eq!(e.slab().live_blocks(), 1);
        assert_eq!(e.get(b"hot").unwrap(), &9_999u32.to_be_bytes()[..]);
    }

    #[test]
    fn test_engine_ring_dispatch() {
        let e = LocalEngine::new();
        let (mut tx, mut rx) = RingBuffer::<Command, 16>::channel();
        for i in 0..8u8 {
            tx.try_publish(Command::Set {
                key: Bytes::copy_from_slice(&[i]),
                value: Bytes::copy_from_slice(&[i, i]),
            })
            .unwrap();
        }
        tx.try_publish(Command::Delete { key: Bytes::from_static(&[3]) }).unwrap();
        let mut applied = 0;
        while applied < 9 {
            applied += e.apply_batch(&mut rx, 4);
        }
        assert_eq!(e.get(&[2]).unwrap(), &[2u8, 2][..]);
        assert!(e.get(&[3]).is_none());
    }

    #[test]
    fn test_engine_set_at_skips_replayed_index() {
        let e = LocalEngine::new();
        assert_eq!(e.set_at(4, b"k", b"first"), ApplyOutcome::Applied);
        assert_eq!(e.set_at(4, b"k", b"again"), ApplyOutcome::Skipped);
        assert_eq!(e.set_at(2, b"k", b"older"), ApplyOutcome::Skipped);
        assert_eq!(e.get(b"k").unwrap(), &b"first"[..]);
        assert_eq!(e.set_at(5, b"k", b"newer"), ApplyOutcome::Applied);
        assert_eq!(e.get(b"k").unwrap(), &b"newer"[..]);
    }

    #[test]
    fn test_engine_apply_at_replay_is_idempotent() {
        enum Op {
            Set(&'static [u8], &'static [u8]),
            Del(&'static [u8]),
            Cas(&'static [u8], Option<&'static [u8]>, Option<&'static [u8]>),
        }
        let ops = [
            Op::Set(b"a", b"1"),
            Op::Set(b"b", b"2"),
            Op::Cas(b"a", Some(b"1"), Some(b"11")),
            Op::Del(b"b"),
            Op::Cas(b"c", None, Some(b"3")),
            Op::Cas(b"a", Some(b"zzz"), Some(b"bad")), // mismatch
            Op::Set(b"b", b"22"),
            Op::Cas(b"c", Some(b"3"), None),
            Op::Del(b"ghost"),
        ];
        let run = |e: &LocalEngine| -> Vec<ApplyOutcome> {
            ops.iter()
                .enumerate()
                .map(|(i, op)| {
                    let n = i as u64 + 1;
                    match op {
                        Op::Set(k, v) => e.set_at(n, k, v),
                        Op::Del(k) => e.delete_at(n, k),
                        Op::Cas(k, exp, des) => e.cas_at(n, k, *exp, *des),
                    }
                })
                .collect()
        };
        let e = LocalEngine::new();
        let first = run(&e);
        assert_eq!(first[5], ApplyOutcome::CasFailed { current: Some(Bytes::from_static(b"11")) });
        let keys: [&[u8]; 4] = [b"a", b"b", b"c", b"ghost"];
        let before: Vec<_> = keys.iter().map(|k| e.get(k)).collect();
        assert_eq!(before[0].as_deref(), Some(&b"11"[..]));
        assert_eq!(before[1].as_deref(), Some(&b"22"[..]));
        assert_eq!(before[2], None);

        // Replaying the identical sequence at the same indices: every write
        // that landed the first time is Skipped, and state is unchanged.
        let second = run(&e);
        for (i, out) in second.iter().enumerate() {
            match (i, out) {
                // The failed CAS never stamped, so its replay is evaluated
                // again and fails identically against the unchanged value.
                (5, ApplyOutcome::CasFailed { current }) => {
                    assert_eq!(current.as_deref(), Some(&b"11"[..]));
                }
                // A delete on a never-slotted key creates no slot, so there is
                // no stamp to skip on; it reports Applied and changes nothing.
                (8, ApplyOutcome::Applied) => {}
                (_, ApplyOutcome::Skipped) => {}
                other => panic!("op {i}: unexpected replay outcome {other:?}"),
            }
        }
        assert_eq!(e.index().stamp_of(b"ghost"), None);
        let after: Vec<_> = keys.iter().map(|k| e.get(k)).collect();
        assert_eq!(before, after);
    }

    #[test]
    fn test_engine_cas_at_failure_returns_current() {
        let e = LocalEngine::new();
        e.set_at(1, b"k", b"v");
        assert_eq!(
            e.cas_at(2, b"k", Some(b"x"), Some(b"y")),
            ApplyOutcome::CasFailed { current: Some(Bytes::from_static(b"v")) }
        );
        assert_eq!(
            e.cas_at(3, b"missing", Some(b"x"), Some(b"y")),
            ApplyOutcome::CasFailed { current: None }
        );
        assert_eq!(e.index().stamp_of(b"k"), Some(1));
        assert_eq!(e.cas_at(4, b"k", Some(b"v"), Some(b"y")), ApplyOutcome::Applied);
        assert_eq!(e.get(b"k").unwrap(), &b"y"[..]);
    }

    #[test]
    fn test_engine_scan_then_load_record_roundtrip() {
        let src = LocalEngine::new();
        for i in 0..20u8 {
            src.set_at(i as u64 + 1, &[i], &[i; 40]);
        }
        src.delete_at(30, &[4]); // newer than threshold: retained tombstone
        src.delete_at(2, &[0]); // stale tombstone: dropped
        let dst = LocalEngine::new();
        let mut n = 0;
        src.scan_versioned(10, |e| {
            dst.load_record(&e.key, e.value.as_deref(), e.stamp);
            n += 1;
        });
        // Key 0's tombstone (stamp 2 <= 10) is dropped; key 4's (stamp 30) is
        // retained, so 19 records: 18 live plus one tombstone.
        assert_eq!(n, 19);
        for i in 0..20u8 {
            if i == 0 {
                continue; // dropped tombstone: absent in dst, absent in src
            }
            assert_eq!(dst.get(&[i]), src.get(&[i]), "key {i}");
            assert_eq!(dst.index().stamp_of(&[i]), src.index().stamp_of(&[i]), "stamp {i}");
        }
        assert_eq!(dst.get(&[0]), None);
        assert_eq!(dst.index().stamp_of(&[4]), Some(30));
        assert_eq!(dst.set_at(5, &[4], b"stale"), ApplyOutcome::Skipped);
    }

    #[test]
    fn test_engine_concurrent_mixed() {
        let e = Arc::new(LocalEngine::new());
        let hs: Vec<_> = (0..8u8)
            .map(|t| {
                let e = Arc::clone(&e);
                std::thread::spawn(move || {
                    for i in 0..5_000u32 {
                        let key = [(i % 32) as u8];
                        e.set(&key, &[key[0]; 100]);
                        if let Some(v) = e.get(&key) {
                            assert!(v.iter().all(|&b| b == key[0]), "corrupt value");
                        }
                        if i % 7 == t as u32 {
                            e.delete(&key);
                        }
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
    }
}
