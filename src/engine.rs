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
use crate::storage::{AtStatus, CasError, KvIndex};
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
