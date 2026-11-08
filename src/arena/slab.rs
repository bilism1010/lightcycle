//! Cacheline-aligned slab pool with power-of-two size classes (64B to 64KB).
//!
//! Every block handed out is aligned to 64 bytes (one cacheline) so that two
//! blocks never share a line. Freed blocks go onto a per-class lock-free free
//! list (`SegQueue`) and are reused before any fresh memory is requested from
//! the system allocator, which keeps steady-state allocation off the global
//! allocator entirely.

use crossbeam::queue::SegQueue;
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Cacheline size in bytes; alignment of every slab block.
pub const CACHELINE: usize = 64;
/// Smallest size class.
pub const MIN_BLOCK_SIZE: usize = 64;
/// Largest size class.
pub const MAX_BLOCK_SIZE: usize = 64 * 1024;
/// Number of size classes: 64B, 128B, ... 64KB.
pub const NUM_CLASSES: usize = 11;

/// A cacheline-aligned block of memory owned by a [`SlabPool`] size class.
///
/// A block must be returned with [`SlabPool::deallocate`]; dropping it
/// without doing so leaks the underlying chunk (the pool cannot see it).
#[derive(Debug)]
pub struct SlabBlock {
    ptr: NonNull<u8>,
    class: u8,
}

// SAFETY: a SlabBlock uniquely owns its chunk; the raw pointer is never aliased.
unsafe impl Send for SlabBlock {}
unsafe impl Sync for SlabBlock {}

impl SlabBlock {
    /// Raw pointer to the start of the block (64-byte aligned).
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Usable capacity in bytes (the size of the block's class).
    pub fn capacity(&self) -> usize {
        MIN_BLOCK_SIZE << self.class
    }

    /// Mutable view of the whole block.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `capacity` bytes and uniquely owned.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity()) }
    }

    /// Shared view of the whole block.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for `capacity` bytes; allocated zeroed-or-written.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.capacity()) }
    }
}

struct Inner {
    free: [SegQueue<SlabBlock>; NUM_CLASSES],
    /// Chunks ever obtained from the system allocator, per class.
    fresh: [AtomicUsize; NUM_CLASSES],
    /// Blocks currently handed out, per class.
    live: [AtomicUsize; NUM_CLASSES],
}

impl Drop for Inner {
    fn drop(&mut self) {
        for (class, q) in self.free.iter().enumerate() {
            while let Some(block) = q.pop() {
                // SAFETY: block was allocated with this class's layout.
                unsafe { dealloc(block.ptr.as_ptr(), class_layout(class)) };
            }
        }
    }
}

fn class_layout(class: usize) -> Layout {
    Layout::from_size_align(MIN_BLOCK_SIZE << class, CACHELINE).expect("valid slab layout")
}

/// Map a request size to its size class index, or `None` if too large.
fn class_for(size: usize) -> Option<usize> {
    if size > MAX_BLOCK_SIZE {
        return None;
    }
    let rounded = size.max(MIN_BLOCK_SIZE).next_power_of_two();
    Some((rounded / MIN_BLOCK_SIZE).trailing_zeros() as usize)
}

/// Cloneable handle to a shared pool of cacheline-aligned slabs.
#[derive(Clone)]
pub struct SlabPool {
    inner: Arc<Inner>,
}

impl Default for SlabPool {
    fn default() -> Self {
        Self::new()
    }
}

impl SlabPool {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                free: std::array::from_fn(|_| SegQueue::new()),
                fresh: std::array::from_fn(|_| AtomicUsize::new(0)),
                live: std::array::from_fn(|_| AtomicUsize::new(0)),
            }),
        }
    }

    /// Allocate a block of at least `size` bytes, 64-byte aligned.
    ///
    /// Returns `None` if `size` exceeds [`MAX_BLOCK_SIZE`] or the system
    /// allocator fails.
    pub fn allocate(&self, size: usize) -> Option<SlabBlock> {
        let class = class_for(size)?;
        let block = match self.inner.free[class].pop() {
            Some(block) => block,
            None => {
                // SAFETY: layout has non-zero size.
                let ptr = NonNull::new(unsafe { alloc(class_layout(class)) })?;
                self.inner.fresh[class].fetch_add(1, Ordering::Relaxed);
                SlabBlock { ptr, class: class as u8 }
            }
        };
        self.inner.live[class].fetch_add(1, Ordering::Relaxed);
        Some(block)
    }

    /// Return a block to its class free list for reuse.
    pub fn deallocate(&self, block: SlabBlock) {
        let class = block.class as usize;
        self.inner.live[class].fetch_sub(1, Ordering::Relaxed);
        self.inner.free[class].push(block);
    }

    /// Number of blocks currently handed out across all classes.
    pub fn live_blocks(&self) -> usize {
        self.inner.live.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// Number of chunks ever requested from the system allocator.
    pub fn fresh_allocations(&self) -> usize {
        self.inner.fresh.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_alignment_and_lifecycle() {
        let pool = SlabPool::new();
        let sizes = [0, 1, 63, 64, 65, 127, 128, 1000, 4096, 5000, 65535, 65536];
        let mut blocks = Vec::new();
        for &s in &sizes {
            let b = pool.allocate(s).expect("allocation within range");
            assert_eq!(b.as_ptr() as usize % 64, 0, "size {s} not 64B aligned");
            assert!(b.capacity() >= s);
            assert!(b.capacity().is_power_of_two() && b.capacity() >= 64);
            blocks.push(b);
        }
        assert_eq!(pool.live_blocks(), sizes.len());
        let fresh_after_first_round = pool.fresh_allocations();
        assert_eq!(fresh_after_first_round, sizes.len());

        for b in blocks.drain(..) {
            pool.deallocate(b);
        }
        assert_eq!(pool.live_blocks(), 0);

        // Reuse: second round must not hit the system allocator at all.
        for _ in 0..100 {
            for &s in &sizes {
                let mut b = pool.allocate(s).unwrap();
                assert_eq!(b.as_ptr() as usize % 64, 0);
                b.as_mut_slice()[0] = 0xAB;
                pool.deallocate(b);
            }
        }
        assert_eq!(pool.fresh_allocations(), fresh_after_first_round);
        assert_eq!(pool.live_blocks(), 0);
    }

    #[test]
    fn test_slab_rejects_oversize() {
        let pool = SlabPool::new();
        assert!(pool.allocate(MAX_BLOCK_SIZE + 1).is_none());
        assert!(pool.allocate(usize::MAX).is_none());
    }

    #[test]
    fn test_slab_concurrent_reuse() {
        let pool = SlabPool::new();
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    for i in 0..5000usize {
                        let mut b = pool.allocate(64 << ((i + t) % 6)).unwrap();
                        assert_eq!(b.as_ptr() as usize % 64, 0);
                        b.as_mut_slice()[0] = t as u8;
                        assert_eq!(b.as_slice()[0], t as u8);
                        pool.deallocate(b);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(pool.live_blocks(), 0);
        // Bounded by peak concurrency, far below total ops (40k).
        assert!(pool.fresh_allocations() <= 8 * 6);
    }
}
