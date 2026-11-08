//! Disruptor-style SPSC ring buffer.
//!
//! Capacity is a compile-time power of two so slot indexing is a bitmask.
//! The publisher cursor (`head`) and consumer cursor (`tail`) each live on
//! their own 64-byte cacheline (`#[repr(align(64))]`), and each side keeps a
//! local cached copy of the opposing cursor so the shared line is only read
//! when the cached view says full/empty. Consumption is batched: a whole run
//! of available slots is drained and the tail cursor is published once.
//!
//! Single-producer/single-consumer is enforced by the type system: the only
//! way to use the buffer is through the non-`Clone` [`Producer`] and
//! [`Consumer`] handles returned by [`RingBuffer::channel`].

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// An atomic sequence cursor padded to occupy a full cacheline.
#[repr(align(64))]
struct Cursor(AtomicUsize);

/// Shared storage for a fixed-capacity SPSC ring.
pub struct RingBuffer<T, const CAPACITY: usize> {
    /// Next sequence to be published (written by producer only).
    head: Cursor,
    /// Next sequence to be consumed (written by consumer only).
    tail: Cursor,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// SAFETY: access to each slot is serialized by the head/tail cursors with
// Release/Acquire ordering; slots hand values across threads, so T: Send.
unsafe impl<T: Send, const CAPACITY: usize> Send for RingBuffer<T, CAPACITY> {}
unsafe impl<T: Send, const CAPACITY: usize> Sync for RingBuffer<T, CAPACITY> {}

impl<T, const CAPACITY: usize> RingBuffer<T, CAPACITY> {
    const MASK: usize = {
        assert!(
            CAPACITY.is_power_of_two() && CAPACITY >= 2,
            "RingBuffer CAPACITY must be a power of two >= 2"
        );
        CAPACITY - 1
    };

    /// Create a connected producer/consumer pair over a fresh ring.
    pub fn channel() -> (Producer<T, CAPACITY>, Consumer<T, CAPACITY>) {
        let _ = Self::MASK; // force the compile-time capacity check
        let slots = (0..CAPACITY)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let ring = Arc::new(Self {
            head: Cursor(AtomicUsize::new(0)),
            tail: Cursor(AtomicUsize::new(0)),
            slots,
        });
        (
            Producer { ring: Arc::clone(&ring), head: 0, cached_tail: 0 },
            Consumer { ring, tail: 0, cached_head: 0 },
        )
    }

    /// Number of items currently queued (approximate under concurrency).
    pub fn len(&self) -> usize {
        let head = self.head.0.load(Ordering::Acquire);
        let tail = self.tail.0.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub const fn capacity(&self) -> usize {
        CAPACITY
    }
}

impl<T, const CAPACITY: usize> Drop for RingBuffer<T, CAPACITY> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let mut tail = *self.tail.0.get_mut();
        while tail != head {
            // SAFETY: slots in [tail, head) are initialized and unconsumed.
            unsafe { self.slots[tail & Self::MASK].get_mut().assume_init_drop() };
            tail = tail.wrapping_add(1);
        }
    }
}

/// Publishing half. Not `Clone`: exactly one publisher exists.
pub struct Producer<T, const CAPACITY: usize> {
    ring: Arc<RingBuffer<T, CAPACITY>>,
    head: usize,
    cached_tail: usize,
}

impl<T, const CAPACITY: usize> Producer<T, CAPACITY> {
    /// Non-blocking publish. Returns the item back if the ring is full.
    pub fn try_publish(&mut self, item: T) -> Result<(), T> {
        if self.head.wrapping_sub(self.cached_tail) == CAPACITY {
            self.cached_tail = self.ring.tail.0.load(Ordering::Acquire);
            if self.head.wrapping_sub(self.cached_tail) == CAPACITY {
                return Err(item);
            }
        }
        // SAFETY: slot `head` is outside [tail, head) so the consumer will not
        // touch it until we publish the new head below.
        unsafe {
            (*self.ring.slots[self.head & RingBuffer::<T, CAPACITY>::MASK].get()).write(item);
        }
        self.head = self.head.wrapping_add(1);
        self.ring.head.0.store(self.head, Ordering::Release);
        Ok(())
    }

    /// Publish, spinning (then yielding) until space is available.
    pub fn publish_spin(&mut self, mut item: T) {
        let mut spins = 0u32;
        loop {
            match self.try_publish(item) {
                Ok(()) => return,
                Err(back) => item = back,
            }
            spins += 1;
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

/// Consuming half. Not `Clone`: exactly one consumer exists.
pub struct Consumer<T, const CAPACITY: usize> {
    ring: Arc<RingBuffer<T, CAPACITY>>,
    tail: usize,
    cached_head: usize,
}

impl<T, const CAPACITY: usize> Consumer<T, CAPACITY> {
    /// Non-blocking single-item consume.
    pub fn try_consume(&mut self) -> Option<T> {
        let mut out = None;
        self.consume_batch(1, |item| out = Some(item));
        out
    }

    /// Drain up to `max` available items, invoking `f` on each in order, and
    /// publish the consumer cursor once. Returns the number consumed.
    pub fn consume_batch(&mut self, max: usize, mut f: impl FnMut(T)) -> usize {
        if self.cached_head == self.tail {
            self.cached_head = self.ring.head.0.load(Ordering::Acquire);
        }
        let n = self.cached_head.wrapping_sub(self.tail).min(max);
        if n == 0 {
            return 0;
        }

        // Publishes progress even if `f` panics, so consumed slots are never
        // dropped twice by the ring's destructor.
        struct Commit<'a, T, const C: usize> {
            consumer: &'a mut Consumer<T, C>,
            done: usize,
        }
        impl<T, const C: usize> Drop for Commit<'_, T, C> {
            fn drop(&mut self) {
                self.consumer.tail = self.consumer.tail.wrapping_add(self.done);
                self.consumer.ring.tail.0.store(self.consumer.tail, Ordering::Release);
            }
        }

        let mut commit = Commit { consumer: self, done: 0 };
        for _ in 0..n {
            let seq = commit.consumer.tail.wrapping_add(commit.done);
            // SAFETY: seq is in [tail, head); the Acquire load of head made
            // the producer's write visible, and the producer won't reuse the
            // slot until we publish tail.
            let item = unsafe {
                (*commit.consumer.ring.slots[seq & RingBuffer::<T, CAPACITY>::MASK].get()).assume_init_read()
            };
            commit.done += 1;
            f(item);
        }
        n
    }

    /// Number of items currently queued.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_cursor_cacheline_padding() {
        assert_eq!(std::mem::align_of::<Cursor>(), 64);
        assert_eq!(std::mem::size_of::<Cursor>(), 64);
        let r = RingBuffer::<u64, 8> {
            head: Cursor(AtomicUsize::new(0)),
            tail: Cursor(AtomicUsize::new(0)),
            slots: Box::new([]),
        };
        let h = &r.head as *const _ as usize;
        let t = &r.tail as *const _ as usize;
        assert!(h.abs_diff(t) >= 64, "cursors share a cacheline");
        assert_eq!(h % 64, 0);
        assert_eq!(t % 64, 0);
    }

    #[test]
    fn test_full_and_empty() {
        let (mut p, mut c) = RingBuffer::<u32, 4>::channel();
        assert!(c.try_consume().is_none());
        for i in 0..4 {
            assert!(p.try_publish(i).is_ok());
        }
        assert_eq!(p.try_publish(99), Err(99));
        assert_eq!(c.try_consume(), Some(0));
        assert!(p.try_publish(4).is_ok());
        let mut got = Vec::new();
        // The consumer's cached view of head may be stale; drain until empty.
        while c.consume_batch(10, |x| got.push(x)) > 0 {}
        assert_eq!(got, vec![1, 2, 3, 4]);
        assert!(c.is_empty());
    }

    #[test]
    fn test_drops_unconsumed_items() {
        let marker = Arc::new(());
        {
            let (mut p, mut c) = RingBuffer::<Arc<()>, 8>::channel();
            for _ in 0..5 {
                p.try_publish(Arc::clone(&marker)).unwrap();
            }
            c.try_consume();
            assert_eq!(Arc::strong_count(&marker), 5);
        }
        assert_eq!(Arc::strong_count(&marker), 1);
    }

    #[test]
    fn test_ring_buffer_spsc_throughput() {
        const N: u64 = 2_000_000;
        let (mut p, mut c) = RingBuffer::<u64, 1024>::channel();
        let start = std::time::Instant::now();
        let producer = std::thread::spawn(move || {
            for i in 0..N {
                p.publish_spin(i);
            }
        });
        let consumer = std::thread::spawn(move || {
            let mut next = 0u64;
            while next < N {
                let n = c.consume_batch(256, |v| {
                    assert_eq!(v, next, "out-of-order or lost item");
                    next += 1;
                });
                if n == 0 {
                    std::hint::spin_loop();
                }
            }
            next
        });
        producer.join().unwrap();
        assert_eq!(consumer.join().unwrap(), N);
        let elapsed = start.elapsed();
        eprintln!("spsc: {N} msgs in {elapsed:?} ({:.1} Mmsg/s)", N as f64 / elapsed.as_secs_f64() / 1e6);
    }
}
