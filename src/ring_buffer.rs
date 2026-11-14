//! Lock-free SPSC ring buffer for passing ticks between two threads.
//!
//! Exactly one producer thread may call [`SpscRingBuffer::push`] and exactly
//! one consumer thread may call [`SpscRingBuffer::pop`]. The indices are
//! published with release stores and observed with acquire loads, so a slot
//! write happens-before the matching read, and a slot is not reused until the
//! consumer has finished reading it.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Atomic index stored on its own cache line so the producer and the consumer
/// do not false-share.
#[repr(align(64))]
struct CacheLineAligned {
    index: AtomicUsize,
}

impl CacheLineAligned {
    fn new(index: usize) -> Self {
        Self {
            index: AtomicUsize::new(index),
        }
    }
}

/// Fixed-capacity single-producer single-consumer queue.
///
/// `N` is the number of slots and must be a power of two. Indices are free-running
/// counters; the slot is selected with a bitmask, so empty (`head == tail`) and
/// full (`head - tail == N`) stay distinct after wraparound.
pub struct SpscRingBuffer<T, const N: usize> {
    head: CacheLineAligned,
    tail: CacheLineAligned,
    slots: [UnsafeCell<MaybeUninit<T>>; N],
}

impl<T, const N: usize> SpscRingBuffer<T, N> {
    /// Creates an empty buffer.
    ///
    /// # Panics
    ///
    /// Fails to compile when `N` is not a power of two. Zero is rejected because
    /// it is not a power of two and a buffer with no slots cannot make progress.
    pub fn new() -> Self {
        const {
            assert!(
                N.is_power_of_two(),
                "SpscRingBuffer capacity N must be a power of two"
            );
        }

        Self {
            head: CacheLineAligned::new(0),
            tail: CacheLineAligned::new(0),
            slots: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
        }
    }

    /// Number of slots.
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Pushes `value` into the next free slot.
    ///
    /// Returns `Err(value)` when every slot is occupied. The caller keeps
    /// ownership and may retry. Only the producer thread may call this method.
    pub fn push(&self, value: T) -> Result<(), T> {
        let head = self.head.index.load(Ordering::Acquire);
        let tail = self.tail.index.load(Ordering::Acquire);

        if head.wrapping_sub(tail) == N {
            return Err(value);
        }

        let slot = head & (N - 1);
        // SAFETY: this slot is free. The consumer release-stores `tail` only
        // after it has finished reading the previous value, and the acquire
        // load above observed that store. The producer is the sole writer.
        unsafe {
            (*self.slots[slot].get()).write(value);
        }

        // Publish the slot. A consumer acquire-load of `head` cannot see this
        // index until the write above is visible.
        self.head.index.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pops the oldest value.
    ///
    /// Returns `None` when the buffer is empty. Only the consumer thread may
    /// call this method.
    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.index.load(Ordering::Acquire);
        let head = self.head.index.load(Ordering::Acquire);

        if tail == head {
            return None;
        }

        let slot = tail & (N - 1);
        // SAFETY: the producer release-stores `head` only after initializing
        // this slot, and the acquire load above observed that store. The
        // consumer is the sole reader and takes ownership of `T`.
        let value = unsafe { (*self.slots[slot].get()).assume_init_read() };

        // Publish that the slot may be reused. A producer acquire-load of
        // `tail` cannot pass this store and overwrite the value early.
        self.tail.index.store(tail.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}

impl<T, const N: usize> Default for SpscRingBuffer<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> Drop for SpscRingBuffer<T, N> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

// SAFETY: `T` moves from the producer thread to the consumer thread. Shared
// access is coordinated by the head/tail protocol; each value is owned by one
// thread at a time, so `T: Sync` is not required.
unsafe impl<T: Send, const N: usize> Send for SpscRingBuffer<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for SpscRingBuffer<T, N> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn push_pop_preserves_order_and_reports_full_and_empty() {
        let buffer = SpscRingBuffer::<i32, 4>::new();

        assert_eq!(buffer.capacity(), 4);
        assert_eq!(buffer.pop(), None);

        assert!(buffer.push(10).is_ok());
        assert!(buffer.push(20).is_ok());
        assert!(buffer.push(30).is_ok());
        assert!(buffer.push(40).is_ok());
        assert_eq!(buffer.push(50), Err(50));

        assert_eq!(buffer.pop(), Some(10));
        assert!(buffer.push(50).is_ok());

        assert_eq!(buffer.pop(), Some(20));
        assert_eq!(buffer.pop(), Some(30));
        assert_eq!(buffer.pop(), Some(40));
        assert_eq!(buffer.pop(), Some(50));
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn indices_wrap_around_the_capacity() {
        let buffer = SpscRingBuffer::<u32, 2>::new();

        for expected in 0..10_000 {
            buffer.push(expected).unwrap();
            assert_eq!(buffer.pop(), Some(expected));
        }

        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn owned_values_are_moved_to_the_consumer() {
        let buffer = SpscRingBuffer::<String, 8>::new();
        buffer.push("bid".to_owned()).unwrap();
        buffer.push("ask".to_owned()).unwrap();

        assert_eq!(buffer.pop().as_deref(), Some("bid"));
        assert_eq!(buffer.pop().as_deref(), Some("ask"));
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn head_and_tail_occupy_distinct_cache_lines() {
        let buffer = SpscRingBuffer::<u8, 2>::new();
        let head = &buffer.head as *const CacheLineAligned as usize;
        let tail = &buffer.tail as *const CacheLineAligned as usize;

        assert_eq!(std::mem::align_of::<CacheLineAligned>(), 64);
        assert_eq!(std::mem::size_of::<CacheLineAligned>(), 64);
        assert_eq!(head % 64, 0);
        assert_eq!(tail % 64, 0);
        assert!(head.abs_diff(tail) >= 64);
    }

    #[test]
    fn producer_and_consumer_threads_transfer_every_tick_in_order() {
        const COUNT: u64 = 100_000;
        let buffer = Arc::new(SpscRingBuffer::<u64, 1024>::new());

        let producer = {
            let buffer = Arc::clone(&buffer);
            thread::spawn(move || {
                for tick in 0..COUNT {
                    let mut pending = tick;
                    loop {
                        match buffer.push(pending) {
                            Ok(()) => break,
                            Err(returned) => {
                                pending = returned;
                                std::hint::spin_loop();
                            }
                        }
                    }
                }
            })
        };

        let consumer = {
            let buffer = Arc::clone(&buffer);
            thread::spawn(move || {
                let mut received = Vec::with_capacity(COUNT as usize);
                while (received.len() as u64) < COUNT {
                    match buffer.pop() {
                        Some(tick) => received.push(tick),
                        None => std::hint::spin_loop(),
                    }
                }
                received
            })
        };

        producer.join().expect("producer thread");
        let received = consumer.join().expect("consumer thread");

        assert_eq!(received.len(), COUNT as usize);
        for (index, tick) in received.into_iter().enumerate() {
            assert_eq!(tick, index as u64);
        }
        assert_eq!(buffer.pop(), None);
    }
}
