// src/queue.rs

use pgrx::PGRXSharedMemory;

pub struct MpMcQueue<T, const N: usize>(pub heapless::mpmc::MpMcQueue<T, N>);

unsafe impl<T, const N: usize> PGRXSharedMemory for MpMcQueue<T, N> {}

impl<T, const N: usize> Default for MpMcQueue<T, N> {
    fn default() -> Self {
        Self(heapless::mpmc::MpMcQueue::default())
    }
}
