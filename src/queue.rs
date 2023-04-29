// src/queue.rs

use pgrx::PGRXSharedMemory;

use std::ops::Deref;
use std::ops::DerefMut;

// MpMpQueue is a wrapper around heapless::mpmc::MpMcQueue that implements
// PGRXSharedMemory. This allows us to use it as a static variable in a
// PgLwLock.
pub struct MpMcQueue<T, const N: usize>(heapless::mpmc::MpMcQueue<T, N>);

unsafe impl<T, const N: usize> PGRXSharedMemory for MpMcQueue<T, N> {}

impl<T, const N: usize> Default for MpMcQueue<T, N> {
    fn default() -> Self {
        Self(heapless::mpmc::MpMcQueue::default())
    }
}

impl<T, const N: usize> Deref for MpMcQueue<T, N> {
    type Target = heapless::mpmc::MpMcQueue<T, N>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, const N: usize> DerefMut for MpMcQueue<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
