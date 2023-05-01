// src/timer.rs

mod bgworker;
mod types;

use heapless::mpmc::MpMcQueue;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::pg_shmem_init;
use pgrx::prelude::*;
use pgrx::shmem::*;

use crate::shmem::SharedObject;

pub use self::types::*;

static CREATE_TIMERS_QUEUE: SharedObject<MpMcQueue<CreateTimer, 128>> = SharedObject::new("horloge-timer-create-timer-queue");

/// The timer handle is used to interact with the timer module via a single
/// point of entry.
pub struct TimerHandle;

impl TimerHandle {
    /// Enqueue the creation of a new entry.
    ///
    /// This function will loop a few times if the queue is full, to give the
    /// background worker a chance to dequeue some events.
    ///
    /// Returns the ID of the timer if the event was successfully enqueued.
    pub fn enqueue_create_timer(event: CreateTimer) -> Option<i64> {
        let id = event.id;

        const LOOPS: usize = 64;

        for _ in 0..LOOPS {
            if CREATE_TIMERS_QUEUE.get().enqueue(event).is_ok() {
                return Some(id);
            }
        }

        None
    }

    pub(self) fn dequeue_create_timer() -> Option<CreateTimer> {
        CREATE_TIMERS_QUEUE.get().dequeue()
    }
}

pub fn pg_init() {
    log!("horloge-timer: pg_init");

    pg_shmem_init!(CREATE_TIMERS_QUEUE);

    BackgroundWorkerBuilder::new("horloge-timer")
        .set_library("horloge")
        .set_function("horloge_timer_main")
        .set_argument(0.into_datum()) // can we use this for something?
        .set_type("horloge-timer")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        // .set_restart_time(StdDuration::from_secs(1).into())
        .enable_shmem_access(None)
        .load();
}
