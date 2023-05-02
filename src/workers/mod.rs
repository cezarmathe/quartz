// src/worker.rs

mod bgworker;

use heapless::mpmc::MpMcQueue;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::pg_shmem_init;
use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use pgrx::shmem::*;

use crate::shmem::SharedObject;
use crate::types::TimerRow;

/// Initialize the workers subsystem.
pub(crate) fn pg_init() {
    log!("horloge-workers: pg_init");

    pg_shmem_init!(WORKER_QUEUE);

    let worker_count = match std::thread::available_parallelism() {
        Ok(value) => value.get() / 2, // fixme??
        Err(e) => {
            warning!("horloge-workers: pg_init failed to determine available parallelism (error: {}), using 1", e);

            1
        }
    };

    for i in 0..worker_count {
        BackgroundWorkerBuilder::new(format!("horloge-worker-{}", i).as_str())
            .set_function("horloge_worker_main")
            .set_argument((i as i32).into_datum()) // worker ID
            .set_library("horloge")
            .set_type("horloge-worker")
            .enable_shmem_access(None)
            .enable_spi_access()
            .load();
    }
}

/// The shared queue used for communicating events to the workers subsystem.
static WORKER_QUEUE: SharedObject<MpMcQueue<WorkerSubsystemEvent, 128>> = SharedObject::new("horloge-workers-queue");

/// WorkerEvent is an event that can be sent to the workers subsystem.
pub enum WorkerSubsystemEvent {
    TimerFired(TimerFiredEvent),
}

/// TimerFiredEvent is an event that is sent to a worker when a timer fires.
pub struct TimerFiredEvent {
    pub table_oid: Oid,
    pub row: TimerRow,
}

/// WorkersHandle is a handle used for interacting with the workers subsystem.
///
/// WorkersHandle is not usable prior to the initialization of the workers
/// subsystem.
pub struct WorkersHandle(());

impl WorkersHandle {
    pub fn get() -> Self {
        Self(())
    }

    /// Enqueue an event to be processed by the workers subsystem.
    pub fn enqueue_event(&self, event: WorkerSubsystemEvent) -> bool {
        WORKER_QUEUE.get().enqueue(event).is_ok()
    }
}

pub(self) struct Worker {
    worker_id: i32,
    events: &'static MpMcQueue<WorkerSubsystemEvent, 128>,
}

impl Worker {
    pub fn new(worker_id: i32) -> Self {
        Self {
            worker_id,
            events: WORKER_QUEUE.get()
        }
    }

    async fn main(self) {}
}
