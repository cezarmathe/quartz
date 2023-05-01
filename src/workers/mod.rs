// src/worker.rs

mod bgworker;

use heapless::mpmc::MpMcQueue;

use once_cell::sync::OnceCell;
use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::pg_shmem_init;
use pgrx::prelude::*;
use pgrx::shmem::*;

use crate::shmem::SharedObject;

static WORKER_QUEUE: SharedObject<MpMcQueue<(), 128>> = SharedObject::new("horloge-workers-queue");

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
        BackgroundWorkerBuilder::new(format!("horloge-worker-{}", worker_count).as_str())
            .set_function("horloge_worker_main")
            .set_argument((i as i32).into_datum()) // worker ID
            .set_library("horloge")
            .set_type("horloge-worker")
            .enable_shmem_access(None)
            .enable_spi_access()
            .load();
    }
}

pub struct WorkersHandle;

impl WorkersHandle {
    pub fn enqueue_event() -> bool {
        WORKER_QUEUE.get().enqueue(()).is_ok()
    }
}

pub(self) struct Worker {
    worker_id: i32,
    queue: &'static SharedObject<MpMcQueue<(), 128>>,
}

impl Worker {
    pub fn new(worker_id: i32) -> Self {
        Self {
            worker_id,
            queue: &WORKER_QUEUE,
        }
    }

    pub fn dequeue_event(&self) -> Option<()> {
        self.queue.get().dequeue()
    }
}
