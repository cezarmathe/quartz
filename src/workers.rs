// src/worker.rs

use heapless::mpmc::MpMcQueue;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::pg_shmem_init;
use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use pgrx::shmem::*;

use tokio::time;
use tokio::time::MissedTickBehavior;

use std::time::Duration;

use crate::config;
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

type WorkerEventsQueueType = MpMcQueue<WorkerSubsystemEvent, 128>;

/// The shared queue used for communicating events to the workers subsystem.
static WORKER_QUEUE: SharedObject<WorkerEventsQueueType> =
    SharedObject::new("horloge-workers-queue");

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
#[derive(Clone, Copy)]
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

#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_worker_main(arg: pg_sys::Datum) {
    let worker_id = unsafe { i32::from_datum(arg, false) }.unwrap();

    log!("horloge-worker-{}: starting", worker_id);

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(config::SPI_DATABASE_NAME, config::SPI_USER_NAME);

    let mut worker = Worker::new(worker_id, WORKER_QUEUE.get());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        // .event_interval(42)
        .max_blocking_threads(1)
        .build()
        .unwrap();

    runtime.block_on(worker.run())
}

pub(self) struct Worker {
    worker_id: i32,
    queue: &'static WorkerEventsQueueType,
}

impl Worker {
    pub fn new(worker_id: i32, queue: &'static WorkerEventsQueueType) -> Self {
        Self { worker_id, queue }
    }

    async fn run(&mut self) {
        let mut poll_term_interval = time::interval(Duration::from_secs(1));
        poll_term_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut poll_timers_interval = time::interval(Duration::from_millis(1));
        poll_timers_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let on_poll_term_inverval = || {
            if BackgroundWorker::sigterm_received() {
                return false;
            }

            if BackgroundWorker::sighup_received() {
                // on SIGHUP, you might want to reload some external configuration or something
            }

            return true;
        };

        let on_poll_timers_interval = || {
            let event = if let Some(value) = self.queue.dequeue() {
                value
            } else {
                return;
            };

            log!("horloge-worker-{}: got event", self.worker_id);

            use WorkerSubsystemEvent::*;
            match event {
                TimerFired(event) => {
                    log!(
                        "horloge-worker-{}: timer {}:{} fired",
                        self.worker_id,
                        event.table_oid,
                        event.row.id
                    );
                }
            }
        };

        loop {
            tokio::select! {
                _ = poll_term_interval.tick() => {
                    if !on_poll_term_inverval() {
                        break;
                    }
                }
                _ = poll_timers_interval.tick() => {
                    on_poll_timers_interval();
                }
            }
        }

        log!(
            "Background Worker '{}' is exiting",
            BackgroundWorker::get_name()
        );
    }
}
