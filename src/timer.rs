// src/timer.rs

use chrono::prelude::*;
use heapless::mpmc::MpMcQueue;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::pg_shmem_init;
use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use pgrx::shmem::*;

use tokio::time;
use tokio::time::MissedTickBehavior;

use std::time::Duration as StdDuration;

use crate::shmem::SharedObject;
use crate::types::*;

/// Initialize the timer subsystem.
pub fn pg_init() {
    log!("horloge-timer: pg_init");

    pg_shmem_init!(TIMER_EVENTS_QUEUE);

    BackgroundWorkerBuilder::new("horloge-timer")
        .set_library("horloge")
        .set_function("horloge_timer_main")
        .set_argument(0.into_datum()) // can we use this for something?
        .set_type("horloge-timer")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(StdDuration::from_secs(1).into())
        .enable_shmem_access(None)
        .enable_spi_access() // fixme: use me
        .load();
}

/// The type of the queue of events that will be processed by the timer.
type TimerEventsQueueType = MpMcQueue<TimerSubsystemEvent, 128>;

/// The queue of events that will be processed by the timer subsystem.
static TIMER_EVENTS_QUEUE: SharedObject<TimerEventsQueueType> =
    SharedObject::new("horloge-timer-create-timer-queue");

/// Events that can be consumed by the timer subsystem.
pub enum TimerSubsystemEvent {
    /// Create a new timer.
    CreateTimer {
        /// The OID of the table that the timer is associated with.
        table_oid: Oid,
        /// The row that was inserted into the table.
        table_row: CreateTimerFromRow,
    },
}

/// The timer handle is a handle for interacting with the timer subsystem.
///
/// TimerHandle is not usable prior to the initialization of the timer
/// subsystem.
pub struct TimerHandle;

impl TimerHandle {
    /// Enqueue an event to be processed by the timer subsystem.
    ///
    /// This function will loop a few times if the queue is full, to give the
    /// background worker a chance to dequeue some events.
    ///
    /// Returns the ID of the timer if the event was successfully enqueued.
    pub fn enqueue_event(mut event: TimerSubsystemEvent) -> bool {
        const LOOPS: usize = 64;

        for _ in 0..LOOPS {
            event = match TIMER_EVENTS_QUEUE.get().enqueue(event) {
                Ok(_) => return true,
                Err(value) => value,
            };
        }

        false
    }
}

/// Main function of the timer subsystem.
///
/// This sets up the BackgroundWorker, the tokio runtime and then creates a new
/// Timer instance and blocks on it's run method.
#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_timer_main(_arg: pg_sys::Datum) {
    log!("horloge-timer: starting");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let mut timer = Timer::new(TIMER_EVENTS_QUEUE.get());
    // fixme: load existing timers

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .event_interval(61) // default - might want to optimize
        .max_blocking_threads(1)
        .build()
        .unwrap();

    runtime.block_on(timer.run());

    log!("horloge-timer: bye bye");
}

/// The timer subsystem.
struct Timer {
    queue: &'static TimerEventsQueueType,
}

impl Timer {
    /// Create a new timer subsystem.
    fn new(queue: &'static TimerEventsQueueType) -> Self {
        Self { queue }
    }

    /// Run the timer subsystem.
    async fn run(&mut self) {
        let mut poll_term_interval = time::interval(StdDuration::from_secs(1));
        poll_term_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut poll_timers_interval = time::interval(StdDuration::from_millis(1));
        poll_timers_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let on_poll_term_interval = || {
            if BackgroundWorker::sigterm_received() {
                return false;
            }

            if BackgroundWorker::sighup_received() {
                // on SIGHUP, you might want to reload some external configuration or something
            }

            true
        };

        let on_poll_timers_interval = || {
            loop {
                let (table_oid, table_row) = match self.queue.dequeue() {
                    Some(TimerSubsystemEvent::CreateTimer { table_oid, table_row }) => (
                        table_oid,
                        table_row,
                    ),
                    Some(_) => todo!("handle other events"),
                    None => break,
                };

                let duration = table_row.expires_at - Local::now();

                // todo: get the abort handle and store it somewhere
                tokio::spawn(async move {
                    time::sleep(duration.to_std().unwrap()).await;

                    log!("timer {} fired", table_row.id);
                });
            }

            true
        };

        loop {
            tokio::select! {
                _ = poll_term_interval.tick() => {
                    if !on_poll_term_interval() {
                        break;
                    }
                }
                _ = poll_timers_interval.tick() => {
                    if !on_poll_timers_interval() {
                        break;
                    }
                }
            }
        }
    }
}
