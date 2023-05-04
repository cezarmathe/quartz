// src/timer.rs

use chrono::prelude::*;
use heapless::mpmc::MpMcQueue;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::pg_shmem_init;
use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use pgrx::shmem::*;

use pgrx::spi::Error as SpiError;
use tokio::task::AbortHandle;
use tokio::time;
use tokio::time::MissedTickBehavior;

use std::collections::HashMap;
use std::time::Duration as StdDuration;

use crate::commands;
use crate::commands::TimerTableData;
use crate::config;
use crate::shmem::SharedObject;
use crate::types::*;
use crate::workers::TimerFiredEvent;
use crate::workers::WorkerSubsystemEvent;
use crate::workers::WorkersHandle;

/// Initialize the timer subsystem.
pub fn pg_init() {
    log!("quartz-timer: pg_init");

    pg_shmem_init!(TIMER_EVENTS_QUEUE);

    BackgroundWorkerBuilder::new("quartz-timer")
        .set_library("quartz")
        .set_function("quartz_timer_main")
        .set_argument(0.into_datum()) // can we use this for something?
        .set_type("quartz-timer")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(StdDuration::from_secs(1).into())
        .enable_shmem_access(None)
        .enable_spi_access()
        .load();
}

/// The type of the queue of events that will be processed by the timer.
type TimerEventsQueueType = MpMcQueue<TimerSubsystemEvent, 128>;

/// The queue of events that will be processed by the timer subsystem.
static TIMER_EVENTS_QUEUE: SharedObject<TimerEventsQueueType> =
    SharedObject::new("quartz-timer-create-timer-queue");

/// Events that can be consumed by the timer subsystem.
pub enum TimerSubsystemEvent {
    /// Create a new timer.
    CreateTimer {
        /// The OID of the table that the timer is associated with.
        table_oid: Oid,
        /// The row that was inserted into the table.
        table_row: CreateTimerFromRow,
    },
    /// Process the expiration of a timer.
    ExpireTimer {
        /// The OID of the table that the timer is associated with.
        table_oid: Oid,
        /// The ID of the timer that should be expired.
        timer_id: i64,
    },
    /// Track a new timers table.
    TrackTimersTable {
        /// The OID of the table that should be tracked.
        table_oid: Oid,
    },
    /// Untrack a timers table.
    UntrackTimersTable {
        /// The OID of the table that should be untracked.
        table_oid: Oid,
    },
}

/// The timer handle is a handle for interacting with the timer subsystem.
///
/// TimerHandle is not usable prior to the initialization of the timer
/// subsystem.
#[derive(Clone, Copy)]
pub struct TimerHandle(());

impl TimerHandle {
    /// Create a new timer handle.
    pub fn get() -> Self {
        Self(())
    }

    /// Enqueue an event to be processed by the timer subsystem.
    ///
    /// This function will loop a few times if the queue is full, to give the
    /// background worker a chance to dequeue some events.
    ///
    /// Returns the ID of the timer if the event was successfully enqueued.
    pub fn enqueue_event(&self, mut event: TimerSubsystemEvent) -> bool {
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
pub extern "C" fn quartz_timer_main(_arg: pg_sys::Datum) {
    log!("quartz-timer: starting");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(config::SPI_DATABASE_NAME, config::SPI_USER_NAME);

    let mut timer = Timer::new(TIMER_EVENTS_QUEUE.get());

    if let Err(e) = timer.initialize_extension() {
        error!("quartz-timer: failed to initialize schema: {}", e);
    }

    if let Err(e) = timer.initialize() {
        error!("quartz-timer: failed to initialize: {}", e);
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .event_interval(61) // default - might want to optimize
        .max_blocking_threads(1)
        .build()
        .unwrap();

    runtime.block_on(timer.run());

    log!("quartz-timer: bye bye");
}

/// A timer entry is an entry in the timer subsystem that tracks the time for
/// a row in a table, as indicated by the table's OID and the row's ID.
struct TimerEntry {
    oid: Oid,
    row: TimerRow,
    handle: AbortHandle,
}

/// The timer subsystem.
struct Timer {
    queue: &'static TimerEventsQueueType,
    timer_handle: TimerHandle,
    timers: HashMap<Oid, HashMap<i64, TimerEntry>>,
    workers_handle: WorkersHandle,
}

impl Timer {
    /// Create a new timer subsystem.
    fn new(queue: &'static TimerEventsQueueType) -> Self {
        Self {
            queue,
            timer_handle: TimerHandle::get(),
            timers: HashMap::new(),
            workers_handle: WorkersHandle::get(),
        }
    }

    /// Initialize the schema for the timer subsystem.
    fn initialize_extension(&mut self) -> Result<(), SpiError> {
        BackgroundWorker::transaction(|| {
            Spi::connect(|mut client| {
                client.update("create extension if not exists quartz", None, None)
            })
            .map(|_| ())
        })
    }

    fn initialize(&mut self) -> Result<(), SpiError> {
        let timer_handle = self.timer_handle.clone();

        BackgroundWorker::transaction(|| {
            let timer_tables = Spi::connect(|client| commands::find_timer_tables(&client))?;

            warning!("quartz-timer: found {} timer tables", timer_tables.len());

            // fixme: this solution can potentially fail if there are too
            // many timers that need loading, because the queue is quite small.
            for timer_table in timer_tables {
                let TimerTableData {
                    relid,
                    schema,
                    table,
                } = timer_table;

                if !timer_handle
                    .enqueue_event(TimerSubsystemEvent::TrackTimersTable { table_oid: relid })
                {
                    error!("failed to enqueue event");
                }

                let timers = Spi::connect(|client| {
                    commands::find_timers_in_table(&client, schema.as_str(), table.as_str())
                })?;

                warning!(
                    "quartz-timer: found {} timers in table {}",
                    timers.len(),
                    table
                );

                for timer in timers {
                    if timer.fired_at.is_some() {
                        continue;
                    }

                    if !timer_handle.enqueue_event(TimerSubsystemEvent::CreateTimer {
                        table_oid: relid,
                        table_row: timer.into(),
                    }) {
                        error!("failed to enqueue event");
                    }
                }
            }

            Ok(())
        })
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

        let mut on_poll_timers_interval = || {
            loop {
                match self.queue.dequeue() {
                    Some(event) => {
                        if !self.process_event(event) {
                            return false;
                        }
                    }
                    None => break,
                };
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

    fn process_event(&mut self, event: TimerSubsystemEvent) -> bool {
        match event {
            TimerSubsystemEvent::CreateTimer {
                table_oid,
                table_row,
            } => {
                self.create_timer(table_oid, table_row);
            }
            TimerSubsystemEvent::ExpireTimer {
                table_oid,
                timer_id,
            } => {
                self.expire_timer(table_oid, timer_id);
            }
            TimerSubsystemEvent::TrackTimersTable { table_oid } => {
                self.track_timers_table(table_oid);
            }
            TimerSubsystemEvent::UntrackTimersTable { table_oid } => {
                self.untrack_timers_table(table_oid);
            }
        }

        true
    }

    fn create_timer(&mut self, table_oid: Oid, row: CreateTimerFromRow) {
        let scoped_timers = if let Some(value) = self.timers.get_mut(&table_oid) {
            value
        } else {
            warning!(
                "quartz-timer: failed to create timer {} ({}): table is not tracked",
                row.id,
                table_oid
            );

            return;
        };

        if scoped_timers.contains_key(&row.id) {
            warning!(
                "quartz-timer: timer {} ({}) is already tracked",
                row.id,
                table_oid
            );

            return;
        }

        let timer_handle = self.timer_handle;

        let row: TimerRow = row.into();
        let row_id = row.id;
        let table_oid = table_oid;

        let handle = tokio::spawn(async move {
            let now = Local::now();

            if now <= row.expires_at {
                let duration = row.expires_at - now;

                log!(
                    "quartz-timer: timer {} ({}) is due in {}",
                    row_id,
                    table_oid,
                    duration,
                );

                time::sleep(duration.to_std().unwrap()).await;
            } else {
                log!(
                    "quartz-timer: timer {} ({}) is already expired",
                    row_id,
                    table_oid,
                );
            }

            timer_handle.enqueue_event(TimerSubsystemEvent::ExpireTimer {
                table_oid,
                timer_id: row.id,
            });
        })
        .abort_handle();

        scoped_timers.insert(
            row.id,
            TimerEntry {
                oid: table_oid,
                row,
                handle,
            },
        );
    }

    fn expire_timer(&mut self, oid: Oid, id: i64) {
        let scoped_timers = self.timers.entry(oid).or_insert_with(Default::default);

        if let Some(entry) = scoped_timers.remove(&id) {
            let event = TimerFiredEvent {
                table_oid: entry.oid,
                row: entry.row,
            };

            self.workers_handle
                .enqueue_event(WorkerSubsystemEvent::TimerFired(event));
        } else {
            error!("timer {} does not exist", id)
        }
    }

    fn track_timers_table(&mut self, oid: Oid) {
        if self.timers.contains_key(&oid) {
            warning!("table {} is already tracked", oid);

            return;
        }

        self.timers.insert(oid, Default::default());

        info!("table {} is now tracked", oid)
    }

    fn untrack_timers_table(&mut self, oid: Oid) {
        let mut scoped_timers = if let Some(value) = self.timers.remove(&oid) {
            value
        } else {
            warning!("table {} is not tracked", oid);

            return;
        };

        for (_, entry) in scoped_timers.drain() {
            entry.handle.abort();
        }
    }
}
