// src/timer.rs

use chrono::Duration;
use chrono::prelude::*;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::pg_shmem_init;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::PgLwLock;

use tokio::time;
use tokio::time::MissedTickBehavior;

use std::time::Duration as StdDuration;

use crate::queue::MpMcQueue;

#[derive(Copy, Clone)]
pub struct CreateTimer {
    pub id: i64,
    pub ts: chrono::DateTime<Utc>,
}

static QUEUE: PgLwLock<MpMcQueue<CreateTimer, 1_048_576>> = PgLwLock::new();

pub fn pg_init() {
    pg_shmem_init!(QUEUE);

    BackgroundWorkerBuilder::new("horloge-timer")
        .set_function("horloge_worker_main")
        .set_argument(0.into_datum()) // can we use this for something?
        .set_library("horloge")
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_timer_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        // .event_interval(42)
        .max_blocking_threads(1)
        .build()
        .unwrap();

    runtime.block_on(self::run_timer());
}

async fn run_timer() {
    log!("hello, this is horloge-timer");

    let mut poll_term_interval = time::interval(StdDuration::from_secs(1));
    poll_term_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut poll_timers_interval = time::interval(StdDuration::from_millis(1));
    poll_timers_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let on_poll_term_inverval = || {
        if BackgroundWorker::sigterm_received() {
            return false;
        }

        if BackgroundWorker::sighup_received() {
            // on SIGHUP, you might want to reload some external configuration or something
        }

        true
    };

    let on_poll_timers_interval = || {
        let event = match QUEUE.share().0.dequeue() {
            Some(event) => event,
            None => return true,
        };

        let duration = event.ts - Utc::now();

        tokio::spawn(async move {
            time::sleep(duration.to_std().unwrap()).await;

            log!("timer {} fired", event.id);
        });

        true
    };
}
