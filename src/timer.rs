// src/timer.rs

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

#[derive(Copy, Clone)]
pub struct CreateTimer {
    pub id: i64, // 8 bytes
    pub ts: chrono::DateTime<Utc>, // 12 bytes

    // { NaiveDateTime { NaiveDate (i32 -> 4 bytes), NaiveTime { secs (u32 -> 4 bytes), frac (u32 -> 4 bytes) } }, Utc (0) }
}

static QUEUE: PgLwLock<heapless::Vec<CreateTimer, 128>> = PgLwLock::new();

pub fn pg_init() {
    log!("horloge-timer: pg_init");

    pg_shmem_init!(QUEUE);

    BackgroundWorkerBuilder::new("horloge-timer")
        .set_library("horloge")
        .set_function("horloge_timer_main")
        .set_argument(0.into_datum()) // can we use this for something?
        .set_type("horloge-timer")
        .set_start_time(BgWorkerStartTime::PostmasterStart)
        // .set_restart_time(StdDuration::from_secs(1).into())
        .enable_shmem_access(None)
        .load();
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C" fn horloge_timer_on_shmem_access() {
    log!("horloge-timer: shmem access");
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_timer_main(_arg: pg_sys::Datum) {
    log!("hello, this is horloge-timer");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .event_interval(61) // default - might want to optimize
        .max_blocking_threads(1)
        .build()
        .unwrap();

    runtime.block_on(self::run_timer());

    log!("bye bye");
}

async fn run_timer() {
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
        let mut q = QUEUE.exclusive();

        loop {
            let event = match q.pop() {
                Some(event) => event,
                None => break
            };

            let duration = event.ts - Utc::now();

            // todo: get the abort handle and store it somewhere
            tokio::spawn(async move {
                time::sleep(duration.to_std().unwrap()).await;

                log!("timer {} fired", event.id);
            });
        }


        true
    };

    loop {
        tokio::select! {
            _ = poll_term_interval.tick() => {
                if !on_poll_term_inverval() {
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

pub(crate) fn enqueue(event: CreateTimer) {
    const LOOPS: usize = 64;

    for _ in 0..LOOPS {
        if QUEUE.exclusive().push(event).is_ok() {
            return;
        }
    }
}
