// src/timer/bgworker.rs

use chrono::prelude::*;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::prelude::*;

use tokio::time;
use tokio::time::MissedTickBehavior;

use std::time::Duration as StdDuration;

use crate::workers::WorkersHandle;

use super::TimerHandle;

#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_timer_main(_arg: pg_sys::Datum) {
    log!("hello, this is horloge-timer");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // fixme: load existing timers

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
            if !WorkersHandle::enqueue_event() { // fixme: remove this
                info!("too many events sent");
            }

            let event = match TimerHandle::dequeue_create_timer() {
                Some(event) => event,
                None => break,
            };

            let duration = event.expires_at - Local::now();

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
