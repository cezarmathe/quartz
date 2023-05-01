// src/workers/bgworker.rs

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::prelude::*;

use tokio::time;
use tokio::time::MissedTickBehavior;

use std::time::Duration;

use super::Worker;

#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_worker_main(arg: pg_sys::Datum) {
    let worker_id = unsafe { i32::from_datum(arg, false) }.unwrap();

    log!("horloge-worker-{}: starting", worker_id);

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        // .event_interval(42)
        .max_blocking_threads(1)
        .build()
        .unwrap();

    runtime.block_on(self::run_worker(Worker::new(worker_id)));
}

async fn run_worker(handle: Worker) {
    // fixme: configurable database connection
    BackgroundWorker::connect_worker_to_spi(Some("horloge"), None);

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
        let event = if let Some(value) = handle.events.dequeue() {
            value
        } else {
            return;
        };

        log!("horloge-worker-{}: got event", handle.worker_id);
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
