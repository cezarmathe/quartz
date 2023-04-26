// src/worker.rs

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::prelude::*;

use tokio::time;
use tokio::time::MissedTickBehavior;

use std::time::Duration;

const WORKERS_COUNT: i32 = 1;

pub(crate) fn init() {
    for i in 0..WORKERS_COUNT {
        BackgroundWorkerBuilder::new(format!("horloge-{}", i).as_str())
            .set_function("horloge_worker_main")
            .set_argument(i.into_datum()) // worker ID
            .set_library("horloge")
            .enable_spi_access()
            .load();
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_worker_main(arg: pg_sys::Datum) {
    let worker_id = unsafe { i32::from_datum(arg, false) }.unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        // .event_interval(42)
        .max_blocking_threads(1)
        .build()
        .unwrap();

    runtime.block_on(self::run_worker(worker_id));
}

async fn run_worker(worker_id: i32) {
    // these are the signals we want to receive.  If we don't attach the SIGTERM handler, then
    // we'll never be able to exit via an external notification
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // fixme: configurable
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    log!(
        "Background Worker '{}' is starting. ID = {}",
        BackgroundWorker::get_name(),
        worker_id,
    );

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
        // within a transaction, execute an SQL statement, and log its results
        BackgroundWorker::transaction(|| {
            Spi::connect(|client| {
                let tuple_table = client.select("select localtimestamp", None, None).unwrap();
                tuple_table.for_each(|tuple| {
                    let ts = tuple
                        .get_datum_by_ordinal(1)
                        .unwrap()
                        .value::<Timestamp>()
                        .unwrap()
                        .unwrap();
                    // log!("Tick: ({})", Into::<i64>::into(ts));
                });
            });
        });
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
