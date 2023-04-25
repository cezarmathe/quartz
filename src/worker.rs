// src/worker.rs

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::prelude::*;

use std::time::Duration;

const WORKERS_COUNT: i32 = 1;

pub(crate) fn init() {
    for i in 0..WORKERS_COUNT {
        // create a new worker
        BackgroundWorkerBuilder::new(format!("horloge-{}", i).as_str())
            // set the function to run
            .set_function("horloge_worker_main")
            // set the arg to the worker id
            .set_argument(i.into_datum())
            // set the library to load
            .set_library("horloge")
            // enable SPI access
            .enable_spi_access()
            // load the worker
            .load();
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn horloge_worker_main(arg: pg_sys::Datum) {
    let worker_id = unsafe { i32::from_datum(arg, false) }.unwrap();
    self::run_worker(worker_id);
}

fn run_worker(worker_id: i32) {
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

    // wake up every 10s or if we received a SIGTERM
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
        if BackgroundWorker::sighup_received() {
            // on SIGHUP, you might want to reload some external configuration or something
        }

        // within a transaction, execute an SQL statement, and log its results
        BackgroundWorker::transaction(|| {
            Spi::connect(|client| {
                let tuple_table = client
                    .select(
                    "select localtimestamp",
                    None,
                    None,
                    )
                    .unwrap();
                tuple_table.for_each(|tuple| {
                    let ts= tuple
                        .get_datum_by_ordinal(1)
                        .unwrap()
                        .value::<Timestamp>()
                        .unwrap()
                        .unwrap();
                    log!("Tick: ({})", Into::<i64>::into(ts));
                });
            });
        });
    }

    log!(
        "Background Worker '{}' is exiting",
        BackgroundWorker::get_name()
    );
}
