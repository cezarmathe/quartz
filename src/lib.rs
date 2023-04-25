// src/lib.rs

mod com;
mod worker;

use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;

use pgrx::PgLwLock;
use pgrx::pg_shmem_init;
use pgrx::prelude::*;
use pgrx::shmem::*;

pgrx::pg_module_magic!();

pub static RUNTIME: PgLwLock<tokio::runtime::Runtime> = PgLwLock::new();
pub static RUNTIME2: PgLwLock<i32> = PgLwLock::new();

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(RUNTIME2);

    worker::init();
}

// // (January 1, 2000, UTC) - (January 1, 1970, UTC) as seconds;
// const PG_EPOCH_SECS: u128 = 946_708_560;
// // (January 1, 2000, UTC) - (January 1, 1970, UTC) as microseconds;
// const PG_EPOCH_MICROS: u128 = PG_EPOCH_SECS * 1_000_000;

#[pg_trigger]
fn on_horloge_timers_event<'a>(trigger: &'a PgTrigger<'a>) -> Result<
    Option<PgHeapTuple<'a, impl WhoAllocated>>,
    PgHeapTupleError,
> {
    let now = Utc::now();

    let id = trigger.new().unwrap().get_by_name::<i64>("id").unwrap().unwrap();
    let ts_pg = trigger.new().unwrap().get_by_name::<Timestamp>("ts").unwrap().unwrap();

    let ts_i64 = Into::<i64>::into(ts_pg);
    let ts = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(ts_i64).unwrap(), Utc);

    // let ts_i64 = Into::<i64>::into(ts);
    // let ts_u64 = u64::try_from(Into::<i64>::into(ts)).unwrap();
    // let ts_u128 = u128::from(ts_u64) + PG_EPOCH_MICROS;
    // let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_micros();

    panic!("ts={}, now={}", ts.timestamp_micros(), now.timestamp_micros());

    if now >= ts_u128 {
        error!("timer is in the past: now={}, new.ts={}", now, ts_u128);
    }

    Ok(Some(trigger.new().or(trigger.old()).expect("neither \"new\" nor \"old\"")))
}
