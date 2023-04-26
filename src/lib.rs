// src/lib.rs

mod com;
mod time;
mod worker;

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

#[pg_trigger]
fn on_horloge_timers_event<'a>(trigger: &'a PgTrigger<'a>) -> Result<
    Option<PgHeapTuple<'a, impl WhoAllocated>>,
    PgHeapTupleError,
> {
    let now = Utc::now();

    if !trigger.event().fired_by_insert(){
        error!("trigger fired by something other than insert");
    }

    let _id = trigger.new().unwrap().get_by_name::<i64>("id").unwrap().unwrap();
    let ts_pg = trigger.new().unwrap().get_by_name::<Timestamp>("ts").unwrap().unwrap();

    let ts = time::pg_to_chrono(ts_pg);

    if now >= ts {
        error!("timer is in the past: now={}, new.ts={}", now, ts);
    }

    // do something with the timer

    Ok(Some(trigger.new().or(trigger.old()).expect("neither \"new\" nor \"old\"")))
}
