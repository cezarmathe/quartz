// src/lib.rs

mod functions; // SQL functions.
mod timestamp; // Timestamp conversion between Postgres and Chrono.
mod timer;     // Timer implementation.
mod triggers;  // Triggers for timer tables.
mod worker;    // Background worker for timer execution.

use pgrx::prelude::*;

pgrx::pg_module_magic!();

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    timer::pg_init();  // Initialize timer sub-module.
    worker::pg_init(); // Initialize worker sub-module.
}
