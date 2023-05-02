// src/lib.rs

mod commands;  /// Internal SQL query commands wrapping SPI calls.
mod config;    /// Configuration for the horloge extension.
mod functions; /// SQL functions.
mod shmem;     /// Shared memory.
mod timer;     /// Timer implementation.
mod timestamp; /// Timestamp conversion between Postgres and Chrono.
mod triggers;  /// Triggers for timer tables.
mod types;     /// Common types.
mod workers;   /// Background worker for timer execution.

use pgrx::prelude::*;

pgrx::extension_sql_file!(
    "../sql/init.sql",
    name = "init", // fixme: Why does pgrx panic on "sql/init.sql"?
);

pgrx::pg_module_magic!();

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    workers::pg_init(); // Initialize workers sub-module.
    timer::pg_init();   // Initialize timer sub-module.
}

/// This module manages the SQL schema for this extension, and the exported
/// triggers and functions.
#[pg_schema]
mod horloge {
    use pgrx::prelude::*;

    /// Export trigger definitions more conveniently.
    macro_rules! export_triggers {
        {$($name: ident => $fn: expr),+} => {
            $(
                #[pg_trigger]
                fn $name<'a>(trigger: &'a PgTrigger<'a>) -> crate::triggers::TriggerResult<'a, impl WhoAllocated> {
                    $fn(trigger)
                }
            )+
        };
    }

    export_triggers! {
        horloge_timers_before_insert => crate::triggers::horloge_timers_before_insert,
        horloge_timers_after_insert  => crate::triggers::horloge_timers_after_insert,
        horloge_timers_before_update => crate::triggers::horloge_timers_before_update,
        horloge_timers_after_update  => crate::triggers::horloge_timers_after_update,
        horloge_timers_before_delete => crate::triggers::horloge_timers_before_delete,
        horloge_timers_after_delete  => crate::triggers::horloge_timers_after_delete
    }

    /// Activate timers for a relation.
    ///
    /// Relation can be:
    ///
    /// - **schema**.**table** - fully qualified
    /// - **table**            - assumes current schema
    #[pg_guard]
    #[pg_extern]
    fn activate_timers(rel: &str) {
        crate::functions::activate_timers(rel)
    }

    /// Deactivate timers for a relation.
    ///
    /// Relation can be:
    ///
    /// - **schema**.**table** - fully qualified
    /// - **table**            - assumes current schema
    #[pg_guard]
    #[pg_extern]
    fn deactivate_timers(rel: &str) {
        crate::functions::deactivate_timers(rel)
    }

    /// Create a timers table with the given name.
    ///
    /// Relation can be:
    ///
    /// - **schema**.**table** - fully qualified
    /// - **table**            - assumes current schema
    #[pg_guard]
    #[pg_extern]
    fn create_timers_table(rel: &str) {
        crate::functions::create_timers_table(rel)
    }
}
