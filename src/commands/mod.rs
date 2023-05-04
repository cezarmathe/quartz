// src/commands.rs

use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use pgrx::spi::SpiClient;

use crate::timestamp;
use crate::types::TimerRow;

pub struct TimerTableData {
    pub relid: Oid,
    pub schema: String,
    pub table: String,
}

pub fn find_timer_table(
    client: &SpiClient<'_>,
    oid: Oid,
) -> Result<Option<TimerTableData>, SpiError> {
    const QUERY: &'static str = include_str!("find_timer_table.sql");

    let args = vec![(PgOid::Custom(pgrx::pg_sys::OIDOID), oid.into_datum())];

    let tuple = match client
        .select(QUERY, None, Some(args))?
        .first()
        .get_heap_tuple()?
    {
        Some(tuple) => tuple,
        None => return Ok(None),
    };

    // ordinal position is 1-based

    let relid = tuple
        .get::<Oid>(1)
        .expect("commands::find_timer_table(): no relid")
        .expect("commands::find_timer_table(): relid is null");
    let schema = tuple
        .get::<String>(2)
        .expect("commands::find_timer_table(): no schema")
        .expect("commands::find_timer_table(): schema is null");
    let table = tuple
        .get::<String>(3)
        .expect("commands::find_timer_table(): no table")
        .expect("commands::find_timer_table(): table is null");

    Ok(Some(TimerTableData {
        relid,
        schema,
        table,
    }))
}

pub fn find_timer_tables(client: &SpiClient<'_>) -> Result<Vec<TimerTableData>, SpiError> {
    const QUERY: &'static str = include_str!("find_timer_tables.sql");

    let tuples = client.select(QUERY, None, None)?;

    let mut vec = Vec::with_capacity(tuples.len());

    for tuple in tuples {
        // ordinal position is 1-based

        let relid = tuple
            .get::<Oid>(1)
            .expect("commands::find_timer_tables(): no relid")
            .expect("commands::find_timer_tables(): relid is null");
        let schema = tuple
            .get::<String>(2)
            .expect("commands::find_timer_tables(): no schema")
            .expect("commands::find_timer_tables(): schema is null");
        let table = tuple
            .get::<String>(3)
            .expect("commands::find_timer_tables(): no table")
            .expect("commands::find_timer_tables(): table is null");

        vec.push(TimerTableData {
            relid,
            schema,
            table,
        });
    }

    Ok(vec)
}

pub fn find_timers_in_table(
    client: &SpiClient<'_>,
    schema: &str,
    table: &str,
) -> Result<Vec<TimerRow>, SpiError> {
    // fixme: this returns all timers, which is not well optimized in case
    // there are many of them. callers either want un-fired timers or
    // fired but un-completed timers.

    let query = format!(
        r#"
        select id, expires_at, fired_at, completed_at from "{}"."{}"
        "#,
        schema, table
    );

    let tuples = client.select(query.as_str(), None, None)?;

    let mut vec = Vec::with_capacity(tuples.len());

    for tuple in tuples {
        // ordinal position is 1-based

        let id = tuple
            .get::<i64>(1)
            .expect("commands::find_timers_in_table(): no id")
            .expect("commands::find_timers_in_table(): id is null");
        let expires_at = tuple
            .get::<TimestampWithTimeZone>(2)
            .expect("commands::find_timers_in_table(): no expires_at")
            .map(timestamp::pg_to_chrono)
            .expect("commands::find_timers_in_table(): expires_at is null");
        let fired_at = tuple
            .get::<TimestampWithTimeZone>(3)
            .expect("commands::find_timers_in_table(): no fired_at")
            .map(timestamp::pg_to_chrono);
        let completed_at = tuple
            .get::<TimestampWithTimeZone>(4)
            .expect("commands::find_timers_in_table(): no completed_at")
            .map(timestamp::pg_to_chrono);

        vec.push(TimerRow {
            id,
            expires_at,
            fired_at,
            completed_at,
        });
    }

    Ok(vec)
}

pub fn mark_timer_as_fired(
    client: &mut SpiClient<'_>,
    schema: &str,
    table: &str,
    id: i64,
) -> Result<(), SpiError> {
    let query = format!(
        r#"
        update "{}"."{}"
        set fired_at = localtimestamp
        where id = $1
        returning id
        "#,
        schema, table
    );

    let args = vec![(PgOid::Custom(pgrx::pg_sys::INT8OID), id.into_datum())];

    client
        .update(query.as_str(), None, Some(args))?
        .first()
        .get_one::<i64>()?
        .expect("Timer not found");

    Ok(())
}
