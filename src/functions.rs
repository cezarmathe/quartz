// src/functions.rs

use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use pgrx::spi::SpiClient;

use crate::timer::TimerHandle;
use crate::timer::TimerSubsystemEvent;

pub fn create_timers_table(rel: &str) {
    if let Err(e) =
        Spi::connect(|mut client| self::create_timers_table_with_client(&mut client, rel))
    {
        error!("horloge.create_timers_table(): {}", e);
    }
}

fn create_timers_table_with_client<'a>(
    client: &mut SpiClient<'a>,
    rel: &str,
) -> Result<(), SpiError> {
    let (schema_str, table) = rel
        .split_once(".")
        .map(|(schema, table)| (Some(schema), table))
        .unwrap_or((None, rel));

    let fq = if let Some(schema) = schema_str {
        format!("\"{}\".\"{}\"", schema, table)
    } else {
        format!("\"{}\"", table)
    };

    let schema_arg = schema_str.map(|s| format!("'{}'", s)).unwrap_or_else(|| "current_schema()".to_string());
    let table_arg = format!("'{}'", table);

    let query = format!(
        r#"
            create table {} (
                id bigint generated always as identity primary key,
                expires_at timestamp not null,
                fired_at timestamp,
                completed_at timestamp
            );

            with table_oid as (
                select c.oid
                from pg_catalog.pg_class c
                join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                where c.relname = {}
                and n.nspname = {}
            )
            insert into horloge.timer_relations (relid)
            select oid from table_oid
            returning relid;
        "#,
        fq, table_arg, schema_arg
    );

    let result = client.update(query.as_str(), None, None)?;

    let table_oid = match result.get_one() {
        Ok(Some(value)) => value,
        Ok(None) => {
            error!("horloge.create_timers_table(): failed to get table OID: no result");
        }
        Err(e) => {
            error!("horloge.create_timers_table(): failed to get table OID: {}", e);
        }
    };

    if let Err(e) = self::activate_timers_with_client(client, rel) {
        error!("horloge.create_timers_table(): failed to activate timers: {}", e);
    }

    if !TimerHandle::get().enqueue_event(TimerSubsystemEvent::TrackTimersTable { table_oid }) {
        error!("horloge.create_timers_table(): failed to enqueue event");
    }

    Ok(())
}

pub fn drop_timers_table(rel: &str) {
    error!("horloge.drop_timers_table(): not implemented");
}

fn drop_timers_table_with_client<'a>(client: &mut SpiClient<'a>, rel: &str) {
    todo!("not implemented")
}

pub fn activate_timers(rel: &str) {
    if let Err(e) = Spi::connect(|mut client| self::activate_timers_with_client(&mut client, rel)) {
        error!("horloge.activate_timers(): {}", e);
    }
}

fn activate_timers_with_client<'a>(client: &mut SpiClient<'a>, rel: &str) -> Result<(), SpiError> {
    let query = format!(
        r#"
        create or replace trigger horloge_timers_before_insert
            before insert on {}
            for each row
            execute procedure horloge.horloge_timers_before_insert();
        create or replace trigger horloge_timers_after_insert
            after insert on {}
            for each row
            execute procedure horloge.horloge_timers_after_insert();
        create or replace trigger horloge_timers_before_update
            before update on {}
            for each row
            execute procedure horloge.horloge_timers_before_update();
        create or replace trigger horloge_timers_after_update
            after update on {}
            for each row
            execute procedure horloge.horloge_timers_after_update();
        create or replace trigger horloge_timers_before_delete
            before delete on {}
            for each row
            execute procedure horloge.horloge_timers_before_delete();
        create or replace trigger horloge_timers_after_delete
            after delete on {}
            for each row
            execute procedure horloge.horloge_timers_after_delete();
        "#,
        rel, rel, rel, rel, rel, rel
    );

    client.update(query.as_str(), None, None).map(|_| ())
}

pub fn deactivate_timers(rel: &str) {
    if let Err(e) = Spi::connect(|mut client| self::deactivate_timers_with_client(&mut client, rel))
    {
        error!("horloge.deactivate_timers(): {}", e);
    }
}

fn deactivate_timers_with_client<'a>(
    client: &mut SpiClient<'a>,
    rel: &str,
) -> Result<(), SpiError> {
    let query = format!(
        r#"
        drop trigger if exists horloge_timers_before_insert on {};
        drop trigger if exists horloge_timers_after_insert on {};
        drop trigger if exists horloge_timers_before_update on {};
        drop trigger if exists horloge_timers_after_update on {};
        drop trigger if exists horloge_timers_before_delete on {};
        drop trigger if exists horloge_timers_after_delete on {};
        "#,
        rel, rel, rel, rel, rel, rel
    );

    client.update(query.as_str(), None, None).map(|_| ())
}
