// src/functions.rs

use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use pgrx::spi::SpiClient;

pub fn create_timers_table(rel: &str) {
    if let Err(e) =
        Spi::connect(|mut client| self::create_timers_table_with_client(&mut client, rel))
    {
        error!("horloge_create_timers_table: {}", e);
    }
}

fn create_timers_table_with_client<'a>(
    client: &mut SpiClient<'a>,
    rel: &str,
) -> Result<(), SpiError> {
    let (schema, table) = rel
        .split_once(".")
        .map(|(schema, table)| (Some(schema), table))
        .unwrap_or((None, rel));

    let fq = if let Some(schema) = schema {
        format!("{}.\"{}\"", schema, table)
    } else {
        format!("\"{}\"", table)
    };

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
                where c.relname = $1
                and n.nspname = $2
            )
            insert into horloge.timer_relations (table_oid)
            select oid from table_oid;
        "#,
        fq,
    );

    client.update(query.as_str(), None, None).map(|_| ())?;

    self::activate_timers_with_client(client, rel)
}

pub fn drop_timers_table(rel: &str) {}

fn drop_timers_table_with_client<'a>(client: &mut SpiClient<'a>, rel: &str) {}

pub fn activate_timers(rel: &str) {
    if let Err(e) = Spi::connect(|mut client| self::activate_timers_with_client(&mut client, rel)) {
        error!("horloge_activate_timers: {}", e);
    }
}

fn activate_timers_with_client<'a>(client: &mut SpiClient<'a>, rel: &str) -> Result<(), SpiError> {
    let query = format!(
        r#"
        create or replace trigger horloge_timers_before_insert
            before insert on {}
            for each row
            execute procedure horloge_timers_before_insert();
        create or replace trigger horloge_timers_after_insert
            after insert on {}
            for each row
            execute procedure horloge_timers_after_insert();
        create or replace trigger horloge_timers_before_update
            before update on {}
            for each row
            execute procedure horloge_timers_before_update();
        create or replace trigger horloge_timers_after_update
            after update on {}
            for each row
            execute procedure horloge_timers_after_update();
        create or replace trigger horloge_timers_before_delete
            before delete on {}
            for each row
            execute procedure horloge_timers_before_delete();
        create or replace trigger horloge_timers_after_delete
            after delete on {}
            for each row
            execute procedure horloge_timers_after_delete();
        "#,
        rel, rel, rel, rel, rel, rel
    );

    client.update(query.as_str(), None, None).map(|_| ())
}

pub fn deactivate_timers(rel: &str) {
    if let Err(e) = Spi::connect(|mut client| self::deactivate_timers_with_client(&mut client, rel))
    {
        error!("horloge_deactivate_timers: {}", e);
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
