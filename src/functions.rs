// src/functions.rs

use pgrx::prelude::*;
use pgrx::spi::Error as SpiError;
use pgrx::spi::SpiClient;

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
            select oid from table_oid;
        "#,
        fq, table_arg, schema_arg
    );

    client.update(query.as_str(), None, None).map(|_| ())?;

    self::activate_timers_with_client(client, rel)
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
