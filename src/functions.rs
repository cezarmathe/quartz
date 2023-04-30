// src/functions.rs

use pgrx::prelude::*;

#[pg_extern]
#[pg_guard]
fn horloge_enable_timers(rel: &str) {
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

    if let Err(e) = Spi::run(query.as_str()) {
        error!("horloge_enable_timers: {}", e);
    }
}

#[pg_extern]
#[pg_guard]
pub fn horloge_disable_timers(rel: &str) {
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

    if let Err(e) = Spi::run(query.as_str()) {
        error!("horloge_disable_timers: {}", e);
    }
}
