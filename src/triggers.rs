// src/triggers.rs

use chrono::prelude::*;
use pgrx::prelude::*;

// Assert that a trigger event matches the provided conditions (as functions on
// the event type itself). Failures are logged at the ERROR level in Postgres,
// thereby causing the trigger to fail.
macro_rules! assert_row_trigger_event {
    ($event: expr, $fn: ident => {$($event_fn: tt),+}) => {
        $(
            if !$event.$event_fn() {
                error!("{}: must be {}", stringify!($fn), stringify!($event_fn));
            }
        )+
    };
}

#[pg_trigger]
fn horloge_timers_before_insert<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgHeapTupleError> {
    let now = Local::now();

    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_insert => {
            fired_by_insert,
            fired_before,
            fired_for_row
        }
    );

    // todo: validation
    // - parse Timer out of new
    // - ensure only ID and TS are set
    // - ensure TS is in the future
    // - ensure ID is unique

    let id = trigger
        .new()
        .unwrap()
        .get_by_name::<i64>("id")
        .unwrap()
        .unwrap();
    let ts_pg = trigger
        .new()
        .unwrap()
        .get_by_name::<Timestamp>("ts")
        .unwrap()
        .unwrap();

    let ts = crate::timestamp::pg_to_chrono(ts_pg);

    if now >= ts {
        error!("timer is in the past: now={}, new.ts={}", now, ts);
    }

    // do something with the timer
    crate::timer::enqueue(crate::timer::CreateTimer { id, ts });

    Ok(Some(
        trigger
            .new()
            .or(trigger.old())
            .expect("neither \"new\" nor \"old\""),
    ))
}

#[pg_trigger]
fn horloge_timers_after_insert<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgHeapTupleError> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_after_insert => {
            fired_by_insert,
            fired_after,
            fired_for_row
        }
    );

    // todo: queue
    // - parse Timer out of new
    // - ensure only ID and TS are set
    // - ensure ID is unique?
    // - enqueue timer to be added

    Ok(trigger.new())
}

#[pg_trigger]
fn horloge_timers_before_update<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgHeapTupleError> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_update,
            fired_before,
            fired_for_row
        }
    );

    // todo: validate
    // - parse Timer out of new
    // - ensure ID is not changed
    // - ensure one of EXP_AT, FIRED_AT or ACK_AT are updated
    // - ensure EXP_AT is not updated if FIRED_AT or ACK_AT are set

    Ok(trigger.new())
}

#[pg_trigger]
fn horloge_timers_after_update<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgHeapTupleError> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_update,
            fired_after,
            fired_for_row
        }
    );

    // todo: queue update
    // - parse Timer out of new
    // - ensure ID is not changed
    // - ensure one of EXP_AT, FIRED_AT or ACK_AT are updated
    // - ensure EXP_AT is not updated if FIRED_AT or ACK_AT are set
    // - ignore FIRED_AT or ACK_AT
    // - queue update to be applied

    Ok(trigger.new())
}

#[pg_trigger]
fn horloge_timers_before_delete<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgHeapTupleError> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_delete,
            fired_before,
            fired_for_row
        }
    );

    // todo: validate
    // - parse Timer out of old
    // - check that ID exists
    // - revoke delete if timer is currently firing

    Ok(trigger.old())
}

#[pg_trigger]
fn horloge_timers_after_delete<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgHeapTupleError> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_delete,
            fired_after,
            fired_for_row
        }
    );

    // todo: queue delete
    // - parse Timer out of old
    // - check that ID exists
    // - revoke delete if timer is currently firing
    // - if fired, ignore
    // - queue delete to be applied

    Ok(trigger.old())
}
