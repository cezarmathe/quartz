// src/triggers.rs

use chrono::prelude::*;
use pgrx::prelude::*;

use crate::timer::TimerHandle;
use crate::timer::TimerSubsystemEvent;
use crate::types::CreateTimerFromRow;

use std::convert::TryFrom;

/// A result returned by a trigger function.
pub type TriggerResult<'a, WhoAllocated> =
    Result<Option<PgHeapTuple<'a, WhoAllocated>>, PgHeapTupleError>;

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

pub fn horloge_timers_before_insert<'a>(
    trigger: &'a PgTrigger<'a>,
) -> TriggerResult<'a, impl WhoAllocated> {
    let now = Local::now();

    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_insert => {
            fired_by_insert,
            fired_before,
            fired_for_row
        }
    );

    let new_row = trigger
        .new()
        .expect("before insert trigger must have \"new\"");

    let new_timer = match CreateTimerFromRow::try_from(&new_row) {
        Ok(value) => value,
        Err(e) => {
            error!("create new timer: {}", e);
        }
    };

    // fixme: ensure that timers are unique across schemas and tables

    if now >= new_timer.expires_at {
        error!(
            "timer is in the past: now={}, new.ts={}",
            now, new_timer.expires_at
        );
    }

    Ok(Some(new_row))
}

pub fn horloge_timers_after_insert<'a>(
    trigger: &'a PgTrigger<'a>,
) -> TriggerResult<'a, impl WhoAllocated> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_after_insert => {
            fired_by_insert,
            fired_after,
            fired_for_row
        }
    );

    let new_row = trigger
        .new()
        .expect("before insert trigger must have \"new\"");

    let new_timer = match CreateTimerFromRow::try_from(&new_row) {
        Ok(value) => value,
        Err(e) => {
            error!("create new timer: {}", e);
        }
    };

    let relation_oid = match trigger.relation().map(|rel| rel.oid()) {
        Ok(value) => value,
        Err(e) => error!("horloge_timers_after_insert: relation ID is unexpectedly unavailable: {}", e),
    };

    let event = TimerSubsystemEvent::CreateTimer {
        table_oid: relation_oid,
        table_row: new_timer,
    };

    if !TimerHandle::get().enqueue_event(event) {
        error!("failed to enqueue timer")
    }

    Ok(Some(new_row))
}

pub fn horloge_timers_before_update<'a>(
    trigger: &'a PgTrigger<'a>,
) -> TriggerResult<'a, impl WhoAllocated> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_update,
            fired_before,
            fired_for_row
        }
    );

    notice!("horloge.horloge_timers_before_update: updates are not yet supported");

    // todo: validate
    // - parse Timer out of new
    // - ensure ID is not changed
    // - ensure one of EXP_AT, FIRED_AT or ACK_AT are updated
    // - ensure EXP_AT is not updated if FIRED_AT or ACK_AT are set

    Ok(trigger.new())
}

pub fn horloge_timers_after_update<'a>(
    trigger: &'a PgTrigger<'a>,
) -> TriggerResult<'a, impl WhoAllocated> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_update,
            fired_after,
            fired_for_row
        }
    );

    notice!("horloge.horloge_timers_after_update: updates are not yet supported");

    // todo: queue update
    // - parse Timer out of new
    // - ensure ID is not changed
    // - ensure one of EXP_AT, FIRED_AT or ACK_AT are updated
    // - ensure EXP_AT is not updated if FIRED_AT or ACK_AT are set
    // - ignore FIRED_AT or ACK_AT
    // - queue update to be applied

    Ok(trigger.new())
}

pub fn horloge_timers_before_delete<'a>(
    trigger: &'a PgTrigger<'a>,
) -> TriggerResult<'a, impl WhoAllocated> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_delete,
            fired_before,
            fired_for_row
        }
    );

    notice!("horloge.horloge_timers_before_delete: deletes are not yet supported");

    // todo: validate
    // - parse Timer out of old
    // - check that ID exists
    // - revoke delete if timer is currently firing

    Ok(trigger.old())
}

pub fn horloge_timers_after_delete<'a>(
    trigger: &'a PgTrigger<'a>,
) -> TriggerResult<'a, impl WhoAllocated> {
    assert_row_trigger_event!(
        trigger.event(),
        horloge_timers_before_update => {
            fired_by_delete,
            fired_after,
            fired_for_row
        }
    );

    notice!("horloge.horloge_timers_after_delete: deletes are not yet supported");

    // todo: queue delete
    // - parse Timer out of old
    // - check that ID exists
    // - revoke delete if timer is currently firing
    // - if fired, ignore
    // - queue delete to be applied

    Ok(trigger.old())
}
