// src/timer/types.rs

use chrono::prelude::*;
use pgrx::TryFromDatumError;
use pgrx::prelude::*;

use std::error::Error;

/// A row in a timer table.
///
/// This is agnostic towards the actual table that the timer is associated with.
pub struct TimerRow {
    // The ID of the timer.
    pub id: i64,

    // The timestamp at which the timer should fire.
    pub expires_at: chrono::DateTime<Local>,

    // The timestamp at which the timer fired, if it has expired.
    pub fired_at: Option<chrono::DateTime<Local>>,

    // The timestamp at which the timer firing was acknowledged, if it has been
    // fired and the action has been successfully completed.
    pub completed_at: Option<chrono::DateTime<Local>>,
}

impl<'a> TryFrom<&'a PgHeapTuple<'a, AllocatedByPostgres>> for TimerRow {
    type Error = Box<dyn Error>;

    fn try_from(tuple: &'a PgHeapTuple<'a, AllocatedByPostgres>) -> Result<Self, Self::Error> {
        let id = match tuple.get_by_name("id") {
            Ok(Some(value)) => value,
            Ok(None) => return Err("id must not be null".into()),
            Err(TryFromDatumError::NoSuchAttributeName(_)) => return Err("missing id column".into()),
            Err(TryFromDatumError::IncompatibleTypes{..}) => return Err("id must be a bigint".into()),
            Err(e) => return Err(format!("unexpected error: {}", e).into()),
        };

        let expires_at = match tuple.get_by_name::<TimestampWithTimeZone>("expires_at") {
            Ok(Some(value)) => crate::timestamp::pg_to_chrono(value),
            Ok(None) => return Err("expires_at must not be null".into()),
            Err(TryFromDatumError::NoSuchAttributeName(_)) => return Err("missing expires_at column".into()),
            Err(TryFromDatumError::IncompatibleTypes{..}) => return Err("expires_at must be a timestamp".into()),
            Err(e) => return Err(format!("unexpected error: {}", e).into()),
        };

        let fired_at = match tuple.get_by_name::<TimestampWithTimeZone>("fired_at") {
            Ok(Some(value)) => Some(crate::timestamp::pg_to_chrono(value)),
            Ok(None) => None,
            Err(TryFromDatumError::NoSuchAttributeName(_)) => return Err("missing fired_at column".into()),
            Err(TryFromDatumError::IncompatibleTypes{..}) => return Err("fired_at must be a timestamp".into()),
            Err(e) => return Err(format!("unexpected error: {}", e).into()),
        };

        let completed_at = match tuple.get_by_name::<TimestampWithTimeZone>("completed_at") {
            Ok(Some(value)) => Some(crate::timestamp::pg_to_chrono(value)),
            Ok(None) => None,
            Err(TryFromDatumError::NoSuchAttributeName(_)) => return Err("missing completed_at column".into()),
            Err(TryFromDatumError::IncompatibleTypes{..}) => return Err("completed_at must be a timestamp".into()),
            Err(e) => return Err(format!("unexpected error: {}", e).into()),
        };

        Ok(Self {
            id,
            expires_at,
            fired_at,
            completed_at,
        })
    }
}

/// Data for creating a timer from a row.
#[derive(Copy, Clone)]
pub struct CreateTimerFromRow {
    pub id: i64, // 8 bytes
    pub expires_at: chrono::DateTime<Local>, // 12 bytes
}

impl<'a> TryFrom<&'a PgHeapTuple<'a, AllocatedByPostgres>> for CreateTimerFromRow {
    type Error = Box<dyn Error>;

    fn try_from(value: &'a PgHeapTuple<'a, AllocatedByPostgres>) -> Result<Self, Self::Error> {
        let timer = TimerRow::try_from(value)?;

        if timer.fired_at.is_some() {
            return Err("creating a fired timer is forbidden".into());
        }

        if timer.completed_at.is_some() {
            return Err("creating a completed timer is forbidden".into());
        }

        Ok(Self {
            id: timer.id,
            expires_at: timer.expires_at,
        })
    }
}

impl From<CreateTimerFromRow> for TimerRow {
    fn from(value: CreateTimerFromRow) -> Self {
        Self {
            id: value.id,
            expires_at: value.expires_at,
            fired_at: None,
            completed_at: None,
        }
    }
}

impl From<TimerRow> for CreateTimerFromRow {
    fn from(value: TimerRow) -> Self {
        Self {
            id: value.id,
            expires_at: value.expires_at,
        }
    }
}
