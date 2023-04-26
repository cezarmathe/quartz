// src/time.rs

use chrono::prelude::*;
use pgrx::Timestamp;

// (January 1, 2000, UTC) - (January 1, 1970, UTC) as seconds;
pub const PG_EPOCH_SECS: i64 = 946_684_800;
// (January 1, 2000, UTC) - (January 1, 1970, UTC) as milliseconds;
pub const PG_EPOCH_MILLIS: i64 = PG_EPOCH_SECS * 1000;
// (January 1, 2000, UTC) - (January 1, 1970, UTC) as microseconds;
pub const PG_EPOCH_MICROS: i64 = PG_EPOCH_MILLIS * 1000;

pub fn pg_to_chrono(ts: Timestamp) -> DateTime<Utc> {
    let ts_i64 = Into::<i64>::into(ts);
    DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp_micros(ts_i64 + PG_EPOCH_MICROS).unwrap(),
        Utc,
    )
}
