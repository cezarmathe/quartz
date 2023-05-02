// src/time.rs

use chrono::prelude::*;
use pgrx::Timestamp;

// (January 1, 2000, UTC) - (January 1, 1970, UTC) as seconds;
const PG_EPOCH_SECS: i64 = 946_684_800;
// (January 1, 2000, UTC) - (January 1, 1970, UTC) as milliseconds;
const PG_EPOCH_MILLIS: i64 = PG_EPOCH_SECS * 1000;
// (January 1, 2000, UTC) - (January 1, 1970, UTC) as microseconds;
const PG_EPOCH_MICROS: i64 = PG_EPOCH_MILLIS * 1000;

// pg_to_chrono converts a pgrx::Timestamp to a chrono::DateTime<Local>.
pub fn pg_to_chrono(ts: Timestamp) -> DateTime<Local> {
    let ts_i64 = Into::<i64>::into(ts);
    let naive_ts = NaiveDateTime::from_timestamp_micros(ts_i64 + PG_EPOCH_MICROS)
        .expect("pg_timestamp_to_chrono: timestamp out of range");
    Local.from_utc_datetime(&naive_ts)
}

// chrono_to_pg converts a chrono::DateTime<Local> to a pgrx::Timestamp.
pub fn chrono_to_pg(ts: DateTime<Local>) -> Timestamp {
    let naive_ts = ts.naive_local(); // fixme: convert back to UTC
    let ts_i64 = naive_ts.timestamp_micros() - PG_EPOCH_MICROS;
    Timestamp::try_from(ts_i64)
        .expect("chrono_to_pg_timestamp: timestamp out of range")
}
