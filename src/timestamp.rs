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
    Local.from_local_datetime(&naive_ts).unwrap()
}

// chrono_to_pg converts a chrono::DateTime<Local> to a pgrx::Timestamp.
pub fn chrono_to_pg(ts: DateTime<Local>) -> Timestamp {
    let naive_ts = ts.naive_local();
    let ts_i64 = naive_ts.timestamp_micros() - PG_EPOCH_MICROS;
    Timestamp::try_from(ts_i64)
        .expect("chrono_to_pg_timestamp: timestamp out of range")
}

// // PgTimestamp is a wrapper around chrono::NaiveDateTime that implements
// // From<pgrx::Timestamp> and Deref<Target=NaiveDateTime>.
// #[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Clone)]
// pub struct PgTimestamp(NaiveDateTime);

// impl PgTimestamp {
//     pub fn now() -> Self {
//         let now = Local::now().naive_local();
//         Self(now)
//     }
// }

// impl Deref for PgTimestamp {
//     type Target = NaiveDateTime;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl DerefMut for PgTimestamp {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }

// impl From<Timestamp> for PgTimestamp {
//     fn from(pg_ts: Timestamp) -> Self {
//         let ts_i64 = Into::<i64>::into(pg_ts);
//         let ts = NaiveDateTime::from_timestamp_micros(ts_i64 + PG_EPOCH_MICROS).unwrap();
//         Self(ts)
//     }
// }

pub struct PgTimestamp(Timestamp);

impl From<Timestamp> for PgTimestamp {
    fn from(pg_ts: Timestamp) -> Self {
        Self(pg_ts)
    }
}

impl Into<NaiveDateTime> for PgTimestamp {
    fn into(self) -> NaiveDateTime {
        let ts_i64 = Into::<i64>::into(self.0);
        NaiveDateTime::from_timestamp_micros(ts_i64 + PG_EPOCH_MICROS).unwrap()
    }
}
