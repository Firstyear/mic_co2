use actix::prelude::*;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::NO_PARAMS;
use std::net::SocketAddr;
use time::OffsetDateTime;

use mic::prelude::*;

macro_rules! ensure_mac {
    ($conn:expr, $mac:expr) => {
        $conn
            .execute_named(
                "INSERT OR REPLACE INTO meter_t (mac) VALUES (:mac)",
                &[(":mac", $mac)],
            )
            .map(|r| {
                debug!("insert -> {:?}", r);
                ()
            })
            .map_err(|e| {
                error!("sqlite execute_named error -> {:?}", e);
                ()
            })?;
    };
}

#[derive(Debug)]
struct DbEvent {
    src: String,
    time: OffsetDateTime,
    temp: u16,
    ppm: u16,
    hum: u16,
}

struct Db {
    pool: Pool<SqliteConnectionManager>,
}

impl Db {
    fn new(path: &str) -> Result<Self, ()> {
        let manager = SqliteConnectionManager::file(path);
        // We only build a single thread. If we need more than one, we'll
        // need to re-do this to account for path = "" for debug.
        let builder1 = Pool::builder().max_size(1);
        let pool = builder1.build(manager).map_err(|e| {
            error!("r2d2 error {:?}", e);
            ()
        })?;
        Ok(Db { pool })
    }

    fn get_conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>, ()> {
        self.pool.get().map_err(|e| {
            error!("Unable to get conn from pool!! -> {:?}", e);
            ()
        })
    }

    fn migrate(self) -> Result<Self, ()> {
        let conn = self.get_conn()?;
        // Create our tables if needed.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meter_t (
                mac TEXT PRIMARY KEY,
                label TEXT
            )
            ",
            NO_PARAMS,
        )
        .map_err(|e| {
            error!("sqlite meter_t create error -> {:?}", e);
            ()
        })?;

        /*
         *  - timestamp (local) --- sqlite supports TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS[+-]HH:MM").
         */
        conn.execute(
            "CREATE TABLE IF NOT EXISTS event_t (
                mac TEXT,
                ts TEXT NOT NULL,
                temp INTEGER NOT NULL,
                ppm INTEGER NOT NULL,
                hum INTEGER NOT NULL,
                FOREIGN KEY(mac) REFERENCES meter_t(mac) ON DELETE CASCADE
            )
            ",
            NO_PARAMS,
        )
        .map_err(|e| {
            error!("sqlite event_t create error -> {:?}", e);
            ()
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS event_t_ts_idx ON event_t (ts)",
            NO_PARAMS,
        )
        .map_err(|e| {
            error!("sqlite event_t_ts_idx create error -> {:?}", e);
            ()
        })?;

        /*
         * - time as YYYY-MM-DD
         */
        conn.execute(
            "CREATE TABLE IF NOT EXISTS history_t (
                mac TEXT,
                t TEXT NOT NULL,
                temp_max INTEGER NOT NULL,
                temp_min INTEGER NOT NULL,
                temp_avg INTEGER NOT NULL,
                ppm_max INTEGER NOT NULL,
                ppm_min INTEGER NOT NULL,
                ppm_avg INTEGER NOT NULL,
                hum_max INTEGER NOT NULL,
                hum_min INTEGER NOT NULL,
                hum_avg INTEGER NOT NULL,
                FOREIGN KEY(mac) REFERENCES meter_t(mac) ON DELETE CASCADE
            )
            ",
            NO_PARAMS,
        )
        .map_err(|e| {
            error!("sqlite history_t create error -> {:?}", e);
            ()
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS history_t_t_idx ON history_t (t)",
            NO_PARAMS,
        )
        .map_err(|e| {
            error!("sqlite history_t_t_idx create error -> {:?}", e);
            ()
        })?;
        Ok(self)
    }

    fn purge_older_than(&self, max: &OffsetDateTime) -> Result<(), ()> {
        let max_str = max.format("%F");

        let conn = self.get_conn()?;

        conn.execute_named("DELETE FROM event_t WHERE ts < :max", &[(":max", &max_str)])
            .map(|r| {
                debug!("delete -> {:?}", r);
                ()
            })
            .map_err(|e| {
                error!("sqlite execute_named error -> {:?}", e);
                ()
            })
    }

    fn add_datum(&self, datum: Datum, ct: OffsetDateTime) -> Result<(), ()> {
        let mac = datum.mac_as_string();
        let (ppm, hum, temp) = datum.data();
        let ts = ct.format("%F %T%z");

        let conn = self.get_conn()?;
        ensure_mac!(conn, &mac);

        conn.execute_named(
            "INSERT OR REPLACE INTO event_t (mac, ts, temp, ppm, hum) VALUES (:mac, :ts, :temp, :ppm, :hum)",
        &[
            (":mac", &mac),
            (":ts", &ts),
            (":temp", &temp),
            (":ppm", &ppm),
            (":hum", &hum),
        ])
        .map(|r| {
            debug!("insert -> {:?}", r);
            ()
        })
        .map_err(|e| {
            error!("sqlite execute_named error -> {:?}", e);
            ()
        })
    }

    fn get_event_range(
        &self,
        src: &str,
        min: &OffsetDateTime,
        max: &OffsetDateTime,
    ) -> Result<Vec<DbEvent>, ()> {
        // select from where >= min and < max
        let min_str = min.format("%F");
        let max_str = max.format("%F");

        let conn = self.get_conn()?;

        let mut stmt = conn.prepare(
            "SELECT ts, temp, ppm, hum FROM event_t WHERE mac = :mac AND ts >= :min AND ts < :max ORDER BY ts ASC"
        )
        .map_err(|e| {
            error!("sqlite prepare and query error -> {:?}", e);
            ()
        })?;

        let data_iter = stmt
            .query_map_named(
                &[(":mac", &src), (":min", &min_str), (":max", &max_str)],
                |row| {
                    Ok((
                        row.get_unwrap::<usize, String>(0),
                        row.get_unwrap(1),
                        row.get_unwrap(2),
                        row.get_unwrap(3),
                    ))
                },
            )
            .map_err(|e| {
                error!("sqlite prepare and query error -> {:?}", e);
                ()
            })?;

        let data: Vec<DbEvent> = data_iter
            .map(|row| match row {
                Ok((ts, temp, ppm, hum)) => DbEvent {
                    src: src.to_string(),
                    time: OffsetDateTime::parse(ts, "%F %T%z").expect("invalid ts"),
                    temp,
                    ppm,
                    hum,
                },
                _ => panic!(),
            })
            .collect();

        Ok(data)
    }
}

impl Actor for DbActor {
    type Context = SyncContext<Self>;
}

pub struct DbActor {
    db: Db,
}

impl DbActor {
    pub fn new(path: &str) -> Result<Self, ()> {
        Ok(DbActor {
            db: Db::new(path).and_then(|db| db.migrate())?,
        })
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct DbPurgeEvent;

impl Handler<DbPurgeEvent> for DbActor {
    type Result = ();

    fn handle(&mut self, _msg: DbPurgeEvent, _: &mut SyncContext<Self>) {
        let ct = OffsetDateTime::now_local();

        // Process our historical data as needed.
        // What can we now remove up to?

        // self.db.purge(ct)
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct DbAddDatumEvent(pub Datum, pub SocketAddr);

impl Handler<DbAddDatumEvent> for DbActor {
    type Result = ();

    fn handle(&mut self, msg: DbAddDatumEvent, _: &mut SyncContext<Self>) {
        let ct = OffsetDateTime::now_local();
        self.db.add_datum(msg.0, ct);
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{Db, DbEvent};
    use mic::prelude::*;
    use time::OffsetDateTime;

    fn add_sample_data(db: &Db, mac: [u8; 6], temp: u16, ppm: u16, hum: u16, ts: &str) {
        let ct = OffsetDateTime::parse(ts, "%F %T%z").expect("invalid ts");
        let datum = Datum::from((mac, ppm, hum, temp));
        db.add_datum(datum, ct).expect("Failed to add data!")
    }

    fn get_event_range(db: &Db, src: &str, min: &str, max: &str) -> Vec<DbEvent> {
        let min_ts = OffsetDateTime::parse(min, "%F %T%z").expect("invalid ts");
        let max_ts = OffsetDateTime::parse(max, "%F %T%z").expect("invalid ts");
        db.get_event_range(src, &min_ts, &max_ts)
            .expect("failed to get range")
    }

    fn purge(db: &Db, max: &str) {
        let max_ts = OffsetDateTime::parse(max, "%F %T%z").expect("invalid ts");
        db.purge_older_than(&max_ts).expect("failed to purge data");
    }

    #[test]
    fn test_db_append_cleanup() {
        // Setup a db.
        let _ = env_logger::builder().is_test(true).try_init();
        let db = Db::new("").unwrap();
        let db = db.migrate().unwrap();
        // Append data
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-05 13:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-05 14:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-06 13:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-06 14:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-07 13:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-07 14:02:19+1000");
        // Can we select a range?
        let data1 = get_event_range(
            &db,
            "00:00:00:00:00:00",
            "2020-04-06 00:00:00+1000",
            "2020-04-07 00:00:00+1000",
        );
        assert!(data1.len() == 2);
        // purge older than X
        purge(&db, "2020-04-06 00:00:00+1000");
        // Any data left?
        let data2 = get_event_range(
            &db,
            "00:00:00:00:00:00",
            "2020-04-06 00:00:00+1000",
            "2021-04-07 00:00:00+1000",
        );
        assert!(data2.len() == 4);
    }
}
