use actix::prelude::*;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::NO_PARAMS;
use std::net::SocketAddr;
use std::time::Duration;
use time::OffsetDateTime;

use mic::prelude::*;

const RETAIN_DAYS: u64 = 4;

macro_rules! ensure_mac {
    ($conn:expr, $mac:expr, $err:expr) => {
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
                $err
            })?;
    };
}

macro_rules! ts_remove_hhmmss {
    ($ts:expr) => {{
        let date = $ts.date();
        let offset = $ts.offset();

        date.midnight().assume_offset(offset)
    }};
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
        ensure_mac!(conn, &mac, ());

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

    fn list_meters(&self) -> Result<Vec<String>, ()> {
        let conn = self.get_conn()?;

        let mut stmt = conn
            .prepare("SELECT DISTINCT mac FROM meter_t")
            .map_err(|e| {
                error!("sqlite prepare and query error -> {:?}", e);
                ()
            })?;

        let data_iter = stmt.query_map(NO_PARAMS, |row| row.get(0)).map_err(|e| {
            error!("sqlite prepare and query error -> {:?}", e);
            ()
        })?;

        let data: Vec<String> = data_iter
            .map(|row| match row {
                Ok(mac) => mac,
                _ => panic!(),
            })
            .collect();

        Ok(data)
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

    fn get_latest_report_date(&self, src: &str) -> Result<OffsetDateTime, ()> {
        let conn = self.get_conn()?;

        let mut stmt = conn
            .prepare("SELECT MAX(t) FROM history_t WHERE mac = :mac")
            .map_err(|e| {
                error!("sqlite prepare and query error -> {:?}", e);
                ()
            })?;

        let data_iter = stmt
            .query_map_named(&[(":mac", &src)], |row| row.get(0))
            .map_err(|e| {
                error!("sqlite prepare and query error -> {:?}", e);
                ()
            })?;

        let data: Result<Vec<OffsetDateTime>, _> = data_iter
            .map(|row: Result<String, _>| {
                row.map(|ts| {
                    ts_remove_hhmmss!(OffsetDateTime::parse(&ts, "%F %T%z").expect("invalid ts"))
                })
            })
            .collect();

        match data {
            Ok(mut v) => {
                return Ok(v.pop().expect("missing max calc?"));
            }
            // Gotta keep going ....
            Err(_) => {}
        }

        // select min(ts) from event_t where mac = "0:0:0:0:0:0";
        let mut stmt = conn
            .prepare("SELECT MIN(ts) FROM event_t WHERE mac = :mac")
            .map_err(|e| {
                error!("sqlite prepare and query error -> {:?}", e);
                ()
            })?;

        let data_iter = stmt
            .query_map_named(&[(":mac", &src)], |row| row.get(0))
            .map_err(|e| {
                error!("sqlite prepare and query error -> {:?}", e);
                ()
            })?;

        let data: Result<Vec<OffsetDateTime>, _> = data_iter
            .map(|row: Result<String, _>| {
                row.map(|ts| {
                    // We remove 1 day here to make it the "day before".
                    ts_remove_hhmmss!(OffsetDateTime::parse(&ts, "%F %T%z").expect("invalid ts"))
                        - Duration::from_secs(86400)
                })
            })
            .collect();

        match data {
            Ok(mut v) => Ok(v.pop().expect("missing max calc?")),
            // Gotta keep going ....
            Err(_) => {
                error!("No data found, unable to generate report data ...?");
                Err(())
            }
        }
    }

    fn extract_report(
        &self,
        src: &str,
        ct: &OffsetDateTime,
    ) -> Result<OffsetDateTime, OffsetDateTime> {
        // What is the earliest event date we have recorded? (alt to latest)
        let latest = self
            .get_latest_report_date(src)
            .map_err(|_| OffsetDateTime::unix_epoch())?;
        let upto = ts_remove_hhmmss!(ct);

        info!(
            "Generating reports between: {:?} -> {:?}",
            latest.format("%F %T%z"),
            upto.format("%F %T%z")
        );

        let mut work_start = latest + Duration::from_secs(86400);
        // Get what days exist between latest report to ct.
        while (work_start < upto) {
            let work_end = work_start + Duration::from_secs(86400);
            // select all data from that range
            let data = self
                .get_event_range(src, &work_start, &work_end)
                .map_err(|_| work_start.clone())?;
            // min, max, avg for that meter.
            let (t_min, t_max, t_sum) =
                data.iter()
                    .fold((0xffff, 0, 0), |(t_min, t_max, t_sum), dbe| {
                        (
                            if dbe.temp < t_min { dbe.temp } else { t_min },
                            if dbe.temp > t_max { dbe.temp } else { t_max },
                            t_sum + dbe.temp,
                        )
                    });
            let t_avg = t_sum / data.len() as u16;
            debug!("t -> {:?}, {:?}, {:?}", t_min, t_max, t_avg);

            let (h_min, h_max, h_sum) =
                data.iter()
                    .fold((0xffff, 0, 0), |(h_min, h_max, h_sum), dbe| {
                        (
                            if dbe.hum < h_min { dbe.hum } else { h_min },
                            if dbe.hum > h_max { dbe.hum } else { h_max },
                            h_sum + dbe.hum,
                        )
                    });
            let h_avg = h_sum / data.len() as u16;
            debug!("h -> {:?}, {:?}, {:?}", h_min, h_max, h_avg);

            let (p_min, p_max, p_sum) =
                data.iter()
                    .fold((0xffff, 0, 0), |(p_min, p_max, p_sum), dbe| {
                        (
                            if dbe.ppm < p_min { dbe.ppm } else { p_min },
                            if dbe.ppm > p_max { dbe.ppm } else { p_max },
                            p_sum + dbe.ppm,
                        )
                    });
            let p_avg = p_sum / data.len() as u16;
            debug!("p -> {:?}, {:?}, {:?}", p_min, p_max, p_avg);
            let ts = work_start.format("%F %T%z");

            // write that as a history event, use work_start as the TS.
            let conn = self.get_conn().map_err(|_| work_start.clone())?;
            ensure_mac!(conn, &src, work_start.clone());

            conn.execute_named(
                "INSERT OR REPLACE INTO history_t (mac, t, temp_max, temp_min, temp_avg, ppm_max, ppm_min, ppm_avg, hum_max, hum_min, hum_avg) VALUES (:mac, :t, :temp_max, :temp_min, :temp_avg, :ppm_max, :ppm_min, :ppm_avg, :hum_max, :hum_min, :hum_avg)",
            &[
                (":mac", &src),
                (":t", &ts),
                (":temp_max", &t_max),
                (":temp_min", &t_min),
                (":temp_avg", &t_avg),
                (":ppm_max", &p_max),
                (":ppm_min", &p_min),
                (":ppm_avg", &p_avg),
                (":hum_max", &h_max),
                (":hum_min", &h_min),
                (":hum_avg", &h_avg),
            ])
            .map(|r| {
                debug!("insert -> {:?}", r);
                ()
            })
            .map_err(|e| {
                error!("sqlite execute_named error -> {:?}", e);
                work_start.clone()
            })?;

            // Increment to the next day.
            work_start = work_start + Duration::from_secs(86400);
        }
        Ok(work_start)
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
        // Get the current time and strip it to a day
        let ct = ts_remove_hhmmss!(OffsetDateTime::now_local());

        let meters = match self.db.list_meters() {
            Ok(meters) => meters,
            Err(_) => {
                error!("Unable to handle dbpurge due to meter list fail.");
                return;
            }
        };
        // for each meter
        meters.iter().for_each(|src| {
            // Process our historical data as needed. -> should return latest report date?
            let r = match self.db.extract_report(&src, &ct) {
                Ok(r) => {
                    info!("Extracted report for {:?}", src);
                    r
                }
                Err(r) => {
                    error!("Failed to extract report for {:?}", src);
                    r
                }
            };
            // purge older than 7 days from the now latest repport
            let purge_upto = r - Duration::from_secs(86400 * RETAIN_DAYS);
            match self.db.purge_older_than(&purge_upto) {
                Ok(_) => {}
                Err(_) => error!(
                    "Failed to purge up to {:?} for {:?}",
                    purge_upto.format("%F %T%z"),
                    src
                ),
            };
        });
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

    fn assert_latest_report_date(db: &Db, src: &str, expect: &str) {
        let latest = db
            .get_latest_report_date(src)
            .expect("Unable to get reportdate");
        let expect_ts = OffsetDateTime::parse(expect, "%F %T%z").expect("invalid ts");
        println!(
            "{:?} == {:?}",
            latest.format("%F %T%z"),
            expect_ts.format("%F %T%z")
        );
        assert!(latest == expect_ts)
    }

    fn generate_report(db: &Db, src: &str, upto: &str) {
        let upto_ts = OffsetDateTime::parse(upto, "%F %T%z").expect("invalid ts");
        db.extract_report(src, &upto_ts)
            .expect("report gen failed.");
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

    #[test]
    fn test_db_report_generation() {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = Db::new("").unwrap();
        let db = db.migrate().unwrap();

        // No report min, no data!
        assert!(Err(()) == db.get_latest_report_date("00:00:00:00:00:00"));

        // Add data
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-05 13:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-05 14:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-06 13:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-06 14:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-07 13:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-07 14:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-08 13:02:19+1000");
        add_sample_data(&db, [0; 6], 123, 415, 123, "2020-04-08 14:02:19+1000");

        // List meters, there should only be one.
        assert!(Ok(vec!["00:00:00:00:00:00".to_string()]) == db.list_meters());

        // Latest report date should be the day before as there is no reports.
        assert_latest_report_date(&db, "00:00:00:00:00:00", "2020-04-04 00:00:00+1000");

        // Generate a report. for 05 (because it's now 06)
        generate_report(&db, "00:00:00:00:00:00", "2020-04-06 00:00:00+1000");

        // The latest report date is now 5
        assert_latest_report_date(&db, "00:00:00:00:00:00", "2020-04-05 00:00:00+1000");

        // Show that from 08 it generates the 06 and 07 reports.
        generate_report(&db, "00:00:00:00:00:00", "2020-04-08 00:00:00+1000");

        // The latest report date is now 7
        assert_latest_report_date(&db, "00:00:00:00:00:00", "2020-04-07 00:00:00+1000");
    }
}
