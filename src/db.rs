use actix::prelude::*;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::NO_PARAMS;
use std::net::SocketAddr;
use time::OffsetDateTime;

use mic::prelude::*;

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

    fn migrate(&self) -> Result<Self, ()> {
        // Create our tables if needed.
        /*
         * METER
         *  - id
         *  - mac
         *  - option<label>
         *
         * EVENT
         *  - fk meter?
         *  - timestamp (utc)
         *  - temp u16
         *  - ppm u16
         *  - hum u16
         */

        unimplemented!();
    }

    fn purge(&self, ct: OffsetDateTime) {
        unimplemented!();
    }

    fn add_datum(&self, datum: Datum, ct: OffsetDateTime) {
        unimplemented!();
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
        self.db.purge(ct)
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
    use crate::db::Db;

    #[test]
    fn test_db_create() {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = Db::new("").unwrap();

        db.migrate().unwrap();
    }
}
