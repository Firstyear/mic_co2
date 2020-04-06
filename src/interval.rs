use crate::db;
use actix::prelude::*;
use std::time::Duration;

const PURGE_FREQUENCY: u64 = 14400;

pub struct IntervalActor {
    pub db_addr: Addr<db::DbActor>,
}

impl IntervalActor {
    fn purge(&mut self) {
        info!("Attempting db purge ...");
        // Make a purge request ...
        self.db_addr.do_send(db::DbPurgeEvent)
    }
}

impl Actor for IntervalActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("Started scheduled tasks ...");
        ctx.run_interval(Duration::from_secs(PURGE_FREQUENCY), move |act, _ctx| {
            act.purge();
        });
    }
}
