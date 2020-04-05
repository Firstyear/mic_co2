use actix::prelude::*;
use gnuplot::{Caption, Color, Figure};
use std::iter::once;

use crate::db;

#[derive(Message)]
#[rtype(result = "Result<(), ()>")]
pub struct RenderEvent {
    pub data: Vec<db::DbEvent>,
}

impl Actor for RenderActor {
    type Context = SyncContext<Self>;
}

pub struct RenderActor;

impl Handler<RenderEvent> for RenderActor {
    type Result = Result<(), ()>;

    fn handle(&mut self, msg: RenderEvent, _: &mut SyncContext<Self>) -> Result<(), ()> {
        // Add a 0,0 point.
        let ts_init = match msg.data.first() {
            Some(t) => t.time.timestamp(),
            None => {
                error!("no data to render");
                return Err(());
            }
        };

        let x: Vec<i64> = once(ts_init)
            .chain(msg.data.iter().map(|dbe| dbe.time.timestamp()))
            .collect();
        // This should be f32
        let ppm_y: Vec<u16> = once(0).chain(msg.data.iter().map(|dbe| dbe.ppm)).collect();
        let hum_y: Vec<f32> = once(0.0)
            .chain(msg.data.iter().map(|dbe| (dbe.hum as f32) / 10.0))
            .collect();
        let temp_y: Vec<f32> = once(0.0)
            .chain(msg.data.iter().map(|dbe| (dbe.temp as f32) / 10.0))
            .collect();

        let mut fg = Figure::new();
        fg.axes2d()
            .lines(&x, &ppm_y, &[Caption("ppm"), Color("black")]);
        fg.save_to_png("./data/render/ppm.png", 800, 600)
            .map(|_| {
                info!("gnuplotlob success");
                ()
            })
            .map_err(|e| {
                error!("gnuplotlib error -> {:?}", e);
                ()
            })?;

        let mut fg = Figure::new();
        fg.axes2d()
            .lines(&x, &hum_y, &[Caption("hum"), Color("black")]);
        fg.save_to_png("./data/render/hum.png", 800, 600)
            .map(|_| {
                info!("gnuplotlob success");
                ()
            })
            .map_err(|e| {
                error!("gnuplotlib error -> {:?}", e);
                ()
            })?;

        let mut fg = Figure::new();
        fg.axes2d()
            .lines(&x, &temp_y, &[Caption("temp"), Color("black")]);
        fg.save_to_png("./data/render/temp.png", 800, 600)
            .map(|_| {
                info!("gnuplotlob success");
                ()
            })
            .map_err(|e| {
                error!("gnuplotlib error -> {:?}", e);
                ()
            })
    }
}
