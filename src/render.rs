use actix::prelude::*;
use gnuplot::AxesCommon;
use gnuplot::{AutoOption, Caption, Color, Figure, LabelOption, Tick, TickOption};
use std::iter::once;

use crate::db;

const PNG_WIDTH: u32 = 1400;
const PNG_HEIGHT: u32 = 800;

const SHORT_DIFF: i64 = 900;
const LONG_DIFF: i64 = 86400;

#[derive(Message)]
#[rtype(result = "Result<(), ()>")]
pub struct RenderEvent {
    pub src: String,
    pub data: Vec<db::DbEvent>,
    pub history: Vec<db::DbHistoryEvent>,
}

impl Actor for RenderActor {
    type Context = SyncContext<Self>;
}

pub struct RenderActor;

macro_rules! format_chart {
    ($axis:expr, $ticks:expr, $y_lab:expr) => {
        $axis
            .set_x_label("Time", &[])
            .set_x_ticks_custom(
                $ticks,
                &[TickOption::OnAxis(false), TickOption::Inward(true)],
                &[
                    LabelOption::TextOffset(-1.0, -1.0),
                    LabelOption::Font("Helvetica", 10.0),
                    LabelOption::Rotate(280.0),
                ],
            )
            .set_y_label($y_lab, &[])
            .set_y_ticks(
                Some((AutoOption::Auto, 5)),
                &[
                    TickOption::OnAxis(false),
                    TickOption::Mirror(true),
                    TickOption::Inward(false),
                ],
                &[],
            )
            .set_x_axis(false, &[])
    };
}

fn render_single_figure(
    title: &str,
    caption: &str,
    colour: &str,
    x: &[i64],
    y: &[f32],
    ticks: &[Tick<i64, String>],
    path: &str,
) -> Result<(), ()> {
    let mut fg = Figure::new();
    format_chart!(
        fg.axes2d().lines(x, y, &[Caption(caption), Color(colour)]),
        ticks,
        title
    );

    fg.save_to_svg(path, PNG_WIDTH, PNG_HEIGHT)
        .map(|_| {
            info!("gnuplotlob success");
            ()
        })
        .map_err(|e| {
            error!("gnuplotlib error -> {:?}", e);
            ()
        })
}

fn render_triple_figure(
    title: &str,
    src: &str,
    colour: &str,
    x: &[i64],
    y_min: &[f32],
    y_max: &[f32],
    y_avg: &[f32],
    ticks: &[Tick<i64, String>],
    path: &str,
) -> Result<(), ()> {
    // get the captions
    let cap_min = format!("min - {}", src);
    let cap_max = format!("max - {}", src);
    let cap_avg = format!("avg - {}", src);

    let mut fg = Figure::new();
    format_chart!(
        fg.axes2d()
            .lines(x, y_min, &[Caption(cap_min.as_str()), Color(colour)]),
        ticks,
        title
    )
    .lines(x, y_max, &[Caption(cap_max.as_str()), Color(colour)])
    .lines(x, y_avg, &[Caption(cap_avg.as_str()), Color(colour)]);

    fg.save_to_svg(path, PNG_WIDTH, PNG_HEIGHT)
        .map(|_| {
            info!("gnuplotlob success");
            ()
        })
        .map_err(|e| {
            error!("gnuplotlib error -> {:?}", e);
            ()
        })
}

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

        // Generate the time/tick scale.
        let mut last_step = 0;
        let ticks: Vec<Tick<i64, String>> = msg
            .data
            .iter()
            .filter_map(|dbe| {
                if dbe.time.timestamp() >= last_step + SHORT_DIFF {
                    last_step = dbe.time.timestamp();
                    Some(Tick::Major(
                        dbe.time.timestamp(),
                        AutoOption::Fix(dbe.time.format(db::TFMT)),
                    ))
                } else {
                    None
                }
            })
            .collect();

        let x: Vec<i64> = once(ts_init)
            .chain(msg.data.iter().map(|dbe| dbe.time.timestamp()))
            .collect();

        let ppm_y: Vec<f32> = once(0.0)
            .chain(msg.data.iter().map(|dbe| dbe.ppm as f32))
            .collect();
        let hum_y: Vec<f32> = once(0.0)
            .chain(msg.data.iter().map(|dbe| (dbe.hum as f32) / 10.0))
            .collect();
        let temp_y: Vec<f32> = once(0.0)
            .chain(msg.data.iter().map(|dbe| (dbe.temp as f32) / 10.0))
            .collect();

        render_single_figure(
            "CO2 PPM",
            msg.src.as_str(),
            "black",
            x.as_slice(),
            ppm_y.as_slice(),
            ticks.as_slice(),
            "./data/render/ppm.svg",
        )?;

        render_single_figure(
            "Relative (%)",
            msg.src.as_str(),
            "black",
            x.as_slice(),
            hum_y.as_slice(),
            ticks.as_slice(),
            "./data/render/hum.svg",
        )?;

        render_single_figure(
            "Degrees (C)",
            msg.src.as_str(),
            "black",
            x.as_slice(),
            temp_y.as_slice(),
            ticks.as_slice(),
            "./data/render/temp.svg",
        )?;

        // === now we render the historical info ===

        let ts_init = match msg.history.first() {
            Some(t) => t.time.timestamp(),
            None => {
                error!("no history data to render");
                // We return ok here, because it's okay to not have the history
                return Ok(());
            }
        };

        let mut last_step = 0;
        let ticks: Vec<Tick<i64, String>> = msg
            .history
            .iter()
            .filter_map(|dbe| {
                if dbe.time.timestamp() >= last_step + LONG_DIFF {
                    last_step = dbe.time.timestamp();
                    Some(Tick::Major(
                        dbe.time.timestamp(),
                        AutoOption::Fix(dbe.time.format(db::TFMT)),
                    ))
                } else {
                    None
                }
            })
            .collect();

        let x: Vec<i64> = once(ts_init)
            .chain(msg.history.iter().map(|dbe| dbe.time.timestamp()))
            .collect();

        // Now we have to make a lot of data sets ..........
        let ppm_min_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| dbe.ppm_min as f32))
            .collect();
        let ppm_max_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| dbe.ppm_max as f32))
            .collect();
        let ppm_avg_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| dbe.ppm_avg as f32))
            .collect();

        let hum_min_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| (dbe.hum_min as f32) / 10.0))
            .collect();
        let hum_max_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| (dbe.hum_max as f32) / 10.0))
            .collect();
        let hum_avg_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| (dbe.hum_avg as f32) / 10.0))
            .collect();

        let temp_min_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| (dbe.temp_min as f32) / 10.0))
            .collect();
        let temp_max_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| (dbe.temp_max as f32) / 10.0))
            .collect();
        let temp_avg_y: Vec<f32> = once(0.0)
            .chain(msg.history.iter().map(|dbe| (dbe.temp_avg as f32) / 10.0))
            .collect();

        render_triple_figure(
            "CO2 PPM",
            msg.src.as_str(),
            "black",
            x.as_slice(),
            ppm_min_y.as_slice(),
            ppm_max_y.as_slice(),
            ppm_avg_y.as_slice(),
            ticks.as_slice(),
            "./data/render/ppm_history.svg",
        )?;

        render_triple_figure(
            "Relative (%)",
            msg.src.as_str(),
            "black",
            x.as_slice(),
            hum_min_y.as_slice(),
            hum_max_y.as_slice(),
            hum_avg_y.as_slice(),
            ticks.as_slice(),
            "./data/render/hum_history.svg",
        )?;

        render_triple_figure(
            "Degrees (C)",
            msg.src.as_str(),
            "black",
            x.as_slice(),
            temp_min_y.as_slice(),
            temp_max_y.as_slice(),
            temp_avg_y.as_slice(),
            ticks.as_slice(),
            "./data/render/temp_history.svg",
        )?;

        Ok(())
    }
}
