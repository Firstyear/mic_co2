#[macro_use]
extern crate log;

use actix::prelude::*;
use futures_util::stream::StreamExt;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use tokio::net::UdpSocket;
use tokio_util::udp::UdpFramed;

use actix_files as fs;
use actix_web::web::{self, Data, HttpResponse};
use actix_web::{middleware, App, HttpServer};
use askama::Template;

use std::time::Duration;
use time::OffsetDateTime;

use mic::prelude::*;

mod db;
mod interval;
mod render;

/* == FE web server == */

struct AppState {
    render_addr: Addr<render::RenderActor>,
    db_addr: Addr<db::DbActor>,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {}

async fn index_view(state: Data<AppState>) -> HttpResponse {
    // Show last day in detail
    let ct = OffsetDateTime::now_local();
    let lower = ct - Duration::from_secs(86400);

    // get_history

    let data = match state
        .db_addr
        .send(db::DbEventRange {
            src: "20:F8:5E:BE:29:D8".to_string(),
            max: ct,
            min: lower,
        })
        .await
    {
        Ok(Ok(d)) => d,
        _ => {
            error!("db unable to complete!");
            return HttpResponse::InternalServerError()
                .content_type("text/html")
                .body("db failure");
        }
    };

    let history = match state
        .db_addr
        .send(db::DbHistory {
            src: "20:F8:5E:BE:29:D8".to_string(),
        })
        .await
    {
        Ok(Ok(d)) => d,
        _ => {
            error!("db unable to complete!");
            return HttpResponse::InternalServerError()
                .content_type("text/html")
                .body("db failure");
        }
    };

    let r = state
        .render_addr
        .send(render::RenderEvent {
            src: "20:F8:5E:BE:29:D8".to_string(),
            data,
            history,
        })
        .await;

    match r {
        Ok(_) => info!("Render thread complete, sending ..."),
        Err(_) => {
            error!("render unable to complete!");
            return HttpResponse::InternalServerError()
                .content_type("text/html")
                .body("renderer failure");
        }
    };

    let t = IndexTemplate {};
    match t.render() {
        Ok(s) => HttpResponse::Ok().content_type("text/html").body(s),
        Err(_e) => HttpResponse::InternalServerError()
            .content_type("text/html")
            .body("template failure"),
    }
}

/* == UDP socket server == */

struct Server {
    db_addr: Addr<db::DbActor>,
}

impl Actor for Server {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "()")]
struct UdpEvent(pub Option<(MicFrame, SocketAddr)>);

impl Handler<UdpEvent> for Server {
    type Result = ();

    fn handle(&mut self, msg: UdpEvent, _: &mut Context<Self>) {
        match msg.0 {
            Some((frame, addr)) => {
                debug!("{:?} <- {:?}", frame, addr);
                self.db_addr.do_send(db::DbAddDatumEvent(frame.data, addr));
            }
            _ => {
                error!("An invalid frame was recieved");
            }
        }
    }
}

#[actix_rt::main]
async fn main() {
    env_logger::init();

    let http_bind = if cfg!(debug_assertions) {
        "127.0.0.1:8082"
    } else {
        "[2001:44b8:2155:2c10:5054:ff:fef4:34af]:8082"
    };

    let addr = if cfg!(debug_assertions) {
        "127.0.0.1"
    } else {
        "172.24.18.140"
    };
    let port = 2014;

    let db_path = if cfg!(debug_assertions) {
        "/tmp/micd.db"
    } else {
        "/data/micd.db"
    };

    info!("Micd udp listening on {}:{}", addr, port);
    info!("Micd http listening on http://{}", http_bind);
    info!("Micd db: {}", db_path);

    let v4_addr = Ipv4Addr::from_str(addr).expect("Failed to parse socket addr");

    let sock_addr = (v4_addr, port);
    let sock = UdpSocket::bind(sock_addr).await.unwrap();

    let stream = UdpFramed::new(sock, MicCodec);

    let db_addr = SyncArbiter::start(1, move || {
        db::DbActor::new(db_path).expect("Failed to start db thread")
    });
    let a_db_addr = db_addr.clone();
    let b_db_addr = db_addr.clone();
    let c_db_addr = db_addr.clone();

    // This runs the scheduled tasks
    let ia = interval::IntervalActor { db_addr: c_db_addr };
    let _ = ia.start();

    Server::create(move |ctx| {
        ctx.add_message_stream(
            // May need to box leak this still?
            stream.map(|r| match r {
                Ok((frame, addr)) => UdpEvent(Some((frame, addr))),
                Err(_) => UdpEvent(None),
            }),
        );
        Server { db_addr: a_db_addr }
    });

    let render_addr = SyncArbiter::start(1, move || render::RenderActor {});
    let a_render_addr = render_addr.clone();

    // Main actix threads are up, get's the webui cracking.

    let server = HttpServer::new(move || {
        App::new()
            .data(AppState {
                render_addr: a_render_addr.clone(),
                db_addr: b_db_addr.clone(),
            })
            .wrap(middleware::Logger::default())
            .service(fs::Files::new("/static", "./static"))
            .service(fs::Files::new("/render", "./data/render"))
            .route("", web::get().to(index_view))
            .route("/", web::get().to(index_view))
    });
    server.bind(http_bind).unwrap().run();

    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down");
    System::current().stop();
}
