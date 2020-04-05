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

use time::OffsetDateTime;
use std::time::Duration;

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
    // For now, show last 8 hours.
    let ct = OffsetDateTime::now_local();
    let lower = ct - Duration::from_secs(14400);

    let data = match state.


    let r = state.render_addr.send(render::RenderEvent).await;

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
        Err(e) => HttpResponse::InternalServerError()
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

    // UNDO THIS SHIT
    let addr = "127.0.0.1";
    let db_path = "/tmp/data.db";

    let http_bind = "127.0.0.1:8080";

    // let addr = "172.24.18.140";
    let port = 2014;
    let v4_addr = Ipv4Addr::from_str(addr).expect("Failed to parse socket addr");

    let sock_addr = (v4_addr, port);
    let sock = UdpSocket::bind(sock_addr).await.unwrap();

    let stream = UdpFramed::new(sock, MicCodec);

    // let db_path = "/data/micd.db";
    let db_addr = SyncArbiter::start(1, move || {
        db::DbActor::new(db_path).expect("Failed to start db thread")
    });
    let a_db_addr = db_addr.clone();
    let b_db_addr = db_addr.clone();
    info!("db located: {}", db_path);

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

    let render_addr = SyncArbiter::start(1, move || render::RenderActor {} );
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
            .route("", web::get().to(index_view))
            .route("/", web::get().to(index_view))
    });
    server.bind(http_bind).unwrap().run();

    info!("Micd udp listening on {}:{}", addr, port);
    info!("Micd http listening on http://{}", http_bind);

    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down");
    System::current().stop();
}
