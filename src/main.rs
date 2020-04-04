#[macro_use]
extern crate log;

use actix::prelude::*;
use futures_util::stream::StreamExt;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use tokio::net::UdpSocket;
use tokio_util::udp::UdpFramed;

use mic::prelude::*;

mod db;
mod interval;

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
    let addr = "172.24.18.140";
    let port = 2014;
    let v4_addr = Ipv4Addr::from_str(addr).expect("Failed to parse socket addr");

    let sock_addr = (v4_addr, port);
    let sock = UdpSocket::bind(sock_addr).await.unwrap();

    let stream = UdpFramed::new(sock, MicCodec);

    let db_path = "/data/micd.db";
    let db_addr = SyncArbiter::start(1, move || {
        db::DbActor::new(db_path).expect("Failed to start db thread")
    });
    info!("db located: {}", db_path);

    Server::create(move |ctx| {
        ctx.add_message_stream(
            // May need to box leak this still?
            stream.map(|r| match r {
                Ok((frame, addr)) => UdpEvent(Some((frame, addr))),
                Err(_) => UdpEvent(None),
            }),
        );
        Server { db_addr }
    });

    info!("Listening on {}:{}", addr, port);

    tokio::signal::ctrl_c().await.unwrap();
    info!("Ctrl-C received, shutting down");
    System::current().stop();
}
