use actix::prelude::*;
use futures_util::stream::StreamExt;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use tokio::net::UdpSocket;
use tokio_util::udp::UdpFramed;

use mic::prelude::*;

struct Server {}

impl Actor for Server {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "()")]
struct UdpEvent(pub Option<(MicFrame, SocketAddr)>);

impl Handler<UdpEvent> for Server {
    type Result = ();

    fn handle(&mut self, msg: UdpEvent, _: &mut Context<Self>) {
        println!("{:?}", msg.0);
        unimplemented!();
    }
}

#[actix_rt::main]
async fn main() {
    let addr = "172.24.18.140";
    let port = 2014;
    let v4_addr = Ipv4Addr::from_str(addr).expect("Failed to parse socket addr");

    let sock_addr = (v4_addr, port);
    let sock = UdpSocket::bind(sock_addr).await.unwrap();

    let stream = UdpFramed::new(sock, MicCodec);

    Server::create(move |ctx| {
        ctx.add_message_stream(
            // May need to box leak this still?
            stream.map(|r| match r {
                Ok((frame, addr)) => UdpEvent(Some((frame, addr))),
                Err(_) => UdpEvent(None),
            }),
        );
        Server {}
    });

    println!("Listening on {}:{}", addr, port);

    tokio::signal::ctrl_c().await.unwrap();
    println!("Ctrl-C received, shutting down");
    System::current().stop();
}
