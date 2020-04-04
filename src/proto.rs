use bytes::BytesMut;
use std::convert::{TryFrom, TryInto};
use std::io;

use tokio_util::codec::{Decoder, Encoder};

pub struct MicCodec;

// Every frame is 17 bytes long.
const FRAME_LEN: usize = 17;

impl Decoder for MicCodec {
    type Item = MicFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() >= FRAME_LEN {
            let buf = src.split_to(FRAME_LEN);
            match Datum::try_from(buf.as_ref()) {
                Ok(data) => Ok(Some(MicFrame { data })),
                Err(e) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Parse error -> {:?}", e),
                )),
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder<()> for MicCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: (), dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Always return an error.
        Err(io::Error::new(
            io::ErrorKind::Other,
            "communication to mic readers is not supported!",
        ))
    }
}

#[derive(Debug)]
pub struct MicFrame {
    data: Datum,
}

#[derive(Debug)]
pub struct Datum {
    mac: [u8; 6],
    ppm: u16,
    humidity: u16,
    temp: u16,
}

impl TryFrom<&[u8]> for Datum {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        datum_parser(value).map(|(_, d)| d).map_err(|_e| ())
    }
}

impl Datum {
    // mac -> to_string
    // temp/humid to float
    // ppm
}

named!( datum_parser<&[u8], Datum>,
    do_parse!(
        mac: take!(6) >>
        _unknown: take!(2) >>
        _hdr1: take!(1) >>
        temp: u16!(nom::number::Endianness::Big) >>
        _hdr2: take!(1) >>
        humidity: u16!(nom::number::Endianness::Big) >>
        _hdr3: take!(1) >>
        ppm: u16!(nom::number::Endianness::Big) >> (
            Datum {
                mac: mac.try_into().unwrap(),
                ppm: ppm,
                humidity: humidity,
                temp: temp,
            }
        )
    )
);

#[cfg(test)]
mod tests {
    use crate::proto::{datum_parser, Datum};
    use std::convert::TryFrom;

    #[test]
    fn test_proto() {
        // 20 F8 5E BE 29 D8 0A 01 01 01 1B 02 02 79 03 02 9F
        let t1 = vec![
            0x20, 0xF8, 0x5E, 0xBE, 0x29, 0xD8, 0x0A, 0x01, 0x01, 0x01, 0x1B, 0x02, 0x02, 0x79,
            0x03, 0x02, 0x9F,
        ];

        let d1 = Datum::try_from(t1.as_slice()).unwrap();
        println!("{:?}", d1);
        assert!(d1.ppm == 671);
        assert!(d1.humidity == 633);
        assert!(d1.temp == 283);
        assert!(d1.mac == [0x20, 0xF8, 0x5E, 0xBE, 0x29, 0xD8,]);
    }
}
