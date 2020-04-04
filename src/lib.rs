#[macro_use]
extern crate log;
#[macro_use]
extern crate nom;

pub mod proto;

pub mod prelude {
    pub use crate::proto::*;
}
