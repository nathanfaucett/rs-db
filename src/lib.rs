#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub use db_facade::{Database, DatabaseError, Row, SqlParams};
