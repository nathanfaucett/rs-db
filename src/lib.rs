#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod database_dispatch;
mod database_types;
mod facade;
pub use database_types::{Database, DatabaseError, Row};
