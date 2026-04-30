#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

pub mod codec;
pub mod persistence;
pub mod schema;
pub mod store;

pub use schema::{ColumnSchema, IndexSchema, TableSchema};
pub use store::{StoreKey, StoreValue};
