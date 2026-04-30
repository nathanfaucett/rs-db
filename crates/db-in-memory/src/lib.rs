#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod in_memory_btree;

pub use in_memory_btree::{InMemoryBTree, InMemoryBTreeTransaction};
