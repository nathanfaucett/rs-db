// Keep this crate root thin: implementation lives in `automerge_btree`.
extern crate alloc;

mod automerge_btree;
pub use automerge_btree::*;
