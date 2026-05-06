#[cfg(not(feature = "std"))]
extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

#[cfg(feature = "automerge")]
use db_automerge::{
  AutomergeEngineStore, AutomergeEntry, DocumentChangeKey, DocumentChangeKeyCodec, VecBytesCodec,
};
use db_engine::{EngineDatabase, EngineKey, EngineRow, EngineValue};
#[cfg(feature = "automerge")]
use db_in_memory::InMemoryBTree;
use db_in_memory::InMemoryNamedBTree;
#[cfg(feature = "redb")]
use db_redb::{REDBBTree, REDBNamedBTree};
#[cfg(feature = "redb")]
use db_types::{EngineKeyCodec, EngineRowCodec};

/// Public facade error type.
#[derive(Debug)]
pub enum DatabaseError {
  Engine(String),
  Other(String),
}

impl fmt::Display for DatabaseError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DatabaseError::Engine(s) => write!(f, "engine error: {s}"),
      DatabaseError::Other(s) => write!(f, "{s}"),
    }
  }
}

impl From<db_engine::EngineError> for DatabaseError {
  fn from(e: db_engine::EngineError) -> Self {
    DatabaseError::Engine(format!("{e}"))
  }
}

/// Simple row type reusing EngineValue
pub type Row = Vec<EngineValue>;

pub type InMemoryEngineStore = InMemoryNamedBTree<EngineKey, EngineRow>;
#[cfg(feature = "redb")]
pub type RedbEngineStore = REDBNamedBTree<EngineKey, EngineRow, EngineKeyCodec, EngineRowCodec>;
#[cfg(all(feature = "automerge", feature = "redb"))]
pub type RedbAutomergeStore = AutomergeEngineStore<
  REDBBTree<DocumentChangeKey, Vec<u8>, DocumentChangeKeyCodec, VecBytesCodec>,
>;
#[cfg(feature = "automerge")]
pub type InMemoryAutomergeStore =
  AutomergeEngineStore<InMemoryBTree<DocumentChangeKey, AutomergeEntry>>;

/// Opaque database handle.
pub enum Database {
  InMemory(EngineDatabase<InMemoryEngineStore>),
  #[cfg(feature = "automerge")]
  AutomergeInMemory(EngineDatabase<InMemoryAutomergeStore>),
  #[cfg(all(feature = "automerge", feature = "redb"))]
  AutomergeRedb(EngineDatabase<RedbAutomergeStore>),
  #[cfg(feature = "redb")]
  Redb(EngineDatabase<RedbEngineStore>),
}

/// Transaction wrapper delegating to EngineTransaction
pub enum Transaction<'db> {
  InMemory(db_engine::EngineTransaction<'db, InMemoryEngineStore>),
  #[cfg(feature = "automerge")]
  AutomergeInMemory(db_engine::EngineTransaction<'db, InMemoryAutomergeStore>),
  #[cfg(all(feature = "automerge", feature = "redb"))]
  AutomergeRedb(db_engine::EngineTransaction<'db, RedbAutomergeStore>),
  #[cfg(feature = "redb")]
  Redb(db_engine::EngineTransaction<'db, RedbEngineStore>),
}
