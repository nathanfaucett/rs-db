#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use db_engine::{EngineDatabase, EngineQuery, EngineResult, EngineValue, StoreKey, StoreValue};
use db_in_memory::InMemoryBTree;
#[cfg(feature = "redb")]
use db_redb::REDBBTree;
#[cfg(feature = "redb")]
use std::path::Path;

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

/// Opaque database handle.
pub enum Database {
  InMemory(EngineDatabase<InMemoryBTree<StoreKey, StoreValue>>),
  #[cfg(feature = "redb")]
  Redb(EngineDatabase<REDBBTree<StoreKey, StoreValue>>),
}

impl Database {
  /// Open an in-memory database (dev/test convenience).
  pub async fn open_in_memory() -> Result<Self, DatabaseError> {
    let store: InMemoryBTree<StoreKey, StoreValue> = InMemoryBTree::new();
    let engine = EngineDatabase::new(store);
    Ok(Self::InMemory(engine))
  }

  #[cfg(feature = "redb")]
  pub async fn open_in_redb(
    path: impl AsRef<Path>,
    table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let store =
      REDBBTree::open(path, table_name).map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let engine = EngineDatabase::new(store);
    Ok(Self::Redb(engine))
  }

  /// Register a table schema with the engine.
  pub async fn register_table(
    &mut self,
    schema: db_engine::TableSchema,
  ) -> Result<(), DatabaseError> {
    match self {
      Database::InMemory(engine) => engine.register_table(schema).await?,
      #[cfg(feature = "redb")]
      Database::Redb(engine) => engine.register_table(schema).await?,
    }
    Ok(())
  }

  /// Execute an `EngineQuery` directly against the engine.
  pub async fn execute_query(&self, query: EngineQuery) -> Result<EngineResult, DatabaseError> {
    let res = match self {
      Database::InMemory(engine) => engine.execute(query).await?,
      #[cfg(feature = "redb")]
      Database::Redb(engine) => engine.execute(query).await?,
    };
    Ok(res)
  }

  /// Convenience: run a closure in a transaction context.
  pub async fn transaction<F, Fut, T>(&self, f: F) -> Result<T, DatabaseError>
  where
    F: FnOnce(&mut Transaction<'_>) -> Fut,
    Fut: core::future::Future<Output = Result<T, DatabaseError>>,
  {
    let mut tx = match self {
      Database::InMemory(engine) => Transaction::InMemory(engine.transaction()),
      #[cfg(feature = "redb")]
      Database::Redb(engine) => Transaction::Redb(engine.transaction()),
    };
    let out = f(&mut tx).await;
    match out {
      Ok(v) => {
        tx.commit().await?;
        Ok(v)
      }
      Err(e) => {
        let _ = tx.rollback().await;
        Err(e)
      }
    }
  }
}

/// Transaction wrapper delegating to EngineTransaction
pub enum Transaction<'db> {
  InMemory(db_engine::EngineTransaction<'db, InMemoryBTree<StoreKey, StoreValue>>),
  #[cfg(feature = "redb")]
  Redb(db_engine::EngineTransaction<'db, REDBBTree<StoreKey, StoreValue>>),
}

impl<'db> Transaction<'db> {
  pub async fn insert_row(&mut self, table: &str, row: Row) -> Result<(), DatabaseError> {
    match self {
      Transaction::InMemory(inner) => inner.insert_row(table, row).await?,
      #[cfg(feature = "redb")]
      Transaction::Redb(inner) => inner.insert_row(table, row).await?,
    }
    Ok(())
  }

  pub async fn commit(self) -> Result<(), DatabaseError> {
    match self {
      Transaction::InMemory(inner) => inner.commit().await?,
      #[cfg(feature = "redb")]
      Transaction::Redb(inner) => inner.commit().await?,
    }
    Ok(())
  }

  pub async fn rollback(self) -> Result<(), DatabaseError> {
    match self {
      Transaction::InMemory(inner) => inner.rollback().await?,
      #[cfg(feature = "redb")]
      Transaction::Redb(inner) => inner.rollback().await?,
    }
    Ok(())
  }
}
