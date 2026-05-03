#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(feature = "automerge")]
use db_automerge::{
  AutomergeEngineStore, AutomergeEntry, DocumentChangeKey, DocumentChangeKeyCodec, VecBytesCodec,
};
use db_engine::{EngineDatabase, EngineKey, EngineQuery, EngineResult, EngineRow};
#[cfg(feature = "redb")]
use db_engine::{EngineKeyCodec, EngineRowCodec};
#[cfg(feature = "automerge")]
use db_in_memory::InMemoryBTree;
use db_in_memory::InMemoryNamedBTree;
#[cfg(feature = "redb")]
use db_redb::{REDBBTree, REDBNamedBTree};
#[cfg(feature = "redb")]
use std::path::Path;

use db_sql_to_engine::SchemaResolver;

use super::dispatch;
use super::types::{Database, DatabaseError, Row, Transaction};

impl SchemaResolver for Database {
  fn describe_table(&self, name: &str) -> Option<db_engine::TableSchema> {
    dispatch::describe_table(self, name)
  }
}

impl Database {
  /// Open an in-memory database (dev/test convenience).
  pub async fn open_in_memory() -> Result<Self, DatabaseError> {
    let store = InMemoryNamedBTree::new();
    let engine = EngineDatabase::new(store);
    Ok(Self::InMemory(engine))
  }

  /// Open an Automerge-backed database (feature-gated).
  #[cfg(feature = "automerge")]
  pub async fn open_automerge_in_memory() -> Result<Self, DatabaseError> {
    let backend = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store);
    Ok(Self::AutomergeInMemory(engine))
  }

  #[cfg(all(feature = "automerge", feature = "redb"))]
  pub async fn open_automerge_with_redb(
    path: impl AsRef<Path>,
    table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let backend = REDBBTree::<
      DocumentChangeKey,
      AutomergeEntry,
      DocumentChangeKeyCodec,
      VecBytesCodec,
    >::open_with_codecs(path, table_name)
    .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store);
    Ok(Self::AutomergeRedb(engine))
  }

  #[cfg(feature = "redb")]
  pub async fn open_in_redb(
    path: impl AsRef<Path>,
    _table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let store =
      REDBNamedBTree::<EngineKey, EngineRow, EngineKeyCodec, EngineRowCodec>::open_with_codecs(
        path,
      )
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let engine = EngineDatabase::new(store);
    Ok(Self::Redb(engine))
  }

  /// Register a table schema with the engine.
  pub async fn register_table(
    &mut self,
    schema: db_engine::TableSchema,
  ) -> Result<(), DatabaseError> {
    dispatch::register_table(self, schema).await
  }

  /// Execute an `EngineQuery` directly against the engine.
  pub async fn execute_query(&self, query: EngineQuery) -> Result<EngineResult, DatabaseError> {
    dispatch::execute_query(self, query).await
  }

  /// Execute a SQL string using the database schema catalog.
  pub async fn execute_sql(&mut self, sql: &str) -> Result<EngineResult, DatabaseError> {
    dispatch::execute_sql(self, sql).await
  }

  /// Convenience: run a closure in a transaction context.
  pub async fn transaction<F, Fut, T>(&self, f: F) -> Result<T, DatabaseError>
  where
    F: FnOnce(&mut Transaction<'_>) -> Fut,
    Fut: core::future::Future<Output = Result<T, DatabaseError>>,
  {
    let mut tx = dispatch::begin_transaction(self);
    let out = f(&mut tx).await;
    match out {
      Ok(v) => {
        dispatch::transaction_commit(tx).await?;
        Ok(v)
      }
      Err(e) => {
        let _ = dispatch::transaction_rollback(tx).await;
        Err(e)
      }
    }
  }
}

impl<'db> Transaction<'db> {
  pub async fn insert_row(&mut self, table: &str, row: Row) -> Result<(), DatabaseError> {
    dispatch::transaction_insert_row(self, table, row).await
  }

  /// Execute a SQL string inside this transaction. Supports INSERT/UPDATE/DELETE.
  /// SELECT is not supported in a write transaction — use `Database::execute_sql`.
  pub async fn execute_sql(
    &mut self,
    resolver: &dyn SchemaResolver,
    sql: &str,
  ) -> Result<EngineResult, DatabaseError> {
    dispatch::transaction_execute_sql(self, resolver, sql).await
  }

  pub async fn commit(self) -> Result<(), DatabaseError> {
    dispatch::transaction_commit(self).await
  }

  pub async fn rollback(self) -> Result<(), DatabaseError> {
    dispatch::transaction_rollback(self).await
  }
}
