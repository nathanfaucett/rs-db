#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(feature = "automerge")]
use db_automerge::{AutomergeEngineStore, AutomergeEntry, DocumentChangeKey};
use db_engine::{EngineDatabase, EngineQuery, EngineResult};
#[cfg(feature = "redb")]
use db_engine::{EngineKey, EngineRow};
#[cfg(feature = "automerge")]
use db_in_memory::InMemoryBTree;
use db_in_memory::InMemoryNamedBTree;
#[cfg(feature = "redb")]
use db_redb::{REDBBTree, REDBNamedBTree};
#[cfg(feature = "redb")]
use db_types::{EngineKeyCodec, EngineRowCodec};
#[cfg(feature = "redb")]
use std::path::Path;

use db_sql_to_engine::SchemaResolver;

use super::dispatch;
#[cfg(all(feature = "automerge", feature = "redb"))]
use super::types::RedbAutomergeStore;
#[cfg(feature = "redb")]
use super::types::RedbEngineStore;
use super::types::{Database, DatabaseError, FacadeStore, InMemoryEngineStore, Row, Transaction};
#[cfg(feature = "automerge")]
use super::types::{FacadeDocumentChangeKeyCodec, FacadeVecBytesCodec, InMemoryAutomergeStore};

impl<S> SchemaResolver for Database<S>
where
  S: FacadeStore,
{
  fn describe_table(&self, name: &str) -> Option<db_engine::TableSchema> {
    dispatch::describe_table(self, name)
  }
}

impl Database<InMemoryEngineStore> {
  /// Open an in-memory database (dev/test convenience).
  pub async fn open_in_memory() -> Result<Self, DatabaseError> {
    let store = InMemoryNamedBTree::new();
    let engine = EngineDatabase::new(store);
    Ok(Self { engine })
  }
}

#[cfg(feature = "automerge")]
impl Database<InMemoryAutomergeStore> {
  /// Open an Automerge-backed database (feature-gated).
  pub async fn open_automerge_in_memory() -> Result<Self, DatabaseError> {
    let backend = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store);
    Ok(Self { engine })
  }
}

#[cfg(all(feature = "automerge", feature = "redb"))]
impl Database<RedbAutomergeStore> {
  pub async fn open_automerge_with_redb(
    path: impl AsRef<Path>,
    table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let backend = REDBBTree::<
      DocumentChangeKey,
      AutomergeEntry,
      FacadeDocumentChangeKeyCodec,
      FacadeVecBytesCodec,
    >::open_with_codecs(path, table_name)
    .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store);
    Ok(Self { engine })
  }
}

#[cfg(feature = "redb")]
impl Database<RedbEngineStore> {
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
    Ok(Self { engine })
  }
}

impl<S> Database<S>
where
  S: FacadeStore,
{
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
    F: FnOnce(&mut Transaction<'_, S>) -> Fut,
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

impl<'db, S> Transaction<'db, S>
where
  S: FacadeStore,
{
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

#[cfg(all(test, feature = "automerge"))]
mod tests {
  use super::*;
  use futures::executor::block_on;

  #[test]
  fn automerge_two_create_tables() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory().await.expect("open");
      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);")
        .await
        .expect("create orders");
    });
  }

  #[test]
  fn automerge_in_memory_join_returns_two_rows() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);")
        .await
        .expect("create orders");

      db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .await
        .expect("insert user 1");
      db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
        .await
        .expect("insert user 2");

      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (1,1,100);")
        .await
        .expect("insert order 1");
      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (2,2,200);")
        .await
        .expect("insert order 2");

      let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
      let res = db.execute_sql(sql).await.expect("execute select");
      assert_eq!(res.rows.len(), 2);
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_join_returns_two_rows() {
    block_on(async {
      let mut path = std::env::temp_dir();
      path.push(format!(
        "aicacia_automerge_redb_test_{}.db",
        std::time::SystemTime::now()
          .duration_since(std::time::UNIX_EPOCH)
          .expect("time went backwards")
          .as_nanos()
      ));
      let _ = std::fs::remove_file(&path);

      let mut db = Database::open_automerge_with_redb(path, "automerge_store")
        .await
        .expect("open automerge redb");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);")
        .await
        .expect("create orders");

      db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .await
        .expect("insert user 1");
      db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
        .await
        .expect("insert user 2");

      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (1,1,100);")
        .await
        .expect("insert order 1");
      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (2,2,200);")
        .await
        .expect("insert order 2");

      let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
      let res = db.execute_sql(sql).await.expect("execute select");
      assert_eq!(res.rows.len(), 2);
    });
  }
}
