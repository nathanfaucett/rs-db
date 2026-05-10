#[cfg(not(feature = "std"))]
extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

#[cfg(feature = "automerge")]
use db_automerge::{AutomergeEngineStore, AutomergeEntry, DocumentChangeKey};
use db_engine::{EngineDatabase, EngineQuery, EngineResult, TableSchema};
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

use db_sql_to_engine::{
  CanonicalStatement, DdlOp, SchemaResolver, parse_and_translate, parse_and_translate_statement,
};

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
  fn describe_table(&self, name: &str) -> Option<TableSchema> {
    self.engine.describe_table(name)
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
  pub async fn register_table(&mut self, schema: TableSchema) -> Result<(), DatabaseError> {
    self.engine.register_table(schema).await?;
    Ok(())
  }

  /// Execute an `EngineQuery` directly against the engine.
  pub async fn execute_query(&self, query: EngineQuery) -> Result<EngineResult, DatabaseError> {
    let result = self.engine.execute(query).await?;
    Ok(result)
  }

  /// Execute a SQL string using the database schema catalog.
  pub async fn execute_sql(&mut self, sql: &str) -> Result<EngineResult, DatabaseError> {
    match parse_and_translate_statement(sql, self) {
      Ok(CanonicalStatement::Query(query)) => self.execute_query(query).await,
      Ok(CanonicalStatement::Ddl(DdlOp::CreateTable(schema))) => {
        self.register_table(schema).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropTable(name))) => {
        self.engine.drop_table(&name).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::CreateIndex(schema))) => {
        self.engine.register_index(schema).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropIndex(name))) => {
        self.engine.drop_index(&name).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Err(e) => Err(DatabaseError::Other(format!("{e}"))),
    }
  }

  /// Convenience: run a closure in a transaction context.
  pub async fn transaction<F, Fut, T>(&self, f: F) -> Result<T, DatabaseError>
  where
    F: FnOnce(&mut Transaction<'_, S>) -> Fut,
    Fut: core::future::Future<Output = Result<T, DatabaseError>>,
  {
    let mut tx = Transaction {
      inner: self.engine.transaction(),
    };
    let out = f(&mut tx).await;
    match out {
      Ok(v) => {
        tx.inner.commit().await?;
        Ok(v)
      }
      Err(e) => {
        let _ = tx.inner.rollback().await;
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
    self.inner.insert_row(table, row).await?;
    Ok(())
  }

  /// Execute a SQL string inside this transaction. Supports INSERT/UPDATE/DELETE.
  /// SELECT is not supported in a write transaction — use `Database::execute_sql`.
  pub async fn execute_sql(
    &mut self,
    resolver: &dyn SchemaResolver,
    sql: &str,
  ) -> Result<EngineResult, DatabaseError> {
    let query =
      parse_and_translate(sql, resolver).map_err(|e| DatabaseError::Other(format!("{e}")))?;
    match query {
      EngineQuery::Insert { table, row } => {
        self.insert_row(&table, row).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      EngineQuery::Update {
        table,
        assignments,
        predicate,
      } => {
        self
          .inner
          .update_rows(&table, assignments, predicate)
          .await?;
        Ok(EngineResult::new(Vec::new()))
      }
      EngineQuery::Delete { table, predicate } => {
        self.inner.delete_rows(&table, predicate).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      EngineQuery::Select { .. } => Err(DatabaseError::Other(
        "SELECT inside transaction not supported; use Database::execute_sql instead".into(),
      )),
    }
  }

  pub async fn commit(self) -> Result<(), DatabaseError> {
    self.inner.commit().await?;
    Ok(())
  }

  pub async fn rollback(self) -> Result<(), DatabaseError> {
    self.inner.rollback().await?;
    Ok(())
  }
}

#[cfg(all(test, feature = "automerge"))]
mod tests {
  use super::*;
  use db_automerge::AutoCommit;
  use db_core::{BTree, BTreeExecutor, BTreeTransaction};
  use db_engine::EngineValue;
  use futures::{StreamExt, executor::block_on};

  use std::collections::BTreeMap;
  #[cfg(feature = "redb")]
  use std::fs;
  use std::path::PathBuf;
  use std::sync::atomic::{AtomicU64, Ordering};
  use std::time::{SystemTime, UNIX_EPOCH};

  use uuid::Uuid;

  fn temp_redb_path(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system time after unix epoch")
      .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    path.push(format!(
      "aicacia_automerge_sync_{}_{}_{}.db",
      label, nanos, id
    ));
    path
  }

  async fn collect_documents<S, B, F>(
    database: &Database<S>,
    get_store: &F,
  ) -> BTreeMap<Uuid, AutoCommit>
  where
    S: FacadeStore,
    B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
    F: Fn(&Database<S>) -> &AutomergeEngineStore<B>,
  {
    let store = get_store(database);
    let guard = store.automerge.read().await;
    let stream = guard.range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
    futures::pin_mut!(stream);

    let mut docs = BTreeMap::new();
    while let Some(item) = stream.next().await {
      let (doc_id, doc) = match item {
        Ok(pair) => pair,
        Err(_) if docs.is_empty() => return docs,
        Err(err) => panic!("collect documents: {err}"),
      };
      docs.insert(doc_id, doc);
    }

    docs
  }

  async fn apply_documents<S, B, F>(
    database: &Database<S>,
    docs: &BTreeMap<Uuid, AutoCommit>,
    get_store: &F,
  ) where
    S: FacadeStore,
    B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
    F: Fn(&Database<S>) -> &AutomergeEngineStore<B>,
  {
    let store = get_store(database);
    let guard = store.automerge.read().await;
    let mut tx = guard.transaction().await.expect("start automerge tx");

    for (doc_id, doc) in docs {
      tx.insert(*doc_id, doc.clone()).await.expect("insert doc");
    }

    tx.commit().await.expect("commit automerge tx");
  }

  async fn sync_databases<S, B, F>(left: &mut Database<S>, right: &mut Database<S>, get_store: &F)
  where
    S: FacadeStore,
    B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
    F: Fn(&Database<S>) -> &AutomergeEngineStore<B>,
  {
    let left_docs = collect_documents(left, get_store).await;
    let right_docs = collect_documents(right, get_store).await;

    apply_documents(right, &left_docs, get_store).await;
    apply_documents(left, &right_docs, get_store).await;

    left
      .engine
      .reload_schema()
      .await
      .expect("reload left schema");
    right
      .engine
      .reload_schema()
      .await
      .expect("reload right schema");
  }

  async fn assert_users<S>(database: &mut Database<S>)
  where
    S: FacadeStore,
  {
    let alice = database
      .execute_sql("SELECT name FROM users WHERE id = 1;")
      .await
      .expect("select alice");
    assert_eq!(alice.rows, vec![vec![EngineValue::Text("Alice".into())]]);

    let bob = database
      .execute_sql("SELECT name FROM users WHERE id = 2;")
      .await
      .expect("select bob");
    assert_eq!(bob.rows, vec![vec![EngineValue::Text("Bob".into())]]);

    let all = database
      .execute_sql("SELECT id, name FROM users;")
      .await
      .expect("select users");
    let mut rows = all.rows;
    rows.sort_by(|left, right| format!("{:?}", left).cmp(&format!("{:?}", right)));
    assert_eq!(
      rows,
      vec![
        vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())],
        vec![EngineValue::Integer(2), EngineValue::Text("Bob".into())],
      ]
    );
  }

  async fn offline_sync_scenario<S, B, F>(
    mut left: Database<S>,
    mut right: Database<S>,
    get_store: &F,
  ) where
    S: FacadeStore,
    B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
    F: Fn(&Database<S>) -> &AutomergeEngineStore<B>,
  {
    left
      .execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
      .await
      .expect("create users on left");

    sync_databases(&mut left, &mut right, get_store).await;

    right
      .execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
      .await
      .expect("insert bob on right");
    left
      .execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
      .await
      .expect("insert alice on left");

    sync_databases(&mut left, &mut right, get_store).await;

    assert_users(&mut left).await;
    assert_users(&mut right).await;

    sync_databases(&mut left, &mut right, get_store).await;

    assert_users(&mut left).await;
    assert_users(&mut right).await;
  }

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

  #[test]
  fn automerge_in_memory_offline_sync_converges() {
    block_on(async {
      let left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      offline_sync_scenario(left, right, &|db| db.engine.store()).await;
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_join_returns_two_rows() {
    block_on(async {
      let path = temp_redb_path("join");
      let _ = fs::remove_file(&path);

      let mut db = Database::open_automerge_with_redb(&path, "automerge_store")
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

      let _ = fs::remove_file(path);
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_converges() {
    block_on(async {
      let left_path = temp_redb_path("left");
      let right_path = temp_redb_path("right");

      let left = Database::open_automerge_with_redb(&left_path, "automerge_store")
        .await
        .expect("open left redb db");
      let right = Database::open_automerge_with_redb(&right_path, "automerge_store")
        .await
        .expect("open right redb db");

      offline_sync_scenario(left, right, &|db| db.engine.store()).await;

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }
}
