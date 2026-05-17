#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{format, vec::Vec};
#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(feature = "automerge")]
use db_automerge::{AutoCommit, AutomergeEngineStore, AutomergeEntry, DocumentChangeKey};
#[cfg(feature = "automerge")]
use db_core::{BTree, BTreeExecutor, BTreeTransaction};
#[cfg(feature = "redb")]
use db_engine::EngineKey;
use db_engine::{
  EngineDatabase, EngineQuery, EngineResult, IndexSchema, Subscriber, SubscriptionId, SyncScope,
  TableSchema,
};
#[cfg(feature = "automerge")]
use db_in_memory::InMemoryBTree;
use db_in_memory::InMemoryNamedBTree;
#[cfg(feature = "redb")]
use db_redb::{REDBBTree, REDBNamedBTree};
#[cfg(feature = "redb")]
use db_types::EngineKeyCodec;
#[cfg(feature = "automerge")]
use std::collections::BTreeMap;
#[cfg(feature = "redb")]
use std::path::Path;

#[cfg(feature = "automerge")]
use futures::StreamExt;
#[cfg(feature = "automerge")]
use uuid::Uuid;

use db_sql_to_engine::{
  CanonicalStatement, DdlOp, SchemaResolver, parse_and_translate, parse_and_translate_statement,
};

#[cfg(all(feature = "automerge", feature = "redb"))]
use super::types::RedbAutomergeStore;
#[cfg(feature = "redb")]
use super::types::RedbEngineStore;
#[cfg(feature = "automerge")]
use super::types::{AutomergeSyncMetrics, InMemoryAutomergeStore};
use super::types::{Database, DatabaseError, FacadeStore, InMemoryEngineStore, Row, Transaction};
#[cfg(all(feature = "automerge", feature = "redb"))]
use super::types::{FacadeDocumentChangeKeyCodec, FacadeVecBytesCodec};

impl<S> SchemaResolver for Database<S>
where
  S: FacadeStore,
{
  fn describe_table(&self, name: &str) -> Option<TableSchema> {
    self.engine.describe_table(name)
  }
}

impl Database<InMemoryEngineStore> {
  pub fn open_in_memory_sync() -> Self {
    let store = InMemoryNamedBTree::new();
    let engine = EngineDatabase::new(store);
    Self { engine }
  }

  /// Open an in-memory database (dev/test convenience).
  pub async fn open_in_memory() -> Result<Self, DatabaseError> {
    Ok(Self::open_in_memory_sync())
  }
}

#[cfg(feature = "automerge")]
async fn collect_documents<B>(
  store: &AutomergeEngineStore<B>,
) -> Result<BTreeMap<Uuid, AutoCommit>, DatabaseError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let guard = store.automerge.read().await;
  let stream = guard.range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
  futures::pin_mut!(stream);

  let mut docs = BTreeMap::new();
  while let Some(item) = stream.next().await {
    let (doc_id, doc) = match item {
      Ok(pair) => pair,
      Err(_) if docs.is_empty() => return Ok(docs),
      Err(err) => return Err(DatabaseError::Engine(format!("{err}"))),
    };
    docs.insert(doc_id, doc);
  }

  Ok(docs)
}

#[cfg(feature = "automerge")]
async fn apply_documents<B>(
  store: &AutomergeEngineStore<B>,
  docs: &BTreeMap<Uuid, AutoCommit>,
) -> Result<(), DatabaseError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let guard = store.automerge.read().await;
  let mut tx = guard
    .transaction()
    .await
    .map_err(|e| DatabaseError::Engine(format!("{e}")))?;

  for (doc_id, doc) in docs {
    tx.insert(*doc_id, doc.clone())
      .await
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
  }

  tx.commit()
    .await
    .map_err(|e| DatabaseError::Engine(format!("{e}")))?;

  Ok(())
}

#[cfg(feature = "automerge")]
async fn sync_automerge_stores<B>(
  left: &AutomergeEngineStore<B>,
  right: &AutomergeEngineStore<B>,
) -> Result<(), DatabaseError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let left_docs = collect_documents(left).await?;
  let right_docs = collect_documents(right).await?;

  let mut merged_docs = left_docs;
  for (doc_id, mut right_doc) in right_docs {
    if let Some(left_doc) = merged_docs.get_mut(&doc_id) {
      left_doc
        .merge(&mut right_doc)
        .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    } else {
      merged_docs.insert(doc_id, right_doc);
    }
  }

  apply_documents(left, &merged_docs).await?;
  apply_documents(right, &merged_docs).await?;

  Ok(())
}

#[cfg(feature = "automerge")]
fn automerge_metrics(docs: &BTreeMap<Uuid, AutoCommit>) -> AutomergeSyncMetrics {
  let document_count = docs.len();
  let total_document_bytes = docs
    .values()
    .map(|doc| {
      let mut copy = doc.clone();
      copy.save().len()
    })
    .sum();

  AutomergeSyncMetrics {
    document_count,
    total_document_bytes,
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

  /// Merge Automerge documents from each peer and reload schema on both sides.
  pub async fn sync_with(&mut self, other: &mut Self) -> Result<(), DatabaseError> {
    sync_automerge_stores(self.engine.store(), other.engine.store()).await?;
    self.engine.reload_schema().await?;
    other.engine.reload_schema().await?;
    Ok(())
  }

  pub async fn automerge_sync_metrics(&self) -> Result<AutomergeSyncMetrics, DatabaseError> {
    let docs = collect_documents(self.engine.store()).await?;
    Ok(automerge_metrics(&docs))
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

  /// Merge Automerge documents from each peer and reload schema on both sides.
  pub async fn sync_with(&mut self, other: &mut Self) -> Result<(), DatabaseError> {
    sync_automerge_stores(self.engine.store(), other.engine.store()).await?;
    self.engine.reload_schema().await?;
    other.engine.reload_schema().await?;
    Ok(())
  }

  pub async fn automerge_sync_metrics(&self) -> Result<AutomergeSyncMetrics, DatabaseError> {
    let docs = collect_documents(self.engine.store()).await?;
    Ok(automerge_metrics(&docs))
  }
}

#[cfg(feature = "redb")]
impl Database<RedbEngineStore> {
  pub async fn open_in_redb(
    path: impl AsRef<Path>,
    _table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let store = REDBNamedBTree::<EngineKey, Vec<u8>, EngineKeyCodec>::open_with_codecs(path)
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let engine = EngineDatabase::new(store);
    Ok(Self { engine })
  }
}

impl<S> Database<S>
where
  S: FacadeStore,
{
  pub fn from_store(store: S) -> Self {
    let engine = EngineDatabase::new(store);
    Self { engine }
  }

  pub async fn open_with_store(store: S) -> Result<Self, DatabaseError> {
    let engine = EngineDatabase::open(store).await?;
    Ok(Self { engine })
  }

  pub fn describe_table(&self, table_name: &str) -> Option<TableSchema> {
    self.engine.describe_table(table_name)
  }

  /// Register a table schema with the engine.
  pub async fn register_table(&mut self, schema: TableSchema) -> Result<(), DatabaseError> {
    self.engine.register_table(schema).await?;
    Ok(())
  }

  pub async fn drop_table(&mut self, table_name: &str) -> Result<(), DatabaseError> {
    self.engine.drop_table(table_name).await?;
    Ok(())
  }

  pub async fn register_index(&mut self, schema: IndexSchema) -> Result<(), DatabaseError> {
    self.engine.register_index(schema).await?;
    Ok(())
  }

  pub async fn drop_index(&mut self, index_name: &str) -> Result<(), DatabaseError> {
    self.engine.drop_index(index_name).await?;
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

  /// Subscribe to an `EngineQuery` with optional scope.
  pub async fn subscribe_query(
    &self,
    query: EngineQuery,
    subscriber: std::sync::Arc<dyn Subscriber>,
    scope: Option<SyncScope>,
  ) -> Result<SubscriptionId, DatabaseError> {
    let scope = scope.unwrap_or_default();
    self
      .engine
      .subscribe(query, &scope, subscriber)
      .await
      .map_err(Into::into)
  }

  /// Subscribe to a SQL SELECT string with optional scope.
  /// Returns an error if the SQL is not a SELECT statement.
  pub async fn subscribe_sql(
    &self,
    sql: &str,
    subscriber: std::sync::Arc<dyn Subscriber>,
    scope: Option<SyncScope>,
  ) -> Result<SubscriptionId, DatabaseError> {
    let query = match parse_and_translate_statement(sql, self) {
      Ok(CanonicalStatement::Query(q)) => q,
      Ok(_) => {
        return Err(DatabaseError::Other(
          "subscribe_sql only accepts SELECT statements".into(),
        ));
      }
      Err(e) => return Err(DatabaseError::Other(format!("{e}"))),
    };
    self.subscribe_query(query, subscriber, scope).await
  }

  /// Subscribe to a query (unrestricted).
  pub async fn subscribe_unrestricted(
    &self,
    query: EngineQuery,
    subscriber: std::sync::Arc<dyn Subscriber>,
  ) -> Result<SubscriptionId, DatabaseError> {
    self.subscribe_query(query, subscriber, None).await
  }

  /// Unsubscribe from a subscription.
  pub async fn unsubscribe(&self, id: SubscriptionId) -> Result<(), DatabaseError> {
    self.engine.unsubscribe(id).await.map_err(Into::into)
  }

  /// Execute a query with scope.
  pub async fn execute_with_scope(
    &self,
    query: EngineQuery,
    scope: &SyncScope,
  ) -> Result<EngineResult, DatabaseError> {
    self
      .engine
      .execute_with_scope(query, scope)
      .await
      .map_err(Into::into)
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
        joins,
        from_tables,
        returning,
      } => {
        let result = self
          .inner
          .update_rows_with_sources_and_returning(
            &table,
            assignments,
            predicate,
            joins,
            from_tables,
            returning,
          )
          .await?;
        Ok(result)
      }
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => self
        .inner
        .delete_rows_with_returning(&table, predicate, returning)
        .await
        .map_err(Into::into),
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
