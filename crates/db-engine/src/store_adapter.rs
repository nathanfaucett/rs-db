#![allow(clippy::manual_async_fn)]

use crate::index_maintainer::IndexMaintainer;
use crate::{EngineError, EngineKey, EngineRow, IndexSchema, StoreKey, StoreValue, TableSchema};
use core::future::Future;
use db_core::BTreeExecutor;
use futures::{StreamExt, pin_mut};

mod persistence;

pub use persistence::EngineStoreTransaction;

pub trait EngineStore: Clone + Send + Sync + 'static {
  type Transaction: EngineStoreTransaction + Send + 'static;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>>;
}

impl<T> EngineStore for T
where
  T: Clone + db_core::StoragePort<StoreKey, StoreValue> + Send + Sync + 'static,
{
  type Transaction = StoragePortTxn<<T as db_core::BTree<StoreKey, StoreValue>>::Transaction>;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>> {
    async move {
      self
        .transaction()
        .await
        .map(StoragePortTxn::new)
        .map_err(EngineError::from)
    }
  }
}

/// Wrapper around an adapter transaction that exposes the engine-level
/// `EngineStoreTransaction` API. This decouples engine code from requiring
/// adapter transaction types to implement the trait directly.
#[derive(Debug)]
pub struct StoragePortTxn<T> {
  inner: T,
}

impl<T> StoragePortTxn<T> {
  pub fn new(inner: T) -> Self {
    Self { inner }
  }
}

impl<T> From<T> for StoragePortTxn<T> {
  fn from(t: T) -> Self {
    StoragePortTxn::new(t)
  }
}

// Implement the lower-level BTree executor/transaction traits by delegating
// to the wrapped adapter transaction. This keeps the wrapper compatible with
// engine internals that still rely on low-level transaction behavior.
impl<T> db_core::BTreeExecutor<StoreKey, StoreValue> for StoragePortTxn<T>
where
  T: db_core::BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl Future<Output = Result<Option<StoreValue>, db_core::BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: core::borrow::Borrow<StoreKey> + Send + 'a,
  {
    self.inner.get(key)
  }

  fn insert(
    &mut self,
    key: StoreKey,
    value: StoreValue,
  ) -> impl Future<Output = Result<(), db_core::BTreeError>> + Send
  where
    StoreKey: Ord,
  {
    self.inner.insert(key, value)
  }

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl Future<Output = Result<Option<StoreValue>, db_core::BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: core::borrow::Borrow<StoreKey> + Send + 'a,
  {
    self.inner.remove(key)
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), db_core::BTreeError>> + Send + 'a
  where
    R: core::ops::RangeBounds<StoreKey> + Send + 'a,
  {
    self.inner.range(range)
  }
}

impl<T> db_core::BTreeTransaction<StoreKey, StoreValue> for StoragePortTxn<T>
where
  T: db_core::BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  fn commit(self) -> impl Future<Output = Result<(), db_core::BTreeError>> {
    async move { self.inner.commit().await }
  }

  fn rollback(self) -> impl Future<Output = Result<(), db_core::BTreeError>> {
    async move { self.inner.rollback().await }
  }
}

impl<T> EngineStoreTransaction for StoragePortTxn<T>
where
  T: db_core::BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  fn collect_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
    predicate: Option<crate::EnginePredicate>,
  ) -> impl Future<Output = Result<Vec<(EngineKey, EngineRow)>, EngineError>> + 'a {
    async move {
      let mut rows = Vec::new();
      let stream = EngineStoreTransaction::range_table_rows(self, table_name);
      pin_mut!(stream);

      while let Some(item) = stream.next().await {
        let (key, value) = item?;
        if let StoreKey::TableRow { primary_key, .. } = key
          && let StoreValue::Row(row) = value
          && predicate
            .as_ref()
            .is_none_or(|predicate| predicate.matches(&row))
        {
          rows.push((primary_key, row));
        }
      }

      Ok(rows)
    }
  }

  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let key = StoreKey::table_row(table_name.to_string(), primary_key.clone());
      match self.get(&key).await.map_err(EngineError::from)? {
        Some(StoreValue::Row(row)) => Ok(Some(row)),
        _ => Ok(None),
      }
    }
  }

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: EngineKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let table_key = StoreKey::table_row(table_name.to_string(), primary_key);
      self
        .insert(table_key, StoreValue::Row(row))
        .await
        .map_err(EngineError::from)
    }
  }

  fn delete_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
    row: &'a EngineRow,
    indexes: &'a [IndexSchema],
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let table_key = StoreKey::table_row(table_name.to_string(), primary_key.clone());

      self.remove(&table_key).await.map_err(EngineError::from)?;

      IndexMaintainer::remove_entries(self, indexes, row, primary_key).await?;

      Ok(())
    }
  }

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let schema_key = StoreKey::table_schema(schema.name.clone());
      self
        .insert(schema_key, StoreValue::TableSchema(schema))
        .await
        .map_err(EngineError::from)
    }
  }

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let schema_key = StoreKey::index_schema(schema.name.clone());
      self
        .insert(schema_key, StoreValue::IndexSchema(schema))
        .await
        .map_err(EngineError::from)
    }
  }

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a {
    async move { persistence::load_catalog_impl(self).await }
  }

  fn lookup_index_rows<'a>(
    &'a mut self,
    table_name: &'a str,
    index: &'a IndexSchema,
    predicate: &'a crate::query::EnginePredicate,
  ) -> impl Future<Output = Result<Vec<EngineRow>, EngineError>> + 'a {
    async move { persistence::lookup_index_rows_impl(self, table_name, index, predicate).await }
  }

  fn insert_raw<'a>(
    &'a mut self,
    key: StoreKey,
    value: StoreValue,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move { self.insert(key, value).await.map_err(EngineError::from) }
  }

  fn remove_raw<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl Future<Output = Result<Option<StoreValue>, EngineError>> + 'a
  where
    Q: core::borrow::Borrow<StoreKey> + Send + 'a,
  {
    async move { self.remove(key).await.map_err(EngineError::from) }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), EngineError>> + 'a
  where
    R: core::ops::RangeBounds<StoreKey> + Send + 'a,
  {
    self
      .inner
      .range(range)
      .map(|res| res.map_err(EngineError::from))
  }

  fn commit(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.commit().await.map_err(EngineError::from) }
  }

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.rollback().await.map_err(EngineError::from) }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    ColumnSchema, EngineKey, EngineType, EngineValue, IndexSchema, StoreKey, StoreValue,
    TableSchema,
  };
  use db_core::{BTree, BTreeExecutor};
  use db_in_memory::InMemoryBTree;
  use futures::executor::block_on;

  fn sample_table_schema() -> TableSchema {
    TableSchema {
      name: "users".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "name".into(),
          data_type: EngineType::Text,
        },
      ],
      primary_key: vec![0],
    }
  }

  #[test]
  fn load_catalog_returns_table_and_index_schemas() {
    block_on(async {
      let store: InMemoryBTree<StoreKey, StoreValue> = InMemoryBTree::new();
      let mut tx = store.transaction().await.expect("open tx");

      let table_schema = sample_table_schema();
      tx.insert(
        StoreKey::table_schema(table_schema.name.clone()),
        StoreValue::TableSchema(table_schema.clone()),
      )
      .await
      .expect("insert table schema");

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };

      tx.insert(
        StoreKey::index_schema(index_schema.name.clone()),
        StoreValue::IndexSchema(index_schema.clone()),
      )
      .await
      .expect("insert index schema");

      let (tables, indexes) = persistence::load_catalog_impl(&mut tx)
        .await
        .expect("load catalog");
      assert_eq!(tables, vec![table_schema]);
      assert_eq!(indexes, vec![index_schema]);
    });
  }

  #[test]
  fn lookup_index_rows_returns_matching_rows() {
    block_on(async {
      let store: InMemoryBTree<StoreKey, StoreValue> = InMemoryBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let row = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];
      let primary_key = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      tx.insert(
        StoreKey::table_row("users".into(), primary_key.clone()),
        StoreValue::Row(row.clone()),
      )
      .await
      .expect("insert row");

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };

      let index_key = index_schema.key_for(&row).expect("build index key");
      tx.insert(
        StoreKey::index_entry(index_schema.name.clone(), index_key.clone(), primary_key),
        StoreValue::IndexEntry,
      )
      .await
      .expect("insert index entry");

      let predicate = crate::query::EnginePredicate::Equals(1, EngineValue::Text("Alice".into()));
      let rows = persistence::lookup_index_rows_impl(&mut tx, "users", &index_schema, &predicate)
        .await
        .expect("lookup index rows");

      assert_eq!(rows, vec![row]);
    });
  }

  #[test]
  fn engine_transaction_insert_commit_and_get_table_row() {
    block_on(async {
      let store: InMemoryBTree<StoreKey, StoreValue> = InMemoryBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let pk = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let row = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];

      tx.insert_table_row("users", pk.clone(), row.clone())
        .await
        .expect("insert table row");

      super::persistence::EngineStoreTransaction::commit(tx)
        .await
        .expect("commit");

      let mut tx2 = store.engine_transaction().await.expect("open tx");
      let got = tx2
        .get_table_row("users", &pk)
        .await
        .expect("get table row");
      assert_eq!(got, Some(row));
    });
  }

  #[test]
  fn engine_transaction_index_entry_helpers() {
    block_on(async {
      let store: InMemoryBTree<StoreKey, StoreValue> = InMemoryBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };

      let index_key = EngineKey::from_values(vec![EngineValue::Text("Alice".into())]);
      let primary_key_a = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let primary_key_b = EngineKey::from_values(vec![EngineValue::Integer(2)]);

      tx.insert_index_entry(&index_schema, &index_key, &primary_key_a)
        .await
        .expect("insert index entry");

      assert!(
        tx.find_conflicting_index_entry(&index_schema, &index_key, &primary_key_a)
          .await
          .expect("find conflicting entry")
          .is_none()
      );

      assert_eq!(
        tx.find_conflicting_index_entry(&index_schema, &index_key, &primary_key_b)
          .await
          .expect("find conflicting entry"),
        Some(primary_key_a)
      );
    });
  }

  #[test]
  fn engine_transaction_range_helpers() {
    block_on(async {
      let store: InMemoryBTree<StoreKey, StoreValue> = InMemoryBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let user_pk = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let user_row = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];
      tx.insert_table_row("users", user_pk.clone(), user_row.clone())
        .await
        .expect("insert user row");

      let order_pk = EngineKey::from_values(vec![EngineValue::Integer(10)]);
      let order_row = vec![EngineValue::Integer(10), EngineValue::Text("OrderA".into())];
      tx.insert_table_row("orders", order_pk.clone(), order_row.clone())
        .await
        .expect("insert order row");

      let rows = {
        let mut rows = Vec::new();
        let stream = tx.range_table_rows("users");
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
          let (key, value) = item.expect("range failed");
          if let StoreKey::TableRow { primary_key, .. } = key
            && let StoreValue::Row(row) = value
          {
            rows.push((primary_key, row));
          }
        }
        rows
      };

      assert_eq!(rows, vec![(user_pk.clone(), user_row.clone())]);

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };
      let index_key = EngineKey::from_values(vec![EngineValue::Text("Alice".into())]);
      tx.insert_index_entry(&index_schema, &index_key, &user_pk)
        .await
        .expect("insert index entry");

      let entries = {
        let mut entries = Vec::new();
        let stream = tx.range_index_entries(&index_schema);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
          let (key, value) = item.expect("range failed");
          if let StoreKey::IndexEntry { row_pk, .. } = key
            && let StoreValue::IndexEntry = value
          {
            entries.push(row_pk);
          }
        }
        entries
      };

      assert_eq!(entries, vec![user_pk]);
    });
  }

  #[test]
  fn engine_transaction_get_and_insert_row_helper() {
    block_on(async {
      let store: InMemoryBTree<StoreKey, StoreValue> = InMemoryBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let pk = EngineKey::from_values(vec![EngineValue::Integer(2)]);
      let row = vec![EngineValue::Integer(2), EngineValue::Text("Bob".into())];

      tx.insert_row("users", pk.clone(), row.clone())
        .await
        .expect("insert row");

      super::persistence::EngineStoreTransaction::commit(tx)
        .await
        .expect("commit");

      let mut tx2 = store.engine_transaction().await.expect("open tx");
      let got = tx2.get_row("users", &pk).await.expect("get row");
      assert_eq!(got, Some(row));
    });
  }
}
