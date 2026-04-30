use core::{borrow::Borrow, marker::PhantomData, ops::RangeBounds};
use futures::Stream;

use crate::store_adapter::EngineStore;
use crate::store_adapter::EngineStoreTransaction;
use crate::{EngineError, EngineKey, EngineRow, IndexSchema};
use db_core::{BTree, BTreeError};

pub struct StoreFacade<B, K, V> {
  backend: B,
  _marker: PhantomData<(K, V)>,
}

/// High-level facade tailored for engine callers. This wraps an `EngineStore`
/// (adapter) and exposes simple table-row helpers that hide codec/adapter
/// transaction plumbing behind a small, stable API.
pub struct EngineStoreFacade<S> {
  store: S,
}

impl<S> EngineStoreFacade<S>
where
  S: EngineStore + Clone + Send + Sync + 'static,
{
  pub fn new(store: S) -> Self {
    Self { store }
  }

  pub async fn get_table_row(
    &self,
    table_name: &str,
    primary_key: &EngineKey,
  ) -> Result<Option<EngineRow>, EngineError> {
    let mut tx = self.store.engine_transaction().await?;

    tx.get_table_row(table_name, primary_key).await
  }

  pub async fn insert_table_row(
    &self,
    table_name: &str,
    primary_key: EngineKey,
    row: EngineRow,
  ) -> Result<(), EngineError> {
    let mut tx = self.store.engine_transaction().await?;
    let result = tx.insert_table_row(table_name, primary_key, row).await;

    if result.is_ok() {
      tx.commit().await?;
    } else {
      let _ = tx.rollback().await;
    }

    result
  }

  pub async fn delete_row(
    &self,
    table_name: &str,
    primary_key: &EngineKey,
    row: &EngineRow,
    indexes: &[IndexSchema],
  ) -> Result<(), EngineError> {
    let mut tx = self.store.engine_transaction().await?;
    let result = tx.delete_row(table_name, primary_key, row, indexes).await;

    if result.is_ok() {
      tx.commit().await?;
    } else {
      let _ = tx.rollback().await;
    }

    result
  }

  pub async fn collect_table_rows(
    &self,
    table_name: &str,
    predicate: Option<crate::EnginePredicate>,
  ) -> Result<Vec<(EngineKey, EngineRow)>, EngineError> {
    let mut tx = self.store.engine_transaction().await?;

    tx.collect_table_rows(table_name, predicate).await
  }
}

impl<B, K, V> StoreFacade<B, K, V>
where
  B: BTree<K, V> + Send + Sync,
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  pub fn new(backend: B) -> Self {
    Self {
      backend,
      _marker: PhantomData,
    }
  }

  pub async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + Send + 'a,
  {
    self.backend.get(key).await
  }

  pub async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError>
  where
    K: Ord,
  {
    self.backend.insert(key, value).await
  }

  pub async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + Send + 'a,
  {
    self.backend.remove(key).await
  }

  pub fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(K, V), BTreeError>> + Send + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + Send + 'a,
  {
    self.backend.range(range)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_core::block_on;
  use futures::StreamExt;

  #[cfg(not(feature = "std"))]
  extern crate alloc;
  #[cfg(not(feature = "std"))]
  use alloc::vec::Vec;
  #[cfg(feature = "std")]
  use std::vec::Vec;

  #[test]
  fn facade_point_ops_inmemory() {
    block_on(async {
      let backend = db_in_memory::InMemoryBTree::<u64, u64>::new();
      let mut facade = StoreFacade::new(backend);

      facade.insert(1, 100).await.expect("insert");
      assert_eq!(facade.get(&1).await.expect("get failed"), Some(100));

      let removed = facade.remove(&1).await.expect("remove failed");
      assert_eq!(removed, Some(100));
    });
  }

  #[test]
  fn facade_range_inmemory() {
    block_on(async {
      let backend = db_in_memory::InMemoryBTree::<u64, u64>::new();
      let mut facade = StoreFacade::new(backend);

      facade.insert(1, 100).await.expect("insert");
      facade.insert(2, 200).await.expect("insert");
      facade.insert(3, 300).await.expect("insert");

      let mut items = Vec::new();
      let stream = facade.range(1..3);
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (k, v) = item.expect("range failed");
        items.push((k, v));
      }

      assert_eq!(items, vec![(1u64, 100u64), (2u64, 200u64)]);
    });
  }

  #[test]
  fn engine_store_facade_insert_and_get_row() {
    block_on(async {
      use crate::{EngineKey, EngineValue, StoreKey, StoreValue};

      let store: db_in_memory::InMemoryBTree<StoreKey, StoreValue> =
        db_in_memory::InMemoryBTree::new();
      let facade = EngineStoreFacade::new(store.clone());

      let pk = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let row = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];

      facade
        .insert_table_row("users", pk.clone(), row.clone())
        .await
        .expect("insert table row");

      let got = facade
        .get_table_row("users", &pk)
        .await
        .expect("get table row");

      assert_eq!(got, Some(row));
    });
  }

  #[test]
  fn engine_store_facade_collect_table_rows() {
    block_on(async {
      use crate::{EngineKey, EngineValue, StoreKey, StoreValue};

      let store: db_in_memory::InMemoryBTree<StoreKey, StoreValue> =
        db_in_memory::InMemoryBTree::new();
      let facade = EngineStoreFacade::new(store.clone());

      let pk1 = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let row1 = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];

      let pk2 = EngineKey::from_values(vec![EngineValue::Integer(2)]);
      let row2 = vec![EngineValue::Integer(2), EngineValue::Text("Bob".into())];

      facade
        .insert_table_row("users", pk1.clone(), row1.clone())
        .await
        .expect("insert 1");

      facade
        .insert_table_row("users", pk2.clone(), row2.clone())
        .await
        .expect("insert 2");

      let rows = facade
        .collect_table_rows("users", None)
        .await
        .expect("collect rows");

      assert!(rows.contains(&(pk1, row1)));
      assert!(rows.contains(&(pk2, row2)));
    });
  }
}
