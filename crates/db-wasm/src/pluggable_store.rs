use async_stream::stream;
use core::borrow::Borrow;
use core::ops::RangeBounds;
use db_core::{BTreeError, BTreeResult, NamedTreeProvider, NamedTreeTransaction};
use db_engine::{EngineKey, EngineRow};
use db_in_memory::{InMemoryNamedBTree, InMemoryNamedTransaction};
use futures::{Stream, StreamExt, pin_mut};

use crate::store_adapter::{StoreAdapterCallbacks, StoreAdapterTransaction, StoreAdapterTree};

#[derive(Clone)]
pub enum PluggableBackendStore {
  InMemory(InMemoryNamedBTree<EngineKey, EngineRow>),
  External(StoreAdapterCallbacks),
}

pub enum PluggableBackendTransaction {
  InMemory(InMemoryNamedTransaction<EngineKey, EngineRow>),
  External(StoreAdapterTransaction),
}

pub enum PluggableBackendTree {
  External(StoreAdapterTree),
}

impl NamedTreeProvider<EngineKey, EngineRow> for PluggableBackendStore {
  type Tree = PluggableBackendTree;
  type Transaction = PluggableBackendTransaction;

  fn get_tree(
    &self,
    name: &str,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Tree>> + Send + '_ {
    let name = name.to_string();
    async move {
      match self {
        PluggableBackendStore::InMemory(_) => Err(BTreeError::UnsupportedOperation),
        PluggableBackendStore::External(adapter) => {
          let tree = adapter.get_tree(&name).await?;
          Ok(PluggableBackendTree::External(tree))
        }
      }
    }
  }

  async fn begin_transaction(&self) -> BTreeResult<Self::Transaction> {
    match self {
      PluggableBackendStore::InMemory(store) => {
        let tx = store.begin_transaction().await?;
        Ok(PluggableBackendTransaction::InMemory(tx))
      }
      PluggableBackendStore::External(adapter) => {
        let tx = adapter.begin_transaction().await?;
        Ok(PluggableBackendTransaction::External(tx))
      }
    }
  }
}

impl NamedTreeTransaction<EngineKey, EngineRow> for PluggableBackendTransaction {
  async fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> BTreeResult<Option<EngineRow>>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.get(tree, key).await,
      PluggableBackendTransaction::External(tx) => tx.get(tree, key).await,
    }
  }

  async fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: EngineRow,
  ) -> BTreeResult<()>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.insert(tree, key, value).await,
      PluggableBackendTransaction::External(tx) => tx.insert(tree, key, value).await,
    }
  }

  async fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> BTreeResult<Option<EngineRow>>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.remove(tree, key).await,
      PluggableBackendTransaction::External(tx) => tx.remove(tree, key).await,
    }
  }

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, EngineRow)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    stream! {
      match self {
        PluggableBackendTransaction::InMemory(tx) => {
          let rows = tx.range(tree, range);
          pin_mut!(rows);
          while let Some(item) = rows.next().await {
            yield item;
          }
        }
        PluggableBackendTransaction::External(tx) => {
          let rows = tx.range(tree, range);
          pin_mut!(rows);
          while let Some(item) = rows.next().await {
            yield item;
          }
        }
      }
    }
  }

  async fn commit(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.commit().await,
      PluggableBackendTransaction::External(tx) => NamedTreeTransaction::commit(tx).await,
    }
  }

  async fn rollback(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.rollback().await,
      PluggableBackendTransaction::External(tx) => NamedTreeTransaction::rollback(tx).await,
    }
  }
}

impl db_core::BTreeExecutor<EngineKey, EngineRow> for PluggableBackendTree {
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<EngineRow>>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    match self {
      PluggableBackendTree::External(tree) => tree.get(key).await,
    }
  }

  async fn insert(&mut self, key: EngineKey, value: EngineRow) -> BTreeResult<()>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTree::External(tree) => tree.insert(key, value).await,
    }
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<EngineRow>>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    match self {
      PluggableBackendTree::External(tree) => tree.remove(key).await,
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, EngineRow)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    stream! {
      match self {
        PluggableBackendTree::External(tree) => {
          let rows = tree.range(range);
          pin_mut!(rows);
          while let Some(item) = rows.next().await {
            yield item;
          }
        }
      }
    }
  }
}

impl db_core::BTree<EngineKey, EngineRow> for PluggableBackendTree {
  type Transaction = StoreAdapterTransaction;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    match self {
      PluggableBackendTree::External(tree) => tree.transaction().await,
    }
  }
}
