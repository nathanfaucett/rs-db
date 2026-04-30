use crate::{
  BTree, BTreeError, BTreeExecutor, BTreeTransaction, TransactionPatch, commit_transaction_patch,
  merge_transaction_patch_range, patch_delete, patch_get, patch_insert, patch_remove,
};
use async_lock::RwLock;
use async_stream::stream;
use core::{borrow::Borrow, ops::RangeBounds};
use futures::Stream;

#[cfg(not(feature = "std"))]
use alloc::{collections::BTreeMap, sync::Arc};
#[cfg(feature = "std")]
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct MockBTree<K, V> {
  inner: Arc<RwLock<BTreeMap<K, V>>>,
}

impl<K, V> MockBTree<K, V>
where
  K: Ord,
{
  pub fn new() -> Self {
    Self {
      inner: Arc::new(RwLock::new(BTreeMap::new())),
    }
  }
}

impl<K, V> Default for MockBTree<K, V>
where
  K: Ord,
{
  fn default() -> Self {
    Self::new()
  }
}

#[derive(Debug, Clone)]
pub struct MockBTreeTransaction<K, V> {
  inner: Arc<RwLock<BTreeMap<K, V>>>,
  patch: TransactionPatch<K, V>,
}

impl<K, V> BTreeExecutor<K, V> for MockBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + Send + 'a,
  {
    let guard = self.inner.read().await;
    Ok(guard.get(key.borrow()).cloned())
  }

  async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError>
  where
    K: Ord,
  {
    let mut guard = self.inner.write().await;
    guard.insert(key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + Send + 'a,
  {
    let mut guard = self.inner.write().await;
    Ok(guard.remove(key.borrow()))
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = Result<(K, V), BTreeError>> + Send + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + Send + 'a,
  {
    let inner = self.inner.clone();
    stream! {
        let guard = inner.read().await;
        for (k, v) in guard.range(range) {
            yield Ok((k.clone(), v.clone()));
        }
    }
  }
}

impl<K, V> BTreeTransaction<K, V> for MockBTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn commit(self) -> Result<(), BTreeError> {
    let mut guard = self.inner.write().await;
    commit_transaction_patch(&mut guard, self.patch);
    Ok(())
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    Ok(())
  }
}

impl<K, V> BTreeExecutor<K, V> for MockBTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + Send + 'a,
  {
    if let Some(value) = patch_get(&self.patch, &key) {
      return Ok(Some(value));
    }
    let guard = self.inner.read().await;
    Ok(guard.get(key.borrow()).cloned())
  }

  async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError>
  where
    K: Ord,
  {
    patch_insert(&mut self.patch, key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord + Clone,
    Q: Borrow<K> + Send + 'a,
  {
    if let Some(value) = patch_remove(&mut self.patch, key.borrow().clone()) {
      return Ok(Some(value));
    }
    let guard = self.inner.read().await;
    let val = guard.get(key.borrow()).cloned();
    if let Some((owned_key, _)) = guard.get_key_value(key.borrow()) {
      patch_delete(&mut self.patch, owned_key.clone());
    }
    Ok(val)
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = Result<(K, V), BTreeError>> + Send + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + Send + 'a,
  {
    let inner = self.inner.clone();
    let patch = self.patch.clone();
    stream! {
      let guard = inner.read().await;
      let merged = merge_transaction_patch_range(&*guard, &patch, range);

      for (k, v) in merged {
        yield Ok((k, v));
      }
    }
  }
}

impl<K, V> BTree<K, V> for MockBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  type Transaction = MockBTreeTransaction<K, V>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    Ok(MockBTreeTransaction {
      inner: self.inner.clone(),
      patch: BTreeMap::new(),
    })
  }
}

// Mark the mock as a storage port for engine tests.
impl<K, V> crate::port::StoragePort<K, V> for MockBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
}

pub fn block_on<F: core::future::Future>(future: F) -> F::Output {
  use core::hint::spin_loop;
  use core::pin::pin;
  use core::task::{Context, Poll, Waker};

  let waker = Waker::noop();
  let mut context = Context::from_waker(waker);
  let mut future = pin!(future);

  loop {
    match future.as_mut().poll(&mut context) {
      Poll::Ready(output) => return output,
      Poll::Pending => spin_loop(),
    }
  }
}
