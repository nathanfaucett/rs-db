#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use core::{borrow::Borrow, error::Error, future::Future, ops::RangeBounds};
#[cfg(feature = "std")]
use std::boxed::Box;
#[cfg(feature = "std")]
use std::{borrow::Borrow, error::Error, future::Future, ops::RangeBounds};

use futures::Stream;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BTreeError {
  #[error("Conflict")]
  Conflict,

  #[error("Commit failed")]
  CommitFailed,

  #[error("Rollback failed")]
  RollbackFailed,

  #[error("Unsupported operation")]
  UnsupportedOperation,

  #[error("Other error: {0}")]
  Other(#[from] Box<dyn Error + Send + Sync>),
}

pub type BTreeResult<T> = Result<T, BTreeError>;

impl BTreeError {
  pub fn other<E>(error: E) -> Self
  where
    E: Error + Send + Sync + 'static,
  {
    BTreeError::Other(Box::new(error))
  }
}

pub trait BTreeExecutor<K, V>: Send + Sync {
  fn get<'a, Q>(&'a self, key: Q) -> impl Future<Output = BTreeResult<Option<V>>> + Send + 'a
  where
    K: Ord,
    Q: Borrow<K> + Send + 'a;

  fn insert<'a>(
    &'a mut self,
    key: K,
    value: V,
  ) -> impl Future<Output = BTreeResult<()>> + Send + 'a
  where
    K: Ord;

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl Future<Output = BTreeResult<Option<V>>> + Send + 'a
  where
    K: Ord,
    Q: Borrow<K> + Send + 'a;

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send + 'a
  where
    K: Ord,
    R: RangeBounds<K> + Send + 'a;
}

pub trait BTreeTransaction<K, V>: BTreeExecutor<K, V> {
  fn commit(self) -> impl Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized;

  fn rollback(self) -> impl Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized;
}

pub trait BTree<K, V>: BTreeExecutor<K, V> {
  type Transaction: BTreeTransaction<K, V>;

  fn transaction<'a>(&'a self) -> impl Future<Output = BTreeResult<Self::Transaction>> + Send + 'a;
}
