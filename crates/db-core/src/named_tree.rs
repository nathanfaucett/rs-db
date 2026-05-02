use core::future::Future;
use core::ops::RangeBounds;

use futures::Stream;

use crate::btree::{BTree, BTreeResult};

/// An atomic transaction that spans multiple named trees within a single backend.
///
/// All mutations are buffered and applied together on [`commit`]. A [`rollback`]
/// discards all pending changes.
pub trait NamedTreeTransaction<K, V>: Send + 'static {
  fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a K,
  ) -> impl Future<Output = BTreeResult<Option<V>>> + Send + 'a
  where
    K: Ord;

  fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: K,
    value: V,
  ) -> impl Future<Output = BTreeResult<()>> + Send + 'a
  where
    K: Ord;

  fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a K,
  ) -> impl Future<Output = BTreeResult<Option<V>>> + Send + 'a
  where
    K: Ord;

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(K, V)>> + Send + 'a
  where
    K: Ord,
    R: RangeBounds<K> + Send + 'a;

  fn commit(self) -> impl Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized;

  fn rollback(self) -> impl Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized;
}

/// A factory that opens or creates a named logical tree from the backend.
///
/// Backends implement this trait to expose multiple isolated key-value trees
/// identified by a string name. This allows the engine to treat each table,
/// index, and schema namespace as a separate tree without encoding routing
/// information inside the key type.
///
/// # Backend contract
///
/// - Two calls to `get_tree` with the same `name` MUST return handles that
///   share the same physical storage.
/// - Two calls with different names MUST be isolated from each other.
/// - Ordering within a tree is determined solely by the tree's own key codec;
///   no cross-tree ordering is required.
/// - Mutations through [`begin_transaction`] MUST be atomic: either all
///   changes across all trees commit together, or none do.
pub trait NamedTreeProvider<K, V>: Clone + Send + Sync {
  type Tree: BTree<K, V>;
  type Transaction: NamedTreeTransaction<K, V>;

  fn get_tree(&self, name: &str) -> impl Future<Output = BTreeResult<Self::Tree>> + Send + '_;

  fn begin_transaction(&self) -> impl Future<Output = BTreeResult<Self::Transaction>> + Send + '_;
}
