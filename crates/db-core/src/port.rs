use crate::btree::BTree;

/// Storage port traits live here. This is the adapter-facing port that the
/// engine can depend on. For now `StoragePort` is a thin marker super-trait
/// of `BTree` so adapters can opt-in explicitly; this makes it simple to
/// migrate adapters without changing existing `BTree` implementations.
pub trait StoragePort<K, V>: BTree<K, V> + Send + Sync {}

/// Convert an adapter value into the canonical `StoragePort` type for a
/// particular key/value pair. Many adapters will already implement
/// `StoragePort<K,V>` and can rely on the blanket impl below.
pub trait IntoStoragePort<K, V> {
  type Port: StoragePort<K, V>;
  fn into_storage_port(self) -> Self::Port;
}

impl<T, K, V> IntoStoragePort<K, V> for T
where
  T: StoragePort<K, V>,
{
  type Port = T;
  fn into_storage_port(self) -> Self::Port {
    self
  }
}
