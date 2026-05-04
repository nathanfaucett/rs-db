use core::{
  borrow::Borrow,
  ops::{Deref, DerefMut, RangeBounds},
};

#[cfg(not(feature = "std"))]
use alloc::collections::BTreeMap;
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use crate::merge_range_maps;

#[derive(Debug, Clone)]
pub enum TransactionEntry<V> {
  Present(V),
  Deleted,
}

impl<V> TransactionEntry<V> {
  pub fn as_option(&self) -> Option<&V> {
    match self {
      TransactionEntry::Present(value) => Some(value),
      TransactionEntry::Deleted => None,
    }
  }
}

#[derive(Debug, Clone)]
pub struct TransactionPatch<K, V>(BTreeMap<K, TransactionEntry<V>>);

impl<K, V> Default for TransactionPatch<K, V> {
  fn default() -> Self {
    Self(BTreeMap::new())
  }
}

impl<K, V> Deref for TransactionPatch<K, V> {
  type Target = BTreeMap<K, TransactionEntry<V>>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<K, V> DerefMut for TransactionPatch<K, V> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<K, V> TransactionPatch<K, V> {
  pub fn get_value<Q>(&self, key: &Q) -> Option<V>
  where
    K: Ord,
    V: Clone,
    Q: Borrow<K>,
  {
    self
      .0
      .get(key.borrow())
      .and_then(|e| e.as_option().cloned())
  }

  pub fn insert(&mut self, key: K, value: V)
  where
    K: Ord,
  {
    self.0.insert(key, TransactionEntry::Present(value));
  }

  pub fn delete<Q>(&mut self, key: Q)
  where
    K: Ord + Clone,
    Q: Borrow<K>,
  {
    self
      .0
      .insert(key.borrow().clone(), TransactionEntry::Deleted);
  }

  pub fn remove<Q>(&mut self, key: Q) -> Option<V>
  where
    K: Ord + Clone,
    V: Clone,
    Q: Borrow<K>,
  {
    if let Some((owned_key, entry)) = self
      .0
      .get_key_value(key.borrow())
      .map(|(k, e)| (k.clone(), e.clone()))
    {
      let result = entry.as_option().cloned();
      self.0.insert(owned_key, TransactionEntry::Deleted);
      result
    } else {
      None
    }
  }

  pub fn commit_into(self, base: &mut BTreeMap<K, V>)
  where
    K: Ord,
  {
    for (key, entry) in self.0 {
      match entry {
        TransactionEntry::Present(value) => {
          base.insert(key, value);
        }
        TransactionEntry::Deleted => {
          base.remove(&key);
        }
      }
    }
  }

  pub fn merge_range<R>(&self, base: &BTreeMap<K, V>, range: R) -> BTreeMap<K, V>
  where
    K: Ord + Clone,
    V: Clone,
    R: RangeBounds<K>,
  {
    merge_range_maps(
      base,
      &self.0,
      range,
      |k| !matches!(self.0.get(k), Some(TransactionEntry::Deleted)),
      |k, entry, merged| match entry {
        TransactionEntry::Present(value) => {
          merged.insert(k.clone(), value.clone());
        }
        TransactionEntry::Deleted => {
          merged.remove(k);
        }
      },
    )
  }
}
