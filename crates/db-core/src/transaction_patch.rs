use core::{borrow::Borrow, ops::RangeBounds};

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

pub type TransactionPatch<K, V> = BTreeMap<K, TransactionEntry<V>>;

pub fn patch_get<K, V, Q>(patch: &TransactionPatch<K, V>, key: &Q) -> Option<V>
where
  K: Ord,
  V: Clone,
  Q: Borrow<K>,
{
  patch
    .get(key.borrow())
    .and_then(|entry| entry.as_option().cloned())
}

pub fn patch_insert<K, V>(patch: &mut TransactionPatch<K, V>, key: K, value: V)
where
  K: Ord,
{
  patch.insert(key, TransactionEntry::Present(value));
}

pub fn patch_delete<K, V, Q>(patch: &mut TransactionPatch<K, V>, key: Q)
where
  K: Ord + Clone,
  Q: Borrow<K>,
{
  patch.insert(key.borrow().clone(), TransactionEntry::Deleted);
}

pub fn patch_remove<K, V, Q>(patch: &mut TransactionPatch<K, V>, key: Q) -> Option<V>
where
  K: Ord + Clone,
  V: Clone,
  Q: Borrow<K>,
{
  if let Some((owned_key, entry)) = patch
    .get_key_value(key.borrow())
    .map(|(owned_key, entry)| (owned_key.clone(), entry.clone()))
  {
    let result = entry.as_option().cloned();
    patch.insert(owned_key, TransactionEntry::Deleted);
    result
  } else {
    None
  }
}

pub fn commit_transaction_patch<K, V>(base: &mut BTreeMap<K, V>, patch: TransactionPatch<K, V>)
where
  K: Ord,
{
  for (key, entry) in patch {
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

pub fn merge_transaction_patch_range<K, V, R>(
  base: &BTreeMap<K, V>,
  patch: &TransactionPatch<K, V>,
  range: R,
) -> BTreeMap<K, V>
where
  K: Ord + Clone,
  V: Clone,
  R: RangeBounds<K>,
{
  merge_range_maps(
    base,
    patch,
    range,
    |k| !matches!(patch.get(k), Some(TransactionEntry::Deleted)),
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
