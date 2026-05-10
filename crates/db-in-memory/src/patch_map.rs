use core::{borrow::Borrow, ops::RangeBounds};
use db_core::TransactionPatch;

#[cfg(not(feature = "std"))]
use alloc::collections::BTreeMap;
#[cfg(feature = "std")]
use std::collections::BTreeMap;

pub(crate) fn get_from_patch_then_map<K, Q, V>(
  patch: &TransactionPatch<K, V>,
  base: &BTreeMap<K, V>,
  key: &Q,
) -> Option<V>
where
  K: Ord,
  V: Clone,
  Q: Borrow<K>,
{
  patch.get_base_value(base, key)
}

pub(crate) fn remove_from_patch_then_map<K, Q, V>(
  patch: &mut TransactionPatch<K, V>,
  base: &BTreeMap<K, V>,
  key: &Q,
) -> Option<V>
where
  K: Ord + Clone,
  V: Clone,
  Q: Borrow<K>,
{
  if let Some(value) = patch.remove(key.borrow().clone()) {
    return Some(value);
  }

  let value = base.get(key.borrow()).cloned();
  if let Some((owned_key, _)) = base.get_key_value(key.borrow()) {
    patch.delete(owned_key.clone());
  }
  value
}

pub(crate) fn merge_patch_range<K, R, V>(
  patch: &TransactionPatch<K, V>,
  base: &BTreeMap<K, V>,
  range: R,
) -> BTreeMap<K, V>
where
  K: Ord + Clone,
  V: Clone,
  R: RangeBounds<K>,
{
  patch.merge_range(base, range)
}

pub(crate) fn commit_patch_into_map<K, V>(patch: TransactionPatch<K, V>, base: &mut BTreeMap<K, V>)
where
  K: Ord,
{
  patch.commit_into(base);
}
