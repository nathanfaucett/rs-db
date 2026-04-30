#![cfg(feature = "registry")]
extern crate alloc;
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::cmp::Ordering;

use crate::StorageCodec;

/// Object-safe comparator for encoded key byte slices.
pub trait EncodedComparator: Send + Sync {
  /// Compare two encoded key byte slices. Implementations must be consistent
  /// with the domain ordering for the codec they represent.
  fn compare_bytes(&self, a: &[u8], b: &[u8]) -> Ordering;

  /// Optional fast path that can return `Some(Ordering)` without full parse.
  fn try_compare_fast(&self, _a: &[u8], _b: &[u8]) -> Option<Ordering> {
    None
  }

  /// If the comparator expects a leading version prefix, return it from the
  /// provided data. Default implementation returns the first byte when present.
  fn version_prefix(&self, data: &[u8]) -> Option<u8> {
    data.get(0).copied()
  }
}

/// Adapter that wraps a typed `StorageCodec` implementation and exposes
/// it as an `EncodedComparator` for use in registries.
#[derive(Debug, Clone)]
pub struct TypedComparatorAdapter<SC>(pub SC);

impl<SC, K, V> EncodedComparator for TypedComparatorAdapter<SC>
where
  SC: StorageCodec<K, V> + Send + Sync + 'static,
{
  fn compare_bytes(&self, a: &[u8], b: &[u8]) -> Ordering {
    self.0.compare_encoded_keys(a, b)
  }

  fn try_compare_fast(&self, a: &[u8], b: &[u8]) -> Option<Ordering> {
    // Delegate to the typed codec's compare; allow codec to implement fast path
    Some(self.0.compare_encoded_keys(a, b))
  }
}

/// Registry that maps codec version bytes to comparators and supports
/// cross-version comparators for incremental upgrades.
pub struct CodecRegistry {
  by_version: BTreeMap<u8, Box<dyn EncodedComparator>>,
  cross: BTreeMap<(u8, u8), Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>>,
  default: Option<Box<dyn EncodedComparator>>,
}

impl CodecRegistry {
  pub fn new() -> Self {
    Self {
      by_version: BTreeMap::new(),
      cross: BTreeMap::new(),
      default: None,
    }
  }

  pub fn register_comparator(&mut self, version: u8, cmp: Box<dyn EncodedComparator>) {
    self.by_version.insert(version, cmp);
  }

  pub fn register_cross_version<F>(&mut self, from: u8, to: u8, f: F)
  where
    F: Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
  {
    self.cross.insert((from, to), Box::new(f));
  }

  pub fn set_default(&mut self, cmp: Box<dyn EncodedComparator>) {
    self.default = Some(cmp);
  }

  /// Compare two encoded keys. If version prefixes are available and a
  /// comparator is registered for the version, the registry dispatches to it.
  /// Cross-version comparators are consulted when available.
  pub fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
    let va = a.get(0).copied();
    let vb = b.get(0).copied();

    match (va, vb) {
      (Some(v1), Some(v2)) if v1 == v2 => {
        if let Some(c) = self.by_version.get(&v1) {
          // If comparator wants payload-only, it can strip prefix itself.
          return c.compare_bytes(a, b);
        }
        // Fallback to lexicographic compare of the remainder
        return a.cmp(b);
      }
      (Some(v1), Some(v2)) => {
        if let Some(f) = self.cross.get(&(v1, v2)) {
          return f(a, b);
        }
        // Deterministic fallback: compare version bytes first, then payload
        return v1.cmp(&v2).then_with(|| a.cmp(b));
      }
      _ => {
        if let Some(default) = &self.default {
          return default.compare_bytes(a, b);
        }
        a.cmp(b)
      }
    }
  }
}
