use core::fmt;

use db_core::{BTreeError, BTreeTransaction};
use futures::StreamExt;
use std::sync::Arc;
use uuid::Uuid;

use super::hash::HashStrategy;
use super::{AutomergeEntry, DocumentChangeKey, DocumentType};

pub trait CompactionPolicy: Send + Sync {
  fn should_compact(&self, delta_count: usize, delta_bytes: usize) -> bool;
}

pub struct ThresholdPolicy {
  pub threshold_count: usize,
  pub threshold_bytes: usize,
}

impl ThresholdPolicy {
  pub const DEFAULT_COUNT: usize = 100;
  pub const DEFAULT_BYTES: usize = 1024 * 1024;
}

impl Default for ThresholdPolicy {
  fn default() -> Self {
    Self {
      threshold_count: Self::DEFAULT_COUNT,
      threshold_bytes: Self::DEFAULT_BYTES,
    }
  }
}

impl CompactionPolicy for ThresholdPolicy {
  fn should_compact(&self, delta_count: usize, delta_bytes: usize) -> bool {
    (self.threshold_count > 0 && delta_count >= self.threshold_count)
      || (self.threshold_bytes > 0 && delta_bytes >= self.threshold_bytes)
  }
}

#[derive(Debug)]
pub enum CompactionError {
  Scan(BTreeError),
  Insert(BTreeError),
  Remove(DocumentChangeKey, BTreeError),
  Commit(BTreeError),
  Rollback(BTreeError),
}

impl fmt::Display for CompactionError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      CompactionError::Scan(source) => write!(f, "compaction scan failed: {source}"),
      CompactionError::Insert(source) => write!(f, "compaction insert failed: {source}"),
      CompactionError::Remove(key, source) => {
        write!(f, "compaction remove failed for key {:?}: {source}", key)
      }
      CompactionError::Commit(source) => write!(f, "compaction commit failed: {source}"),
      CompactionError::Rollback(source) => write!(f, "compaction rollback failed: {source}"),
    }
  }
}

impl std::error::Error for CompactionError {}

/// Perform transactional compaction: insert a snapshot for `doc_id` with `state` and remove older change keys.
pub async fn run_compaction<T>(
  mut tx: T,
  start: DocumentChangeKey,
  end: DocumentChangeKey,
  doc_id: Uuid,
  state: Vec<u8>,
  hash_strategy: &Arc<dyn HashStrategy>,
) -> Result<(), CompactionError>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry>,
{
  let new_hash = hash_strategy.make_change_hash(&state);
  let new_key = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Snapshot,
    change_hash: new_hash,
  };
  let new_entry = state.clone();

  let to_remove: alloc::vec::Vec<DocumentChangeKey> = {
    let range_stream = tx.range(start.clone()..=end.clone());
    futures::pin_mut!(range_stream);
    let mut collected: alloc::vec::Vec<DocumentChangeKey> = alloc::vec::Vec::new();
    while let Some(item) = range_stream.next().await {
      let (k, _v) = match item {
        Ok(pair) => pair,
        Err(err) => return Err(CompactionError::Scan(err)),
      };
      if k.change_hash != new_hash {
        collected.push(k);
      }
    }
    collected
  };

  if let Err(err) = tx.insert(new_key.clone(), new_entry).await {
    if let Err(rollback_err) = tx.rollback().await {
      return Err(CompactionError::Rollback(rollback_err));
    }
    return Err(CompactionError::Insert(err));
  }

  for k in to_remove {
    if let Err(err) = tx.remove(k.clone()).await {
      let rollback_err = tx.rollback().await;
      eprintln!(
        "Automerge compaction remove failed for key {:?}, attempting rollback: {:?}",
        k, rollback_err
      );
      return Err(CompactionError::Remove(k, err));
    }
  }

  tx.commit().await.map_err(CompactionError::Commit)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn thresholds_respected() {
    let policy = ThresholdPolicy {
      threshold_count: 5,
      threshold_bytes: 512,
    };
    assert!(policy.should_compact(10, 0));
    assert!(policy.should_compact(0, 1024));
    assert!(!policy.should_compact(2, 10));
  }
}
