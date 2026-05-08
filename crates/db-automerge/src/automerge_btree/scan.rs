use std::ops::RangeBounds;

use automerge::AutoCommit;
use db_core::BTreeError;
use uuid::Uuid;

use super::DocumentType;
use super::reconstruction::reconstruct;

pub(super) struct ReconstructionAccumulator {
  latest_snapshot: Option<Vec<u8>>,
  deltas_after_snapshot: Vec<Vec<u8>>,
}

impl ReconstructionAccumulator {
  pub(super) fn new() -> Self {
    Self {
      latest_snapshot: None,
      deltas_after_snapshot: Vec::new(),
    }
  }

  pub(super) fn apply(&mut self, doc_type: DocumentType, entry: Vec<u8>) {
    if doc_type.is_snapshot() {
      self.latest_snapshot = Some(entry);
      self.deltas_after_snapshot.clear();
    } else {
      self.deltas_after_snapshot.push(entry);
    }
  }

  pub(super) fn finish(self) -> Vec<u8> {
    let state = self.latest_snapshot.unwrap_or_default();
    reconstruct(&state, &self.deltas_after_snapshot)
  }

  pub(super) fn is_empty(&self) -> bool {
    self.latest_snapshot.is_none() && self.deltas_after_snapshot.is_empty()
  }
}

pub(super) fn load_document(bytes: &[u8]) -> Result<AutoCommit, BTreeError> {
  AutoCommit::load(bytes).map_err(BTreeError::other)
}

pub(super) fn uuid_in_range<R>(range: &R, doc_id: &Uuid) -> bool
where
  R: RangeBounds<Uuid>,
{
  let start_ok = match range.start_bound() {
    std::ops::Bound::Included(lower) => doc_id >= lower,
    std::ops::Bound::Excluded(lower) => doc_id > lower,
    std::ops::Bound::Unbounded => true,
  };
  let end_ok = match range.end_bound() {
    std::ops::Bound::Included(upper) => doc_id <= upper,
    std::ops::Bound::Excluded(upper) => doc_id < upper,
    std::ops::Bound::Unbounded => true,
  };
  start_ok && end_ok
}

pub(super) fn flush_reconstructed_doc(
  current_doc: &mut Option<Uuid>,
  accumulator: &mut ReconstructionAccumulator,
) -> Option<Result<(Uuid, AutoCommit), BTreeError>> {
  let doc_id = current_doc.take()?;
  let state = core::mem::replace(accumulator, ReconstructionAccumulator::new()).finish();
  Some(load_document(&state).map(|doc| (doc_id, doc)))
}
