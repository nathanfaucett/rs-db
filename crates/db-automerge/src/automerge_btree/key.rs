use uuid::Uuid;

/// Document change key: (doc_id, doc_type, change_hash)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentChangeKey {
  pub doc_id: Uuid,
  /// Snapshot vs incremental flag.
  pub doc_type: DocumentType,
  pub change_hash: [u8; 32],
}

pub type AutomergeEntry = Vec<u8>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentType {
  Incremental,
  Snapshot,
}

impl DocumentType {
  pub fn is_snapshot(self) -> bool {
    matches!(self, DocumentType::Snapshot)
  }
}

impl core::cmp::Ord for DocumentChangeKey {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    match self.doc_id.cmp(&other.doc_id) {
      Ordering::Equal => {
        let a = match self.doc_type {
          DocumentType::Incremental => 0u8,
          DocumentType::Snapshot => 1u8,
        };
        let b = match other.doc_type {
          DocumentType::Incremental => 0u8,
          DocumentType::Snapshot => 1u8,
        };
        match a.cmp(&b) {
          Ordering::Equal => self.change_hash.cmp(&other.change_hash),
          ord => ord,
        }
      }
      ord => ord,
    }
  }
}

impl core::cmp::PartialOrd for DocumentChangeKey {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

pub(super) fn document_entry_bounds(doc_id: Uuid) -> (DocumentChangeKey, DocumentChangeKey) {
  (
    DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: [0u8; 32],
    },
    DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Snapshot,
      change_hash: [255u8; 32],
    },
  )
}

pub(super) fn all_document_bounds() -> (DocumentChangeKey, DocumentChangeKey) {
  (
    DocumentChangeKey {
      doc_id: Uuid::from_u128(0),
      doc_type: DocumentType::Incremental,
      change_hash: [0u8; 32],
    },
    DocumentChangeKey {
      doc_id: Uuid::from_u128(u128::MAX),
      doc_type: DocumentType::Snapshot,
      change_hash: [255u8; 32],
    },
  )
}
