use db_core::{KeyCodec, ValueCodec};
use uuid::Uuid;

use super::{AutomergeEntry, DocumentChangeKey, DocumentType};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DocumentKeyCodec;

impl ValueCodec<DocumentChangeKey> for DocumentKeyCodec {
  type Bytes<'a>
    = alloc::vec::Vec<u8>
  where
    Self: 'a,
    DocumentChangeKey: 'a;

  fn fixed_width() -> Option<usize> {
    Some(16 + 1 + 32)
  }

  fn encode<'a>(value: &'a DocumentChangeKey) -> Self::Bytes<'a> {
    let mut v = alloc::vec::Vec::with_capacity(49);
    v.extend_from_slice(value.doc_id.as_bytes());
    // Encode as: uuid (16) + type (1) + change_hash (32)
    let t = match value.doc_type {
      DocumentType::Incremental => 0u8,
      DocumentType::Snapshot => 1u8,
    };
    v.push(t);
    v.extend_from_slice(&value.change_hash);
    v
  }

  fn decode(data: &[u8]) -> DocumentChangeKey {
    let mut idx = 0;
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&data[idx..idx + 16]);
    idx += 16;
    let b = data[idx];
    idx += 1;
    let doc_type = if b == 1u8 {
      DocumentType::Snapshot
    } else {
      DocumentType::Incremental
    };
    let mut change_hash = [0u8; 32];
    change_hash.copy_from_slice(&data[idx..idx + 32]);
    DocumentChangeKey {
      doc_id: Uuid::from_bytes(id_bytes),
      doc_type,
      change_hash,
    }
  }
}

impl KeyCodec<DocumentChangeKey> for DocumentKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    left.cmp(right)
  }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default)]
pub struct AutomergeValueCodec;

impl ValueCodec<AutomergeEntry> for AutomergeValueCodec {
  type Bytes<'a>
    = alloc::vec::Vec<u8>
  where
    Self: 'a,
    AutomergeEntry: 'a;

  fn fixed_width() -> Option<usize> {
    None
  }

  fn encode<'a>(value: &'a AutomergeEntry) -> Self::Bytes<'a> {
    value.clone()
  }

  fn decode(data: &[u8]) -> AutomergeEntry {
    data.to_vec()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use uuid::Uuid;

  #[test]
  fn key_codec_roundtrip() {
    let doc_id = Uuid::new_v4();
    let key = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Snapshot,
      change_hash: [1u8; 32],
    };
    let enc = DocumentKeyCodec::encode(&key);
    let dec = DocumentKeyCodec::decode(&enc);
    assert_eq!(key, dec);
  }

  #[test]
  fn value_codec_roundtrip() {
    let v = b"hello".to_vec();
    let enc = AutomergeValueCodec::encode(&v);
    let dec = AutomergeValueCodec::decode(&enc);
    assert_eq!(v, dec);
  }

  #[test]
  fn key_compare_bytes_order() {
    let doc_id = Uuid::new_v4();
    let a = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: [0u8; 32],
    };
    let b = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Snapshot,
      change_hash: [255u8; 32],
    };
    let enc_a = DocumentKeyCodec::encode(&a);
    let enc_b = DocumentKeyCodec::encode(&b);
    assert_eq!(DocumentKeyCodec::compare(&enc_a, &enc_b), enc_a.cmp(&enc_b));
  }
}
