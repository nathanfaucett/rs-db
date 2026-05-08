use db_core::BufferSink;
use uuid::Uuid;

use super::AutomergeEntry;
use super::key::{DocumentChangeKey, DocumentType};

pub(super) fn encode_doc_key_range<KC>(doc_id: Uuid, codec: &KC) -> (Vec<u8>, Vec<u8>)
where
  KC: db_core::FastKeyCodec<DocumentChangeKey>,
{
  let start = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Incremental,
    change_hash: [0u8; 32],
  };
  let end = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Snapshot,
    change_hash: [255u8; 32],
  };

  let mut s1 = db_core::KeyScratch::with_capacity(49);
  let mut s2 = db_core::KeyScratch::with_capacity(49);
  <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(codec, &start, &mut s1);
  <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(codec, &end, &mut s2);
  (s1.buf, s2.buf)
}

pub(super) fn encode_doc_key_range_value_codec<KC>(doc_id: Uuid) -> (Vec<u8>, Vec<u8>)
where
  KC: db_core::ValueCodec<DocumentChangeKey>,
{
  let start = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Incremental,
    change_hash: [0u8; 32],
  };
  let end = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Snapshot,
    change_hash: [255u8; 32],
  };

  (
    <KC as db_core::ValueCodec<DocumentChangeKey>>::encode(&start)
      .as_ref()
      .to_vec(),
    <KC as db_core::ValueCodec<DocumentChangeKey>>::encode(&end)
      .as_ref()
      .to_vec(),
  )
}

#[cfg(test)]
pub(super) fn uuid_prefix_range(doc_id: Uuid) -> (Vec<u8>, Vec<u8>) {
  encode_doc_key_range(doc_id, &DocumentChangeKeyCodec)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DocumentChangeKeyCodec;

impl db_core::ValueCodec<DocumentChangeKey> for DocumentChangeKeyCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    DocumentChangeKey: 'a;

  fn encode<'a>(value: &'a DocumentChangeKey) -> Self::Bytes<'a> {
    let mut out = Vec::with_capacity(49);
    out.extend_from_slice(value.doc_id.as_bytes());
    out.push(match value.doc_type {
      DocumentType::Incremental => 0u8,
      DocumentType::Snapshot => 1u8,
    });
    out.extend_from_slice(&value.change_hash);
    out
  }

  fn decode(data: &[u8]) -> DocumentChangeKey {
    if data.len() < 49 {
      panic!("invalid DocumentChangeKey encoding");
    }
    let id = Uuid::from_slice(&data[0..16]).expect("uuid decode");
    let doc_type = match data[16] {
      0 => DocumentType::Incremental,
      1 => DocumentType::Snapshot,
      _ => panic!("invalid doc_type"),
    };
    let mut change_hash = [0u8; 32];
    change_hash.copy_from_slice(&data[17..49]);
    DocumentChangeKey {
      doc_id: id,
      doc_type,
      change_hash,
    }
  }

  fn decode_checked(data: &[u8]) -> Result<DocumentChangeKey, db_core::DecodeError> {
    if data.len() < 49 {
      return Err(db_core::DecodeError::Truncated);
    }
    Ok(Self::decode(data))
  }
}

impl db_core::KeyCodec<DocumentChangeKey> for DocumentChangeKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    left.cmp(right)
  }
}

impl db_core::FastKeyCodec<DocumentChangeKey> for DocumentChangeKeyCodec {
  fn encode_into(&self, value: &DocumentChangeKey, scratch: &mut db_core::KeyScratch) {
    scratch.push_bytes(value.doc_id.as_bytes());
    let dt = match value.doc_type {
      DocumentType::Incremental => 0u8,
      DocumentType::Snapshot => 1u8,
    };
    scratch.push_bytes(&[dt]);
    scratch.push_bytes(&value.change_hash);
  }

  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    <Self as db_core::KeyCodec<DocumentChangeKey>>::compare(left, right)
  }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VecBytesCodec;

impl db_core::ValueCodec<AutomergeEntry> for VecBytesCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    AutomergeEntry: 'a;

  fn encode<'a>(value: &'a AutomergeEntry) -> Self::Bytes<'a> {
    value.clone()
  }

  fn decode(data: &[u8]) -> AutomergeEntry {
    data.to_vec()
  }

  fn decode_checked(data: &[u8]) -> Result<AutomergeEntry, db_core::DecodeError> {
    Ok(data.to_vec())
  }
}
