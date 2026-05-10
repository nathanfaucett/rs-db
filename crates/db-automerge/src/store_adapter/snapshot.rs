use automerge::AutoCommit;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use db_core::{BTreeError, Cursor, DecodeError, encode_with_version};
use db_types::codec::{
  decode_engine_key, decode_engine_row, decode_store_key, decode_store_value,
  encode_engine_key_into_sink, encode_engine_row_into_sink, encode_store_key, encode_store_value,
};
use db_types::{EngineKey, EngineRow, StoreKey, StoreValue};

use base64::{Engine as _, engine::general_purpose};

pub(crate) trait SnapshotAdapter {
  type Key: Clone + Eq;
  type Value: Clone;

  fn decode_key(cursor: &mut Cursor<'_>) -> Result<Self::Key, BTreeError>;
  fn decode_value(cursor: &mut Cursor<'_>) -> Result<Self::Value, BTreeError>;
  fn encode_key(buffer: &mut Vec<u8>, key: &Self::Key);
  fn encode_value(buffer: &mut Vec<u8>, value: &Self::Value);
}

type SnapshotEntries<A> = Vec<(<A as SnapshotAdapter>::Key, <A as SnapshotAdapter>::Value)>;
#[cfg(test)]
type SnapshotRemoveOutcome<A> = (Option<<A as SnapshotAdapter>::Value>, Option<Vec<u8>>);

pub(crate) struct StoreSnapshotAdapter;

impl SnapshotAdapter for StoreSnapshotAdapter {
  type Key = StoreKey;
  type Value = StoreValue;

  fn decode_key(cursor: &mut Cursor<'_>) -> Result<Self::Key, BTreeError> {
    decode_store_key(cursor).map_err(BTreeError::other)
  }

  fn decode_value(cursor: &mut Cursor<'_>) -> Result<Self::Value, BTreeError> {
    decode_store_value(cursor).map_err(BTreeError::other)
  }

  fn encode_key(buffer: &mut Vec<u8>, key: &Self::Key) {
    encode_store_key(buffer, key);
  }

  fn encode_value(buffer: &mut Vec<u8>, value: &Self::Value) {
    encode_store_value(buffer, value);
  }
}

pub(crate) struct EngineSnapshotAdapter;

impl SnapshotAdapter for EngineSnapshotAdapter {
  type Key = EngineKey;
  type Value = EngineRow;

  fn decode_key(cursor: &mut Cursor<'_>) -> Result<Self::Key, BTreeError> {
    decode_engine_key(cursor).map_err(BTreeError::other)
  }

  fn decode_value(cursor: &mut Cursor<'_>) -> Result<Self::Value, BTreeError> {
    decode_engine_row(cursor).map_err(BTreeError::other)
  }

  fn encode_key(buffer: &mut Vec<u8>, key: &Self::Key) {
    encode_with_version(buffer, |sink| encode_engine_key_into_sink(sink, key));
  }

  fn encode_value(buffer: &mut Vec<u8>, value: &Self::Value) {
    encode_with_version(buffer, |sink| encode_engine_row_into_sink(sink, value));
  }
}

pub(crate) fn parse_entries<A: SnapshotAdapter>(
  buf: &[u8],
) -> Result<SnapshotEntries<A>, BTreeError> {
  let mut out = Vec::new();
  let mut cursor = Cursor::new(buf);

  loop {
    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(DecodeError::Truncated) => break,
      Err(e) => return Err(BTreeError::other(e)),
    }

    let key = A::decode_key(&mut cursor)?;

    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(e) => return Err(BTreeError::other(e)),
    }

    let value = A::decode_value(&mut cursor)?;
    out.push((key, value));
  }

  Ok(out)
}

pub(crate) fn encode_entries<A: SnapshotAdapter>(entries: &[(A::Key, A::Value)]) -> Vec<u8> {
  let mut buf = Vec::new();
  for (key, value) in entries {
    A::encode_key(&mut buf, key);
    A::encode_value(&mut buf, value);
  }
  buf
}

pub(crate) fn find_entry<A: SnapshotAdapter>(
  buf: &[u8],
  needle: &A::Key,
) -> Result<Option<A::Value>, BTreeError> {
  for (key, value) in parse_entries::<A>(buf)? {
    if &key == needle {
      return Ok(Some(value));
    }
  }
  Ok(None)
}

pub(crate) fn key_in_range<K, R>(key: &K, range: &R) -> bool
where
  K: Ord,
  R: core::ops::RangeBounds<K>,
{
  use core::ops::Bound;

  let start = match range.start_bound() {
    Bound::Included(lower) => key >= lower,
    Bound::Excluded(lower) => key > lower,
    Bound::Unbounded => true,
  };
  let end = match range.end_bound() {
    Bound::Included(upper) => key <= upper,
    Bound::Excluded(upper) => key < upper,
    Bound::Unbounded => true,
  };
  start && end
}

pub(crate) fn set_entry<A: SnapshotAdapter>(
  buf: Option<&[u8]>,
  key: &A::Key,
  value: &A::Value,
) -> Result<Vec<u8>, BTreeError> {
  let mut entries = if let Some(buf) = buf {
    parse_entries::<A>(buf)?
  } else {
    Vec::new()
  };

  if let Some((_, existing)) = entries.iter_mut().find(|(existing, _)| existing == key) {
    *existing = value.clone();
  } else {
    entries.push((key.clone(), value.clone()));
  }

  Ok(encode_entries::<A>(&entries))
}

#[cfg(test)]
pub(crate) fn remove_entry<A: SnapshotAdapter>(
  buf: Option<&[u8]>,
  key: &A::Key,
) -> Result<SnapshotRemoveOutcome<A>, BTreeError> {
  let mut entries = if let Some(buf) = buf {
    parse_entries::<A>(buf)?
  } else {
    Vec::new()
  };

  let mut removed = None;
  entries.retain(|(existing, value)| {
    if existing == key {
      removed = Some(value.clone());
      false
    } else {
      true
    }
  });

  if removed.is_none() {
    return Ok((None, buf.map(|bytes| bytes.to_vec())));
  }

  if entries.is_empty() {
    Ok((removed, None))
  } else {
    Ok((removed, Some(encode_entries::<A>(&entries))))
  }
}

pub(crate) fn decode_snapshot_base64(value: impl ToString) -> Result<Vec<u8>, BTreeError> {
  let text = value.to_string();
  let encoded = text
    .strip_prefix('"')
    .and_then(|s| s.strip_suffix('"'))
    .unwrap_or(&text);

  general_purpose::STANDARD
    .decode(encoded.as_bytes())
    .map_err(BTreeError::other)
}

pub(crate) fn encode_snapshot_base64(bytes: &[u8]) -> String {
  general_purpose::STANDARD.encode(bytes)
}

pub(crate) fn snapshot_bytes(doc: &AutoCommit) -> Result<Option<Vec<u8>>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
    Ok(Some(decode_snapshot_base64(value)?))
  } else {
    Ok(None)
  }
}

pub(crate) fn snapshot_doc(snapshot: &[u8]) -> Result<AutoCommit, BTreeError> {
  let snapshot_str = encode_snapshot_base64(snapshot);
  let mut doc = AutoCommit::new();
  doc
    .put(&automerge::ROOT, "snapshot", snapshot_str)
    .map_err(BTreeError::other)?;
  Ok(doc)
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_types::EngineValue;

  #[test]
  fn store_snapshot_roundtrip_and_remove() {
    let key = StoreKey::table_row(
      "users".to_string(),
      EngineKey::from_values(vec![EngineValue::Integer(1)]),
    );
    let value = StoreValue::Row(vec![EngineValue::Text("alice".to_string())]);

    let bytes = set_entry::<StoreSnapshotAdapter>(None, &key, &value).expect("set");
    assert_eq!(
      find_entry::<StoreSnapshotAdapter>(&bytes, &key).expect("find"),
      Some(value.clone())
    );

    let (removed, updated) =
      remove_entry::<StoreSnapshotAdapter>(Some(&bytes), &key).expect("remove");
    assert_eq!(removed, Some(value));
    assert_eq!(updated, None);
  }

  #[test]
  fn engine_snapshot_updates_existing_key() {
    let key = EngineKey::from_values(vec![EngineValue::Integer(7)]);
    let value1 = vec![EngineValue::Text("first".to_string())];
    let value2 = vec![EngineValue::Text("second".to_string())];

    let first = set_entry::<EngineSnapshotAdapter>(None, &key, &value1).expect("first");
    let second = set_entry::<EngineSnapshotAdapter>(Some(&first), &key, &value2).expect("second");

    assert_eq!(
      find_entry::<EngineSnapshotAdapter>(&second, &key).expect("find"),
      Some(value2)
    );
  }
}
