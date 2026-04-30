use std::cmp::Ordering;

use db_core::{
  FastKeyCodec, KeyCodec, KeyScratch, ValueCodec, compare_encoded_keys, decode_value_to_vec,
  encode_key_into_scratch, encode_key_to_vec, encode_value_to_vec, encode_version_into_sink,
};
use db_types::codec::{
  decode_store_key, decode_store_value, encode_store_key, encode_store_key_into_sink,
  encode_store_value, encode_store_value_into_sink,
};

use crate::{StoreKey, StoreValue};

/// Adapter implementing `db_core::StorageCodec` by delegating to the existing
/// store encoding helpers in this crate. This lets callers treat the pair of
/// `StoreKey`/`StoreValue` encoders as a single `StorageCodec` for convenience.
#[derive(Debug, Clone, Copy, Default)]
pub struct EngineStorageCodec;

impl db_core::StorageCodec<StoreKey, StoreValue> for EngineStorageCodec {
  fn encode_key(&self, key: &StoreKey, dst: &mut Vec<u8>) {
    encode_store_key(dst, key);
  }

  fn encode_value(&self, value: &StoreValue, dst: &mut Vec<u8>) {
    encode_store_value(dst, value);
  }

  fn decode_value(&self, src: &[u8]) -> Result<StoreValue, db_core::DecodeError> {
    db_core::decode_with_version(src, decode_store_value)
  }

  fn compare_encoded_keys(&self, a: &[u8], b: &[u8]) -> Ordering {
    // Decode both and compare domain ordering; this mirrors the previous
    // behavior which decoded then compared.
    let left =
      db_core::decode_with_version(a, decode_store_key).unwrap_or_else(|e| panic!("{}", e));

    let right =
      db_core::decode_with_version(b, decode_store_key).unwrap_or_else(|e| panic!("{}", e));

    left.cmp(&right)
  }

  fn encode_key_into_scratch(&self, key: &StoreKey, scratch: &mut KeyScratch) {
    // Efficient encode directly into the provided scratch buffer.
    encode_store_key(&mut scratch.buf, key);
  }
}

// Use the central codec version from `db-core` to avoid duplication.

/// Encodes and compares StoreKey values for codec-backed storage backends.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreKeyCodec;

/// Encodes StoreValue payloads for codec-backed storage backends.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreValueCodec;

impl ValueCodec<StoreKey> for StoreKeyCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    StoreKey: 'a;

  fn encode<'a>(value: &'a StoreKey) -> Self::Bytes<'a> {
    encode_key_to_vec(&EngineStorageCodec, value)
  }

  fn decode(data: &[u8]) -> StoreKey {
    db_core::decode_with_version(data, decode_store_key).unwrap_or_else(|e| panic!("{}", e))
  }
}

impl StoreKeyCodec {
  /// Encode `key` into a generic `BufferSink` without allocating a `Vec`.
  pub fn encode_key_into_sink<S: db_core::BufferSink>(&self, key: &StoreKey, sink: &mut S) {
    // Forward to existing sink helper which mirrors the EngineStorageCodec behavior.
    encode_version_into_sink(sink);
    encode_store_key_into_sink(sink, key);
  }
}

impl KeyCodec<StoreKey> for StoreKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> Ordering {
    // Use the facade compare helper via the StorageCodec adapter for clarity.
    compare_encoded_keys(&EngineStorageCodec, left, right)
  }
}

impl FastKeyCodec<StoreKey> for StoreKeyCodec {
  fn encode_into(&self, value: &StoreKey, scratch: &mut KeyScratch) {
    // Delegate to the shared helper which writes into the provided scratch.
    encode_key_into_scratch(&EngineStorageCodec, value, scratch);
  }

  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> Ordering {
    <StoreKeyCodec as KeyCodec<StoreKey>>::compare(left, right)
  }
}

impl ValueCodec<StoreValue> for StoreValueCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    StoreValue: 'a;

  fn encode<'a>(value: &'a StoreValue) -> Self::Bytes<'a> {
    encode_value_to_vec(&EngineStorageCodec, value)
  }

  fn decode(data: &[u8]) -> StoreValue {
    // Preserve previous semantics: panic on version mismatch/unsupported
    // to match existing behavior in the crate (tests rely on panic).
    db_core::decode_with_version(data, decode_store_value).expect("decode failed")
  }

  fn decode_checked(data: &[u8]) -> Result<StoreValue, db_core::DecodeError> {
    decode_value_to_vec(&EngineStorageCodec, data)
  }
}

impl StoreValueCodec {
  /// Encode `value` into a generic `BufferSink` without allocating a `Vec`.
  pub fn encode_value_into_sink<S: db_core::BufferSink>(&self, value: &StoreValue, sink: &mut S) {
    encode_version_into_sink(sink);
    encode_store_value_into_sink(sink, value);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{ColumnSchema, EngineKey, EngineType, EngineValue, TableSchema};

  #[test]
  fn store_key_round_trips() {
    let value = StoreKey::index_entry(
      "users_name_idx".into(),
      EngineKey::from_values(vec![EngineValue::Text("Bob".into())]),
      EngineKey::from_values(vec![EngineValue::Integer(2)]),
    );

    let encoded = <StoreKeyCodec as ValueCodec<StoreKey>>::encode_to_vec(&value);
    let decoded =
      <StoreKeyCodec as ValueCodec<StoreKey>>::decode_checked(&encoded).expect("decode failed");

    assert_eq!(decoded, value);
  }

  #[test]
  fn store_value_round_trips_schema_payloads() {
    let value = StoreValue::TableSchema(TableSchema {
      name: "users".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "name".into(),
          data_type: EngineType::Text,
        },
      ],
      primary_key: vec![0],
    });

    let encoded = <StoreValueCodec as ValueCodec<StoreValue>>::encode_to_vec(&value);
    let decoded =
      <StoreValueCodec as ValueCodec<StoreValue>>::decode_checked(&encoded).expect("decode failed");

    assert_eq!(decoded, value);
  }

  #[test]
  fn key_compare_matches_store_key_ordering() {
    let left = StoreKey::table_row(
      "numbers".into(),
      EngineKey::from_values(vec![EngineValue::Integer(2)]),
    );
    let right = StoreKey::table_row(
      "numbers".into(),
      EngineKey::from_values(vec![EngineValue::Float(3.0)]),
    );

    let mut left_scratch = KeyScratch::with_capacity(128);
    let codec = StoreKeyCodec;
    codec.encode_into(&left, &mut left_scratch);
    let left_encoded = left_scratch.as_slice().to_vec();

    let mut right_scratch = KeyScratch::with_capacity(128);
    codec.encode_into(&right, &mut right_scratch);
    let right_encoded = right_scratch.as_slice().to_vec();

    assert_eq!(
      StoreKeyCodec::compare(&left_encoded, &right_encoded),
      left.cmp(&right)
    );
  }

  #[test]
  fn float_codec_canonicalizes_nan_and_signed_zero() {
    let nan_row = StoreValue::Row(vec![EngineValue::Float(f64::from_bits(
      0x7ff8_0000_0000_0001,
    ))]);
    let zero_row = StoreValue::Row(vec![EngineValue::Float(-0.0)]);

    let nan_decoded = <StoreValueCodec as ValueCodec<StoreValue>>::decode_checked(
      &<StoreValueCodec as ValueCodec<StoreValue>>::encode_to_vec(&nan_row),
    )
    .expect("decode failed");
    let zero_decoded = <StoreValueCodec as ValueCodec<StoreValue>>::decode_checked(
      &<StoreValueCodec as ValueCodec<StoreValue>>::encode_to_vec(&zero_row),
    )
    .expect("decode failed");

    assert_eq!(nan_decoded, nan_row);
    assert_eq!(zero_decoded, StoreValue::Row(vec![EngineValue::Float(0.0)]));
  }

  #[test]
  fn decode_version_mismatch_returns_err() {
    // Construct a buffer with an invalid/unsupported version byte.
    let buf = vec![db_core::CURRENT_CODEC_VERSION.wrapping_add(1)];
    // Decoding should return a DecodeError for unsupported version.
    let res = <StoreValueCodec as ValueCodec<StoreValue>>::decode_checked(&buf);
    assert!(matches!(res, Err(db_core::DecodeError::InvalidVersion(_))));
  }

  #[test]
  fn sink_encoding_matches_vec_encoding_for_keys() {
    let value = StoreKey::table_row(
      "numbers".into(),
      EngineKey::from_values(vec![EngineValue::Integer(42)]),
    );

    let mut v = Vec::new();
    let codec = StoreKeyCodec;
    codec.encode_key_into_sink(&value, &mut v);

    let encoded = <StoreKeyCodec as ValueCodec<StoreKey>>::encode_to_vec(&value);
    assert_eq!(v, encoded);
  }

  #[test]
  fn sink_encoding_matches_vec_encoding_for_values() {
    let value = StoreValue::Row(vec![EngineValue::Text("hello".into())]);

    let mut v = Vec::new();
    let codec = StoreValueCodec;
    codec.encode_value_into_sink(&value, &mut v);

    let encoded = <StoreValueCodec as ValueCodec<StoreValue>>::encode_to_vec(&value);
    assert_eq!(v, encoded);
  }
}
