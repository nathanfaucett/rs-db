use std::cmp::Ordering;
use std::vec::Vec;

use db_core::{
  EngineKey, EngineRow, FastKeyCodec, KeyCodec, KeyScratch, ValueCodec, decode_engine_key,
  decode_engine_row, decode_with_version, encode_engine_key_into_sink, encode_engine_row_into_sink,
  encode_version_into_sink,
};

/// Encodes and compares engine keys for codec-backed named-tree backends.
#[derive(Debug, Clone, Copy, Default)]
pub struct EngineKeyCodec;

/// Encodes engine row payloads for codec-backed named-tree backends.
#[derive(Debug, Clone, Copy, Default)]
pub struct EngineRowCodec;

impl ValueCodec<EngineKey> for EngineKeyCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    EngineKey: 'a;

  fn encode<'a>(value: &'a EngineKey) -> Self::Bytes<'a> {
    let mut out = Vec::new();
    encode_version_into_sink(&mut out);
    encode_engine_key_into_sink(&mut out, value);
    out
  }

  fn decode(data: &[u8]) -> EngineKey {
    decode_with_version(data, decode_engine_key).expect("decode engine key failed")
  }

  fn decode_checked(data: &[u8]) -> Result<EngineKey, db_core::DecodeError> {
    decode_with_version(data, decode_engine_key)
  }
}

impl KeyCodec<EngineKey> for EngineKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> Ordering {
    let left = <Self as ValueCodec<EngineKey>>::decode(left);
    let right = <Self as ValueCodec<EngineKey>>::decode(right);
    left.cmp(&right)
  }
}

impl FastKeyCodec<EngineKey> for EngineKeyCodec {
  fn encode_into(&self, value: &EngineKey, scratch: &mut KeyScratch) {
    encode_version_into_sink(&mut scratch.buf);
    encode_engine_key_into_sink(&mut scratch.buf, value);
  }

  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> Ordering {
    <Self as KeyCodec<EngineKey>>::compare(left, right)
  }
}

impl ValueCodec<EngineRow> for EngineRowCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    EngineRow: 'a;

  fn encode<'a>(value: &'a EngineRow) -> Self::Bytes<'a> {
    let mut out = Vec::new();
    encode_version_into_sink(&mut out);
    encode_engine_row_into_sink(&mut out, value);
    out
  }

  fn decode(data: &[u8]) -> EngineRow {
    decode_with_version(data, decode_engine_row).expect("decode engine row failed")
  }

  fn decode_checked(data: &[u8]) -> Result<EngineRow, db_core::DecodeError> {
    decode_with_version(data, decode_engine_row)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_core::EngineValue;

  #[test]
  fn engine_key_round_trips() {
    let value = EngineKey::from_values(vec![
      EngineValue::Integer(1),
      EngineValue::Text("Alice".into()),
    ]);

    let encoded = <EngineKeyCodec as ValueCodec<EngineKey>>::encode_to_vec(&value);
    let decoded =
      <EngineKeyCodec as ValueCodec<EngineKey>>::decode_checked(&encoded).expect("decode failed");

    assert_eq!(decoded, value);
  }

  #[test]
  fn engine_row_round_trips() {
    let value = vec![
      EngineValue::Integer(1),
      EngineValue::Text("Alice".into()),
      EngineValue::Blob(vec![1, 2, 3]),
    ];

    let encoded = <EngineRowCodec as ValueCodec<EngineRow>>::encode_to_vec(&value);
    let decoded =
      <EngineRowCodec as ValueCodec<EngineRow>>::decode_checked(&encoded).expect("decode failed");

    assert_eq!(decoded, value);
  }

  #[test]
  fn key_compare_matches_engine_key_ordering() {
    let left = EngineKey::from_values(vec![EngineValue::Integer(2)]);
    let right = EngineKey::from_values(vec![EngineValue::Float(3.0)]);

    let codec = EngineKeyCodec;
    let mut left_scratch = KeyScratch::with_capacity(128);
    codec.encode_into(&left, &mut left_scratch);

    let mut right_scratch = KeyScratch::with_capacity(128);
    codec.encode_into(&right, &mut right_scratch);

    assert_eq!(
      EngineKeyCodec::compare(left_scratch.as_slice(), right_scratch.as_slice()),
      left.cmp(&right)
    );
  }
}
