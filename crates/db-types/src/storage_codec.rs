#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use db_core::{KeyCodec, ValueCodec, decode_with_version, encode_version_into_sink};

use crate::{
  EngineKey, EngineRow,
  codec::{
    decode_engine_key, decode_engine_row, encode_engine_key_into_sink, encode_engine_row_into_sink,
  },
};

fn encode_key_versioned(value: &EngineKey) -> Vec<u8> {
  let mut out = Vec::new();
  encode_version_into_sink(&mut out);
  encode_engine_key_into_sink(&mut out, value);
  out
}

fn decode_key_versioned(data: &[u8]) -> Result<EngineKey, db_core::DecodeError> {
  decode_with_version(data, decode_engine_key)
}

fn encode_row_versioned(value: &EngineRow) -> Vec<u8> {
  let mut out = Vec::new();
  encode_version_into_sink(&mut out);
  encode_engine_row_into_sink(&mut out, value);
  out
}

fn decode_row_versioned(data: &[u8]) -> Result<EngineRow, db_core::DecodeError> {
  decode_with_version(data, decode_engine_row)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineKeyCodec;

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineRowCodec;

impl ValueCodec<EngineKey> for EngineKeyCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    EngineKey: 'a;

  fn encode<'a>(value: &'a EngineKey) -> Self::Bytes<'a> {
    encode_key_versioned(value)
  }

  fn decode(data: &[u8]) -> EngineKey {
    decode_key_versioned(data).expect("decode engine key failed")
  }

  fn decode_checked(data: &[u8]) -> Result<EngineKey, db_core::DecodeError> {
    decode_key_versioned(data)
  }
}

impl KeyCodec<EngineKey> for EngineKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    let left = <Self as ValueCodec<EngineKey>>::decode(left);
    let right = <Self as ValueCodec<EngineKey>>::decode(right);
    left.cmp(&right)
  }
}

impl ValueCodec<EngineRow> for EngineRowCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    EngineRow: 'a;

  fn encode<'a>(value: &'a EngineRow) -> Self::Bytes<'a> {
    encode_row_versioned(value)
  }

  fn decode(data: &[u8]) -> EngineRow {
    decode_row_versioned(data).expect("decode engine row failed")
  }

  fn decode_checked(data: &[u8]) -> Result<EngineRow, db_core::DecodeError> {
    decode_row_versioned(data)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::EngineValue;

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

    let left_encoded = <EngineKeyCodec as ValueCodec<EngineKey>>::encode_to_vec(&left);
    let right_encoded = <EngineKeyCodec as ValueCodec<EngineKey>>::encode_to_vec(&right);

    assert_eq!(
      EngineKeyCodec::compare(&left_encoded, &right_encoded),
      left.cmp(&right)
    );
  }
}
