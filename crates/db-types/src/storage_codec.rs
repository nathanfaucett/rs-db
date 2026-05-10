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

fn encode_versioned<T>(value: &T, encode: impl FnOnce(&mut Vec<u8>, &T)) -> Vec<u8> {
  let mut out = Vec::new();
  encode_version_into_sink(&mut out);
  encode(&mut out, value);
  out
}

fn encode_key_versioned(value: &EngineKey) -> Vec<u8> {
  encode_versioned(value, encode_engine_key_into_sink)
}

fn decode_key_versioned(data: &[u8]) -> Result<EngineKey, db_core::DecodeError> {
  decode_with_version(data, decode_engine_key)
}

fn decode_row_versioned(data: &[u8]) -> Result<EngineRow, db_core::DecodeError> {
  decode_with_version(data, decode_engine_row)
}

fn encode_row_versioned(value: &EngineRow) -> Vec<u8> {
  encode_versioned(value, encode_engine_row_into_sink)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineKeyCodec;

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineRowCodec;

macro_rules! impl_engine_value_codec {
  ($codec:ty, $value:ty, $encode:ident, $decode:ident, $expect:literal) => {
    impl ValueCodec<$value> for $codec {
      type Bytes<'a>
        = Vec<u8>
      where
        Self: 'a,
        $value: 'a;

      fn encode<'a>(value: &'a $value) -> Self::Bytes<'a> {
        $encode(value)
      }

      fn decode(data: &[u8]) -> $value {
        $decode(data).expect($expect)
      }

      fn decode_checked(data: &[u8]) -> Result<$value, db_core::DecodeError> {
        $decode(data)
      }
    }
  };
}

impl_engine_value_codec!(
  EngineKeyCodec,
  EngineKey,
  encode_key_versioned,
  decode_key_versioned,
  "decode engine key failed"
);

impl KeyCodec<EngineKey> for EngineKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    let left = <Self as ValueCodec<EngineKey>>::decode(left);
    let right = <Self as ValueCodec<EngineKey>>::decode(right);
    left.cmp(&right)
  }
}

impl_engine_value_codec!(
  EngineRowCodec,
  EngineRow,
  encode_row_versioned,
  decode_row_versioned,
  "decode engine row failed"
);

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
