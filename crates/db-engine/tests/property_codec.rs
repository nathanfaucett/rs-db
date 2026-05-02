use db_core::{FastKeyCodec, KeyCodec};
use proptest::prelude::*;

use db_engine::{EngineKey, EngineKeyCodec, EngineRowCodec, EngineValue};

/// Use the fully-qualified trait methods for encoding/decoding to avoid
/// needing to import the trait into the test scope.
fn engine_value_strategy() -> impl Strategy<Value = EngineValue> {
  prop_oneof![
    Just(EngineValue::Null),
    any::<i64>().prop_map(EngineValue::Integer),
    proptest::num::f64::ANY.prop_map(EngineValue::Float),
    any::<String>().prop_map(EngineValue::Text),
    any::<Vec<u8>>().prop_map(EngineValue::Blob),
  ]
}

proptest! {
  #[test]
  fn engine_row_roundtrip(values in prop::collection::vec(engine_value_strategy(), 0..6)) {
    let value = values;

    let encoded = <EngineRowCodec as db_core::ValueCodec<Vec<EngineValue>>>::encode_to_vec(&value);
    let decoded = <EngineRowCodec as db_core::ValueCodec<Vec<EngineValue>>>::decode_checked(&encoded)
      .expect("decode failed");

    prop_assert_eq!(decoded, value);
  }

  #[test]
  fn engine_key_ordering(a in engine_value_strategy(), b in engine_value_strategy()) {
    let left_key = EngineKey::from_values(vec![a.clone()]);
    let right_key = EngineKey::from_values(vec![b.clone()]);

    let codec = EngineKeyCodec;
    let mut left_scratch = db_core::KeyScratch::with_capacity(128);
    codec.encode_into(&left_key, &mut left_scratch);
    let left_encoded = left_scratch.as_slice().to_vec();

    let mut right_scratch = db_core::KeyScratch::with_capacity(128);
    codec.encode_into(&right_key, &mut right_scratch);
    let right_encoded = right_scratch.as_slice().to_vec();

    let cmp_encoded = EngineKeyCodec::compare(&left_encoded, &right_encoded);
    let cmp_keys = left_key.cmp(&right_key);

    prop_assert_eq!(cmp_encoded, cmp_keys);
  }
}
