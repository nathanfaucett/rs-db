// Codec roundtrip example: encode a `StoreValue::Row` and decode it back.
use db_engine::{EngineValue, StoreValue, StoreValueCodec};

fn main() {
  let value = StoreValue::Row(vec![
    EngineValue::Integer(1),
    EngineValue::Text("Alice".into()),
  ]);

  // Use the fully-qualified trait methods for encoding/decoding
  let encoded = <StoreValueCodec as db_core::ValueCodec<StoreValue>>::encode_to_vec(&value);
  let decoded = <StoreValueCodec as db_core::ValueCodec<StoreValue>>::decode_checked(&encoded)
    .expect("decode failed");

  println!("original: {:?}", value);
  println!("decoded:  {:?}", decoded);

  assert_eq!(decoded, value);
  println!("Codec roundtrip succeeded");
}
