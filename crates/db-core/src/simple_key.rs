// Lightweight, ergonomic key/value codecs for common types.
#![allow(clippy::missing_const_for_fn)]
extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cmp::Ordering;

use crate::{FastKeyCodec, KeyCodec, KeyScratch, ValueCodec};

/// Fixed-width 8-byte, order-preserving `i64` codec.
#[derive(Debug, Clone, Copy, Default)]
pub struct IntegerI64Codec;

impl ValueCodec<i64> for IntegerI64Codec {
  type Bytes<'a>
    = alloc::vec::Vec<u8>
  where
    Self: 'a,
    i64: 'a;

  fn fixed_width() -> Option<usize> {
    Some(8)
  }

  fn encode<'a>(value: &'a i64) -> Self::Bytes<'a> {
    let bits = (*value as u64) ^ 0x8000_0000_0000_0000u64;
    bits.to_be_bytes().to_vec()
  }

  fn decode(data: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&data[..8]);
    let enc = u64::from_be_bytes(arr);
    let bits = enc ^ 0x8000_0000_0000_0000u64;
    i64::from_be_bytes(bits.to_be_bytes())
  }
}

impl KeyCodec<i64> for IntegerI64Codec {
  fn compare(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
  }
}

impl FastKeyCodec<i64> for IntegerI64Codec {
  fn encode_into(&self, value: &i64, scratch: &mut KeyScratch) {
    let bytes = <IntegerI64Codec as ValueCodec<i64>>::encode(value);
    scratch.buf.extend_from_slice(bytes.as_ref());
  }
}

/// UTF-8 string codec (owned `String`).
#[derive(Debug, Clone, Copy, Default)]
pub struct Utf8Codec;

impl ValueCodec<String> for Utf8Codec {
  type Bytes<'a>
    = alloc::vec::Vec<u8>
  where
    Self: 'a,
    String: 'a;

  fn fixed_width() -> Option<usize> {
    None
  }

  fn encode<'a>(value: &'a String) -> Self::Bytes<'a> {
    value.as_bytes().to_vec()
  }

  fn decode(data: &[u8]) -> String {
    String::from_utf8(data.to_vec()).expect("invalid utf-8")
  }
}

impl KeyCodec<String> for Utf8Codec {
  fn compare(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
  }
}

impl FastKeyCodec<String> for Utf8Codec {
  fn encode_into(&self, value: &String, scratch: &mut KeyScratch) {
    let bytes = <Utf8Codec as ValueCodec<String>>::encode(value);
    scratch.buf.extend_from_slice(bytes.as_ref());
  }
}

/// Generic 2-tuple composite codec. Uses an escape+terminator scheme for
/// variable-length components and concatenates fixed-width components.
#[derive(Debug, Clone, Copy, Default)]
pub struct Tuple2Codec<CA, CB>(core::marker::PhantomData<(CA, CB)>);

fn append_escaped(src: &[u8], dst: &mut Vec<u8>) {
  for &b in src {
    if b == 0 {
      dst.push(0);
      dst.push(0xFF);
    } else {
      dst.push(b);
    }
  }
  dst.push(0);
  dst.push(0);
}

fn parse_escaped(src: &[u8]) -> (Vec<u8>, usize) {
  let mut out = Vec::new();
  let mut i = 0usize;
  while i < src.len() {
    let b = src[i];
    if b == 0 {
      let next = src.get(i + 1).copied().unwrap_or(0);
      if next == 0 {
        i += 2;
        break;
      } else if next == 0xFF {
        out.push(0);
        i += 2;
      } else {
        out.push(next);
        i += 2;
      }
    } else {
      out.push(b);
      i += 1;
    }
  }
  (out, i)
}

impl<CA, CB, A, B> ValueCodec<(A, B)> for Tuple2Codec<CA, CB>
where
  CA: ValueCodec<A> + KeyCodec<A> + FastKeyCodec<A>,
  CB: ValueCodec<B> + KeyCodec<B> + FastKeyCodec<B>,
{
  type Bytes<'a>
    = alloc::vec::Vec<u8>
  where
    Self: 'a,
    (A, B): 'a;

  fn fixed_width() -> Option<usize> {
    None
  }

  fn encode<'a>(value: &'a (A, B)) -> Self::Bytes<'a> {
    let mut out = Vec::new();
    // first component
    if let Some(_w) = <CA as ValueCodec<A>>::fixed_width() {
      let b = <CA as ValueCodec<A>>::encode(&value.0);
      out.extend_from_slice(b.as_ref());
    } else {
      let b = <CA as ValueCodec<A>>::encode(&value.0);
      append_escaped(b.as_ref(), &mut out);
    }
    // second component
    if let Some(_w) = <CB as ValueCodec<B>>::fixed_width() {
      let b = <CB as ValueCodec<B>>::encode(&value.1);
      out.extend_from_slice(b.as_ref());
    } else {
      let b = <CB as ValueCodec<B>>::encode(&value.1);
      append_escaped(b.as_ref(), &mut out);
    }
    out
  }

  fn decode(data: &[u8]) -> (A, B) {
    let mut offset = 0usize;
    let a = if let Some(w) = <CA as ValueCodec<A>>::fixed_width() {
      let end = offset + w;
      let v = <CA as ValueCodec<A>>::decode(&data[offset..end]);
      offset = end;
      v
    } else {
      let (vec_a, consumed) = parse_escaped(&data[offset..]);
      offset += consumed;
      <CA as ValueCodec<A>>::decode(&vec_a)
    };

    let b = if let Some(w) = <CB as ValueCodec<B>>::fixed_width() {
      let end = offset + w;
      <CB as ValueCodec<B>>::decode(&data[offset..end])
    } else {
      let (vec_b, _consumed) = parse_escaped(&data[offset..]);
      <CB as ValueCodec<B>>::decode(&vec_b)
    };
    (a, b)
  }
}

impl<CA, CB, A, B> KeyCodec<(A, B)> for Tuple2Codec<CA, CB>
where
  CA: ValueCodec<A> + KeyCodec<A> + FastKeyCodec<A>,
  CB: ValueCodec<B> + KeyCodec<B> + FastKeyCodec<B>,
{
  fn compare(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
  }
}

impl<CA, CB, A, B> FastKeyCodec<(A, B)> for Tuple2Codec<CA, CB>
where
  CA: ValueCodec<A> + KeyCodec<A> + FastKeyCodec<A>,
  CB: ValueCodec<B> + KeyCodec<B> + FastKeyCodec<B>,
{
  fn encode_into(&self, value: &(A, B), scratch: &mut KeyScratch) {
    let bytes = <Tuple2Codec<CA, CB> as ValueCodec<(A, B)>>::encode(value);
    scratch.buf.extend_from_slice(bytes.as_ref());
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn integer_roundtrip_and_ordering() {
    let values = vec![-100i64, -1, 0, 1, 42, i64::MAX, i64::MIN + 1];
    for v in &values {
      let enc = <IntegerI64Codec as ValueCodec<i64>>::encode(v);
      let dec = <IntegerI64Codec as ValueCodec<i64>>::decode(&enc);
      assert_eq!(dec, *v);
    }

    let mut pairs: Vec<(i64, Vec<u8>)> = values
      .iter()
      .map(|v| (*v, <IntegerI64Codec as ValueCodec<i64>>::encode(v)))
      .collect();
    pairs.sort_by_key(|a| a.0);
    let enc_sorted: Vec<Vec<u8>> = pairs.iter().map(|p| p.1.clone()).collect();

    let mut enc_only: Vec<Vec<u8>> = values
      .iter()
      .map(<IntegerI64Codec as ValueCodec<i64>>::encode)
      .collect();
    enc_only.sort();
    assert_eq!(enc_only, enc_sorted);
  }

  #[test]
  fn tuple_roundtrip_and_ordering() {
    type TC = Tuple2Codec<IntegerI64Codec, Utf8Codec>;
    let items = vec![
      (1i64, String::from("a")),
      (1i64, String::from("b")),
      (0i64, String::from("zz")),
      (2i64, String::from("")),
    ];
    for it in &items {
      let enc = TC::encode(it);
      let dec = TC::decode(&enc);
      assert_eq!(&dec, it);
    }

    let mut tuple_sorted = items.clone();
    tuple_sorted.sort();

    let mut encoded: Vec<(Vec<u8>, (i64, String))> =
      items.into_iter().map(|t| (TC::encode(&t), t)).collect();
    encoded.sort_by(|a, b| a.0.cmp(&b.0));
    let decoded_sorted: Vec<(i64, String)> = encoded.into_iter().map(|(_, t)| t).collect();
    assert_eq!(decoded_sorted, tuple_sorted);
  }
}
