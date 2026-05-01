use alloc::vec::Vec;
use core::cmp::Ordering;

/// Encodes and decodes a value for a storage backend.
pub trait ValueCodec<T>: Send + Sync + 'static {
  type Bytes<'a>: AsRef<[u8]> + 'a
  where
    Self: 'a,
    T: 'a;

  /// Returns the fixed byte width for the encoded value when known.
  fn fixed_width() -> Option<usize> {
    None
  }

  /// Encodes the provided value.
  fn encode<'a>(value: &'a T) -> Self::Bytes<'a>;

  /// Decodes a stored value.
  fn decode(data: &[u8]) -> T;

  /// Decode a stored value, returning a `Result` for callers that want to
  /// handle decode failures instead of panicking. Default implementation
  /// simply calls `decode` and wraps the result in `Ok`, allowing existing
  /// codecs to opt into fallible decoding by overriding this method.
  fn decode_checked(data: &[u8]) -> Result<T, crate::DecodeError> {
    Ok(Self::decode(data))
  }

  /// Convenience helper for codecs that always allocate.
  fn encode_to_vec(value: &T) -> Vec<u8> {
    Self::encode(value).as_ref().to_vec()
  }
}

/// Extends a value codec with key comparison over encoded bytes.
pub trait KeyCodec<T>: ValueCodec<T> {
  /// Compares two encoded keys using the domain ordering.
  fn compare(left: &[u8], right: &[u8]) -> Ordering;
}

/// Stable wire format version currently used by engine codecs.
pub const CURRENT_CODEC_VERSION: u8 = 1;

/// Errors that can occur while decoding a stored value.
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DecodeError {
  #[error("invalid version: {0}")]
  InvalidVersion(u8),

  #[error("truncated data")]
  Truncated,

  #[error("malformed data")]
  Malformed,
}

/// Minimal codec trait that engine code can depend on at the storage boundary.
/// Implementations live in engine/backends and must guarantee that
/// `compare_encoded_keys(encode_key(k1), encode_key(k2)) == k1.cmp(&k2)`.
pub trait StorageCodec<K, V>: Send + Sync + 'static {
  /// Encode a canonical, ordering-preserving key into `dst`.
  /// Implementations SHOULD prefix a version byte and append the payload.
  fn encode_key(&self, key: &K, dst: &mut Vec<u8>);

  /// Encode a value blob into `dst` (may include versioning metadata).
  fn encode_value(&self, value: &V, dst: &mut Vec<u8>);

  /// Decode a value previously produced by `encode_value`.
  fn decode_value(&self, src: &[u8]) -> Result<V, DecodeError>;

  /// Compare two encoded keys (byte slices). Must be consistent across versions.
  fn compare_encoded_keys(&self, a: &[u8], b: &[u8]) -> Ordering;

  /// Convenience helper: encode `key` into a provided `KeyScratch` to avoid
  /// allocating temporary `Vec<u8>` on hot paths. Implementations may
  /// override this to provide more efficient, allocation-free encoders.
  fn encode_key_into_scratch(&self, key: &K, scratch: &mut KeyScratch) {
    // Default fallback: encode into a temporary Vec and append to scratch.
    let mut tmp: Vec<u8> = Vec::new();
    self.encode_key(key, &mut tmp);
    scratch.buf.extend_from_slice(&tmp);
  }
}

/// Reusable scratch buffer used by hot-path encode helpers to avoid allocations.
pub struct KeyScratch {
  pub buf: Vec<u8>,
}

impl KeyScratch {
  pub fn with_capacity(cap: usize) -> Self {
    Self {
      buf: Vec::with_capacity(cap),
    }
  }

  pub fn clear(&mut self) {
    self.buf.clear();
  }

  pub fn as_slice(&self) -> &[u8] {
    &self.buf
  }

  pub fn len(&self) -> usize {
    self.buf.len()
  }

  pub fn is_empty(&self) -> bool {
    self.buf.is_empty()
  }
}

/// Lightweight sink trait for writer-based/streaming encoders.
///
/// This abstraction lets encoder helpers append bytes into either a
/// `Vec<u8>`, a `KeyScratch`, or any `std::io::Write` when the `std`
/// feature is enabled without allocating intermediate `Vec`s.
pub trait BufferSink {
  /// Append bytes to the sink.
  fn push_bytes(&mut self, bytes: &[u8]);
}

impl BufferSink for KeyScratch {
  fn push_bytes(&mut self, bytes: &[u8]) {
    self.buf.extend_from_slice(bytes);
  }
}

#[cfg(not(feature = "std"))]
impl BufferSink for Vec<u8> {
  fn push_bytes(&mut self, bytes: &[u8]) {
    self.extend_from_slice(bytes);
  }
}

#[cfg(feature = "std")]
impl<T: std::io::Write> BufferSink for T {
  fn push_bytes(&mut self, bytes: &[u8]) {
    // Best-effort: ignore write errors inside codec helpers; callers may
    // prefer explicit writers and handle errors themselves. Keep this
    // intentionally simple for hot-path encoders.
    let _ = self.write_all(bytes);
  }
}

/// Encode `key` using `codec` and return an owned `Vec<u8>`.
pub fn encode_key_to_vec<S, K, V>(codec: &S, key: &K) -> Vec<u8>
where
  S: StorageCodec<K, V>,
{
  let mut out: Vec<u8> = Vec::new();
  codec.encode_key(key, &mut out);
  out
}

/// Encode `key` into the provided scratch buffer (avoids allocation).
pub fn encode_key_into_scratch<S, K, V>(codec: &S, key: &K, scratch: &mut KeyScratch)
where
  S: StorageCodec<K, V>,
{
  codec.encode_key_into_scratch(key, scratch);
}

/// Compare two encoded keys using the codec's comparison function.
pub fn compare_encoded_keys<S, K, V>(codec: &S, a: &[u8], b: &[u8]) -> Ordering
where
  S: StorageCodec<K, V>,
{
  codec.compare_encoded_keys(a, b)
}

/// Encode a value into an owned `Vec<u8>`.
pub fn encode_value_to_vec<S, K, V>(codec: &S, value: &V) -> Vec<u8>
where
  S: StorageCodec<K, V>,
{
  let mut out: Vec<u8> = Vec::new();
  codec.encode_value(value, &mut out);
  out
}

/// Decode a stored value produced by `encode_value`.
pub fn decode_value_to_vec<S, K, V>(codec: &S, src: &[u8]) -> Result<V, DecodeError>
where
  S: StorageCodec<K, V>,
{
  codec.decode_value(src)
}

/// Fast-path helpers for common engine callers. Implementations SHOULD provide
/// efficient, allocation-minimizing encoders that append into a provided
/// `KeyScratch`. The trait extends `KeyCodec<T>` so default helpers can fall
/// back to the existing encoding when a specialized implementation is not
/// available.
pub trait FastKeyCodec<T>: KeyCodec<T> {
  /// Append an encoded representation of `value` into `scratch`.
  fn encode_into(&self, value: &T, scratch: &mut KeyScratch) {
    // Default fallback: allocate via `ValueCodec::encode` and append.
    let bytes = <Self as ValueCodec<T>>::encode(value);
    scratch.buf.extend_from_slice(bytes.as_ref());
  }

  /// Compare two encoded byte slices using codec ordering. Default falls back
  /// to decoding and comparing via `KeyCodec::compare`.
  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> Ordering {
    <Self as KeyCodec<T>>::compare(left, right)
  }
}

/// Extends a value codec with an allocation-free encoding path.
pub trait FastValueCodec<T>: ValueCodec<T> {
  /// Encode `value` directly into `dst`.
  fn encode_into(&self, value: &T, dst: &mut Vec<u8>) {
    dst.extend_from_slice(<Self as ValueCodec<T>>::encode(value).as_ref());
  }
}

impl<T, V> FastValueCodec<V> for T where T: ValueCodec<V> {}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::IntegerI64Codec;

  struct TestStorageCodec;

  impl StorageCodec<i64, ()> for TestStorageCodec {
    fn encode_key(&self, key: &i64, dst: &mut Vec<u8>) {
      // simple version prefix + integer encoding
      dst.push(CURRENT_CODEC_VERSION);
      let enc = <IntegerI64Codec as ValueCodec<i64>>::encode(key);
      dst.extend_from_slice(enc.as_ref());
    }

    fn encode_value(&self, _value: &(), _dst: &mut Vec<u8>) {
      // no-op for test
    }

    fn decode_value(&self, _src: &[u8]) -> Result<(), DecodeError> {
      Ok(())
    }

    fn compare_encoded_keys(&self, a: &[u8], b: &[u8]) -> Ordering {
      let a_payload = if !a.is_empty() { &a[1..] } else { a };
      let b_payload = if !b.is_empty() { &b[1..] } else { b };
      <IntegerI64Codec as KeyCodec<i64>>::compare(a_payload, b_payload)
    }
  }

  #[test]
  fn encode_into_scratch_matches_encode_key() {
    let codec = TestStorageCodec;
    let mut tmp = Vec::new();
    codec.encode_key(&42, &mut tmp);

    let mut s = KeyScratch::with_capacity(32);
    codec.encode_key_into_scratch(&42, &mut s);

    assert_eq!(s.as_slice(), tmp.as_slice());
  }

  #[test]
  fn compare_encoded_consistent_with_domain() {
    let codec = TestStorageCodec;
    let mut a = Vec::new();
    codec.encode_key(&-10, &mut a);
    let mut b = Vec::new();
    codec.encode_key(&10, &mut b);
    assert_eq!(codec.compare_encoded_keys(&a, &b), (-10i64).cmp(&10i64));
  }
}
