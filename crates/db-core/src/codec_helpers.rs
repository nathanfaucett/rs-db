use alloc::vec::Vec;

use crate::codec_primitives::Cursor;

/// Small helpers to reduce repeated encode/decode boilerplate.
///
/// `encode_with_version` prefixes `buffer` with the canonical codec
/// version byte and then runs the provided closure to append payload.
pub fn encode_with_version<F>(buffer: &mut Vec<u8>, f: F)
where
  F: FnOnce(&mut Vec<u8>),
{
  crate::codec_primitives::encode_version(buffer);
  f(buffer);
}

/// Decode a value from `data` using a `Cursor` and ensure the cursor
/// consumed the entire slice (helps centralize the common pattern).
pub fn decode_from_slice<T, F>(data: &[u8], f: F) -> Result<T, crate::DecodeError>
where
  F: FnOnce(&mut Cursor<'_>) -> Result<T, crate::DecodeError>,
{
  let mut c = Cursor::new(data);
  let out = f(&mut c)?;
  c.finish()?;
  Ok(out)
}

/// Decode a buffer that begins with the canonical codec version byte.
/// This is the common pattern for store-level payload decoding.
pub fn decode_with_version<T, F>(data: &[u8], f: F) -> Result<T, crate::DecodeError>
where
  F: FnOnce(&mut Cursor<'_>) -> Result<T, crate::DecodeError>,
{
  decode_from_slice(data, |cursor| {
    crate::codec_primitives::decode_version(cursor)?;
    f(cursor)
  })
}
