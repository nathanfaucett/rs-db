use alloc::string::String;
use alloc::vec::Vec;

use crate::BufferSink;
use crate::CURRENT_CODEC_VERSION;

/// Writer/sink-oriented helpers for engine-level values (EngineType/Value/Key/Row).
pub fn encode_version_into_sink<S: BufferSink>(sink: &mut S) {
  sink.push_bytes(&[CURRENT_CODEC_VERSION]);
}

pub fn encode_u32_into_sink<S: BufferSink>(sink: &mut S, value: u32) {
  sink.push_bytes(&value.to_be_bytes());
}

pub fn encode_u64_into_sink<S: BufferSink>(sink: &mut S, value: u64) {
  sink.push_bytes(&value.to_be_bytes());
}

pub fn encode_i64_into_sink<S: BufferSink>(sink: &mut S, value: i64) {
  sink.push_bytes(&value.to_be_bytes());
}

pub fn encode_len_into_sink<S: BufferSink>(sink: &mut S, len: usize) {
  let len = u32::try_from(len)
    .unwrap_or_else(|_| panic!("invalid store codec payload: length exceeds u32"));
  encode_u32_into_sink(sink, len);
}

pub fn encode_string_into_sink<S: BufferSink>(sink: &mut S, value: &str) {
  encode_len_into_sink(sink, value.len());
  sink.push_bytes(value.as_bytes());
}

pub fn encode_bytes_into_sink<S: BufferSink>(sink: &mut S, value: &[u8]) {
  encode_len_into_sink(sink, value.len());
  sink.push_bytes(value);
}

pub fn encode_usize_into_sink<S: BufferSink>(sink: &mut S, value: usize) {
  let value = u64::try_from(value)
    .unwrap_or_else(|_| panic!("invalid store codec payload: usize exceeds u64"));
  encode_u64_into_sink(sink, value);
}

pub fn canonical_f64_bits_into_sink<S: BufferSink>(sink: &mut S, value: f64) {
  let bits = canonical_f64_bits(value);
  encode_u64_into_sink(sink, bits);
}

pub fn encode_bool_into_sink<S: BufferSink>(sink: &mut S, value: bool) {
  sink.push_bytes(&[u8::from(value)]);
}

// --- Cursor and decoding helpers ---------------------------------------

pub struct Cursor<'a> {
  data: &'a [u8],
  position: usize,
}

impl<'a> Cursor<'a> {
  pub fn new(data: &'a [u8]) -> Self {
    Self { data, position: 0 }
  }
  pub fn finish(self) -> Result<(), crate::DecodeError> {
    if self.position != self.data.len() {
      return Err(crate::DecodeError::Malformed);
    }
    Ok(())
  }

  pub fn read_u8(&mut self) -> Result<u8, crate::DecodeError> {
    let s = self.read_exact(1)?;
    Ok(s[0])
  }

  pub fn read_u32(&mut self) -> Result<u32, crate::DecodeError> {
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(self.read_exact(4)?);
    Ok(u32::from_be_bytes(bytes))
  }

  pub fn read_u64(&mut self) -> Result<u64, crate::DecodeError> {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(self.read_exact(8)?);
    Ok(u64::from_be_bytes(bytes))
  }

  pub fn read_i64(&mut self) -> Result<i64, crate::DecodeError> {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(self.read_exact(8)?);
    Ok(i64::from_be_bytes(bytes))
  }

  pub fn read_exact(&mut self, len: usize) -> Result<&'a [u8], crate::DecodeError> {
    let end = self
      .position
      .checked_add(len)
      .filter(|end| *end <= self.data.len())
      .ok_or(crate::DecodeError::Truncated)?;
    let slice = &self.data[self.position..end];
    self.position = end;
    Ok(slice)
  }
}

pub fn encode_version(buffer: &mut Vec<u8>) {
  encode_version_into_sink(buffer);
}

pub fn decode_version(cursor: &mut Cursor<'_>) -> Result<(), crate::DecodeError> {
  let version = cursor.read_u8()?;
  if version != CURRENT_CODEC_VERSION {
    return Err(crate::DecodeError::InvalidVersion(version));
  }
  Ok(())
}

pub fn encode_bool(buffer: &mut Vec<u8>, value: bool) {
  encode_bool_into_sink(buffer, value);
}

pub fn decode_bool(cursor: &mut Cursor<'_>) -> Result<bool, crate::DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(false),
    1 => Ok(true),
    _ => Err(crate::DecodeError::Malformed),
  }
}

pub fn encode_u32(buffer: &mut Vec<u8>, value: u32) {
  encode_u32_into_sink(buffer, value);
}

pub fn encode_u64(buffer: &mut Vec<u8>, value: u64) {
  encode_u64_into_sink(buffer, value);
}

pub fn encode_i64(buffer: &mut Vec<u8>, value: i64) {
  encode_i64_into_sink(buffer, value);
}

pub fn encode_len(buffer: &mut Vec<u8>, len: usize) {
  encode_len_into_sink(buffer, len);
}

pub fn decode_len(cursor: &mut Cursor<'_>) -> Result<usize, crate::DecodeError> {
  let v = cursor.read_u32()?;
  usize::try_from(v).map_err(|_| crate::DecodeError::Malformed)
}

pub fn encode_string(buffer: &mut Vec<u8>, value: &str) {
  encode_string_into_sink(buffer, value);
}

pub fn decode_string(cursor: &mut Cursor<'_>) -> Result<String, crate::DecodeError> {
  let len = decode_len(cursor)?;
  let bytes = cursor.read_exact(len)?;
  String::from_utf8(bytes.to_vec()).map_err(|_| crate::DecodeError::Malformed)
}

pub fn encode_bytes(buffer: &mut Vec<u8>, value: &[u8]) {
  encode_bytes_into_sink(buffer, value);
}

pub fn decode_bytes(cursor: &mut Cursor<'_>) -> Result<Vec<u8>, crate::DecodeError> {
  let len = decode_len(cursor)?;
  Ok(cursor.read_exact(len)?.to_vec())
}

pub fn encode_usize(buffer: &mut Vec<u8>, value: usize) {
  encode_usize_into_sink(buffer, value);
}

pub fn decode_usize(cursor: &mut Cursor<'_>) -> Result<usize, crate::DecodeError> {
  let v = cursor.read_u64()?;
  usize::try_from(v).map_err(|_| crate::DecodeError::Malformed)
}

pub fn canonical_f64_bits(value: f64) -> u64 {
  if value == 0.0 {
    0
  } else if value.is_nan() {
    f64::NAN.to_bits()
  } else {
    value.to_bits()
  }
}
