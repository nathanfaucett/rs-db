use alloc::string::String;
use alloc::vec::Vec;

use crate::BufferSink;
use crate::CURRENT_CODEC_VERSION;
use crate::{EngineKey, EngineRow, EngineType, EngineValue};

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

pub fn encode_engine_type_into_sink<S: BufferSink>(sink: &mut S, value: &EngineType) {
  let tag = match value {
    EngineType::Integer => 0,
    EngineType::Float => 1,
    EngineType::Text => 2,
    EngineType::Blob => 3,
  };
  sink.push_bytes(&[tag]);
}

pub fn encode_engine_value_into_sink<S: BufferSink>(sink: &mut S, value: &EngineValue) {
  match value {
    EngineValue::Null => sink.push_bytes(&[0]),
    EngineValue::Integer(integer) => {
      sink.push_bytes(&[1]);
      encode_i64_into_sink(sink, *integer);
    }
    EngineValue::Float(float) => {
      sink.push_bytes(&[2]);
      canonical_f64_bits_into_sink(sink, *float);
    }
    EngineValue::Text(text) => {
      sink.push_bytes(&[3]);
      encode_string_into_sink(sink, text);
    }
    EngineValue::Blob(bytes) => {
      sink.push_bytes(&[4]);
      encode_bytes_into_sink(sink, bytes);
    }
  }
}

pub fn encode_engine_row_into_sink<S: BufferSink>(sink: &mut S, row: &EngineRow) {
  encode_len_into_sink(sink, row.len());
  for value in row {
    encode_engine_value_into_sink(sink, value);
  }
}

pub fn encode_engine_key_into_sink<S: BufferSink>(sink: &mut S, value: &EngineKey) {
  match value {
    EngineKey::Scalar(scalar) => {
      sink.push_bytes(&[0]);
      encode_engine_value_into_sink(sink, scalar);
    }
    EngineKey::Tuple(values) => {
      sink.push_bytes(&[1]);
      encode_len_into_sink(sink, values.len());
      for value in values {
        encode_engine_value_into_sink(sink, value);
      }
    }
  }
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

pub fn encode_engine_type(buffer: &mut Vec<u8>, value: &EngineType) {
  encode_engine_type_into_sink(buffer, value);
}

pub fn decode_engine_type(cursor: &mut Cursor<'_>) -> Result<EngineType, crate::DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(EngineType::Integer),
    1 => Ok(EngineType::Float),
    2 => Ok(EngineType::Text),
    3 => Ok(EngineType::Blob),
    _ => Err(crate::DecodeError::Malformed),
  }
}

pub fn encode_engine_value(buffer: &mut Vec<u8>, value: &EngineValue) {
  encode_engine_value_into_sink(buffer, value);
}

pub fn decode_engine_value(cursor: &mut Cursor<'_>) -> Result<EngineValue, crate::DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(EngineValue::Null),
    1 => Ok(EngineValue::Integer(cursor.read_i64()?)),
    2 => Ok(EngineValue::Float(f64::from_bits(cursor.read_u64()?))),
    3 => Ok(EngineValue::Text(decode_string(cursor)?)),
    4 => Ok(EngineValue::Blob(decode_bytes(cursor)?)),
    _ => Err(crate::DecodeError::Malformed),
  }
}

pub fn encode_engine_row(buffer: &mut Vec<u8>, row: &EngineRow) {
  encode_engine_row_into_sink(buffer, row);
}

pub fn decode_engine_row(cursor: &mut Cursor<'_>) -> Result<EngineRow, crate::DecodeError> {
  let len = decode_len(cursor)?;
  let mut out = Vec::with_capacity(len);
  for _ in 0..len {
    out.push(decode_engine_value(cursor)?);
  }
  Ok(out)
}

pub fn decode_engine_key(cursor: &mut Cursor<'_>) -> Result<EngineKey, crate::DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(EngineKey::Scalar(decode_engine_value(cursor)?)),
    1 => {
      let len = decode_len(cursor)?;
      let mut values = Vec::with_capacity(len);
      for _ in 0..len {
        values.push(decode_engine_value(cursor)?);
      }
      Ok(EngineKey::Tuple(values))
    }
    _ => Err(crate::DecodeError::Malformed),
  }
}
