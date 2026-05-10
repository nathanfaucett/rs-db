//! Shared store-level codec helpers (sink + decode helpers)
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

use db_core::{
  BufferSink, Cursor, DecodeError, canonical_f64_bits_into_sink, decode_bool, decode_bytes,
  decode_len, decode_string, decode_usize, encode_bool_into_sink, encode_bytes_into_sink,
  encode_i64_into_sink, encode_len_into_sink, encode_string_into_sink, encode_usize_into_sink,
  encode_with_version,
};

use crate::engine_types::{EngineKey, EngineRow, EngineType, EngineValue};
use crate::schema::{ColumnSchema, IndexSchema, TableSchema};
use crate::store::{StoreKey, StoreValue};

// Engine type/value/key/row codec (moved from db-core) -----------------

pub fn encode_engine_type_into_sink<S: BufferSink>(sink: &mut S, value: &EngineType) {
  let tag = match value {
    EngineType::Integer => 0,
    EngineType::Float => 1,
    EngineType::Text => 2,
    EngineType::Blob => 3,
  };
  sink.push_bytes(&[tag]);
}

pub fn decode_engine_type(cursor: &mut Cursor<'_>) -> Result<EngineType, DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(EngineType::Integer),
    1 => Ok(EngineType::Float),
    2 => Ok(EngineType::Text),
    3 => Ok(EngineType::Blob),
    _ => Err(DecodeError::Malformed),
  }
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

pub fn decode_engine_value(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(EngineValue::Null),
    1 => Ok(EngineValue::Integer(cursor.read_i64()?)),
    2 => Ok(EngineValue::Float(f64::from_bits(cursor.read_u64()?))),
    3 => Ok(EngineValue::Text(decode_string(cursor)?)),
    4 => Ok(EngineValue::Blob(decode_bytes(cursor)?)),
    _ => Err(DecodeError::Malformed),
  }
}

fn decode_vec<T, F>(cursor: &mut Cursor<'_>, mut decode: F) -> Result<Vec<T>, DecodeError>
where
  F: FnMut(&mut Cursor<'_>) -> Result<T, DecodeError>,
{
  let len = decode_len(cursor)?;
  let mut out = Vec::with_capacity(len);
  for _ in 0..len {
    out.push(decode(cursor)?);
  }
  Ok(out)
}

pub fn encode_engine_row_into_sink<S: BufferSink>(sink: &mut S, row: &EngineRow) {
  encode_len_into_sink(sink, row.len());
  for value in row {
    encode_engine_value_into_sink(sink, value);
  }
}

pub fn decode_engine_row(cursor: &mut Cursor<'_>) -> Result<EngineRow, DecodeError> {
  decode_vec(cursor, decode_engine_value)
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

pub fn decode_engine_key(cursor: &mut Cursor<'_>) -> Result<EngineKey, DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(EngineKey::Scalar(decode_engine_value(cursor)?)),
    1 => {
      let values = decode_vec(cursor, decode_engine_value)?;
      Ok(EngineKey::Tuple(values))
    }
    _ => Err(DecodeError::Malformed),
  }
}

// Schema codec ---------------------------------------------------------

pub fn encode_column_schema_into_sink<S: BufferSink>(sink: &mut S, value: &ColumnSchema) {
  encode_string_into_sink(sink, &value.name);
  encode_engine_type_into_sink(sink, &value.data_type);
}

pub fn encode_table_schema_into_sink<S: BufferSink>(sink: &mut S, value: &TableSchema) {
  encode_string_into_sink(sink, &value.name);
  encode_len_into_sink(sink, value.columns.len());
  for column in &value.columns {
    encode_column_schema_into_sink(sink, column);
  }
  encode_len_into_sink(sink, value.primary_key.len());
  for index in &value.primary_key {
    encode_usize_into_sink(sink, *index);
  }
}

pub fn encode_index_schema_into_sink<S: BufferSink>(sink: &mut S, value: &IndexSchema) {
  encode_string_into_sink(sink, &value.name);
  encode_string_into_sink(sink, &value.table_name);
  encode_len_into_sink(sink, value.column_indices.len());
  for index in &value.column_indices {
    encode_usize_into_sink(sink, *index);
  }
  encode_bool_into_sink(sink, value.unique);
}

pub fn decode_column_schema(cursor: &mut Cursor<'_>) -> Result<ColumnSchema, DecodeError> {
  Ok(ColumnSchema {
    name: decode_string(cursor)?,
    data_type: decode_engine_type(cursor)?,
  })
}

pub fn decode_table_schema(cursor: &mut Cursor<'_>) -> Result<TableSchema, DecodeError> {
  let name = decode_string(cursor)?;
  let columns = decode_vec(cursor, decode_column_schema)?;
  let primary_key = decode_vec(cursor, decode_usize)?;

  Ok(TableSchema {
    name,
    columns,
    primary_key,
  })
}

pub fn decode_index_schema(cursor: &mut Cursor<'_>) -> Result<IndexSchema, DecodeError> {
  let name = decode_string(cursor)?;
  let table_name = decode_string(cursor)?;
  let column_indices = decode_vec(cursor, decode_usize)?;
  let unique = decode_bool(cursor)?;

  Ok(IndexSchema {
    name,
    table_name,
    column_indices,
    unique,
  })
}

pub fn encode_store_key_into_sink<S: BufferSink>(sink: &mut S, value: &StoreKey) {
  match value {
    StoreKey::TableRow {
      table_name,
      primary_key,
    } => {
      sink.push_bytes(&[0]);
      encode_string_into_sink(sink, table_name);
      encode_engine_key_into_sink(sink, primary_key);
    }
    StoreKey::IndexEntry {
      index_name,
      index_key,
      row_pk,
    } => {
      sink.push_bytes(&[1]);
      encode_string_into_sink(sink, index_name);
      encode_engine_key_into_sink(sink, index_key);
      encode_engine_key_into_sink(sink, row_pk);
    }
    StoreKey::TableSchema { table_name } => {
      sink.push_bytes(&[2]);
      encode_string_into_sink(sink, table_name);
    }
    StoreKey::IndexSchema { index_name } => {
      sink.push_bytes(&[3]);
      encode_string_into_sink(sink, index_name);
    }
  }
}

pub fn encode_store_value_into_sink<S: BufferSink>(sink: &mut S, value: &StoreValue) {
  match value {
    StoreValue::Row(row) => {
      sink.push_bytes(&[0]);
      encode_engine_row_into_sink(sink, row);
    }
    StoreValue::IndexEntry => sink.push_bytes(&[1]),
    StoreValue::TableSchema(schema) => {
      sink.push_bytes(&[2]);
      encode_table_schema_into_sink(sink, schema);
    }
    StoreValue::IndexSchema(schema) => {
      sink.push_bytes(&[3]);
      encode_index_schema_into_sink(sink, schema);
    }
  }
}

// Decoding helpers (cursor-based) --------------------------------------
pub fn decode_store_key(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(StoreKey::table_row(
      decode_string(cursor)?,
      decode_engine_key(cursor)?,
    )),
    1 => Ok(StoreKey::index_entry(
      decode_string(cursor)?,
      decode_engine_key(cursor)?,
      decode_engine_key(cursor)?,
    )),
    2 => Ok(StoreKey::table_schema(decode_string(cursor)?)),
    3 => Ok(StoreKey::index_schema(decode_string(cursor)?)),
    _ => Err(DecodeError::Malformed),
  }
}

pub fn decode_store_value(cursor: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(StoreValue::Row(decode_engine_row(cursor)?)),
    1 => Ok(StoreValue::IndexEntry),
    2 => Ok(StoreValue::TableSchema(decode_table_schema(cursor)?)),
    3 => Ok(StoreValue::IndexSchema(decode_index_schema(cursor)?)),
    _ => Err(DecodeError::Malformed),
  }
}

// Convenience owned-vector helpers (keep small and allocation-friendly)
pub fn encode_store_key(buffer: &mut Vec<u8>, value: &StoreKey) {
  encode_with_version(buffer, |b| encode_store_key_into_sink(b, value));
}

pub fn encode_store_value(buffer: &mut Vec<u8>, value: &StoreValue) {
  encode_with_version(buffer, |b| encode_store_value_into_sink(b, value));
}
