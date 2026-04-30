//! Shared store-level codec helpers (sink + decode helpers)
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

use db_core::*;

use crate::schema::{ColumnSchema, IndexSchema, TableSchema};
use crate::store::{StoreKey, StoreValue};

pub fn encode_column_schema_into_sink<S: db_core::BufferSink>(sink: &mut S, value: &ColumnSchema) {
  encode_string_into_sink(sink, &value.name);
  encode_engine_type_into_sink(sink, &value.data_type);
}

pub fn encode_table_schema_into_sink<S: db_core::BufferSink>(sink: &mut S, value: &TableSchema) {
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

pub fn encode_index_schema_into_sink<S: db_core::BufferSink>(sink: &mut S, value: &IndexSchema) {
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
  let columns_len = decode_len(cursor)?;
  let mut columns = Vec::with_capacity(columns_len);
  for _ in 0..columns_len {
    columns.push(decode_column_schema(cursor)?);
  }
  let primary_key_len = decode_len(cursor)?;
  let mut primary_key = Vec::with_capacity(primary_key_len);
  for _ in 0..primary_key_len {
    primary_key.push(decode_usize(cursor)?);
  }

  Ok(TableSchema {
    name,
    columns,
    primary_key,
  })
}

pub fn decode_index_schema(cursor: &mut Cursor<'_>) -> Result<IndexSchema, DecodeError> {
  let name = decode_string(cursor)?;
  let table_name = decode_string(cursor)?;
  let indices_len = decode_len(cursor)?;
  let mut column_indices = Vec::with_capacity(indices_len);
  for _ in 0..indices_len {
    column_indices.push(decode_usize(cursor)?);
  }
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
