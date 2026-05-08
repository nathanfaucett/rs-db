#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "std")]
use std::string::String;

use db_core::BTreeTransaction;
use futures::{StreamExt, pin_mut};

use crate::codec::{
  decode_index_schema, decode_table_schema, encode_index_schema_into_sink,
  encode_table_schema_into_sink,
};
use crate::engine_types::{EngineKey, EngineRow, EngineValue};
use crate::schema::{IndexSchema, TableSchema};
use crate::store::{StoreKey, StoreValue};

fn decode_schema_rows<I, T, F>(rows: I, decode: F) -> Result<Vec<T>, db_core::DecodeError>
where
  I: IntoIterator<Item = EngineRow>,
  F: Fn(&[EngineValue]) -> Result<T, db_core::DecodeError>,
{
  rows.into_iter().map(|row| decode(&row)).collect()
}

/// Prefix used for row trees: `"t:{table_name}"`.
pub fn row_tree(table_name: &str) -> String {
  let mut name = String::from("t:");
  name.push_str(table_name);
  name
}

/// Prefix used for index trees: `"i:{index_name}"`.
pub fn index_tree(index_name: &str) -> String {
  let mut name = String::from("i:");
  name.push_str(index_name);
  name
}

/// Well-known tree holding all table schemas.
pub const TABLE_SCHEMA_TREE: &str = "sys:table_schemas";
/// Well-known tree holding all index schemas.
pub const INDEX_SCHEMA_TREE: &str = "sys:index_schemas";

pub fn table_schema_entry_key(table_name: impl Into<String>) -> EngineKey {
  EngineKey::Scalar(EngineValue::Text(table_name.into()))
}

pub fn index_schema_entry_key(index_name: impl Into<String>) -> EngineKey {
  EngineKey::Scalar(EngineValue::Text(index_name.into()))
}

/// Load catalog entries (table schemas and index schemas) from a storage
/// transaction. Returns storage-level `BTreeError` so callers may map to
/// engine-level errors as appropriate.
pub async fn load_catalog_impl<T>(
  tx: &mut T,
) -> Result<(Vec<TableSchema>, Vec<IndexSchema>), db_core::BTreeError>
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  let mut tables = Vec::new();
  let mut indexes = Vec::new();

  let table_schema_stream = range_table_schema_entries_impl(tx);
  pin_mut!(table_schema_stream);
  while let Some(item) = table_schema_stream.next().await {
    let (_, value) = item?;
    if let StoreValue::TableSchema(schema) = value {
      tables.push(schema);
    }
  }

  let index_schema_stream = range_index_schema_entries_impl(tx);
  pin_mut!(index_schema_stream);
  while let Some(item) = index_schema_stream.next().await {
    let (_, value) = item?;
    if let StoreValue::IndexSchema(schema) = value {
      indexes.push(schema);
    }
  }

  Ok((tables, indexes))
}

pub fn range_table_schema_entries_impl<'a, T>(
  tx: &'a T,
) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), db_core::BTreeError>> + 'a
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  let start = StoreKey::table_schema(String::new());
  tx.range(start..).take_while(move |res| {
    futures::future::ready(match res {
      Ok((key, _)) => matches!(key, StoreKey::TableSchema { .. }),
      Err(_) => false,
    })
  })
}

pub fn range_index_schema_entries_impl<'a, T>(
  tx: &'a T,
) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), db_core::BTreeError>> + 'a
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  let start = StoreKey::index_schema(String::new());
  tx.range(start..).take_while(move |res| {
    futures::future::ready(match res {
      Ok((key, _)) => matches!(key, StoreKey::IndexSchema { .. }),
      Err(_) => false,
    })
  })
}

pub fn range_index_entries_impl<'a, T>(
  tx: &'a T,
  index_name: &'a str,
) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), db_core::BTreeError>> + 'a
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  let start = StoreKey::index_entry(
    String::from(index_name),
    EngineKey::Scalar(EngineValue::Null),
    EngineKey::Scalar(EngineValue::Null),
  );
  tx.range(start..).take_while(move |res| {
    futures::future::ready(match res {
      Ok((key, _)) => {
        matches!(key, StoreKey::IndexEntry { index_name: name, .. } if name == index_name)
      }
      Err(_) => false,
    })
  })
}

/// Collect the row primary keys for a given `index_name` and `index_key`.
pub async fn lookup_index_row_pks_impl<T>(
  tx: &mut T,
  index_name: &str,
  index_key: &EngineKey,
) -> Result<Vec<EngineKey>, db_core::BTreeError>
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  let mut row_pks = Vec::new();
  let stream = range_index_entries_impl(tx, index_name);
  pin_mut!(stream);

  while let Some(item) = stream.next().await {
    let (key, _value) = item?;
    if let StoreKey::IndexEntry {
      index_name: name,
      index_key: entry_key,
      row_pk,
    } = key
      && name == index_name
      && entry_key == *index_key
    {
      row_pks.push(row_pk);
    }
  }

  Ok(row_pks)
}

pub fn encode_table_schema(schema: &TableSchema) -> Vec<EngineValue> {
  let mut buf = Vec::new();
  encode_table_schema_into_sink(&mut buf, schema);
  Vec::from([EngineValue::Blob(buf)])
}

pub fn decode_table_schema_row(row: &[EngineValue]) -> Result<TableSchema, db_core::DecodeError> {
  match row.first() {
    Some(EngineValue::Blob(bytes)) => db_core::decode_from_slice(bytes, decode_table_schema),
    _ => Err(db_core::DecodeError::Malformed),
  }
}

pub fn decode_table_schema_rows<I>(rows: I) -> Result<Vec<TableSchema>, db_core::DecodeError>
where
  I: IntoIterator<Item = EngineRow>,
{
  decode_schema_rows(rows, decode_table_schema_row)
}

pub fn encode_index_schema(schema: &IndexSchema) -> Vec<EngineValue> {
  let mut buf = Vec::new();
  encode_index_schema_into_sink(&mut buf, schema);
  Vec::from([EngineValue::Blob(buf)])
}

pub fn decode_index_schema_row(row: &[EngineValue]) -> Result<IndexSchema, db_core::DecodeError> {
  match row.first() {
    Some(EngineValue::Blob(bytes)) => db_core::decode_from_slice(bytes, decode_index_schema),
    _ => Err(db_core::DecodeError::Malformed),
  }
}

pub fn decode_index_schema_rows<I>(rows: I) -> Result<Vec<IndexSchema>, db_core::DecodeError>
where
  I: IntoIterator<Item = EngineRow>,
{
  decode_schema_rows(rows, decode_index_schema_row)
}

pub fn make_index_entry_key(
  index: &IndexSchema,
  index_key: &EngineKey,
  row_pk: &EngineKey,
) -> EngineKey {
  let n = index.column_indices.len();
  let mut values = Vec::with_capacity(n + row_pk.values().len());
  values.extend_from_slice(index_key.values());
  values.extend_from_slice(row_pk.values());
  EngineKey::from_values(values)
}

pub fn split_index_entry_key(composite: &EngineKey, n_index_cols: usize) -> (EngineKey, EngineKey) {
  let values = composite.values();
  let n = n_index_cols.min(values.len());
  let index_key = EngineKey::from_values(values[..n].to_vec());
  let row_pk = EngineKey::from_values(values[n..].to_vec());
  (index_key, row_pk)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::engine_types::EngineType;
  use crate::schema::ColumnSchema;

  #[test]
  fn schema_entry_keys_use_text_scalars() {
    assert_eq!(
      table_schema_entry_key("users"),
      EngineKey::Scalar(EngineValue::Text("users".into()))
    );
    assert_eq!(
      index_schema_entry_key("users_name_idx"),
      EngineKey::Scalar(EngineValue::Text("users_name_idx".into()))
    );
  }

  #[test]
  fn decode_schema_rows_round_trip() {
    let table = TableSchema {
      name: "users".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "name".into(),
          data_type: EngineType::Text,
        },
      ],
      primary_key: vec![0],
    };
    let index = IndexSchema {
      name: "users_name_idx".into(),
      table_name: "users".into(),
      column_indices: vec![1],
      unique: true,
    };

    assert_eq!(
      decode_table_schema_rows(vec![encode_table_schema(&table)]).expect("decode tables"),
      vec![table]
    );
    assert_eq!(
      decode_index_schema_rows(vec![encode_index_schema(&index)]).expect("decode indexes"),
      vec![index]
    );
  }
}
