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

use crate::engine_types::{EngineKey, EngineValue};
use crate::schema::{IndexSchema, TableSchema};
use crate::store::{StoreKey, StoreValue};

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
