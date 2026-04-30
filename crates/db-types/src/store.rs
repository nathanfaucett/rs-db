#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "std")]
use std::string::String;

// `Vec` is not referenced directly in this file; avoid unused-import warnings.

use db_core::{EngineKey, EngineRow};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
  feature = "ts",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub enum StoreKey {
  TableRow {
    table_name: String,
    primary_key: EngineKey,
  },
  IndexEntry {
    index_name: String,
    index_key: EngineKey,
    row_pk: EngineKey,
  },
  TableSchema {
    table_name: String,
  },
  IndexSchema {
    index_name: String,
  },
}

impl StoreKey {
  pub fn table_row(table_name: String, primary_key: EngineKey) -> Self {
    StoreKey::TableRow {
      table_name,
      primary_key,
    }
  }

  pub fn index_entry(index_name: String, index_key: EngineKey, row_pk: EngineKey) -> Self {
    StoreKey::IndexEntry {
      index_name,
      index_key,
      row_pk,
    }
  }

  pub fn table_schema(table_name: String) -> Self {
    StoreKey::TableSchema { table_name }
  }

  pub fn index_schema(index_name: String) -> Self {
    StoreKey::IndexSchema { index_name }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreValue {
  Row(EngineRow),
  IndexEntry,
  TableSchema(crate::schema::TableSchema),
  IndexSchema(crate::schema::IndexSchema),
}

impl StoreValue {
  pub fn as_row(&self) -> Option<&EngineRow> {
    match self {
      StoreValue::Row(row) => Some(row),
      _ => None,
    }
  }

  pub fn as_table_schema(&self) -> Option<&crate::schema::TableSchema> {
    match self {
      StoreValue::TableSchema(schema) => Some(schema),
      _ => None,
    }
  }

  pub fn as_index_schema(&self) -> Option<&crate::schema::IndexSchema> {
    match self {
      StoreValue::IndexSchema(schema) => Some(schema),
      _ => None,
    }
  }
}
