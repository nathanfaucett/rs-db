#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "wasm")]
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format, string::ToString};
#[cfg(feature = "std")]
use std::string::String;

// `Vec` is not referenced directly in this file; avoid unused-import warnings.

use crate::{EngineKey, EngineRow};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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

macro_rules! store_value_accessor {
  ($name:ident, $variant:ident, $value:ty) => {
    pub fn $name(&self) -> Option<&$value> {
      match self {
        StoreValue::$variant(value) => Some(value),
        _ => None,
      }
    }
  };
}

impl StoreValue {
  store_value_accessor!(as_row, Row, EngineRow);
  store_value_accessor!(as_table_schema, TableSchema, crate::schema::TableSchema);
  store_value_accessor!(as_index_schema, IndexSchema, crate::schema::IndexSchema);
}
