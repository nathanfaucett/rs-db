#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "std")]
use std::string::String;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::format;

use db_core::{EngineKey, EngineRow, EngineType, EngineValue};

/// Errors returned by schema-level validation in the `db-types` crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
  SchemaMismatch(String),
  TypeMismatch(String),
  PrimaryKeyMissing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "ts",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "ts", tsify(into_wasm_abi, from_wasm_abi))]
pub struct ColumnSchema {
  pub name: String,
  pub data_type: EngineType,
}

impl ColumnSchema {
  pub fn accepts(&self, value: &EngineValue) -> bool {
    matches!(
      (&self.data_type, value),
      (EngineType::Integer, EngineValue::Integer(_))
        | (EngineType::Float, EngineValue::Float(_))
        | (EngineType::Text, EngineValue::Text(_))
        | (EngineType::Blob, EngineValue::Blob(_))
        | (_, EngineValue::Null)
    )
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "ts",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "ts", tsify(into_wasm_abi, from_wasm_abi))]
pub struct TableSchema {
  pub name: String,
  pub columns: Vec<ColumnSchema>,
  pub primary_key: Vec<usize>,
}

impl TableSchema {
  pub fn validate_row(&self, row: &EngineRow) -> Result<(), SchemaError> {
    if row.len() != self.columns.len() {
      return Err(SchemaError::SchemaMismatch(format!(
        "row has {} values but table {} expects {} columns",
        row.len(),
        self.name,
        self.columns.len()
      )));
    }

    for (index, (value, column)) in row.iter().zip(self.columns.iter()).enumerate() {
      if !column.accepts(value) {
        return Err(SchemaError::TypeMismatch(format!(
          "column {} expects {:?} but found {}",
          index, column.data_type, value
        )));
      }
    }

    if self.primary_key.is_empty() {
      return Err(SchemaError::PrimaryKeyMissing);
    }

    Ok(())
  }

  pub fn primary_key(&self, row: &EngineRow) -> Result<EngineKey, SchemaError> {
    let values = self
      .primary_key
      .iter()
      .map(|index| {
        row
          .get(*index)
          .cloned()
          .ok_or(SchemaError::SchemaMismatch(format!(
            "primary key index {} is out of bounds for table {}",
            index, self.name
          )))
      })
      .collect::<Result<Vec<_>, _>>()?;

    Ok(EngineKey::from_values(values))
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "ts",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "ts", tsify(into_wasm_abi, from_wasm_abi))]
pub struct IndexSchema {
  pub name: String,
  pub table_name: String,
  pub column_indices: Vec<usize>,
  pub unique: bool,
}

impl IndexSchema {
  pub fn validate_for_table(&self, table: &TableSchema) -> Result<(), SchemaError> {
    if self.table_name != table.name {
      return Err(SchemaError::SchemaMismatch(format!(
        "index {} belongs to {} but table is {}",
        self.name, self.table_name, table.name
      )));
    }

    if self.column_indices.is_empty() {
      return Err(SchemaError::SchemaMismatch(
        "index must contain at least one column".into(),
      ));
    }

    for index in &self.column_indices {
      if *index >= table.columns.len() {
        return Err(SchemaError::SchemaMismatch(format!(
          "index column index {} is out of range for table {}",
          index, table.name
        )));
      }
    }

    Ok(())
  }

  pub fn key_for(&self, row: &EngineRow) -> Result<EngineKey, SchemaError> {
    let values = self
      .column_indices
      .iter()
      .map(|index| {
        row
          .get(*index)
          .cloned()
          .ok_or(SchemaError::SchemaMismatch(format!(
            "index column index {} is out of bounds",
            index
          )))
      })
      .collect::<Result<Vec<_>, _>>()?;

    Ok(EngineKey::from_values(values))
  }
}
