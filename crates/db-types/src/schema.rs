#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "wasm")]
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::ToString};
#[cfg(feature = "std")]
use std::string::String;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::format;

use crate::{EngineKey, EngineRow, EngineType, EngineValue};

fn key_from_indices<F>(
  row: &EngineRow,
  indices: &[usize],
  error: F,
) -> Result<EngineKey, SchemaError>
where
  F: Fn(usize) -> SchemaError,
{
  let values = indices
    .iter()
    .map(|index| row.get(*index).cloned().ok_or_else(|| error(*index)))
    .collect::<Result<Vec<_>, _>>()?;

  Ok(EngineKey::from_values(values))
}

/// Errors returned by schema-level validation in the `db-types` crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
  SchemaMismatch(String),
  TypeMismatch(String),
  PrimaryKeyMissing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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
    key_from_indices(row, &self.primary_key, |index| {
      SchemaError::SchemaMismatch(format!(
        "primary key index {} is out of bounds for table {}",
        index, self.name
      ))
    })
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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
    key_from_indices(row, &self.column_indices, |index| {
      SchemaError::SchemaMismatch(format!("index column index {} is out of bounds", index))
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample_table() -> TableSchema {
    TableSchema {
      name: "t".into(),
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
    }
  }

  #[test]
  fn accepts_null_for_any_column_type() {
    let col = ColumnSchema {
      name: "x".into(),
      data_type: EngineType::Integer,
    };
    assert!(col.accepts(&EngineValue::Null));
  }

  #[test]
  fn validate_row_accepts_matching_row() {
    let table = sample_table();
    let row = vec![EngineValue::Integer(1), EngineValue::Text("a".into())];
    assert!(table.validate_row(&row).is_ok());
  }

  #[test]
  fn validate_row_rejects_wrong_length() {
    let table = sample_table();
    let row = vec![EngineValue::Integer(1)];
    assert!(matches!(
      table.validate_row(&row),
      Err(SchemaError::SchemaMismatch(_))
    ));
  }

  #[test]
  fn validate_row_rejects_type_mismatch() {
    let table = sample_table();
    let row = vec![
      EngineValue::Text("nope".into()),
      EngineValue::Text("a".into()),
    ];
    assert!(matches!(
      table.validate_row(&row),
      Err(SchemaError::TypeMismatch(_))
    ));
  }

  #[test]
  fn validate_row_requires_primary_key() {
    let mut table = sample_table();
    table.primary_key.clear();
    let row = vec![EngineValue::Integer(1), EngineValue::Text("a".into())];
    assert_eq!(
      table.validate_row(&row),
      Err(SchemaError::PrimaryKeyMissing)
    );
  }

  #[test]
  fn index_validate_for_table_rejects_wrong_name() {
    let table = sample_table();
    let index = IndexSchema {
      name: "ix".into(),
      table_name: "other".into(),
      column_indices: vec![0],
      unique: true,
    };
    assert!(index.validate_for_table(&table).is_err());
  }

  #[test]
  fn index_key_for_extracts_columns() {
    let table = sample_table();
    let index = IndexSchema {
      name: "ix".into(),
      table_name: "t".into(),
      column_indices: vec![1],
      unique: false,
    };
    assert!(index.validate_for_table(&table).is_ok());
    let row = vec![EngineValue::Integer(1), EngineValue::Text("hi".into())];
    let key = index.key_for(&row).expect("key");
    assert_eq!(
      key,
      EngineKey::from_values(vec![EngineValue::Text("hi".into())])
    );
  }
}
