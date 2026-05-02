use core::future::Future;

use db_types::codec::{
  decode_index_schema, decode_table_schema, encode_index_schema_into_sink,
  encode_table_schema_into_sink,
};
use futures::{Stream, StreamExt, pin_mut};

use crate::{EngineError, EngineKey, EngineRow, EngineValue, IndexSchema, TableSchema};

/// Prefix used for row trees: `"t:{table_name}"`.
pub(crate) fn row_tree(table_name: &str) -> String {
  format!("t:{}", table_name)
}

/// Prefix used for index trees: `"i:{index_name}"`.
pub(crate) fn index_tree(index_name: &str) -> String {
  format!("i:{}", index_name)
}

/// Well-known tree holding all table schemas.
pub(crate) const TABLE_SCHEMA_TREE: &str = "sys:table_schemas";
/// Well-known tree holding all index schemas.
pub(crate) const INDEX_SCHEMA_TREE: &str = "sys:index_schemas";

/// Encode a `TableSchema` as a single-element `EngineRow` containing its
/// serialised bytes.
pub(crate) fn encode_table_schema(schema: &TableSchema) -> EngineRow {
  let mut buf = Vec::new();
  encode_table_schema_into_sink(&mut buf, schema);
  vec![EngineValue::Blob(buf)]
}

/// Decode a `TableSchema` from an `EngineRow` produced by [`encode_table_schema`].
pub(crate) fn decode_table_schema_row(row: &EngineRow) -> Result<TableSchema, EngineError> {
  match row.first() {
    Some(EngineValue::Blob(bytes)) => db_core::decode_from_slice(bytes, decode_table_schema)
      .map_err(|e| EngineError::SchemaMismatch(e.to_string())),
    _ => Err(EngineError::SchemaMismatch(
      "invalid table schema encoding".into(),
    )),
  }
}

/// Encode an `IndexSchema` as a single-element `EngineRow` containing its
/// serialised bytes.
pub(crate) fn encode_index_schema(schema: &IndexSchema) -> EngineRow {
  let mut buf = Vec::new();
  encode_index_schema_into_sink(&mut buf, schema);
  vec![EngineValue::Blob(buf)]
}

/// Decode an `IndexSchema` from an `EngineRow` produced by [`encode_index_schema`].
pub(crate) fn decode_index_schema_row(row: &EngineRow) -> Result<IndexSchema, EngineError> {
  match row.first() {
    Some(EngineValue::Blob(bytes)) => db_core::decode_from_slice(bytes, decode_index_schema)
      .map_err(|e| EngineError::SchemaMismatch(e.to_string())),
    _ => Err(EngineError::SchemaMismatch(
      "invalid index schema encoding".into(),
    )),
  }
}

/// Flatten an index key and row primary key into a single composite
/// `EngineKey` for storage in the index tree.
///
/// The first `n_index_cols` values in the result belong to `index_key`;
/// the remaining values belong to `row_pk`.
pub(crate) fn make_index_entry_key(
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

/// Split a composite index entry key back into `(index_key, row_pk)`.
pub(crate) fn split_index_entry_key(
  composite: &EngineKey,
  n_index_cols: usize,
) -> (EngineKey, EngineKey) {
  let values = composite.values();
  let n = n_index_cols.min(values.len());
  let index_key = EngineKey::from_values(values[..n].to_vec());
  let row_pk = EngineKey::from_values(values[n..].to_vec());
  (index_key, row_pk)
}

/// An engine-level storage transaction. All methods operate on typed engine
/// values; no storage-level key encoding appears in this interface.
///
/// The backing store routes each operation to the appropriate named tree.
pub trait EngineStoreTransaction: Send + 'static {
  // Row operations.

  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: EngineKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl Stream<Item = Result<(EngineKey, EngineRow), EngineError>> + 'a;

  // Schema operations.

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a;

  // Index operations.

  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn delete_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl Stream<Item = Result<(EngineKey, EngineKey), EngineError>> + 'a;

  // Transaction control.

  fn commit(self) -> impl Future<Output = Result<(), EngineError>>;

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>>;

  // Derived operations.

  fn collect_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
    predicate: Option<crate::EnginePredicate>,
  ) -> impl Future<Output = Result<Vec<(EngineKey, EngineRow)>, EngineError>> + 'a {
    async move {
      let mut rows = Vec::new();
      let stream = self.range_table_rows(table_name);
      pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (pk, row) = item?;
        if predicate.as_ref().is_none_or(|p| p.matches(&row)) {
          rows.push((pk, row));
        }
      }
      Ok(rows)
    }
  }

  fn delete_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
    row: &'a EngineRow,
    indexes: &'a [IndexSchema],
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      self.remove_table_row(table_name, primary_key).await?;
      for index in indexes {
        let index_key = index.key_for(row).map_err(EngineError::from)?;
        self
          .delete_index_entry(index, &index_key, primary_key)
          .await?;
      }
      Ok(())
    }
  }

  fn remove_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let keys = {
        let stream = self.range_table_rows(table_name);
        pin_mut!(stream);
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
          let (pk, _) = item?;
          keys.push(pk);
        }
        keys
      };
      for pk in keys {
        self.remove_table_row(table_name, &pk).await?;
      }
      Ok(())
    }
  }

  fn remove_index_entries<'a>(
    &'a mut self,
    index: &'a IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let keys = {
        let stream = self.range_index_entries(index);
        pin_mut!(stream);
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
          let (idx_key, row_pk) = item?;
          keys.push((idx_key, row_pk));
        }
        keys
      };
      for (idx_key, row_pk) in keys {
        self.delete_index_entry(index, &idx_key, &row_pk).await?;
      }
      Ok(())
    }
  }

  fn find_conflicting_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineKey>, EngineError>> + 'a {
    async move {
      let stream = self.range_index_entries(index);
      pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (entry_idx_key, entry_pk) = item?;
        if entry_idx_key == *index_key && entry_pk != *row_pk {
          return Ok(Some(entry_pk));
        }
      }
      Ok(None)
    }
  }

  fn lookup_index_rows<'a>(
    &'a mut self,
    table_name: &'a str,
    index: &'a IndexSchema,
    predicate: &'a crate::query::EnginePredicate,
  ) -> impl Future<Output = Result<Vec<EngineRow>, EngineError>> + 'a {
    async move {
      let index_key = predicate
        .index_key_for(index)
        .ok_or_else(|| EngineError::SchemaMismatch("predicate does not match index key".into()))?;

      let row_pks = {
        let stream = self.range_index_entries(index);
        pin_mut!(stream);
        let mut pks = Vec::new();
        while let Some(item) = stream.next().await {
          let (entry_key, row_pk) = item?;
          if entry_key == index_key {
            pks.push(row_pk);
          }
        }
        pks
      };

      let mut rows = Vec::new();
      for pk in row_pks {
        if let Some(row) = self.get_table_row(table_name, &pk).await? {
          rows.push(row);
        }
      }
      Ok(rows)
    }
  }

  // Backward-compat aliases.

  fn get_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    self.get_table_row(table_name, primary_key)
  }

  fn insert_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: EngineKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    self.insert_table_row(table_name, primary_key, row)
  }

  fn get_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
    predicate: Option<crate::EnginePredicate>,
  ) -> impl Future<Output = Result<Vec<(EngineKey, EngineRow)>, EngineError>> + 'a {
    self.collect_table_rows(table_name, predicate)
  }

  fn load_catalog_entries<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a {
    self.load_catalog()
  }
}
