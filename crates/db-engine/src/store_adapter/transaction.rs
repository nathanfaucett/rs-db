use core::future::Future;
use futures::{Stream, StreamExt, pin_mut};

use crate::{EngineError, EngineKey, EngineRow, IndexSchema, TableSchema};

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
    predicate: Option<crate::QualifiedPredicate>,
  ) -> impl Future<Output = Result<Vec<(EngineKey, EngineRow)>, EngineError>> + 'a {
    async move {
      let mut rows = Vec::new();
      let stream = self.range_table_rows(table_name);
      pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (pk, row) = item?;
        if predicate
          .as_ref()
          .is_none_or(|p| p.matches_row(table_name, &row))
        {
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
    predicate: &'a crate::query::QualifiedPredicate,
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
}
