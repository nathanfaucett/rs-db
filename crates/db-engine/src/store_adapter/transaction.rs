use core::future::Future;
use futures::Stream;

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
}
