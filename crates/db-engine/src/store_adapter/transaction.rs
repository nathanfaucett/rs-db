use core::future::Future;
use futures::Stream;

use crate::{EngineError, EngineKey, EngineRow, IndexSchema, PrimaryKey, TableSchema};

/// Read and write row data for a single table within a transaction.
///
/// This trait only concerns row access by primary key (or table scan). It does
/// not resolve secondary index predicates.
pub trait RowStore: Send + 'static {
  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl Stream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a;
}

/// Read and write catalog schemas (tables and indexes) within a transaction.
pub trait SchemaStore: Send + 'static {
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
}

/// Read and write index entries within a transaction.
///
/// This trait only concerns index access and index-entry maintenance. It should
/// return primary-key identities, not materialized rows.
pub trait IndexStore: Send + 'static {
  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn delete_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl Stream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a;
}

/// Lifecycle control for a transaction (commit or rollback).
pub trait TransactionControl: Send + 'static {
  fn commit(self) -> impl Future<Output = Result<(), EngineError>>;

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>>;
}

/// Full engine-level storage transaction. Composed of [`RowStore`],
/// [`SchemaStore`], [`IndexStore`], and [`TransactionControl`]. Any type
/// implementing all four sub-traits satisfies this trait via the blanket impl.
///
/// All methods operate on typed engine values; no storage-level key encoding
/// appears in this interface.
pub trait EngineStoreTransaction: RowStore + SchemaStore + IndexStore + TransactionControl {}

impl<T> EngineStoreTransaction for T where
  T: RowStore + SchemaStore + IndexStore + TransactionControl
{
}
