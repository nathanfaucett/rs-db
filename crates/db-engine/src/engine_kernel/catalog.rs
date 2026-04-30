use std::collections::HashMap;

use crate::store_adapter::{EngineStore, EngineStoreTransaction};
use crate::{EngineError, EngineRow, IndexSchema, TableSchema};

#[derive(Debug, Clone, Default)]
pub(crate) struct EngineCatalog {
  tables: HashMap<String, TableSchema>,
  indexes: HashMap<String, IndexSchema>,
}

impl EngineCatalog {
  pub(crate) fn new() -> Self {
    Self::default()
  }

  pub(crate) fn table(&self, table_name: &str) -> Result<&TableSchema, EngineError> {
    self
      .tables
      .get(table_name)
      .ok_or_else(|| EngineError::TableNotFound(table_name.into()))
  }

  pub(crate) fn contains_table(&self, table_name: &str) -> bool {
    self.tables.contains_key(table_name)
  }

  pub(crate) fn contains_index(&self, index_name: &str) -> bool {
    self.indexes.contains_key(index_name)
  }

  pub(crate) fn insert_table(&mut self, schema: TableSchema) {
    self.tables.insert(schema.name.clone(), schema);
  }

  pub(crate) fn insert_index(&mut self, schema: IndexSchema) {
    self.indexes.insert(schema.name.clone(), schema);
  }

  pub(crate) fn indexes_for_table(&self, table_name: &str) -> Vec<IndexSchema> {
    self
      .indexes
      .values()
      .filter(|index| index.table_name == table_name)
      .cloned()
      .collect()
  }

  pub(crate) fn find_index_for_predicate(
    &self,
    table_name: &str,
    predicate: &crate::query::EnginePredicate,
  ) -> Option<IndexSchema> {
    self
      .indexes_for_table(table_name)
      .into_iter()
      .find(|index| predicate.index_key_for(index).is_some())
  }

  pub(crate) fn project_row(
    &self,
    row: &EngineRow,
    projection: &[usize],
  ) -> Result<EngineRow, EngineError> {
    if projection.is_empty() {
      return Ok(row.clone());
    }

    let mut projected = Vec::with_capacity(projection.len());
    for index in projection {
      projected.push(row.get(*index).cloned().ok_or_else(|| {
        EngineError::SchemaMismatch(format!("projection index {} is out of bounds", index))
      })?);
    }

    Ok(projected)
  }

  pub(crate) async fn load_from_store<S>(&mut self, store: &S) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    let mut tx = store.engine_transaction().await?;
    let (tables, indexes) = tx.load_catalog().await?;

    self.tables.clear();
    self.indexes.clear();

    for table in tables {
      self.tables.insert(table.name.clone(), table);
    }
    for index in indexes {
      self.indexes.insert(index.name.clone(), index);
    }

    Ok(())
  }

  pub(crate) async fn register_table<S>(
    &mut self,
    store: &S,
    schema: TableSchema,
  ) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    self.load_from_store(store).await?;

    if self.contains_table(&schema.name) {
      return Err(EngineError::DuplicateTable(schema.name));
    }

    let mut tx = store.engine_transaction().await?;
    tx.insert_table_schema(schema.clone()).await?;
    tx.commit().await?;

    self.insert_table(schema);
    Ok(())
  }

  pub(crate) async fn register_index<S>(
    &mut self,
    store: &S,
    schema: IndexSchema,
  ) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    self.load_from_store(store).await?;

    if self.contains_index(&schema.name) {
      return Err(EngineError::DuplicateIndex(schema.name));
    }

    let table = self.table(&schema.table_name)?;
    schema.validate_for_table(table)?;

    let mut tx = store.engine_transaction().await?;
    tx.insert_index_schema(schema.clone()).await?;
    tx.commit().await?;

    self.insert_index(schema);
    Ok(())
  }
}
