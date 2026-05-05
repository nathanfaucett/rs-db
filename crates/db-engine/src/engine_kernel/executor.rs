use super::catalog::EngineCatalog;
use crate::store_adapter::{EngineStore, EngineStoreTransaction};
use crate::{
  EngineError, EngineKey, EngineRow, EngineValue, IndexSchema, query::QualifiedPredicate,
};

async fn ensure_indexes_unique<TX>(
  tx: &mut TX,
  indexes: &[IndexSchema],
  row: &EngineRow,
  row_pk: &EngineKey,
) -> Result<(), EngineError>
where
  TX: crate::store_adapter::EngineStoreTransaction,
{
  for index in indexes.iter().filter(|index| index.unique) {
    let index_key = index.key_for(row)?;
    if tx
      .find_conflicting_index_entry(index, &index_key, row_pk)
      .await?
      .is_some()
    {
      return Err(EngineError::UniqueIndexViolation(index.name.clone()));
    }
  }
  Ok(())
}

async fn insert_all_index_entries<TX>(
  tx: &mut TX,
  indexes: &[IndexSchema],
  row: &EngineRow,
  primary_key: &EngineKey,
) -> Result<(), EngineError>
where
  TX: crate::store_adapter::EngineStoreTransaction,
{
  for index in indexes {
    let index_key = index.key_for(row)?;
    tx.insert_index_entry(index, &index_key, primary_key)
      .await?;
  }
  Ok(())
}

#[derive(Debug)]
pub(crate) struct EngineWriteTxn<'db, S>
where
  S: EngineStore,
  S::Transaction: EngineStoreTransaction,
{
  pub(crate) store: &'db S,
  pub(crate) catalog: &'db EngineCatalog,
  pub(crate) tx: Option<S::Transaction>,
}

impl<'db, S> EngineWriteTxn<'db, S>
where
  S: EngineStore,
  S::Transaction: EngineStoreTransaction,
{
  pub(crate) async fn transaction(&mut self) -> Result<&mut S::Transaction, EngineError> {
    if self.tx.is_none() {
      let tx = self.store.engine_transaction().await?;
      self.tx = Some(tx);
    }

    Ok(self.tx.as_mut().expect("transaction should be initialized"))
  }

  pub(crate) async fn insert(
    &mut self,
    table_name: &str,
    row: EngineRow,
  ) -> Result<(), EngineError> {
    let table = self.catalog.table(table_name)?;
    table.validate_row(&row)?;
    let pk = table.primary_key(&row)?;
    let indexes = self.catalog.indexes_for_table(table_name);

    let tx = self.transaction().await?;
    if tx.get_table_row(table_name, &pk).await?.is_some() {
      return Err(EngineError::DuplicatePrimaryKey(pk));
    }

    ensure_indexes_unique(tx, &indexes, &row, &pk).await?;

    tx.insert_table_row(table_name, pk.clone(), row.clone())
      .await?;

    insert_all_index_entries(tx, &indexes, &row, &pk).await?;

    Ok(())
  }

  pub(crate) async fn delete(
    &mut self,
    table_name: &str,
    predicate: Option<QualifiedPredicate>,
  ) -> Result<(), EngineError> {
    self.catalog.table(table_name)?;
    let indexes = self.catalog.indexes_for_table(table_name);
    let tx = self.transaction().await?;
    let rows = Self::collect_table_rows(tx, table_name, predicate).await?;

    for (primary_key, row) in rows {
      Self::delete_row(tx, table_name, &primary_key, &row, &indexes).await?;
    }

    Ok(())
  }

  pub(crate) async fn update(
    &mut self,
    table_name: &str,
    assignments: Vec<(usize, EngineValue)>,
    predicate: Option<QualifiedPredicate>,
  ) -> Result<(), EngineError> {
    let table = self.catalog.table(table_name)?.clone();
    if assignments.is_empty() {
      return Ok(());
    }

    let indexes = self.catalog.indexes_for_table(table_name);
    let tx = self.transaction().await?;
    let rows = Self::collect_table_rows(tx, table_name, predicate).await?;

    for (old_pk, row) in rows {
      let updated_row = Self::apply_assignments(&row, &assignments)?;
      table.validate_row(&updated_row)?;
      let new_pk = table.primary_key(&updated_row)?;

      if new_pk != old_pk && tx.get_table_row(table_name, &new_pk).await?.is_some() {
        return Err(EngineError::DuplicatePrimaryKey(new_pk));
      }

      Self::delete_row(tx, table_name, &old_pk, &row, &indexes).await?;
      ensure_indexes_unique(tx, &indexes, &updated_row, &new_pk).await?;

      tx.insert_table_row(table_name, new_pk.clone(), updated_row.clone())
        .await?;

      insert_all_index_entries(tx, &indexes, &updated_row, &new_pk).await?;
    }

    Ok(())
  }

  async fn delete_row(
    tx: &mut S::Transaction,
    table_name: &str,
    primary_key: &EngineKey,
    row: &EngineRow,
    indexes: &[IndexSchema],
  ) -> Result<(), EngineError> {
    tx.delete_row(table_name, primary_key, row, indexes).await
  }

  fn apply_assignments(
    row: &EngineRow,
    assignments: &[(usize, EngineValue)],
  ) -> Result<EngineRow, EngineError> {
    let mut updated = row.clone();

    for (index, value) in assignments {
      let cell = updated.get_mut(*index).ok_or_else(|| {
        EngineError::SchemaMismatch(format!("update index {} is out of bounds", index))
      })?;
      *cell = value.clone();
    }

    Ok(updated)
  }

  pub(crate) async fn collect_table_rows(
    tx: &mut S::Transaction,
    table_name: &str,
    predicate: Option<QualifiedPredicate>,
  ) -> Result<Vec<(EngineKey, EngineRow)>, EngineError> {
    tx.collect_table_rows(table_name, predicate).await
  }

  pub(crate) async fn commit(mut self) -> Result<(), EngineError> {
    if let Some(tx) = self.tx.take() {
      tx.commit().await?;
    }
    Ok(())
  }

  pub(crate) async fn rollback(mut self) -> Result<(), EngineError> {
    if let Some(tx) = self.tx.take() {
      tx.rollback().await?;
    }
    Ok(())
  }
}
