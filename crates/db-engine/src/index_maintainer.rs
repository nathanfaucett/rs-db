use crate::store_adapter::EngineStoreTransaction;
use crate::{EngineError, EngineKey, EngineRow, IndexSchema};

pub(crate) struct IndexMaintainer;

impl IndexMaintainer {
  pub(crate) async fn ensure_unique<TX>(
    tx: &mut TX,
    indexes: &[IndexSchema],
    row: &EngineRow,
    row_pk: &EngineKey,
  ) -> Result<(), EngineError>
  where
    TX: EngineStoreTransaction,
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

  pub(crate) async fn insert_entries<TX>(
    tx: &mut TX,
    indexes: &[IndexSchema],
    row: &EngineRow,
    primary_key: &EngineKey,
  ) -> Result<(), EngineError>
  where
    TX: EngineStoreTransaction,
  {
    for index in indexes {
      let index_key = index.key_for(row)?;
      tx.insert_index_entry(index, &index_key, primary_key)
        .await?;
    }

    Ok(())
  }

  pub(crate) async fn remove_entries<TX>(
    tx: &mut TX,
    indexes: &[IndexSchema],
    row: &EngineRow,
    primary_key: &EngineKey,
  ) -> Result<(), EngineError>
  where
    TX: EngineStoreTransaction,
  {
    for index in indexes {
      let index_key = index.key_for(row)?;
      tx.delete_index_entry(index, &index_key, primary_key)
        .await?;
    }

    Ok(())
  }
}
