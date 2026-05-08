use futures::{Stream, StreamExt, pin_mut};

use crate::{EngineError, EngineKey, EngineRow, IndexSchema};

use super::EngineStoreTransaction;

async fn try_collect<T, S>(stream: S) -> Result<Vec<T>, EngineError>
where
  S: Stream<Item = Result<T, EngineError>>,
{
  let mut values = Vec::new();
  pin_mut!(stream);
  while let Some(item) = stream.next().await {
    values.push(item?);
  }
  Ok(values)
}

async fn collect_matching_table_rows<S>(
  stream: S,
  table_name: &str,
  predicate: Option<&crate::QualifiedPredicate>,
) -> Result<Vec<(EngineKey, EngineRow)>, EngineError>
where
  S: Stream<Item = Result<(EngineKey, EngineRow), EngineError>>,
{
  let rows = try_collect(stream).await?;
  Ok(
    rows
      .into_iter()
      .filter(|(_, row)| predicate.is_none_or(|p| p.matches_row(table_name, row)))
      .collect(),
  )
}

async fn collect_matching_index_row_pks<S>(
  stream: S,
  wanted_index_key: &EngineKey,
) -> Result<Vec<EngineKey>, EngineError>
where
  S: Stream<Item = Result<(EngineKey, EngineKey), EngineError>>,
{
  let entries = try_collect(stream).await?;
  Ok(
    entries
      .into_iter()
      .filter_map(|(entry_key, row_pk)| (entry_key == *wanted_index_key).then_some(row_pk))
      .collect(),
  )
}

pub(crate) async fn collect_table_rows<TX>(
  tx: &mut TX,
  table_name: &str,
  predicate: Option<crate::QualifiedPredicate>,
) -> Result<Vec<(EngineKey, EngineRow)>, EngineError>
where
  TX: EngineStoreTransaction,
{
  collect_matching_table_rows(
    tx.range_table_rows(table_name),
    table_name,
    predicate.as_ref(),
  )
  .await
}

pub(crate) async fn delete_row<TX>(
  tx: &mut TX,
  table_name: &str,
  primary_key: &EngineKey,
  row: &EngineRow,
  indexes: &[IndexSchema],
) -> Result<(), EngineError>
where
  TX: EngineStoreTransaction,
{
  tx.remove_table_row(table_name, primary_key).await?;
  for index in indexes {
    let index_key = index.key_for(row).map_err(EngineError::from)?;
    tx.delete_index_entry(index, &index_key, primary_key)
      .await?;
  }
  Ok(())
}

pub(crate) async fn remove_table_rows<TX>(tx: &mut TX, table_name: &str) -> Result<(), EngineError>
where
  TX: EngineStoreTransaction,
{
  let keys = try_collect(
    tx.range_table_rows(table_name)
      .map(|item| item.map(|(pk, _)| pk)),
  )
  .await?;
  for pk in keys {
    tx.remove_table_row(table_name, &pk).await?;
  }
  Ok(())
}

pub(crate) async fn remove_index_entries<TX>(
  tx: &mut TX,
  index: &IndexSchema,
) -> Result<(), EngineError>
where
  TX: EngineStoreTransaction,
{
  let keys = try_collect(tx.range_index_entries(index)).await?;
  for (idx_key, row_pk) in keys {
    tx.delete_index_entry(index, &idx_key, &row_pk).await?;
  }
  Ok(())
}

pub(crate) async fn find_conflicting_index_entry<TX>(
  tx: &mut TX,
  index: &IndexSchema,
  index_key: &EngineKey,
  row_pk: &EngineKey,
) -> Result<Option<EngineKey>, EngineError>
where
  TX: EngineStoreTransaction,
{
  let stream = tx.range_index_entries(index);
  pin_mut!(stream);
  while let Some(item) = stream.next().await {
    let (entry_idx_key, entry_pk) = item?;
    if entry_idx_key == *index_key && entry_pk != *row_pk {
      return Ok(Some(entry_pk));
    }
  }
  Ok(None)
}

pub(crate) async fn lookup_index_rows<TX>(
  tx: &mut TX,
  table_name: &str,
  index: &IndexSchema,
  predicate: &crate::query::QualifiedPredicate,
) -> Result<Vec<EngineRow>, EngineError>
where
  TX: EngineStoreTransaction,
{
  let index_key = predicate
    .index_key_for(index)
    .ok_or_else(|| EngineError::SchemaMismatch("predicate does not match index key".into()))?;

  let row_pks = collect_matching_index_row_pks(tx.range_index_entries(index), &index_key).await?;

  let mut rows = Vec::new();
  for pk in row_pks {
    if let Some(row) = tx.get_table_row(table_name, &pk).await? {
      rows.push(row);
    }
  }
  Ok(rows)
}
