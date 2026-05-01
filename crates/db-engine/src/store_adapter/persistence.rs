use core::borrow::Borrow;
use core::future::Future;
use core::ops::RangeBounds;

use db_core::BTreeTransaction;
use futures::{pin_mut, stream::StreamExt};

use crate::{
  EngineError, EngineKey, EngineRow, EngineValue, IndexSchema, StoreKey, StoreValue, TableSchema,
};

pub trait EngineStoreTransaction: Send + 'static {
  fn collect_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
    predicate: Option<crate::EnginePredicate>,
  ) -> impl Future<Output = Result<Vec<(EngineKey, EngineRow)>, EngineError>> + 'a;

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

  fn delete_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
    row: &'a EngineRow,
    indexes: &'a [IndexSchema],
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

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

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a;

  fn lookup_index_rows<'a>(
    &'a mut self,
    table_name: &'a str,
    index: &'a IndexSchema,
    predicate: &'a crate::query::EnginePredicate,
  ) -> impl Future<Output = Result<Vec<EngineRow>, EngineError>> + 'a;

  fn insert_raw<'a>(
    &'a mut self,
    key: StoreKey,
    value: StoreValue,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a;

  fn remove_raw<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl Future<Output = Result<Option<StoreValue>, EngineError>> + 'a
  where
    Q: Borrow<StoreKey> + Send + 'a;

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: String,
  ) -> impl Future<Output = Result<Option<StoreValue>, EngineError>> + 'a {
    async move {
      let key = StoreKey::table_schema(table_name);
      self.remove_raw(&key).await
    }
  }

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: String,
  ) -> impl Future<Output = Result<Option<StoreValue>, EngineError>> + 'a {
    async move {
      let key = StoreKey::index_schema(index_name);
      self.remove_raw(&key).await
    }
  }

  fn remove_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let keys = {
        let this: &Self = &*self;
        let stream = this.range_table_rows(table_name);
        pin_mut!(stream);
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
          let (key, _) = item?;
          keys.push(key);
        }
        keys
      };
      for key in keys {
        self.remove_raw(&key).await?;
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
        let this: &Self = &*self;
        let stream = this.range_index_entries(index);
        pin_mut!(stream);
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
          let (key, _) = item?;
          keys.push(key);
        }
        keys
      };
      for key in keys {
        self.remove_raw(&key).await?;
      }
      Ok(())
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), EngineError>> + 'a
  where
    R: RangeBounds<StoreKey> + Send + 'a;

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), EngineError>> + 'a {
    let start = StoreKey::table_row(table_name.to_string(), EngineKey::Scalar(EngineValue::Null));

    self.range(start..).take_while(move |res| {
      futures::future::ready(match res {
        Ok((key, _)) => {
          matches!(key, StoreKey::TableRow { table_name: name, .. } if name == table_name)
        }
        Err(_) => false,
      })
    })
  }

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), EngineError>> + 'a {
    let start = StoreKey::index_entry(
      index.name.clone(),
      EngineKey::Scalar(EngineValue::Null),
      EngineKey::Scalar(EngineValue::Null),
    );

    self.range(start..).take_while(move |res| {
      futures::future::ready(match res {
        Ok((key, _)) => {
          matches!(key, StoreKey::IndexEntry { index_name, .. } if index_name == &index.name)
        }
        Err(_) => false,
      })
    })
  }

  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let entry_key = StoreKey::index_entry(index.name.clone(), index_key.clone(), row_pk.clone());

      self.insert_raw(entry_key, StoreValue::IndexEntry).await
    }
  }

  fn delete_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<StoreValue>, EngineError>> + 'a {
    async move {
      let entry_key = StoreKey::index_entry(index.name.clone(), index_key.clone(), row_pk.clone());

      self.remove_raw(&entry_key).await
    }
  }

  fn find_conflicting_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineKey>, EngineError>> + 'a {
    async move {
      let stream = EngineStoreTransaction::range_index_entries(self, index);
      pin_mut!(stream);

      while let Some(item) = stream.next().await {
        let (key, _value) = item?;
        if let StoreKey::IndexEntry {
          index_name,
          index_key: entry_key,
          row_pk: existing_pk,
        } = key
          && index_name == index.name
          && entry_key == *index_key
          && existing_pk != *row_pk
        {
          return Ok(Some(existing_pk));
        }
      }

      Ok(None)
    }
  }

  fn commit(self) -> impl Future<Output = Result<(), EngineError>>;

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>>;
}

pub(crate) async fn load_catalog_impl<T>(
  tx: &mut T,
) -> Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  db_types::persistence::load_catalog_impl(tx)
    .await
    .map_err(EngineError::from)
}

pub(crate) fn range_index_entries_impl<'a, T>(
  tx: &'a T,
  index: &'a IndexSchema,
) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), EngineError>> + 'a
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  db_types::persistence::range_index_entries_impl(tx, &index.name)
    .map(|res| res.map_err(EngineError::from))
}

pub(crate) async fn lookup_index_rows_impl<T>(
  tx: &mut T,
  table_name: &str,
  index: &IndexSchema,
  predicate: &crate::query::EnginePredicate,
) -> Result<Vec<EngineRow>, EngineError>
where
  T: BTreeTransaction<StoreKey, StoreValue> + Send + 'static,
{
  let index_key = predicate
    .index_key_for(index)
    .ok_or_else(|| EngineError::SchemaMismatch("predicate does not match index key".into()))?;

  let row_pks = {
    let mut row_pks = Vec::new();
    let stream = range_index_entries_impl(tx, index);
    pin_mut!(stream);

    while let Some(item) = stream.next().await {
      let (key, _value) = item?;
      if let StoreKey::IndexEntry {
        index_name,
        index_key: entry_key,
        row_pk,
      } = key
        && index_name == index.name
        && entry_key == index_key
      {
        row_pks.push(row_pk);
      }
    }

    row_pks
  };

  let mut rows = Vec::new();
  for row_pk in row_pks {
    let table_key = StoreKey::table_row(table_name.to_string(), row_pk.clone());
    if let Ok(Some(StoreValue::Row(row))) = tx.get(&table_key).await.map_err(EngineError::from) {
      rows.push(row);
    }
  }

  Ok(rows)
}
