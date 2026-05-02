#![allow(clippy::manual_async_fn)]

use crate::{EngineError, EngineKey, EngineRow, IndexSchema, TableSchema};
use async_stream::stream;
use core::future::Future;
use db_core::{NamedTreeProvider, NamedTreeTransaction};
use db_types::EngineValue;
use futures::{Stream, StreamExt, pin_mut};

mod persistence;

pub use persistence::EngineStoreTransaction;

use persistence::{
  INDEX_SCHEMA_TREE, TABLE_SCHEMA_TREE, decode_index_schema_row, decode_table_schema_row,
  encode_index_schema, encode_table_schema, index_tree, make_index_entry_key, row_tree,
  split_index_entry_key,
};

pub trait EngineStore: Clone + Send + Sync + 'static {
  type Transaction: EngineStoreTransaction + Send + 'static;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>>;
}

/// Engine transaction that routes operations to named trees via a
/// `NamedTreeTransaction<EngineKey, EngineRow>`.
pub struct NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, EngineRow>,
{
  inner: T,
}

impl<T> NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, EngineRow>,
{
  pub fn new(inner: T) -> Self {
    Self { inner }
  }
}

impl<T> EngineStoreTransaction for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, EngineRow> + 'static,
{
  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      self
        .inner
        .get(&row_tree(table_name), primary_key)
        .await
        .map_err(EngineError::from)
    }
  }

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: EngineKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      self
        .inner
        .insert(&row_tree(table_name), primary_key, row)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a EngineKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      self
        .inner
        .remove(&row_tree(table_name), primary_key)
        .await
        .map_err(EngineError::from)
    }
  }

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl Stream<Item = Result<(EngineKey, EngineRow), EngineError>> + 'a {
    let tree = row_tree(table_name);
    let inner = &self.inner;
    stream! {
      let s = inner.range(&tree, ..);
      pin_mut!(s);
      while let Some(item) = s.next().await {
        yield item.map_err(EngineError::from);
      }
    }
  }

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = EngineKey::Scalar(EngineValue::Text(schema.name.clone()));
      let value = encode_table_schema(&schema);
      self
        .inner
        .insert(TABLE_SCHEMA_TREE, key, value)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = EngineKey::Scalar(EngineValue::Text(table_name.into()));
      self
        .inner
        .remove(TABLE_SCHEMA_TREE, &key)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = EngineKey::Scalar(EngineValue::Text(schema.name.clone()));
      let value = encode_index_schema(&schema);
      self
        .inner
        .insert(INDEX_SCHEMA_TREE, key, value)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = EngineKey::Scalar(EngineValue::Text(index_name.into()));
      self
        .inner
        .remove(INDEX_SCHEMA_TREE, &key)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a {
    async move {
      let mut tables = Vec::new();
      {
        let stream = self.inner.range(TABLE_SCHEMA_TREE, ..);
        pin_mut!(stream);
        while let Some(item) = stream.next().await {
          let (_key, row) = item.map_err(EngineError::from)?;
          tables.push(decode_table_schema_row(&row)?);
        }
      }
      let mut indexes = Vec::new();
      {
        let stream = self.inner.range(INDEX_SCHEMA_TREE, ..);
        pin_mut!(stream);
        while let Some(item) = stream.next().await {
          let (_key, row) = item.map_err(EngineError::from)?;
          indexes.push(decode_index_schema_row(&row)?);
        }
      }
      Ok((tables, indexes))
    }
  }

  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let composite = make_index_entry_key(index, index_key, row_pk);
      self
        .inner
        .insert(&index_tree(&index.name), composite, Vec::new())
        .await
        .map_err(EngineError::from)
    }
  }

  fn delete_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a EngineKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let composite = make_index_entry_key(index, index_key, row_pk);
      self
        .inner
        .remove(&index_tree(&index.name), &composite)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl Stream<Item = Result<(EngineKey, EngineKey), EngineError>> + 'a {
    let tree = index_tree(&index.name);
    let n = index.column_indices.len();
    let inner = &self.inner;
    stream! {
      let s = inner.range(&tree, ..);
      pin_mut!(s);
      while let Some(item) = s.next().await {
        yield item
          .map(|(composite, _)| split_index_entry_key(&composite, n))
          .map_err(EngineError::from);
      }
    }
  }

  fn commit(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.commit().await.map_err(EngineError::from) }
  }

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.rollback().await.map_err(EngineError::from) }
  }
}

/// Blanket impl: any `NamedTreeProvider<EngineKey, EngineRow>` is a valid
/// engine store.
impl<T> EngineStore for T
where
  T: Clone + NamedTreeProvider<EngineKey, EngineRow> + Send + Sync + 'static,
{
  type Transaction = NamedTreeEngineTransaction<T::Transaction>;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>> {
    async move {
      self
        .begin_transaction()
        .await
        .map(NamedTreeEngineTransaction::new)
        .map_err(EngineError::from)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{ColumnSchema, EngineKey, EngineType, EngineValue, IndexSchema, TableSchema};
  use db_core::block_on;
  use db_in_memory::InMemoryNamedBTree;

  fn sample_table_schema() -> TableSchema {
    TableSchema {
      name: "users".into(),
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
  fn load_catalog_returns_table_and_index_schemas() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, EngineRow> = InMemoryNamedBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let table_schema = sample_table_schema();
      tx.insert_table_schema(table_schema.clone())
        .await
        .expect("insert table schema");

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };
      tx.insert_index_schema(index_schema.clone())
        .await
        .expect("insert index schema");

      let (tables, indexes) = tx.load_catalog().await.expect("load catalog");
      assert_eq!(tables, vec![table_schema]);
      assert_eq!(indexes, vec![index_schema]);
    });
  }

  #[test]
  fn lookup_index_rows_returns_matching_rows() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, EngineRow> = InMemoryNamedBTree::new();
      let mut tx = store.engine_transaction().await.expect("open tx");

      let row = vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())];
      let primary_key = EngineKey::from_values(vec![EngineValue::Integer(1)]);

      tx.insert_table_row("users", primary_key.clone(), row.clone())
        .await
        .expect("insert row");

      let index_schema = IndexSchema {
        name: "users_name_idx".into(),
        table_name: "users".into(),
        column_indices: vec![1],
        unique: true,
      };
      let index_key = index_schema.key_for(&row).expect("build index key");
      tx.insert_index_entry(&index_schema, &index_key, &primary_key)
        .await
        .expect("insert index entry");

      tx.commit().await.expect("commit");

      let mut tx2 = store.engine_transaction().await.expect("open tx2");
      let predicate = crate::query::QualifiedPredicate::Equals(
        crate::query::QualifiedOperand::Column(crate::query::QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        }),
        crate::query::QualifiedOperand::Value(EngineValue::Text("Alice".into())),
      );
      let rows = tx2
        .lookup_index_rows("users", &index_schema, &predicate)
        .await
        .expect("lookup");

      assert_eq!(rows, vec![row]);
    });
  }
}
