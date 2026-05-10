use std::{borrow::Borrow, sync::Arc};

use async_lock::RwLock;
use async_stream::stream;
use automerge::AutoCommit;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use db_core::encode_with_version;
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction};
use db_types::codec::encode_engine_key_into_sink;
use db_types::{StoreKey, StoreValue};

mod named;
mod snapshot;

pub use named::{AutomergeNamedTransaction, AutomergeNamedTree, AutomergeNamedTreeTransaction};
use snapshot::{
  StoreSnapshotAdapter, decode_snapshot_base64, encode_snapshot_base64, find_entry, parse_entries,
  set_entry,
};

/// Automerge-backed engine store: each logical collection (table/index/schema)
/// is represented by an Automerge `AutoCommit` document stored in the
/// `AutomergeBTree<B>` backend. Engine-level keys/values are encoded into a
/// single snapshot blob inside the document and decoded on read.
#[derive(Clone)]
pub struct AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub automerge: Arc<RwLock<AutomergeBTree<B>>>,
}

impl<B> AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new_with_backend(backend: B) -> Self {
    let automerge = Arc::new(RwLock::new(AutomergeBTree::new(backend)));
    Self { automerge }
  }
}

pub(crate) fn make_doc_id(prefix: &str, name: &str) -> Uuid {
  let mut hasher = Sha256::new();
  hasher.update(prefix.as_bytes());
  hasher.update(name.as_bytes());
  let digest = hasher.finalize();
  Uuid::from_slice(&digest[..16]).unwrap_or(Uuid::nil())
}

fn doc_id_for_key(key: &StoreKey) -> Uuid {
  match key {
    StoreKey::TableRow {
      table_name,
      primary_key,
    } => {
      let mut hasher = Sha256::new();
      hasher.update(b"table:row:");
      hasher.update(table_name.as_bytes());
      let mut key_buf: Vec<u8> = Vec::new();
      encode_with_version(&mut key_buf, |sink| {
        encode_engine_key_into_sink(sink, primary_key)
      });
      hasher.update(&key_buf);
      let digest = hasher.finalize();
      Uuid::from_slice(&digest[..16]).unwrap_or(Uuid::nil())
    }
    StoreKey::IndexEntry {
      index_name,
      index_key,
      row_pk,
    } => {
      let mut hasher = Sha256::new();
      hasher.update(b"index:entry:");
      hasher.update(index_name.as_bytes());
      let mut key_buf: Vec<u8> = Vec::new();
      encode_with_version(&mut key_buf, |sink| {
        encode_engine_key_into_sink(sink, index_key)
      });
      hasher.update(&key_buf);
      let mut pk_buf: Vec<u8> = Vec::new();
      encode_with_version(&mut pk_buf, |sink| {
        encode_engine_key_into_sink(sink, row_pk)
      });
      hasher.update(&pk_buf);
      let digest = hasher.finalize();
      Uuid::from_slice(&digest[..16]).unwrap_or(Uuid::nil())
    }
    StoreKey::TableSchema { table_name } => make_doc_id("table:schema:", table_name),
    StoreKey::IndexSchema { index_name } => make_doc_id("index:schema:", index_name),
  }
}

fn parse_snapshot(buf: &[u8]) -> Result<Vec<(StoreKey, StoreValue)>, BTreeError> {
  parse_entries::<StoreSnapshotAdapter>(buf)
}

fn find_in_snapshot(buf: &[u8], needle: &StoreKey) -> Result<Option<StoreValue>, BTreeError> {
  find_entry::<StoreSnapshotAdapter>(buf, needle)
}

fn set_in_snapshot(
  buf: Option<&[u8]>,
  key: &StoreKey,
  value: &StoreValue,
) -> Result<Vec<u8>, BTreeError> {
  set_entry::<StoreSnapshotAdapter>(buf, key, value)
}

pub struct AutomergeEngineStoreTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: <AutomergeBTree<B> as BTree<Uuid, AutoCommit>>::Transaction,
}

impl<B> BTreeExecutor<StoreKey, StoreValue> for AutomergeEngineStoreTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let key = key.borrow().clone();
    let inner = &self.inner;
    async move {
      let doc_id = doc_id_for_key(&key);
      match inner.get(&doc_id).await? {
        None => Ok(None),
        Some(doc) => {
          if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
            let bytes = decode_snapshot_base64(value)?;
            return find_in_snapshot(&bytes, &key);
          }
          Ok(None)
        }
      }
    }
  }

  fn insert<'a>(
    &'a mut self,
    key: StoreKey,
    value: StoreValue,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
  {
    let inner = &mut self.inner;
    async move {
      let doc_id = doc_id_for_key(&key);
      let new_buf = set_in_snapshot(None, &key, &value)?;
      let snapshot_str = encode_snapshot_base64(&new_buf);
      let mut doc = AutoCommit::new();
      doc
        .put(&automerge::ROOT, "snapshot", snapshot_str)
        .map_err(BTreeError::other)?;
      inner.insert(doc_id, doc).await
    }
  }

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let key = key.borrow().clone();
    let inner = &mut self.inner;
    async move {
      let doc_id = doc_id_for_key(&key);
      let existing = inner.get(&doc_id).await?;
      if existing.is_none() {
        return Ok(None);
      }
      let doc = existing.expect("checked is_some");
      let buf_opt = if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
        Some(decode_snapshot_base64(value)?)
      } else {
        None
      };
      let prev = if let Some(ref b) = buf_opt {
        find_in_snapshot(b, &key)?
      } else {
        None
      };
      let _ = inner.remove(&doc_id).await?;
      Ok(prev)
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), BTreeError>> + Send + 'a
  where
    StoreKey: Ord + Clone,
    R: core::ops::RangeBounds<StoreKey> + Send + 'a,
  {
    let doc_stream = self
      .inner
      .range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
    stream! {
      pin_mut!(doc_stream);
      while let Some(item) = doc_stream.next().await {
        let (_doc_id, doc) = item?;
        if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
          let bytes = match decode_snapshot_base64(value) {
            Ok(b) => b,
            Err(e) => { yield Err(e); continue; }
          };
          match parse_snapshot(&bytes) {
            Ok(pairs) => {
              for (k, v) in pairs.into_iter() {
                let in_range = match range.start_bound() {
                  std::ops::Bound::Included(lower) => k >= *lower,
                  std::ops::Bound::Excluded(lower) => k > *lower,
                  std::ops::Bound::Unbounded => true,
                } && match range.end_bound() {
                  std::ops::Bound::Included(upper) => k <= *upper,
                  std::ops::Bound::Excluded(upper) => k < *upper,
                  std::ops::Bound::Unbounded => true,
                };
                if in_range {
                  yield Ok((k, v));
                }
              }
            }
            Err(e) => yield Err(e),
          }
        }
      }
    }
  }
}

impl<B> BTreeTransaction<StoreKey, StoreValue> for AutomergeEngineStoreTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  #[allow(clippy::manual_async_fn)]
  fn commit(self) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send {
    async move { self.inner.commit().await }
  }

  #[allow(clippy::manual_async_fn)]
  fn rollback(self) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send {
    async move { self.inner.rollback().await }
  }
}

impl<B> BTreeExecutor<StoreKey, StoreValue> for AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let automerge = self.automerge.clone();
    async move {
      let k = key.borrow().clone();
      let doc_id = doc_id_for_key(&k);
      let guard = automerge.read().await;
      match guard.get(&doc_id).await? {
        None => Ok(None),
        Some(doc) => {
          if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
            let bytes = decode_snapshot_base64(value)?;
            return find_in_snapshot(&bytes, &k);
          }
          Ok(None)
        }
      }
    }
  }

  fn insert<'a>(
    &'a mut self,
    key: StoreKey,
    value: StoreValue,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
  {
    let automerge = self.automerge.clone();
    async move {
      let doc_id = doc_id_for_key(&key);
      let guard = automerge.read().await;
      let mut tx = guard.transaction().await?;
      let new_buf = set_in_snapshot(None, &key, &value)?;
      let snapshot_str = encode_snapshot_base64(&new_buf);
      let mut doc = AutoCommit::new();
      doc
        .put(&automerge::ROOT, "snapshot", snapshot_str)
        .map_err(BTreeError::other)?;
      tx.insert(doc_id, doc).await?;
      tx.commit().await
    }
  }

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let automerge = self.automerge.clone();
    let key = key.borrow().clone();
    async move {
      let doc_id = doc_id_for_key(&key);
      let guard = automerge.read().await;
      let mut tx = guard.transaction().await?;
      let existing = tx.get(&doc_id).await?;
      if existing.is_none() {
        return Ok(None);
      }
      let doc = existing.expect("checked is_some");
      let buf_opt = if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
        Some(decode_snapshot_base64(value)?)
      } else {
        None
      };
      let prev = if let Some(ref b) = buf_opt {
        find_in_snapshot(b, &key)?
      } else {
        None
      };
      let _ = tx.remove(&doc_id).await?;
      tx.commit().await?;
      Ok(prev)
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), BTreeError>> + Send + 'a
  where
    StoreKey: Ord + Clone,
    R: core::ops::RangeBounds<StoreKey> + Send + 'a,
  {
    let automerge = self.automerge.clone();
    stream! {
      let guard = automerge.read().await;
      let doc_stream = guard.range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
      pin_mut!(doc_stream);
      while let Some(item) = doc_stream.next().await {
        let (_doc_id, doc) = item?;
        if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
          let bytes = match decode_snapshot_base64(value) {
            Ok(b) => b,
            Err(e) => { yield Err(e); continue; }
          };
          match parse_snapshot(&bytes) {
            Ok(pairs) => {
              for (k, v) in pairs.into_iter() {
                let in_range = match range.start_bound() {
                  std::ops::Bound::Included(lower) => k >= *lower,
                  std::ops::Bound::Excluded(lower) => k > *lower,
                  std::ops::Bound::Unbounded => true,
                } && match range.end_bound() {
                  std::ops::Bound::Included(upper) => k <= *upper,
                  std::ops::Bound::Excluded(upper) => k < *upper,
                  std::ops::Bound::Unbounded => true,
                };
                if in_range {
                  yield Ok((k, v));
                }
              }
            }
            Err(e) => yield Err(e),
          }
        }
      }
    }
  }
}

impl<B> BTree<StoreKey, StoreValue> for AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeEngineStoreTransaction<B>;

  fn transaction<'a>(
    &'a self,
  ) -> impl core::future::Future<Output = Result<Self::Transaction, BTreeError>> + Send + 'a {
    let automerge = self.automerge.clone();
    async move {
      let guard = automerge.read().await;
      let inner_tx = guard.transaction().await?;
      Ok(AutomergeEngineStoreTransaction { inner: inner_tx })
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_core::{NamedTreeProvider, NamedTreeTransaction, block_on};
  use db_in_memory::InMemoryBTree;
  use db_types::{EngineKey, EngineValue};

  fn store() -> AutomergeEngineStore<InMemoryBTree<DocumentChangeKey, AutomergeEntry>> {
    AutomergeEngineStore::new_with_backend(InMemoryBTree::new())
  }

  #[test]
  fn named_transaction_commits_isolated_trees() {
    block_on(async {
      let store = store();
      let mut tx = store.begin_transaction().await.expect("begin");
      let key = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let row = vec![EngineValue::Text("first".into())];

      tx.insert("first", key.clone(), row.clone())
        .await
        .expect("insert first");
      tx.insert(
        "second",
        key.clone(),
        vec![EngineValue::Text("second".into())],
      )
      .await
      .expect("insert second");
      tx.commit().await.expect("commit");

      let mut read = store.begin_transaction().await.expect("read");
      assert_eq!(read.get("first", &key).await.expect("get first"), Some(row));
      assert_eq!(
        read.get("second", &key).await.expect("get second"),
        Some(vec![EngineValue::Text("second".into())])
      );
    });
  }
  #[test]
  fn named_transaction_range_returns_all_rows() {
    block_on(async {
      let store = store();
      let mut tx = store.begin_transaction().await.expect("begin");
      let key1 = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let row1 = vec![EngineValue::Text("alice".into())];
      let key2 = EngineKey::from_values(vec![EngineValue::Integer(2)]);
      let row2 = vec![EngineValue::Text("bob".into())];

      tx.insert("users", key1.clone(), row1.clone())
        .await
        .expect("insert alice");
      tx.insert("users", key2.clone(), row2.clone())
        .await
        .expect("insert bob");
      tx.commit().await.expect("commit");

      let read = store.begin_transaction().await.expect("read");
      let mut rows = Vec::new();
      let stream = read.range("users", ..);
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (key, value) = item.expect("range failed");
        rows.push((key, value));
      }

      assert_eq!(rows.len(), 2);
      assert_eq!(rows[0].0, key1);
      assert_eq!(rows[0].1, row1);
      assert_eq!(rows[1].0, key2);
      assert_eq!(rows[1].1, row2);
    });
  }

  #[test]
  fn named_range_after_two_separate_committed_transactions() {
    block_on(async {
      let store = store();

      let key1 = EngineKey::from_values(vec![EngineValue::Integer(1)]);
      let row1 = vec![EngineValue::Text("alice".into())];
      let key2 = EngineKey::from_values(vec![EngineValue::Integer(2)]);
      let row2 = vec![EngineValue::Text("bob".into())];

      {
        let mut tx = store.begin_transaction().await.expect("begin tx1");
        tx.insert("schemas", key1.clone(), row1.clone())
          .await
          .expect("insert 1");
        tx.commit().await.expect("commit tx1");
      }
      {
        let mut tx = store.begin_transaction().await.expect("begin tx2");
        tx.insert("schemas", key2.clone(), row2.clone())
          .await
          .expect("insert 2");
        tx.commit().await.expect("commit tx2");
      }

      let read = store.begin_transaction().await.expect("begin read");
      let mut rows = Vec::new();
      let stream = read.range("schemas", ..);
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        rows.push(item.expect("range item"));
      }

      assert_eq!(rows.len(), 2, "expected 2 rows, got: {:?}", rows);
    });
  }

  #[test]
  fn two_separate_tables_load_catalog() {
    block_on(async {
      let store = store();

      // First schema
      let key1 = EngineKey::from_values(vec![EngineValue::Text("users".into())]);
      let val1 = vec![EngineValue::Blob(b"users_schema_bytes".to_vec())];

      {
        let mut tx = store.begin_transaction().await.expect("tx1");
        tx.insert("sys:table_schemas", key1.clone(), val1.clone())
          .await
          .expect("insert schema 1");
        tx.commit().await.expect("commit tx1");
      }

      // Range should return 1 row
      {
        let read = store.begin_transaction().await.expect("read tx");
        let mut rows = Vec::new();
        let stream = read.range("sys:table_schemas", ..);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
          rows.push(item.expect("range item"));
        }
        assert_eq!(rows.len(), 1, "expected 1 schema row, got: {:?}", rows);
        assert_eq!(rows[0].0, key1);
        assert_eq!(rows[0].1, val1);
      }
    });
  }
}
