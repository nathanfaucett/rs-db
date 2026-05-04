use std::{borrow::Borrow, sync::Arc};

use async_lock::RwLock;
use async_stream::stream;
use automerge::AutoCommit;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction};
use db_types::{StoreKey, StoreValue};

mod named;
mod snapshot;

pub use named::{AutomergeNamedTransaction, AutomergeNamedTree, AutomergeNamedTreeTransaction};
use snapshot::{
  StoreSnapshotAdapter, decode_snapshot_base64, encode_snapshot_base64, find_entry, parse_entries,
  remove_entry, set_entry,
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
    StoreKey::TableRow { table_name, .. } => make_doc_id("table:rows:", table_name),
    StoreKey::TableSchema { table_name } => make_doc_id("table:schema:", table_name),
    StoreKey::IndexEntry { index_name, .. } => make_doc_id("index:entries:", index_name),
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

fn remove_from_snapshot(buf: Option<&[u8]>, key: &StoreKey) -> Result<Option<Vec<u8>>, BTreeError> {
  Ok(remove_entry::<StoreSnapshotAdapter>(buf, key)?.1)
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
      let existing = inner.get(&doc_id).await?;
      let buf_opt = if let Some(doc) = existing {
        if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
          Some(decode_snapshot_base64(value)?)
        } else {
          None
        }
      } else {
        None
      };

      let new_buf = set_in_snapshot(buf_opt.as_deref(), &key, &value)?;
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
      let new_opt = remove_from_snapshot(buf_opt.as_deref(), &key)?;
      match new_opt {
        None => {
          let _ = inner.remove(&doc_id).await?;
        }
        Some(new_buf) => {
          let snapshot_str = encode_snapshot_base64(&new_buf);
          let mut new_doc = AutoCommit::new();
          new_doc
            .put(&automerge::ROOT, "snapshot", snapshot_str)
            .map_err(BTreeError::other)?;
          inner.insert(doc_id, new_doc).await?;
        }
      }
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

      let existing = tx.get(&doc_id).await?;
      let buf_opt = if let Some(doc) = existing {
        if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
          Some(decode_snapshot_base64(value)?)
        } else {
          None
        }
      } else {
        None
      };

      let new_buf = set_in_snapshot(buf_opt.as_deref(), &key, &value)?;
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
      let new_opt = remove_from_snapshot(buf_opt.as_deref(), &key)?;
      match new_opt {
        None => {
          let _ = tx.remove(&doc_id).await?;
        }
        Some(new_buf) => {
          let snapshot_str = encode_snapshot_base64(&new_buf);
          let mut new_doc = AutoCommit::new();
          new_doc
            .put(&automerge::ROOT, "snapshot", snapshot_str)
            .map_err(BTreeError::other)?;
          tx.insert(doc_id, new_doc).await?;
        }
      }
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

impl<B> db_core::StoragePort<StoreKey, StoreValue> for AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
  StoreKey: Clone + Ord + Send + Sync + 'static,
  StoreValue: Clone + Send + Sync + 'static,
{
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
}
