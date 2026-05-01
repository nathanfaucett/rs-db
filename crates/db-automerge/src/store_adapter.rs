use std::{borrow::Borrow, sync::Arc, vec::Vec};

use async_lock::RwLock;
use async_stream::stream;
use automerge::AutoCommit;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction, Cursor, DecodeError};
use db_in_memory::InMemoryBTree;
use db_types::codec::{decode_store_key, decode_store_value, encode_store_key, encode_store_value};
use db_types::{StoreKey, StoreValue};

use base64::{Engine as _, engine::general_purpose};

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

pub fn new_in_memory_store()
-> AutomergeEngineStore<InMemoryBTree<DocumentChangeKey, AutomergeEntry>> {
  AutomergeEngineStore::new_with_backend(InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new())
}

pub type AutomergeEngineStoreInMemory =
  AutomergeEngineStore<InMemoryBTree<DocumentChangeKey, AutomergeEntry>>;

fn make_doc_id(prefix: &str, name: &str) -> Uuid {
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
  let mut out: Vec<(StoreKey, StoreValue)> = Vec::new();
  let mut cursor = Cursor::new(buf);
  loop {
    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(DecodeError::Truncated) => break,
      Err(e) => return Err(BTreeError::other(e)),
    }

    let key = decode_store_key(&mut cursor).map_err(BTreeError::other)?;

    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(e) => return Err(BTreeError::other(e)),
    }

    let val = decode_store_value(&mut cursor).map_err(BTreeError::other)?;
    out.push((key, val));
  }
  Ok(out)
}

fn encode_snapshot(entries: &[(StoreKey, StoreValue)]) -> Vec<u8> {
  let mut buf: Vec<u8> = Vec::new();
  for (k, v) in entries.iter() {
    encode_store_key(&mut buf, k);
    encode_store_value(&mut buf, v);
  }
  buf
}

fn find_in_snapshot(buf: &[u8], needle: &StoreKey) -> Result<Option<StoreValue>, BTreeError> {
  let pairs = parse_snapshot(buf)?;
  for (k, v) in pairs.into_iter() {
    if &k == needle {
      return Ok(Some(v));
    }
  }
  Ok(None)
}

fn set_in_snapshot(
  buf: Option<&[u8]>,
  key: &StoreKey,
  value: &StoreValue,
) -> Result<Vec<u8>, BTreeError> {
  let mut pairs = if let Some(b) = buf {
    parse_snapshot(b)?
  } else {
    Vec::new()
  };
  let mut replaced = false;
  for (k, v) in pairs.iter_mut() {
    if k == key {
      *v = value.clone();
      replaced = true;
      break;
    }
  }
  if !replaced {
    pairs.push((key.clone(), value.clone()));
  }
  Ok(encode_snapshot(&pairs))
}

fn remove_from_snapshot(buf: Option<&[u8]>, key: &StoreKey) -> Result<Option<Vec<u8>>, BTreeError> {
  let mut pairs = if let Some(b) = buf {
    parse_snapshot(b)?
  } else {
    Vec::new()
  };
  let before = pairs.len();
  pairs.retain(|(k, _)| k != key);
  if pairs.len() == before {
    return Ok(buf.map(|b| b.to_vec()));
  }
  if pairs.is_empty() {
    Ok(None)
  } else {
    Ok(Some(encode_snapshot(&pairs)))
  }
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
            let s = value.to_string();
            let bytes = general_purpose::STANDARD
              .decode(s.as_bytes())
              .map_err(BTreeError::other)?;
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
          Some(
            general_purpose::STANDARD
              .decode(value.to_string().as_bytes())
              .map_err(BTreeError::other)?,
          )
        } else {
          None
        }
      } else {
        None
      };

      let new_buf = set_in_snapshot(buf_opt.as_deref(), &key, &value)?;
      let snapshot_str = general_purpose::STANDARD.encode(&new_buf);
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
      let doc = existing.unwrap();
      let buf_opt = if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
        Some(
          general_purpose::STANDARD
            .decode(value.to_string().as_bytes())
            .map_err(BTreeError::other)?,
        )
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
          let snapshot_str = general_purpose::STANDARD.encode(&new_buf);
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
          let s = value.to_string();
          let bytes = match general_purpose::STANDARD.decode(s.as_bytes()) {
            Ok(b) => b,
            Err(e) => { yield Err(BTreeError::other(e)); continue; }
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
  fn commit(self) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send {
    async move { self.inner.commit().await }
  }

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
            let s = value.to_string();
            let bytes = general_purpose::STANDARD
              .decode(s.as_bytes())
              .map_err(BTreeError::other)?;
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
          Some(
            general_purpose::STANDARD
              .decode(value.to_string().as_bytes())
              .map_err(BTreeError::other)?,
          )
        } else {
          None
        }
      } else {
        None
      };

      let new_buf = set_in_snapshot(buf_opt.as_deref(), &key, &value)?;
      let snapshot_str = general_purpose::STANDARD.encode(&new_buf);
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
      let doc = existing.unwrap();
      let buf_opt = if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
        Some(
          general_purpose::STANDARD
            .decode(value.to_string().as_bytes())
            .map_err(BTreeError::other)?,
        )
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
          let snapshot_str = general_purpose::STANDARD.encode(&new_buf);
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
          let s = value.to_string();
          let bytes = match general_purpose::STANDARD.decode(s.as_bytes()) {
            Ok(b) => b,
            Err(e) => { yield Err(BTreeError::other(e)); continue; }
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
