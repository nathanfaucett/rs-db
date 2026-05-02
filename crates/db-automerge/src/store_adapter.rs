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
use db_core::encode_with_version;
use db_core::{
  BTree, BTreeError, BTreeExecutor, BTreeTransaction, Cursor, DecodeError, NamedTreeProvider,
  NamedTreeTransaction,
};
use db_types::codec::{decode_store_key, decode_store_value, encode_store_key, encode_store_value};
use db_types::{
  EngineKey, EngineRow,
  codec::{
    decode_engine_key, decode_engine_row, encode_engine_key_into_sink, encode_engine_row_into_sink,
  },
};
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

fn decode_snapshot_base64(value: impl ToString) -> Result<Vec<u8>, BTreeError> {
  let text = value.to_string();
  let encoded = text
    .strip_prefix('"')
    .and_then(|s| s.strip_suffix('"'))
    .unwrap_or(&text);

  general_purpose::STANDARD
    .decode(encoded.as_bytes())
    .map_err(BTreeError::other)
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

fn parse_named_snapshot(buf: &[u8]) -> Result<Vec<(EngineKey, EngineRow)>, BTreeError> {
  let mut out = Vec::new();
  let mut cursor = Cursor::new(buf);

  loop {
    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(DecodeError::Truncated) => break,
      Err(e) => return Err(BTreeError::other(e)),
    }

    let key = decode_engine_key(&mut cursor).map_err(BTreeError::other)?;

    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(e) => return Err(BTreeError::other(e)),
    }

    let row = decode_engine_row(&mut cursor).map_err(BTreeError::other)?;
    out.push((key, row));
  }

  Ok(out)
}

type NamedSnapshotEntries = Vec<(EngineKey, EngineRow)>;

fn encode_named_snapshot(entries: &[(EngineKey, EngineRow)]) -> Vec<u8> {
  let mut buf = Vec::new();
  for (key, row) in entries {
    encode_with_version(&mut buf, |sink| encode_engine_key_into_sink(sink, key));
    encode_with_version(&mut buf, |sink| encode_engine_row_into_sink(sink, row));
  }
  buf
}

fn named_snapshot_bytes(doc: &AutoCommit) -> Result<Option<Vec<u8>>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
    Ok(Some(decode_snapshot_base64(value)?))
  } else {
    Ok(None)
  }
}

fn named_snapshot_doc(entries: &[(EngineKey, EngineRow)]) -> Result<AutoCommit, BTreeError> {
  let snapshot_str = general_purpose::STANDARD.encode(encode_named_snapshot(entries));
  let mut doc = AutoCommit::new();
  doc
    .put(&automerge::ROOT, "snapshot", snapshot_str)
    .map_err(BTreeError::other)?;
  Ok(doc)
}

fn find_in_named_snapshot(buf: &[u8], needle: &EngineKey) -> Result<Option<EngineRow>, BTreeError> {
  for (key, row) in parse_named_snapshot(buf)? {
    if &key == needle {
      return Ok(Some(row));
    }
  }
  Ok(None)
}

fn set_in_named_snapshot(
  buf: Option<&[u8]>,
  key: EngineKey,
  row: EngineRow,
) -> Result<Vec<(EngineKey, EngineRow)>, BTreeError> {
  let mut entries = if let Some(buf) = buf {
    parse_named_snapshot(buf)?
  } else {
    Vec::new()
  };

  if let Some((_, value)) = entries.iter_mut().find(|(existing, _)| existing == &key) {
    *value = row;
  } else {
    entries.push((key, row));
  }

  Ok(entries)
}

fn remove_from_named_snapshot(
  buf: Option<&[u8]>,
  key: &EngineKey,
) -> Result<(Option<EngineRow>, NamedSnapshotEntries), BTreeError> {
  let mut entries = if let Some(buf) = buf {
    parse_named_snapshot(buf)?
  } else {
    Vec::new()
  };

  let mut removed = None;
  entries.retain(|(existing, row)| {
    if existing == key {
      removed = Some(row.clone());
      false
    } else {
      true
    }
  });

  Ok((removed, entries))
}

fn named_doc_id(tree: &str) -> Uuid {
  make_doc_id("named:", tree)
}

fn in_engine_key_range<R>(key: &EngineKey, range: &R) -> bool
where
  R: core::ops::RangeBounds<EngineKey>,
{
  let start = match range.start_bound() {
    std::ops::Bound::Included(lower) => key >= lower,
    std::ops::Bound::Excluded(lower) => key > lower,
    std::ops::Bound::Unbounded => true,
  };
  let end = match range.end_bound() {
    std::ops::Bound::Included(upper) => key <= upper,
    std::ops::Bound::Excluded(upper) => key < upper,
    std::ops::Bound::Unbounded => true,
  };
  start && end
}

#[derive(Clone)]
pub struct AutomergeNamedTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  store: AutomergeEngineStore<B>,
  name: String,
}

pub struct AutomergeNamedTreeTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: AutomergeNamedTransaction<B>,
  name: String,
}

pub struct AutomergeNamedTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: <AutomergeBTree<B> as BTree<Uuid, AutoCommit>>::Transaction,
}

impl<B> NamedTreeTransaction<EngineKey, EngineRow> for AutomergeNamedTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<EngineRow>, BTreeError>
  where
    EngineKey: Ord,
  {
    let doc_id = named_doc_id(tree);
    let Some(doc) = self.inner.get(&doc_id).await? else {
      return Ok(None);
    };
    let Some(bytes) = named_snapshot_bytes(&doc)? else {
      return Ok(None);
    };
    find_in_named_snapshot(&bytes, key)
  }

  async fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: EngineRow,
  ) -> Result<(), BTreeError>
  where
    EngineKey: Ord,
  {
    let doc_id = named_doc_id(tree);
    let existing = self.inner.get(&doc_id).await?;
    let bytes = if let Some(doc) = existing.as_ref() {
      named_snapshot_bytes(doc)?
    } else {
      None
    };
    let entries = set_in_named_snapshot(bytes.as_deref(), key, value)?;
    self
      .inner
      .insert(doc_id, named_snapshot_doc(&entries)?)
      .await
  }

  async fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<EngineRow>, BTreeError>
  where
    EngineKey: Ord,
  {
    let doc_id = named_doc_id(tree);
    let Some(existing) = self.inner.get(&doc_id).await? else {
      return Ok(None);
    };
    let bytes = named_snapshot_bytes(&existing)?;
    let (removed, entries) = remove_from_named_snapshot(bytes.as_deref(), key)?;

    if entries.is_empty() {
      let _ = self.inner.remove(&doc_id).await?;
    } else {
      self
        .inner
        .insert(doc_id, named_snapshot_doc(&entries)?)
        .await?;
    }

    Ok(removed)
  }

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl futures::Stream<Item = Result<(EngineKey, EngineRow), BTreeError>> + Send + 'a
  where
    EngineKey: Ord,
    R: core::ops::RangeBounds<EngineKey> + Send + 'a,
  {
    let doc_id = named_doc_id(tree);
    let inner = &self.inner;
    stream! {
      let Some(doc) = (match inner.get(&doc_id).await {
        Ok(doc) => doc,
        Err(e) => { yield Err(e); return; }
      }) else {
        return;
      };

      let bytes = match named_snapshot_bytes(&doc) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return,
        Err(e) => { yield Err(e); return; }
      };

      let mut entries = match parse_named_snapshot(&bytes) {
        Ok(entries) => entries,
        Err(e) => { yield Err(e); return; }
      };
      entries.sort_by(|(left, _), (right, _)| left.cmp(right));

      for (key, row) in entries {
        if in_engine_key_range(&key, &range) {
          yield Ok((key, row));
        }
      }
    }
  }

  async fn commit(self) -> Result<(), BTreeError> {
    self.inner.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner.rollback().await
  }
}

impl<B> BTreeExecutor<EngineKey, EngineRow> for AutomergeNamedTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<EngineRow>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let mut tx = self.store.begin_transaction().await?;
    tx.get(&self.name, key.borrow()).await
  }

  async fn insert(&mut self, key: EngineKey, value: EngineRow) -> Result<(), BTreeError>
  where
    EngineKey: Ord,
  {
    let mut tx = self.store.begin_transaction().await?;
    tx.insert(&self.name, key, value).await?;
    tx.commit().await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<EngineRow>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let mut tx = self.store.begin_transaction().await?;
    let removed = tx.remove(&self.name, key.borrow()).await?;
    tx.commit().await?;
    Ok(removed)
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(EngineKey, EngineRow), BTreeError>> + Send + 'a
  where
    EngineKey: Ord + Clone,
    R: core::ops::RangeBounds<EngineKey> + Send + 'a,
  {
    let store = self.store.clone();
    let name = self.name.clone();
    stream! {
      let tx = match store.begin_transaction().await {
        Ok(tx) => tx,
        Err(e) => { yield Err(e); return; }
      };
      let range_stream = tx.range(&name, range);
      pin_mut!(range_stream);
      while let Some(item) = range_stream.next().await {
        yield item;
      }
    }
  }
}

impl<B> BTreeTransaction<EngineKey, EngineRow> for AutomergeNamedTreeTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn commit(self) -> Result<(), BTreeError> {
    self.inner.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner.rollback().await
  }
}

impl<B> BTreeExecutor<EngineKey, EngineRow> for AutomergeNamedTreeTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<EngineRow>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let doc_id = named_doc_id(&self.name);
    let Some(doc) = self.inner.inner.get(&doc_id).await? else {
      return Ok(None);
    };
    let Some(bytes) = named_snapshot_bytes(&doc)? else {
      return Ok(None);
    };
    find_in_named_snapshot(&bytes, key.borrow())
  }

  async fn insert(&mut self, key: EngineKey, value: EngineRow) -> Result<(), BTreeError>
  where
    EngineKey: Ord,
  {
    self.inner.insert(&self.name, key, value).await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<EngineRow>, BTreeError>
  where
    EngineKey: Ord + Clone,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    self.inner.remove(&self.name, key.borrow()).await
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(EngineKey, EngineRow), BTreeError>> + Send + 'a
  where
    EngineKey: Ord + Clone,
    R: core::ops::RangeBounds<EngineKey> + Send + 'a,
  {
    self.inner.range(&self.name, range)
  }
}

impl<B> BTree<EngineKey, EngineRow> for AutomergeNamedTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeNamedTreeTransaction<B>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    Ok(AutomergeNamedTreeTransaction {
      inner: self.store.begin_transaction().await?,
      name: self.name.clone(),
    })
  }
}

impl<B> NamedTreeProvider<EngineKey, EngineRow> for AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Tree = AutomergeNamedTree<B>;
  type Transaction = AutomergeNamedTransaction<B>;

  fn get_tree<'a>(
    &'a self,
    name: &str,
  ) -> impl core::future::Future<Output = Result<Self::Tree, BTreeError>> + Send + 'a {
    let store = self.clone();
    let name = name.to_string();
    async move { Ok(AutomergeNamedTree { store, name }) }
  }

  fn begin_transaction<'a>(
    &'a self,
  ) -> impl core::future::Future<Output = Result<Self::Transaction, BTreeError>> + Send + 'a {
    let automerge = self.automerge.clone();
    async move {
      let guard = automerge.read().await;
      let inner = guard.transaction().await?;
      Ok(AutomergeNamedTransaction { inner })
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
  use db_types::EngineValue;

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
