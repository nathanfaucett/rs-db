use std::borrow::Borrow;

use async_stream::stream;
use automerge::AutoCommit;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use futures::{StreamExt, pin_mut};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{
  BTree, BTreeError, BTreeExecutor, BTreeTransaction, NamedTreeProvider, NamedTreeTransaction,
};
use db_types::{EngineKey, EngineRow};

use super::snapshot::{
  EngineSnapshotAdapter, decode_snapshot_base64, encode_snapshot_base64, find_entry, parse_entries,
  remove_entry, set_entry,
};
use super::{AutomergeEngineStore, make_doc_id};

fn parse_named_snapshot(buf: &[u8]) -> Result<Vec<(EngineKey, EngineRow)>, BTreeError> {
  parse_entries::<EngineSnapshotAdapter>(buf)
}

fn named_snapshot_bytes(doc: &AutoCommit) -> Result<Option<Vec<u8>>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
    Ok(Some(decode_snapshot_base64(value)?))
  } else {
    Ok(None)
  }
}

fn named_snapshot_doc(snapshot: &[u8]) -> Result<AutoCommit, BTreeError> {
  let snapshot_str = encode_snapshot_base64(snapshot);
  let mut doc = AutoCommit::new();
  doc
    .put(&automerge::ROOT, "snapshot", snapshot_str)
    .map_err(BTreeError::other)?;
  Ok(doc)
}

fn find_in_named_snapshot(buf: &[u8], needle: &EngineKey) -> Result<Option<EngineRow>, BTreeError> {
  find_entry::<EngineSnapshotAdapter>(buf, needle)
}

fn set_in_named_snapshot(
  buf: Option<&[u8]>,
  key: EngineKey,
  row: EngineRow,
) -> Result<Vec<u8>, BTreeError> {
  set_entry::<EngineSnapshotAdapter>(buf, &key, &row)
}

fn remove_from_named_snapshot(
  buf: Option<&[u8]>,
  key: &EngineKey,
) -> Result<(Option<EngineRow>, Option<Vec<u8>>), BTreeError> {
  remove_entry::<EngineSnapshotAdapter>(buf, key)
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
    let snapshot = set_in_named_snapshot(bytes.as_deref(), key, value)?;
    self
      .inner
      .insert(doc_id, named_snapshot_doc(&snapshot)?)
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
    let (removed, snapshot) = remove_from_named_snapshot(bytes.as_deref(), key)?;

    if let Some(snapshot) = snapshot.as_deref() {
      self
        .inner
        .insert(doc_id, named_snapshot_doc(snapshot)?)
        .await?;
    } else {
      let _ = self.inner.remove(&doc_id).await?;
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
