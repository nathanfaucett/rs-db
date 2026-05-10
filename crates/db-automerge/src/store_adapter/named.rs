use std::borrow::Borrow;

use async_stream::stream;
use automerge::AutoCommit;
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::encode_with_version;
use db_core::{
  BTree, BTreeError, BTreeExecutor, BTreeTransaction, NamedTreeProvider, NamedTreeTransaction,
};
use db_types::codec::encode_engine_key_into_sink;
use db_types::{EngineKey, EngineRow};

use super::AutomergeEngineStore;
use super::snapshot::{
  EngineSnapshotAdapter, find_entry, key_in_range, parse_entries, set_entry, snapshot_bytes,
  snapshot_doc,
};

fn parse_named_snapshot(buf: &[u8]) -> Result<Vec<(EngineKey, EngineRow)>, BTreeError> {
  parse_entries::<EngineSnapshotAdapter>(buf)
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

/// Derive a UUID for a specific row in a named tree.
/// Layout: first 8 bytes = SHA-256("named:", tree)[0..8]
///         last  8 bytes = SHA-256(encoded_key)[0..8]
/// This keeps all rows for a given tree contiguous in UUID space.
fn row_doc_id(tree: &str, key: &EngineKey) -> Uuid {
  let mut hasher = Sha256::new();
  hasher.update(b"named:");
  hasher.update(tree.as_bytes());
  let tree_digest = hasher.finalize_reset();

  let mut key_buf: Vec<u8> = Vec::new();
  encode_with_version(&mut key_buf, |sink| encode_engine_key_into_sink(sink, key));
  hasher.update(&key_buf);
  let key_digest = hasher.finalize();

  let mut bytes = [0u8; 16];
  bytes[..8].copy_from_slice(&tree_digest[..8]);
  bytes[8..].copy_from_slice(&key_digest[..8]);
  Uuid::from_bytes(bytes)
}

/// UUID range covering all rows stored for `tree`.
fn tree_uuid_range(tree: &str) -> (Uuid, Uuid) {
  let mut hasher = Sha256::new();
  hasher.update(b"named:");
  hasher.update(tree.as_bytes());
  let digest = hasher.finalize();

  let mut start_bytes = [0u8; 16];
  let mut end_bytes = [0u8; 16];
  start_bytes[..8].copy_from_slice(&digest[..8]);
  end_bytes[..8].copy_from_slice(&digest[..8]);
  end_bytes[8..].fill(0xff);

  (Uuid::from_bytes(start_bytes), Uuid::from_bytes(end_bytes))
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
    let doc_id = row_doc_id(tree, key);
    let Some(doc) = self.inner.get(&doc_id).await? else {
      return Ok(None);
    };
    let Some(bytes) = snapshot_bytes(&doc)? else {
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
    let doc_id = row_doc_id(tree, &key);
    let snapshot = set_in_named_snapshot(None, key, value)?;
    self.inner.insert(doc_id, snapshot_doc(&snapshot)?).await
  }

  async fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<EngineRow>, BTreeError>
  where
    EngineKey: Ord,
  {
    let doc_id = row_doc_id(tree, key);
    let Some(existing) = self.inner.get(&doc_id).await? else {
      return Ok(None);
    };
    let bytes = snapshot_bytes(&existing)?;
    let removed = if let Some(ref b) = bytes {
      find_in_named_snapshot(b, key)?
    } else {
      None
    };
    if removed.is_some() {
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
    let (tree_start, tree_end) = tree_uuid_range(tree);
    let inner = &self.inner;
    stream! {
      let doc_stream = inner.range(tree_start..=tree_end);
      pin_mut!(doc_stream);

      let mut entries: alloc::vec::Vec<(EngineKey, EngineRow)> = alloc::vec::Vec::new();
      while let Some(item) = doc_stream.next().await {
        let (_uuid, doc) = item?;
        let bytes = match snapshot_bytes(&doc) {
          Ok(Some(b)) => b,
          Ok(None) => continue,
          Err(e) => { yield Err(e); return; }
        };
        match parse_named_snapshot(&bytes) {
          Ok(pairs) => entries.extend(pairs),
          Err(e) => { yield Err(e); return; }
        }
      }
      entries.sort_by(|(a, _), (b, _)| a.cmp(b));
      for (key, row) in entries {
        if key_in_range(&key, &range) {
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
    let doc_id = row_doc_id(&self.name, key.borrow());
    let Some(doc) = self.inner.inner.get(&doc_id).await? else {
      return Ok(None);
    };
    let Some(bytes) = snapshot_bytes(&doc)? else {
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
