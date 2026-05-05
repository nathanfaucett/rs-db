use std::{
  borrow::Borrow,
  collections::BTreeMap,
  ops::{Bound, RangeBounds},
};

use async_stream::stream;
use futures::{Stream, StreamExt};

use automerge::AutoCommit;
use db_core::{BTreeError, BTreeExecutor, BTreeTransaction};
use uuid::Uuid;

use super::{AutomergeEntry, DocumentChangeKey, DocumentType, hash::hash_heads, reconstruct};

fn uuid_prefix_range(doc_id: Uuid) -> (Vec<u8>, Vec<u8>) {
  let start = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Incremental,
    change_hash: [0u8; 32],
  };
  let end = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Snapshot,
    change_hash: [255u8; 32],
  };
  (
    {
      let mut s = db_core::KeyScratch::with_capacity(49);
      <super::DocumentChangeKeyCodec as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
        &super::DocumentChangeKeyCodec,
        &start,
        &mut s,
      );
      s.buf
    },
    {
      let mut s = db_core::KeyScratch::with_capacity(49);
      <super::DocumentChangeKeyCodec as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
        &super::DocumentChangeKeyCodec,
        &end,
        &mut s,
      );
      s.buf
    },
  )
}

pub struct AutomergeTransaction<T> {
  inner_tx: T,
  pending: BTreeMap<Uuid, Option<AutoCommit>>,
}

impl<T> AutomergeTransaction<T>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry> + Send,
{
  pub fn new(inner_tx: T) -> Self {
    Self {
      inner_tx,
      pending: BTreeMap::new(),
    }
  }

  async fn reconstruct_inner_doc(&self, doc_id: Uuid) -> Result<Option<Vec<u8>>, BTreeError> {
    let start = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: [0u8; 32],
    };
    let end = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Snapshot,
      change_hash: [255u8; 32],
    };
    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

    let stream = self.inner_tx.range(start.clone()..=end.clone());
    futures::pin_mut!(stream);
    let mut has_entries = false;
    while let Some(item) = stream.next().await {
      let (key, entry) = item?;
      has_entries = true;
      if key.doc_type.is_snapshot() {
        latest_snapshot = Some(entry.clone());
        deltas_after_snapshot.clear();
      } else {
        deltas_after_snapshot.push(entry.clone());
      }
    }

    if !has_entries {
      return Ok(None);
    }

    let mut state = latest_snapshot.unwrap_or_default();
    state = reconstruct(&state, &deltas_after_snapshot);
    Ok(Some(state))
  }

  fn key_in_range<K: Ord, R: RangeBounds<K>>(range: &R, key: &K) -> bool {
    match range.start_bound() {
      Bound::Included(lower) => {
        if key < lower {
          return false;
        }
      }
      Bound::Excluded(lower) => {
        if key <= lower {
          return false;
        }
      }
      Bound::Unbounded => {}
    }

    match range.end_bound() {
      Bound::Included(upper) => {
        if key > upper {
          return false;
        }
      }
      Bound::Excluded(upper) => {
        if key >= upper {
          return false;
        }
      }
      Bound::Unbounded => {}
    }

    true
  }
}

impl<T> BTreeTransaction<Uuid, AutoCommit> for AutomergeTransaction<T>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry> + Send,
{
  async fn commit(self) -> Result<(), BTreeError> {
    let AutomergeTransaction {
      mut inner_tx,
      pending,
    } = self;
    for (doc_id, op) in pending {
      if let Some(snapshot_doc) = op {
        // staged insert: write a snapshot internal entry
        let mut doc = snapshot_doc;
        let change_hash = hash_heads(&doc.get_heads());
        let bytes = doc.save();

        // If an identical snapshot already exists in the underlying
        // transaction, treat this as idempotent and skip inserting.
        let start = DocumentChangeKey {
          doc_id,
          doc_type: DocumentType::Snapshot,
          change_hash: [0u8; 32],
        };
        let end = DocumentChangeKey {
          doc_id,
          doc_type: DocumentType::Snapshot,
          change_hash: [255u8; 32],
        };

        let mut found_identical = false;
        {
          let stream = inner_tx.range(start.clone()..=end.clone());
          futures::pin_mut!(stream);
          while let Some(item) = stream.next().await {
            let (_k, v) = item?;
            if v == bytes {
              found_identical = true;
              break;
            }
          }
        }

        if found_identical {
          continue;
        }

        let key = DocumentChangeKey {
          doc_id,
          doc_type: DocumentType::Snapshot,
          change_hash,
        };
        inner_tx.insert(key, bytes).await?;
      } else {
        // staged delete: remove all internal entries for this doc_id
        let start = DocumentChangeKey {
          doc_id,
          doc_type: DocumentType::Incremental,
          change_hash: [0u8; 32],
        };
        let end = DocumentChangeKey {
          doc_id,
          doc_type: DocumentType::Snapshot,
          change_hash: [255u8; 32],
        };

        let keys_to_remove: alloc::vec::Vec<DocumentChangeKey> = {
          let mut collected: alloc::vec::Vec<DocumentChangeKey> = alloc::vec::Vec::new();
          let stream = inner_tx.range(start.clone()..=end.clone());
          futures::pin_mut!(stream);
          while let Some(item) = stream.next().await {
            let (k, _v) = item?;
            collected.push(k);
          }
          collected
        };

        for k in keys_to_remove {
          inner_tx.remove(&k).await?;
        }
      }
    }
    inner_tx.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner_tx.rollback().await
  }
}

#[allow(clippy::needless_lifetimes)]
impl<T> BTreeExecutor<Uuid, AutoCommit> for AutomergeTransaction<T>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry> + Send,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();
    if let Some(pending) = self.pending.get(&doc_id) {
      return Ok(pending.clone());
    }
    match self.reconstruct_inner_doc(doc_id).await? {
      Some(bytes) => match AutoCommit::load(&bytes) {
        Ok(doc) => Ok(Some(doc)),
        Err(e) => Err(BTreeError::other(e)),
      },
      None => Ok(None),
    }
  }

  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    self.pending.insert(key, Some(value));
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();

    if let Some(existing) = self.pending.remove(&doc_id) {
      self.pending.insert(doc_id, None);
      return Ok(existing);
    }

    let existing_bytes = self.reconstruct_inner_doc(doc_id).await?;
    if existing_bytes.is_some() {
      self.pending.insert(doc_id, None);
    }
    match existing_bytes {
      None => Ok(None),
      Some(bytes) => match AutoCommit::load(&bytes) {
        Ok(doc) => Ok(Some(doc)),
        Err(e) => Err(BTreeError::other(e)),
      },
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(Uuid, AutoCommit), BTreeError>> + Send + 'a
  where
    Uuid: Ord,
    R: RangeBounds<Uuid> + Send + 'a,
  {
    stream! {
      // Map to internal document key range
      let start_doc = DocumentChangeKey {
        doc_id: Uuid::from_u128(0),
        doc_type: DocumentType::Incremental,
        change_hash: [0u8; 32],
      };
      let end_doc = DocumentChangeKey {
        doc_id: Uuid::from_u128(u128::MAX),
        doc_type: DocumentType::Snapshot,
        change_hash: [255u8; 32],
      };

      // Collect per-doc reconstructed state from inner_tx
      let mut merged: std::collections::BTreeMap<Uuid, Vec<u8>> = std::collections::BTreeMap::new();

      let mut current_doc: Option<Uuid> = None;
      let mut latest_snapshot: Option<Vec<u8>> = None;
      let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

      let stream = self.inner_tx.range(start_doc.clone()..=end_doc.clone());
      futures::pin_mut!(stream);

      while let Some(item) = stream.next().await {
        let (k, v) = item?;
        if current_doc.is_none() {
          current_doc = Some(k.doc_id);
        }

        if current_doc.unwrap() != k.doc_id {
          let doc_id = current_doc.take().unwrap();
          let mut state = latest_snapshot.take().unwrap_or_default();
          state = reconstruct(&state, &deltas_after_snapshot);
          deltas_after_snapshot.clear();
          merged.insert(doc_id, state);
          current_doc = Some(k.doc_id);
        }

        if k.doc_type.is_snapshot() {
          latest_snapshot = Some(v.clone());
          deltas_after_snapshot.clear();
        } else {
          deltas_after_snapshot.push(v.clone());
        }
      }

      if let Some(doc_id) = current_doc {
        let mut state = latest_snapshot.take().unwrap_or_default();
        state = reconstruct(&state, &deltas_after_snapshot);
        merged.insert(doc_id, state);
      }

      // Apply pending overrides/removals within the requested uuid range
      let mut pending_items: Vec<(Uuid, Option<AutoCommit>)> = Vec::new();
      for (k, v_opt) in self.pending.iter() {
        if Self::key_in_range(&range, k) {
          pending_items.push((*k, v_opt.clone()));
        }
      }

      for (k, v_opt) in pending_items.into_iter() {
        if let Some(v_doc) = v_opt {
          let mut d = v_doc;
          let bytes = d.save();
          merged.insert(k, bytes);
        } else {
          merged.remove(&k);
        }
      }

      for (doc_id, state) in merged.into_iter() {
        match AutoCommit::load(&state) {
          Ok(doc) => yield Ok((doc_id, doc)),
          Err(e) => yield Err(BTreeError::other(e)),
        }
      }
    }
  }
}

/// Encoded-backed transaction that wraps an inner transaction storing
/// `Vec<u8>` keys/values and uses codecs to translate to `DocumentChangeKey`/
/// `AutomergeEntry`.
pub struct AutomergeEncodedTransaction<T, KC, VC> {
  inner_tx: T,
  pending: BTreeMap<Uuid, Option<AutoCommit>>,
  key_codec: KC,
  val_codec: VC,
}

impl<T, KC, VC> AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::ValueCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new(inner_tx: T, key_codec: KC, val_codec: VC) -> Self {
    Self {
      inner_tx,
      pending: BTreeMap::new(),
      key_codec,
      val_codec,
    }
  }

  async fn reconstruct_inner_doc(&self, doc_id: Uuid) -> Result<Option<Vec<u8>>, BTreeError> {
    let (start_enc, end_enc) = uuid_prefix_range(doc_id);

    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

    let stream = self.inner_tx.range(start_enc.clone()..=end_enc.clone());
    futures::pin_mut!(stream);
    let mut has_entries = false;
    while let Some(item) = stream.next().await {
      let (k_enc, entry_enc) = item?;
      has_entries = true;
      let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) {
        Ok(kd) => kd,
        Err(_) => continue,
      };
      if k.doc_type.is_snapshot() {
        let v = <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&entry_enc)
          .unwrap_or_else(|_| entry_enc.clone());
        latest_snapshot = Some(v);
        deltas_after_snapshot.clear();
      } else {
        let v = <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&entry_enc)
          .unwrap_or_else(|_| entry_enc.clone());
        deltas_after_snapshot.push(v);
      }
    }

    if !has_entries {
      return Ok(None);
    }

    let mut state = latest_snapshot.unwrap_or_default();
    state = reconstruct(&state, &deltas_after_snapshot);
    Ok(Some(state))
  }

  fn key_in_range<K: Ord, R: RangeBounds<K>>(range: &R, key: &K) -> bool {
    // reuse existing helper logic
    match range.start_bound() {
      Bound::Included(lower) => {
        if key < lower {
          return false;
        }
      }
      Bound::Excluded(lower) => {
        if key <= lower {
          return false;
        }
      }
      Bound::Unbounded => {}
    }
    match range.end_bound() {
      Bound::Included(upper) => {
        if key > upper {
          return false;
        }
      }
      Bound::Excluded(upper) => {
        if key >= upper {
          return false;
        }
      }
      Bound::Unbounded => {}
    }
    true
  }
}

impl<T, KC, VC> BTreeTransaction<Uuid, AutoCommit> for AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::FastValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn commit(self) -> Result<(), BTreeError> {
    let AutomergeEncodedTransaction {
      mut inner_tx,
      pending,
      key_codec,
      val_codec,
    } = self;
    for (doc_id, op) in pending {
      if let Some(snapshot_doc) = op {
        let mut doc = snapshot_doc;
        let change_hash = hash_heads(&doc.get_heads());
        let bytes = doc.save();

        let (start_enc, end_enc) = uuid_prefix_range(doc_id);

        let mut found_identical = false;
        {
          let stream = inner_tx.range(start_enc.clone()..=end_enc.clone());
          futures::pin_mut!(stream);
          while let Some(item) = stream.next().await {
            let (_k_enc, v_enc) = item?;
            let v = <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&v_enc)
              .unwrap_or_else(|_| v_enc.clone());
            if v == bytes {
              found_identical = true;
              break;
            }
          }
        }

        if found_identical {
          continue;
        }

        let key = DocumentChangeKey {
          doc_id,
          doc_type: DocumentType::Snapshot,
          change_hash,
        };
        let mut key_scratch = db_core::KeyScratch::with_capacity(49);
        <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
          &key_codec,
          &key,
          &mut key_scratch,
        );
        let mut val_enc: Vec<u8> = Vec::new();
        <VC as db_core::FastValueCodec<AutomergeEntry>>::encode_into(
          &val_codec,
          &bytes,
          &mut val_enc,
        );
        inner_tx.insert(key_scratch.buf, val_enc).await?;
      } else {
        let (start_enc, end_enc) = uuid_prefix_range(doc_id);

        let keys_to_remove: alloc::vec::Vec<Vec<u8>> = {
          let mut collected: alloc::vec::Vec<Vec<u8>> = alloc::vec::Vec::new();
          let stream = inner_tx.range(start_enc.clone()..=end_enc.clone());
          futures::pin_mut!(stream);
          while let Some(item) = stream.next().await {
            let (k_enc, _v_enc) = item?;
            collected.push(k_enc);
          }
          collected
        };
        for k in keys_to_remove {
          inner_tx.remove(&k).await?;
        }
      }
    }
    inner_tx.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner_tx.rollback().await
  }
}

#[allow(clippy::needless_lifetimes)]
impl<T, KC, VC> BTreeExecutor<Uuid, AutoCommit> for AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::FastValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();
    if let Some(pending) = self.pending.get(&doc_id) {
      return Ok(pending.clone());
    }
    match self.reconstruct_inner_doc(doc_id).await? {
      Some(bytes) => match AutoCommit::load(&bytes) {
        Ok(doc) => Ok(Some(doc)),
        Err(e) => Err(BTreeError::other(e)),
      },
      None => Ok(None),
    }
  }

  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    self.pending.insert(key, Some(value));
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();
    if let Some(existing) = self.pending.remove(&doc_id) {
      self.pending.insert(doc_id, None);
      return Ok(existing);
    }
    let existing_bytes = self.reconstruct_inner_doc(doc_id).await?;
    if existing_bytes.is_some() {
      self.pending.insert(doc_id, None);
    }
    match existing_bytes {
      None => Ok(None),
      Some(bytes) => match AutoCommit::load(&bytes) {
        Ok(doc) => Ok(Some(doc)),
        Err(e) => Err(BTreeError::other(e)),
      },
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(Uuid, AutoCommit), BTreeError>> + Send + 'a
  where
    Uuid: Ord,
    R: RangeBounds<Uuid> + Send + 'a,
  {
    stream! {
      let start_doc = DocumentChangeKey { doc_id: Uuid::from_u128(0), doc_type: DocumentType::Incremental, change_hash: [0u8;32] };
      let end_doc = DocumentChangeKey { doc_id: Uuid::from_u128(u128::MAX), doc_type: DocumentType::Snapshot, change_hash: [255u8;32] };
      let mut start_scratch = db_core::KeyScratch::with_capacity(49);
      let mut end_scratch = db_core::KeyScratch::with_capacity(49);
      <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
        &self.key_codec,
        &start_doc,
        &mut start_scratch,
      );
      <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
        &self.key_codec,
        &end_doc,
        &mut end_scratch,
      );
      let start_enc = start_scratch.buf;
      let end_enc = end_scratch.buf;

      let mut merged: std::collections::BTreeMap<Uuid, Vec<u8>> = std::collections::BTreeMap::new();

      let mut current_doc: Option<Uuid> = None;
      let mut latest_snapshot: Option<Vec<u8>> = None;
      let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

      let stream = self.inner_tx.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(stream);

      while let Some(item) = stream.next().await {
        let (k_enc, v_enc) = item?;
        let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) { Ok(kd) => kd, Err(_) => continue };
        if current_doc.is_none() { current_doc = Some(k.doc_id); }
        if current_doc.unwrap() != k.doc_id {
          let doc_id = current_doc.take().unwrap();
          let mut state = latest_snapshot.take().unwrap_or_default();
          state = reconstruct(&state, &deltas_after_snapshot);
          deltas_after_snapshot.clear();
          merged.insert(doc_id, state);
          current_doc = Some(k.doc_id);
        }
        if k.doc_type.is_snapshot() { latest_snapshot = Some(<VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&v_enc).unwrap_or_else(|_| v_enc.clone())); deltas_after_snapshot.clear(); }
        else { deltas_after_snapshot.push(<VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&v_enc).unwrap_or_else(|_| v_enc.clone())); }
      }

      if let Some(doc_id) = current_doc { let mut state = latest_snapshot.take().unwrap_or_default(); state = reconstruct(&state, &deltas_after_snapshot); merged.insert(doc_id, state); }

      let mut pending_items: Vec<(Uuid, Option<AutoCommit>)> = Vec::new();
      for (k, v_opt) in self.pending.iter() { if Self::key_in_range(&range, k) { pending_items.push((*k, v_opt.clone())); } }

      for (k, v_opt) in pending_items.into_iter() {
        if let Some(v_doc) = v_opt { let mut d = v_doc; let bytes = d.save(); merged.insert(k, bytes); } else { merged.remove(&k); }
      }

      for (doc_id, state) in merged.into_iter() {
        match AutoCommit::load(&state) { Ok(doc) => yield Ok((doc_id, doc)), Err(e) => yield Err(BTreeError::other(e)), }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_core::{BTree, block_on};
  use db_in_memory::InMemoryBTree;

  use automerge::transaction::Transactable;

  use super::super::AutomergeBTree;

  #[test]
  fn transaction_get_reflects_pending_changes() {
    block_on(async {
      let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
      let store = AutomergeBTree::new(underlying.clone());
      let mut tx = store.transaction().await.expect("start transaction");
      let doc_id = Uuid::new_v4();

      let mut doc = AutoCommit::new();
      doc.put(&automerge::ROOT, "v", "hello").expect("put");
      let mut expected = doc.clone();

      tx.insert(doc_id, doc).await.expect("insert pending delta");
      let got = tx.get(&doc_id).await.expect("tx get");
      let mut got_doc = got.expect("missing");
      assert_eq!(got_doc.save(), expected.save());

      tx.rollback().await.expect("rollback");
    });
  }

  #[test]
  fn transaction_commit_applies_pending_changes() {
    block_on(async {
      let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
      let store = AutomergeBTree::new(underlying.clone());
      let mut tx = store.transaction().await.expect("start transaction");
      let doc_id = Uuid::new_v4();

      let mut doc = AutoCommit::new();
      doc.put(&automerge::ROOT, "v", "hello").expect("put");
      let mut expected = doc.clone();

      tx.insert(doc_id, doc).await.expect("insert pending delta");
      tx.commit().await.expect("commit");

      let got = store.get(&doc_id).await.expect("get after commit");
      let mut got_doc = got.expect("missing");
      assert_eq!(got_doc.save(), expected.save());
    });
  }
}
