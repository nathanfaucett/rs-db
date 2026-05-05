use std::{borrow::Borrow, fmt::Debug, ops::RangeBounds};

use async_stream::stream;
use automerge::AutoCommit;
use db_core::BufferSink;
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction};
use futures::{Stream, StreamExt};
use uuid::Uuid;

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

  let mut s1 = db_core::KeyScratch::with_capacity(49);
  let mut s2 = db_core::KeyScratch::with_capacity(49);
  <DocumentChangeKeyCodec as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
    &DocumentChangeKeyCodec,
    &start,
    &mut s1,
  );
  <DocumentChangeKeyCodec as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
    &DocumentChangeKeyCodec,
    &end,
    &mut s2,
  );
  (s1.buf, s2.buf)
}

/// Document change key: (doc_id, doc_type, change_hash)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentChangeKey {
  pub doc_id: Uuid,
  /// Snapshot vs incremental flag.
  pub doc_type: DocumentType,
  pub change_hash: [u8; 32],
}

pub type AutomergeEntry = Vec<u8>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentType {
  Incremental,
  Snapshot,
}

impl DocumentType {
  pub fn is_snapshot(self) -> bool {
    matches!(self, DocumentType::Snapshot)
  }
}

impl core::cmp::Ord for DocumentChangeKey {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    match self.doc_id.cmp(&other.doc_id) {
      Ordering::Equal => {
        let a = match self.doc_type {
          DocumentType::Incremental => 0u8,
          DocumentType::Snapshot => 1u8,
        };
        let b = match other.doc_type {
          DocumentType::Incremental => 0u8,
          DocumentType::Snapshot => 1u8,
        };
        match a.cmp(&b) {
          Ordering::Equal => self.change_hash.cmp(&other.change_hash),
          ord => ord,
        }
      }
      ord => ord,
    }
  }
}

impl core::cmp::PartialOrd for DocumentChangeKey {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

mod compaction;
mod hash;
mod reconstruction;
mod transaction;

use crate::automerge_btree::hash::{hash_hashes, hash_heads};

use self::compaction::{CompactionPolicy, ThresholdPolicy, run_compaction};
use self::reconstruction::reconstruct;
use self::transaction::AutomergeTransaction;

struct ReconstructionAccumulator {
  latest_snapshot: Option<Vec<u8>>,
  deltas_after_snapshot: Vec<Vec<u8>>,
}

impl ReconstructionAccumulator {
  fn new() -> Self {
    Self {
      latest_snapshot: None,
      deltas_after_snapshot: Vec::new(),
    }
  }

  fn apply(&mut self, doc_type: DocumentType, entry: Vec<u8>) {
    if doc_type.is_snapshot() {
      self.latest_snapshot = Some(entry);
      self.deltas_after_snapshot.clear();
    } else {
      self.deltas_after_snapshot.push(entry);
    }
  }

  fn finish(self) -> Vec<u8> {
    let state = self.latest_snapshot.unwrap_or_default();
    reconstruct(&state, &self.deltas_after_snapshot)
  }

  fn is_empty(&self) -> bool {
    self.latest_snapshot.is_none() && self.deltas_after_snapshot.is_empty()
  }
}

fn load_document(bytes: &[u8]) -> Result<AutoCommit, BTreeError> {
  AutoCommit::load(bytes).map_err(BTreeError::other)
}

/// Backend-agnostic Automerge wrapper. Parameterized over any `BTree` backend.
pub struct AutomergeBTree<B> {
  inner: B,
  policy: Box<dyn CompactionPolicy>,
}

impl<B> AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new(inner: B) -> Self {
    Self {
      inner,
      policy: Box::new(ThresholdPolicy::default()),
    }
  }

  pub fn with_compaction(
    inner: B,
    compaction_threshold_count: usize,
    compaction_threshold_bytes: usize,
  ) -> Self {
    Self {
      inner,
      policy: Box::new(ThresholdPolicy {
        threshold_count: compaction_threshold_count,
        threshold_bytes: compaction_threshold_bytes,
      }),
    }
  }

  pub fn new_with_policy(inner: B, policy: Box<dyn CompactionPolicy>) -> Self {
    Self { inner, policy }
  }

  /// Reconstruct the latest document state for `doc_id`.
  /// If the compaction policy triggers, perform inline compaction (atomic snapshot + cleanup).
  async fn get_document(&self, doc_id: Uuid) -> Option<Vec<u8>> {
    // scan across all type variants for this doc_id
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

    let stream = self.inner.range(start.clone()..=end.clone());
    futures::pin_mut!(stream);

    let mut accumulator = ReconstructionAccumulator::new();
    let mut delta_count: usize = 0;
    let mut delta_bytes: usize = 0;
    let mut has_entries = false;

    while let Some(item) = stream.next().await {
      let (k, entry) = match item {
        Ok(pair) => pair,
        Err(_) => continue,
      };
      has_entries = true;
      if k.doc_type.is_snapshot() {
        delta_count = 0;
        delta_bytes = 0;
      } else {
        delta_count += 1;
        delta_bytes += entry.len();
      }
      accumulator.apply(k.doc_type, entry);
    }

    if !has_entries {
      return None;
    }

    let state = accumulator.finish();

    if self.policy.should_compact(delta_count, delta_bytes) {
      match self.inner.transaction().await {
        Ok(tx) => {
          if let Err(err) =
            run_compaction(tx, start.clone(), end.clone(), doc_id, state.clone()).await
          {
            eprintln!("Automerge compaction failed: {err}");
          }
        }
        Err(err) => {
          eprintln!("Automerge compaction transaction failed: {err}");
        }
      }
    }

    Some(state)
  }
}

#[allow(clippy::needless_lifetimes)]
impl<B> BTreeExecutor<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();

    match self.get_document(doc_id).await {
      None => Ok(None),
      Some(bytes) => load_document(&bytes).map(Some),
    }
  }

  async fn insert<'a>(&'a mut self, key: Uuid, mut value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    let (internal_key, bytes) = if let Some(mut current_doc) = self.get(key).await? {
      let changes = value.get_changes(&current_doc.get_heads());
      let mut bytes = Vec::new();
      let mut change_hashes = Vec::with_capacity(changes.len());
      for c in &changes {
        bytes.extend_from_slice(c.raw_bytes());
        change_hashes.push(c.hash().0);
      }
      let change_hash = hash_hashes(change_hashes);

      let internal_key = DocumentChangeKey {
        doc_id: key,
        doc_type: DocumentType::Incremental,
        change_hash,
      };
      (internal_key, bytes)
    } else {
      let heads = value.get_heads();
      let change_hash = hash_heads(&heads);

      let internal_key = DocumentChangeKey {
        doc_id: key,
        doc_type: DocumentType::Snapshot,
        change_hash,
      };
      let bytes = value.save();
      (internal_key, bytes)
    };

    self.inner.insert(internal_key, bytes).await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();
    // capture previous state
    let prev = self.get_document(doc_id).await;

    // Prefer atomic removal via transaction on the underlying tree.
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

    match self.inner.transaction().await {
      Ok(mut tx) => {
        let keys_to_remove: alloc::vec::Vec<DocumentChangeKey> = {
          let mut collected: alloc::vec::Vec<DocumentChangeKey> = alloc::vec::Vec::new();
          let stream = tx.range(start.clone()..=end.clone());
          futures::pin_mut!(stream);
          while let Some(item) = stream.next().await {
            let (k, _v) = item?;
            collected.push(k);
          }
          collected
        };
        for k in keys_to_remove {
          tx.remove(&k).await?;
        }
        tx.commit().await?;
      }
      Err(_) => {
        // Fallback: non-atomic removal by iterating the main tree.
        let keys_to_remove: alloc::vec::Vec<DocumentChangeKey> = {
          let mut collected: alloc::vec::Vec<DocumentChangeKey> = alloc::vec::Vec::new();
          let stream = self.inner.range(start.clone()..=end.clone());
          futures::pin_mut!(stream);
          while let Some(item) = stream.next().await {
            let (k, _v) = item?;
            collected.push(k);
          }
          collected
        };
        for k in keys_to_remove {
          let _ = self.inner.remove(&k).await;
        }
      }
    }

    match prev {
      None => Ok(None),
      Some(bytes) => load_document(&bytes).map(Some),
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

      let inner_stream = self.inner.range(start_doc.clone()..=end_doc.clone());
      futures::pin_mut!(inner_stream);

      let mut current_doc: Option<Uuid> = None;
      let mut accumulator = ReconstructionAccumulator::new();

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;
        // filter by requested Uuid range
        let in_range = match range.start_bound() {
          std::ops::Bound::Included(lower) => &k.doc_id >= lower,
          std::ops::Bound::Excluded(lower) => &k.doc_id > lower,
          std::ops::Bound::Unbounded => true,
        } && match range.end_bound() {
          std::ops::Bound::Included(upper) => &k.doc_id <= upper,
          std::ops::Bound::Excluded(upper) => &k.doc_id < upper,
          std::ops::Bound::Unbounded => true,
        };

        if !in_range {
          continue;
        }

        if current_doc.is_none() {
          current_doc = Some(k.doc_id);
        } else if current_doc.as_ref().expect("doc id present") != &k.doc_id {
          if let Some(doc_id_to_yield) = current_doc.take() {
            let state = accumulator.finish();
            match load_document(&state) {
              Ok(doc) => yield Ok((doc_id_to_yield, doc)),
              Err(e) => yield Err(e),
            }
          }
          current_doc = Some(k.doc_id);
          accumulator = ReconstructionAccumulator::new();
        }

        accumulator.apply(k.doc_type, v.clone());
      }

      if let Some(doc_id) = current_doc {
        let state = accumulator.finish();
        match load_document(&state) {
          Ok(doc) => yield Ok((doc_id, doc)),
          Err(e) => yield Err(e),
        }
      }
    }
  }
}

impl<B> BTree<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeTransaction<B::Transaction>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    let inner_tx = self.inner.transaction().await?;
    Ok(AutomergeTransaction::new(inner_tx))
  }
}

/// Encoded-backed Automerge wrapper: stores encoded keys/values in `B` and
/// uses provided codecs to convert to/from `DocumentChangeKey`/`AutomergeEntry`.
#[allow(dead_code)]
pub struct AutomergeBTreeEncoded<B, KC = DocumentChangeKeyCodec, VC = VecBytesCodec>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
  KC: db_core::ValueCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: B,
  key_codec: KC,
  val_codec: VC,
  compaction_threshold_count: usize,
  compaction_threshold_bytes: usize,
}

impl<B> AutomergeBTreeEncoded<B>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
{
  pub fn new(inner: B) -> Self {
    Self::with_codecs_generic(inner, DocumentChangeKeyCodec, VecBytesCodec)
  }

  pub fn with_codecs_generic<KC, VC>(
    inner: B,
    key_codec: KC,
    val_codec: VC,
  ) -> AutomergeBTreeEncoded<B, KC, VC>
  where
    KC: db_core::KeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
    VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
  {
    AutomergeBTreeEncoded {
      inner,
      key_codec,
      val_codec,
      compaction_threshold_count: 100,
      compaction_threshold_bytes: 1024 * 1024,
    }
  }

  #[allow(dead_code)]
  fn decode_key<KC2: db_core::ValueCodec<DocumentChangeKey>>(
    data: &[u8],
  ) -> Result<DocumentChangeKey, db_core::DecodeError> {
    <KC2 as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(data)
  }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DocumentChangeKeyCodec;

impl db_core::ValueCodec<DocumentChangeKey> for DocumentChangeKeyCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    DocumentChangeKey: 'a;

  fn encode<'a>(value: &'a DocumentChangeKey) -> Self::Bytes<'a> {
    let mut out = Vec::with_capacity(49);
    out.extend_from_slice(value.doc_id.as_bytes());
    out.push(match value.doc_type {
      DocumentType::Incremental => 0u8,
      DocumentType::Snapshot => 1u8,
    });
    out.extend_from_slice(&value.change_hash);
    out
  }

  fn decode(data: &[u8]) -> DocumentChangeKey {
    if data.len() < 49 {
      panic!("invalid DocumentChangeKey encoding");
    }
    let id = Uuid::from_slice(&data[0..16]).expect("uuid decode");
    let doc_type = match data[16] {
      0 => DocumentType::Incremental,
      1 => DocumentType::Snapshot,
      _ => panic!("invalid doc_type"),
    };
    let mut change_hash = [0u8; 32];
    change_hash.copy_from_slice(&data[17..49]);
    DocumentChangeKey {
      doc_id: id,
      doc_type,
      change_hash,
    }
  }

  fn decode_checked(data: &[u8]) -> Result<DocumentChangeKey, db_core::DecodeError> {
    if data.len() < 49 {
      return Err(db_core::DecodeError::Truncated);
    }
    Ok(Self::decode(data))
  }
}

impl db_core::KeyCodec<DocumentChangeKey> for DocumentChangeKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    left.cmp(right)
  }
}

impl db_core::FastKeyCodec<DocumentChangeKey> for DocumentChangeKeyCodec {
  fn encode_into(&self, value: &DocumentChangeKey, scratch: &mut db_core::KeyScratch) {
    scratch.push_bytes(value.doc_id.as_bytes());
    let dt = match value.doc_type {
      DocumentType::Incremental => 0u8,
      DocumentType::Snapshot => 1u8,
    };
    scratch.push_bytes(&[dt]);
    scratch.push_bytes(&value.change_hash);
  }

  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    <Self as db_core::KeyCodec<DocumentChangeKey>>::compare(left, right)
  }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VecBytesCodec;

impl db_core::ValueCodec<AutomergeEntry> for VecBytesCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    AutomergeEntry: 'a;

  fn encode<'a>(value: &'a AutomergeEntry) -> Self::Bytes<'a> {
    value.clone()
  }

  fn decode(data: &[u8]) -> AutomergeEntry {
    data.to_vec()
  }

  fn decode_checked(data: &[u8]) -> Result<AutomergeEntry, db_core::DecodeError> {
    Ok(data.to_vec())
  }
}

#[allow(clippy::needless_lifetimes)]
impl<B, KC, VC> BTreeExecutor<Uuid, AutoCommit> for AutomergeBTreeEncoded<B, KC, VC>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::FastValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();

    // reconstruct by scanning encoded entries for this doc_id
    let (start_enc, end_enc) = uuid_prefix_range(doc_id);

    let stream = self.inner.range(start_enc.clone()..=end_enc.clone());
    futures::pin_mut!(stream);

    let mut accumulator = ReconstructionAccumulator::new();

    while let Some(item) = stream.next().await {
      let (k_enc, entry_enc) = match item {
        Ok(pair) => pair,
        Err(_) => continue,
      };
      let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) {
        Ok(kd) => kd,
        Err(_) => continue,
      };
      let value = <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&entry_enc)
        .unwrap_or_else(|_| entry_enc.clone());
      accumulator.apply(k.doc_type, value);
    }

    if accumulator.is_empty() {
      return Ok(None);
    }

    let state = accumulator.finish();
    load_document(&state).map(Some)
  }

  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    let mut doc = value;
    let change_hash = hash_heads(&doc.get_heads());
    let bytes = doc.save();

    let (start_enc, end_enc) = uuid_prefix_range(key);

    let found_identical = {
      let mut found = false;
      let stream = self.inner.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (_k_enc, v_enc) = item?;
        let existing = <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&v_enc)
          .unwrap_or_else(|_| v_enc.clone());
        if existing == bytes {
          found = true;
          break;
        }
      }
      found
    };
    if found_identical {
      return Ok(());
    }

    let internal_key = DocumentChangeKey {
      doc_id: key,
      doc_type: DocumentType::Snapshot,
      change_hash,
    };
    let mut key_scratch = db_core::KeyScratch::with_capacity(49);
    <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
      &self.key_codec,
      &internal_key,
      &mut key_scratch,
    );
    let mut val_enc: Vec<u8> = Vec::new();
    <VC as db_core::FastValueCodec<AutomergeEntry>>::encode_into(
      &self.val_codec,
      &bytes,
      &mut val_enc,
    );
    self.inner.insert(key_scratch.buf, val_enc).await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + Send + 'a,
  {
    let doc_id = *key.borrow();
    // capture previous state by scanning
    let prev = {
      // reuse get logic but without compaction
      let (start_enc, end_enc) = uuid_prefix_range(doc_id);

      let stream = self.inner.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(stream);
      let mut accumulator = ReconstructionAccumulator::new();
      let mut has_entries = false;

      while let Some(item) = stream.next().await {
        let (k_enc, entry_enc) = match item {
          Ok(pair) => pair,
          Err(_) => continue,
        };
        has_entries = true;
        let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) {
          Ok(kd) => kd,
          Err(_) => continue,
        };
        let value = <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&entry_enc)
          .unwrap_or_else(|_| entry_enc.clone());
        accumulator.apply(k.doc_type, value);
      }

      if !has_entries {
        None
      } else {
        Some(accumulator.finish())
      }
    };

    // non-atomic removal: collect keys and remove
    let (start_enc, _) = uuid_prefix_range(Uuid::from_u128(0));
    let (_, end_enc) = uuid_prefix_range(Uuid::from_u128(u128::MAX));

    let _keys_to_remove: alloc::vec::Vec<Vec<u8>> = alloc::vec::Vec::new();
    let keys_to_remove: alloc::vec::Vec<Vec<u8>> = {
      let mut collected: alloc::vec::Vec<Vec<u8>> = alloc::vec::Vec::new();
      let stream = self.inner.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (k_enc, _v_enc) = item?;
        collected.push(k_enc);
      }
      collected
    };
    for k in keys_to_remove {
      let _ = self.inner.remove(&k).await;
    }

    match prev {
      None => Ok(None),
      Some(bytes) => load_document(&bytes).map(Some),
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

      let inner_stream = self.inner.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(inner_stream);

      let mut current_doc: Option<Uuid> = None;
      let mut accumulator = ReconstructionAccumulator::new();

      while let Some(item) = inner_stream.next().await {
        let (k_enc, v_enc) = item?;
        let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) {
          Ok(kd) => kd,
          Err(_) => continue,
        };
        // filter by requested Uuid range
        let in_range = match range.start_bound() {
          std::ops::Bound::Included(lower) => &k.doc_id >= lower,
          std::ops::Bound::Excluded(lower) => &k.doc_id > lower,
          std::ops::Bound::Unbounded => true,
        } && match range.end_bound() {
          std::ops::Bound::Included(upper) => &k.doc_id <= upper,
          std::ops::Bound::Excluded(upper) => &k.doc_id < upper,
          std::ops::Bound::Unbounded => true,
        };

        if !in_range { continue; }

        if current_doc.is_none() {
          current_doc = Some(k.doc_id);
        } else if current_doc.as_ref().expect("doc id present") != &k.doc_id {
          if let Some(doc_id_to_yield) = current_doc.take() {
            let state = accumulator.finish();
            match load_document(&state) {
              Ok(doc) => yield Ok((doc_id_to_yield, doc)),
              Err(e) => yield Err(e),
            }
          }
          current_doc = Some(k.doc_id);
          accumulator = ReconstructionAccumulator::new();
        }

        let value = <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(&v_enc)
          .unwrap_or_else(|_| v_enc.clone());
        accumulator.apply(k.doc_type, value);
      }

      if let Some(doc_id) = current_doc {
        let state = accumulator.finish();
        match load_document(&state) {
          Ok(doc) => yield Ok((doc_id, doc)),
          Err(e) => yield Err(e),
        }
      }
    }
  }
}

impl<B, KC, VC> BTree<Uuid, AutoCommit> for AutomergeBTreeEncoded<B, KC, VC>
where
  B: BTree<Vec<u8>, Vec<u8>> + Clone + Send + Sync + 'static,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::FastValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction =
    crate::automerge_btree::transaction::AutomergeEncodedTransaction<B::Transaction, KC, VC>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    let inner_tx = self.inner.transaction().await?;
    Ok(
      crate::automerge_btree::transaction::AutomergeEncodedTransaction::new(
        inner_tx,
        self.key_codec.clone(),
        self.val_codec.clone(),
      ),
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures::{StreamExt, future};

  use automerge::transaction::Transactable;
  use db_core::{BTreeTransaction, block_on};
  use db_in_memory::InMemoryBTree;
  use std::fs;

  fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("db-automerge-{}.db", name));
    path
  }

  #[test]
  fn insert_and_get_latest() {
    let _ = fs::remove_file(tmp_path("basic"));

    let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let mut store = AutomergeBTree::new(underlying);

    let doc_id = Uuid::new_v4();
    let doc = AutoCommit::new();
    let mut expected = doc.clone();

    block_on(store.insert(doc_id, doc)).expect("insert");
    let mut got = block_on(store.get(&doc_id)).expect("get").expect("missing");
    let expected_bytes = expected.save();
    let got_bytes = got.save();
    assert_eq!(got_bytes, expected_bytes);
  }

  #[test]
  fn range_ordering() {
    let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let mut store = AutomergeBTree::new(underlying);

    let mut ids: Vec<Uuid> = Vec::new();
    for i in 0..3 {
      let id = Uuid::new_v4();
      ids.push(id);
      let mut doc = AutoCommit::new();
      doc
        .put(&automerge::ROOT, "v", format!("v{}", i))
        .expect("put");
      block_on(store.insert(id, doc)).expect("insert");
      std::thread::sleep(std::time::Duration::from_millis(1));
    }

    let start = &Uuid::nil();
    let end = &Uuid::from_u128(u128::MAX);

    let s = store.range(start..=end);
    let items = block_on(async move {
      let mut collected = Vec::new();
      futures::pin_mut!(s);
      while let Some(item) = s.next().await {
        let (k, v) = item.expect("range failed");
        collected.push((k, v));
      }
      collected
    });

    assert_eq!(items.len(), 3);
  }

  #[test]
  fn compaction_with_concurrent_writer() {
    block_on(async {
      let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
      let automerge = AutomergeBTree::with_compaction(underlying.clone(), 1, 1);
      let doc_id = Uuid::new_v4();

      let mut base_doc = AutoCommit::new();
      base_doc
        .put(&automerge::ROOT, "message", "hello")
        .expect("put base value");
      let base_changes = base_doc.get_changes(&[]);
      let mut base_delta = Vec::new();
      for change in &base_changes {
        base_delta.extend_from_slice(change.raw_bytes());
      }

      let delta_key = DocumentChangeKey {
        doc_id,
        doc_type: DocumentType::Incremental,
        change_hash: super::hash::hash_hashes(base_changes.iter().map(|c| c.hash().0)),
      };

      // insert initial delta directly into underlying storage
      {
        let mut tx = underlying.transaction().await.expect("start tx");
        tx.insert(delta_key.clone(), base_delta.clone())
          .await
          .expect("insert delta");
        tx.commit().await.expect("commit tx");
      }

      let reader = automerge;
      let writer_store = underlying.clone();
      let mut writer_doc = base_doc.clone();
      writer_doc
        .put(&automerge::ROOT, "tail", "!")
        .expect("put writer value");
      let writer_changes = writer_doc.get_changes(&base_doc.get_heads());
      let mut writer_delta = Vec::new();
      for change in &writer_changes {
        writer_delta.extend_from_slice(change.raw_bytes());
      }
      let writer_key = DocumentChangeKey {
        doc_id,
        doc_type: DocumentType::Incremental,
        change_hash: super::hash::hash_hashes(writer_changes.iter().map(|c| c.hash().0)),
      };
      let writer_key_clone = writer_key.clone();
      let writer_delta_clone = writer_delta.clone();

      let expected_read_state = base_delta.clone();

      let read_future = reader.get_document(doc_id);
      let write_future = async move {
        let mut tx = writer_store.transaction().await.expect("start writer tx");
        tx.insert(writer_key_clone, writer_delta_clone)
          .await
          .expect("insert writer delta");
        tx.commit().await.expect("commit writer tx");
      };

      let (read_state, ()) = future::join(read_future, write_future).await;
      assert_eq!(read_state.unwrap(), expected_read_state.clone());

      let final_store = AutomergeBTree::with_compaction(underlying.clone(), 1, 1);
      let final_state = final_store
        .get_document(doc_id)
        .await
        .expect("final get_document");
      assert_eq!(final_state, expected_read_state);

      let actual_writer_value = underlying
        .get(&writer_key)
        .await
        .expect("get writer key failed")
        .expect("writer key missing");
      assert_eq!(actual_writer_value, writer_delta);
    });
  }

  #[test]
  fn uuid_prefix_vs_short_prefix() {
    let _ = fs::remove_file(tmp_path("prefix_test"));

    let underlying = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();

    let id = Uuid::new_v4();
    let key = DocumentChangeKey {
      doc_id: id,
      doc_type: DocumentType::Snapshot,
      change_hash: [1u8; 32],
    };
    let mut key_scratch = db_core::KeyScratch::with_capacity(49);
    <DocumentChangeKeyCodec as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
      &DocumentChangeKeyCodec,
      &key,
      &mut key_scratch,
    );
    let key_enc = key_scratch.buf;

    // insert encoded key into underlying storage via a transaction
    block_on(async {
      let mut tx = underlying.transaction().await.expect("start tx");
      tx.insert(key_enc.clone(), b"v".to_vec())
        .await
        .expect("insert");
      tx.commit().await.expect("commit");
    });

    // short (16-byte) prefix bounds - should NOT match encoded entries
    let short_start = id.as_bytes().to_vec();
    let short_end = id.as_bytes().to_vec();
    let short_count = block_on(async {
      let s = underlying.range(short_start.clone()..=short_end.clone());
      futures::pin_mut!(s);
      let mut cnt = 0usize;
      while let Some(item) = s.next().await {
        let (_k, _v) = item.expect("range");
        cnt += 1;
      }
      cnt
    });
    assert_eq!(short_count, 0);

    // full-encoded bounds should include the entry
    let (start_enc, end_enc) = uuid_prefix_range(id);
    let full_count = block_on(async {
      let s = underlying.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(s);
      let mut cnt = 0usize;
      while let Some(item) = s.next().await {
        let (_k, _v) = item.expect("range");
        cnt += 1;
      }
      cnt
    });
    assert_eq!(full_count, 1);
  }
}
