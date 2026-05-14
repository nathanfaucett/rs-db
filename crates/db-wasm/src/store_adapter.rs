use core::fmt;
use core::future;
use core::ops::{Bound, RangeBounds};
use db_core::{BTreeError, BTreeResult, NamedTreeProvider, NamedTreeTransaction, block_on};
use db_engine::{EngineKey, EngineRow, PrimaryKey};
use db_types::persistence::decode_index_schema_row;
use futures::Stream;
use js_sys::{Function, Promise, Reflect};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(typescript_custom_section)]
const STORE_ADAPTER_TS: &str = r#"
export type PrimaryKey = [
  number, number, number, number,
  number, number, number, number,
  number, number, number, number,
  number, number, number, number,
];

export interface PrimaryKeyRangeRequest {
  start?: PrimaryKey;
  startInclusive: boolean;
  end?: PrimaryKey;
  endInclusive: boolean;
}

export type PrimaryKeyEntry = {
  primaryKey: PrimaryKey;
  row: EngineValue[];
};

export type IndexEntry = {
  indexKey: EngineKey;
  rowPrimaryKey: PrimaryKey;
};

export interface PrimaryKeyStore {
  get(table: string, primaryKey: PrimaryKey): Promise<EngineValue[] | null | undefined>;
  put(table: string, primaryKey: PrimaryKey, row: EngineValue[]): Promise<void>;
  delete(table: string, primaryKey: PrimaryKey): Promise<EngineValue[] | null | undefined>;
  range(table: string, range: PrimaryKeyRangeRequest): Promise<PrimaryKeyEntry[]>;
}

export interface IndexStore {
  add(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void>;
  remove(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void>;
  range(index: string): Promise<IndexEntry[]>;
}

export interface StoreAdapterRangeRequest {
  start?: EngineKey;
  startInclusive: boolean;
  end?: EngineKey;
  endInclusive: boolean;
}

export type StoreAdapterEntry = {
  key: EngineKey;
  value: EngineValue[];
};

export type StoreAdapterCommitOp =
  | { op: "insert"; tree: string; key: EngineKey; value: EngineValue[] }
  | { op: "remove"; tree: string; key: EngineKey };

export type EngineRow = EngineValue[];

export interface DatabaseEngineOptions {
  primaryKeyStore?: PrimaryKeyStore;
  indexStore?: IndexStore;
  get?(tree: string, key: EngineKey): Promise<EngineValue[] | null | undefined>;
  insert?(tree: string, key: EngineKey, value: EngineValue[]): Promise<void>;
  remove?(tree: string, key: EngineKey): Promise<EngineValue[] | null | undefined>;
  range?(tree: string, range: StoreAdapterRangeRequest): Promise<StoreAdapterEntry[]>;
  commit?(ops: StoreAdapterCommitOp[]): Promise<void>;
  rollback?(): Promise<void>;
}

// Backward-compatible alias.
export type StoreAdapter = DatabaseEngineOptions;
"#;

#[wasm_bindgen]
extern "C" {
  #[wasm_bindgen(typescript_type = "DatabaseEngineOptions")]
  pub type DatabaseEngineOptions;
}

pub type StoreAdapter = DatabaseEngineOptions;

#[derive(Debug)]
struct StoreAdapterError(String);

impl fmt::Display for StoreAdapterError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::error::Error for StoreAdapterError {}

fn js_error(value: JsValue) -> BTreeError {
  let message = value
    .as_string()
    .unwrap_or_else(|| "store adapter callback error".to_string());
  BTreeError::other(StoreAdapterError(message))
}

fn serde_error(message: impl fmt::Display) -> BTreeError {
  BTreeError::other(StoreAdapterError(message.to_string()))
}

fn to_js<T: Serialize>(value: &T) -> BTreeResult<JsValue> {
  serde_wasm_bindgen::to_value(value).map_err(serde_error)
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> BTreeResult<T> {
  serde_wasm_bindgen::from_value(value).map_err(serde_error)
}

fn resolve_js(value: JsValue) -> BTreeResult<JsValue> {
  let promise = Promise::resolve(&value);
  block_on(JsFuture::from(promise)).map_err(js_error)
}

fn call0(function: &Function) -> BTreeResult<JsValue> {
  let value = function.call0(&JsValue::NULL).map_err(js_error)?;
  resolve_js(value)
}

fn call1(function: &Function, arg0: &JsValue) -> BTreeResult<JsValue> {
  let value = function.call1(&JsValue::NULL, arg0).map_err(js_error)?;
  resolve_js(value)
}

fn call2(function: &Function, arg0: &JsValue, arg1: &JsValue) -> BTreeResult<JsValue> {
  let value = function
    .call2(&JsValue::NULL, arg0, arg1)
    .map_err(js_error)?;
  resolve_js(value)
}

fn call3(
  function: &Function,
  arg0: &JsValue,
  arg1: &JsValue,
  arg2: &JsValue,
) -> BTreeResult<JsValue> {
  let value = function
    .call3(&JsValue::NULL, arg0, arg1, arg2)
    .map_err(js_error)?;
  resolve_js(value)
}

fn load_optional_function(adapter: &JsValue, name: &str) -> Result<Option<Function>, String> {
  let key = JsValue::from_str(name);
  let value =
    Reflect::get(adapter, &key).map_err(|_| format!("invalid adapter property: {name}"))?;
  if value.is_null() || value.is_undefined() {
    return Ok(None);
  }
  value
    .dyn_into::<Function>()
    .map(Some)
    .map_err(|_| format!("adapter property is not a function: {name}"))
}

#[derive(Clone)]
struct CallbackRegistry {
  get: Option<Function>,
  insert: Option<Function>,
  remove: Option<Function>,
  range: Option<Function>,
  commit: Option<Function>,
  rollback: Option<Function>,
  pk_get: Option<Function>,
  pk_put: Option<Function>,
  pk_delete: Option<Function>,
  pk_range: Option<Function>,
  idx_add: Option<Function>,
  idx_remove: Option<Function>,
  idx_range: Option<Function>,
}

#[derive(Clone)]
pub struct StoreAdapterCallbacks {
  callbacks: CallbackRegistry,
  index_key_widths: Arc<Mutex<HashMap<String, usize>>>,
}

impl TryFrom<JsValue> for StoreAdapterCallbacks {
  type Error = String;

  fn try_from(value: JsValue) -> Result<Self, Self::Error> {
    let primary_key_store = Reflect::get(&value, &JsValue::from_str("primaryKeyStore")).ok();
    let index_store = Reflect::get(&value, &JsValue::from_str("indexStore")).ok();

    let (pk_get, pk_put, pk_delete, pk_range) = if let Some(store) = primary_key_store {
      (
        load_optional_function(&store, "get")?,
        load_optional_function(&store, "put")?,
        load_optional_function(&store, "delete")?,
        load_optional_function(&store, "range")?,
      )
    } else {
      (None, None, None, None)
    };

    let (idx_add, idx_remove, idx_range) = if let Some(store) = index_store {
      (
        load_optional_function(&store, "add")?,
        load_optional_function(&store, "remove")?,
        load_optional_function(&store, "range")?,
      )
    } else {
      (None, None, None)
    };

    let callbacks = CallbackRegistry {
      get: load_optional_function(&value, "get")?,
      insert: load_optional_function(&value, "insert")?,
      remove: load_optional_function(&value, "remove")?,
      range: load_optional_function(&value, "range")?,
      commit: load_optional_function(&value, "commit")?,
      rollback: load_optional_function(&value, "rollback")?,
      pk_get,
      pk_put,
      pk_delete,
      pk_range,
      idx_add,
      idx_remove,
      idx_range,
    };

    if callbacks.get.is_none() && callbacks.pk_get.is_none() {
      return Err(
        "store adapter must provide either get(tree,key) or primaryKeyStore.get(table,primaryKey)"
          .into(),
      );
    }

    Ok(Self {
      callbacks,
      index_key_widths: Arc::new(Mutex::new(HashMap::new())),
    })
  }
}

#[derive(Clone)]
pub struct StoreAdapterTree {
  callbacks: CallbackRegistry,
  index_key_widths: Arc<Mutex<HashMap<String, usize>>>,
  tree: String,
}

pub struct StoreAdapterTransaction {
  callbacks: CallbackRegistry,
  index_key_widths: Arc<Mutex<HashMap<String, usize>>>,
  pending_ops: Vec<PendingOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum PendingOp {
  Insert {
    tree: String,
    key: EngineKey,
    value: EngineRow,
  },
  Remove {
    tree: String,
    key: EngineKey,
  },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyValuePair {
  key: EngineKey,
  value: EngineRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RangeRequest {
  start: Option<EngineKey>,
  start_inclusive: bool,
  end: Option<EngineKey>,
  end_inclusive: bool,
}

fn key_in_range<R>(key: &EngineKey, range: &R) -> bool
where
  R: RangeBounds<EngineKey>,
{
  let start_ok = match range.start_bound() {
    Bound::Included(start) => key >= start,
    Bound::Excluded(start) => key > start,
    Bound::Unbounded => true,
  };

  let end_ok = match range.end_bound() {
    Bound::Included(end) => key <= end,
    Bound::Excluded(end) => key < end,
    Bound::Unbounded => true,
  };

  start_ok && end_ok
}

impl StoreAdapterCallbacks {
  fn row_table_name<'a>(&self, tree: &'a str) -> Option<&'a str> {
    tree.strip_prefix("t:")
  }

  fn index_name<'a>(&self, tree: &'a str) -> Option<&'a str> {
    tree.strip_prefix("i:")
  }

  fn primary_key_from_engine_key(&self, key: &EngineKey) -> BTreeResult<PrimaryKey> {
    PrimaryKey::from_engine_key(key)
      .ok_or_else(|| serde_error("row primary key must be UUID scalar"))
  }

  fn split_composite_index_key(
    &self,
    index_name: &str,
    composite: &EngineKey,
  ) -> BTreeResult<(EngineKey, PrimaryKey)> {
    let width = self
      .index_key_widths
      .lock()
      .map_err(|_| serde_error("index schema cache lock poisoned"))?
      .get(index_name)
      .copied()
      .ok_or_else(|| serde_error(format!("missing index schema width for {index_name}")))?;

    let values = composite.values();
    if values.len() < width + 1 {
      return Err(serde_error("malformed composite index key"));
    }

    let index_key = EngineKey::from_values(values[..width].to_vec());
    let row_pk_key = EngineKey::from_values(values[width..].to_vec());
    let row_pk = PrimaryKey::from_engine_key(&row_pk_key)
      .ok_or_else(|| serde_error("index entry row primary key must be UUID scalar"))?;
    Ok((index_key, row_pk))
  }

  fn compose_composite_index_key(&self, index_key: &EngineKey, row_pk: PrimaryKey) -> EngineKey {
    let mut values = index_key.values().to_vec();
    values.push(db_engine::EngineValue::Uuid(*row_pk.as_bytes()));
    EngineKey::from_values(values)
  }

  fn maybe_update_index_schema_widths(&self, tree: &str, key: &EngineKey, row: &EngineRow) {
    if tree != "sys:index_schemas" {
      return;
    }
    if let (EngineKey::Scalar(db_engine::EngineValue::Text(index_name)), Ok(schema)) =
      (key, decode_index_schema_row(row))
      && let Ok(mut guard) = self.index_key_widths.lock()
    {
      guard.insert(index_name.clone(), schema.column_indices.len());
    }
  }

  fn maybe_remove_index_schema_width(&self, tree: &str, key: &EngineKey) {
    if tree != "sys:index_schemas" {
      return;
    }
    if let EngineKey::Scalar(db_engine::EngineValue::Text(index_name)) = key
      && let Ok(mut guard) = self.index_key_widths.lock()
    {
      let _ = guard.remove(index_name);
    }
  }

  fn callback_get(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<EngineRow>> {
    if let (Some(table), Some(callback)) = (self.row_table_name(tree), &self.callbacks.pk_get) {
      let table_js = JsValue::from_str(table);
      let pk = self.primary_key_from_engine_key(key)?;
      let pk_js = to_js(&pk)?;
      let value = call2(callback, &table_js, &pk_js)?;
      if value.is_null() || value.is_undefined() {
        return Ok(None);
      }
      return from_js(value);
    }

    let tree_js = JsValue::from_str(tree);
    let key_js = to_js(key)?;
    let get = self
      .callbacks
      .get
      .as_ref()
      .ok_or_else(|| serde_error("missing get callback for legacy tree adapter"))?;
    let value = call2(get, &tree_js, &key_js)?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  fn callback_insert(&self, tree: &str, key: &EngineKey, row: &EngineRow) -> BTreeResult<()> {
    if let (Some(table), Some(callback)) = (self.row_table_name(tree), &self.callbacks.pk_put) {
      let table_js = JsValue::from_str(table);
      let pk = self.primary_key_from_engine_key(key)?;
      let pk_js = to_js(&pk)?;
      let row_js = to_js(row)?;
      let _ = call3(callback, &table_js, &pk_js, &row_js)?;
      self.maybe_update_index_schema_widths(tree, key, row);
      return Ok(());
    }

    if let (Some(index_name), Some(callback)) = (self.index_name(tree), &self.callbacks.idx_add) {
      let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
      let index_js = JsValue::from_str(index_name);
      let index_key_js = to_js(&index_key)?;
      let row_pk_js = to_js(&row_pk)?;
      let _ = call3(callback, &index_js, &index_key_js, &row_pk_js)?;
      return Ok(());
    }

    let tree_js = JsValue::from_str(tree);
    let key_js = to_js(key)?;
    let row_js = to_js(row)?;
    let insert = self
      .callbacks
      .insert
      .as_ref()
      .ok_or_else(|| serde_error("missing insert callback for legacy tree adapter"))?;
    let _ = call3(insert, &tree_js, &key_js, &row_js)?;
    self.maybe_update_index_schema_widths(tree, key, row);
    Ok(())
  }

  fn callback_remove(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<EngineRow>> {
    if let (Some(table), Some(callback)) = (self.row_table_name(tree), &self.callbacks.pk_delete) {
      let table_js = JsValue::from_str(table);
      let pk = self.primary_key_from_engine_key(key)?;
      let pk_js = to_js(&pk)?;
      let value = call2(callback, &table_js, &pk_js)?;
      if value.is_null() || value.is_undefined() {
        return Ok(None);
      }
      return from_js(value);
    }

    if let (Some(index_name), Some(callback)) = (self.index_name(tree), &self.callbacks.idx_remove)
    {
      let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
      let index_js = JsValue::from_str(index_name);
      let index_key_js = to_js(&index_key)?;
      let row_pk_js = to_js(&row_pk)?;
      let _ = call3(callback, &index_js, &index_key_js, &row_pk_js)?;
      return Ok(None);
    }

    let tree_js = JsValue::from_str(tree);
    let key_js = to_js(key)?;
    let remove = self
      .callbacks
      .remove
      .as_ref()
      .ok_or_else(|| serde_error("missing remove callback for legacy tree adapter"))?;
    let value = call2(remove, &tree_js, &key_js)?;
    if value.is_null() || value.is_undefined() {
      self.maybe_remove_index_schema_width(tree, key);
      return Ok(None);
    }
    self.maybe_remove_index_schema_width(tree, key);
    from_js(value)
  }

  fn callback_range<R>(&self, tree: &str, range: &R) -> BTreeResult<Vec<(EngineKey, EngineRow)>>
  where
    R: RangeBounds<EngineKey>,
  {
    if let (Some(table), Some(callback)) = (self.row_table_name(tree), &self.callbacks.pk_range) {
      let request = RangeRequest {
        start: match range.start_bound() {
          Bound::Included(key) | Bound::Excluded(key) => {
            let pk = self.primary_key_from_engine_key(key)?;
            Some(pk.into_engine_key())
          }
          Bound::Unbounded => None,
        },
        start_inclusive: matches!(range.start_bound(), Bound::Included(_)),
        end: match range.end_bound() {
          Bound::Included(key) | Bound::Excluded(key) => {
            let pk = self.primary_key_from_engine_key(key)?;
            Some(pk.into_engine_key())
          }
          Bound::Unbounded => None,
        },
        end_inclusive: matches!(range.end_bound(), Bound::Included(_)),
      };

      #[derive(Deserialize)]
      struct PrimaryKeyEntry {
        primary_key: PrimaryKey,
        row: EngineRow,
      }

      let table_js = JsValue::from_str(table);
      let req_js = to_js(&request)?;
      let value = call2(callback, &table_js, &req_js)?;
      let rows: Vec<PrimaryKeyEntry> = from_js(value)?;
      return Ok(
        rows
          .into_iter()
          .map(|entry| (entry.primary_key.into_engine_key(), entry.row))
          .collect(),
      );
    }

    if let (Some(index_name), Some(callback)) = (self.index_name(tree), &self.callbacks.idx_range) {
      #[derive(Deserialize)]
      struct IndexEntry {
        index_key: EngineKey,
        row_primary_key: PrimaryKey,
      }

      let index_js = JsValue::from_str(index_name);
      let value = call1(callback, &index_js)?;
      let entries: Vec<IndexEntry> = from_js(value)?;
      return Ok(
        entries
          .into_iter()
          .map(|entry| {
            (
              self.compose_composite_index_key(&entry.index_key, entry.row_primary_key),
              Vec::new(),
            )
          })
          .collect(),
      );
    }

    let request = RangeRequest {
      start: match range.start_bound() {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
        Bound::Unbounded => None,
      },
      start_inclusive: matches!(range.start_bound(), Bound::Included(_)),
      end: match range.end_bound() {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
        Bound::Unbounded => None,
      },
      end_inclusive: matches!(range.end_bound(), Bound::Included(_)),
    };

    let tree_js = JsValue::from_str(tree);
    let req_js = to_js(&request)?;
    let range_fn = self
      .callbacks
      .range
      .as_ref()
      .ok_or_else(|| serde_error("missing range callback for legacy tree adapter"))?;
    let value = call2(range_fn, &tree_js, &req_js)?;
    let rows: Vec<KeyValuePair> = from_js(value)?;
    let out: Vec<(EngineKey, EngineRow)> = rows
      .into_iter()
      .map(|entry| (entry.key, entry.value))
      .collect();

    if tree == "sys:index_schemas" {
      for (key, row) in &out {
        self.maybe_update_index_schema_widths(tree, key, row);
      }
    }

    Ok(out)
  }
}

impl NamedTreeProvider<EngineKey, EngineRow> for StoreAdapterCallbacks {
  type Tree = StoreAdapterTree;
  type Transaction = StoreAdapterTransaction;

  fn get_tree(
    &self,
    name: &str,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Tree>> + Send + '_ {
    let tree = StoreAdapterTree {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
      tree: name.to_string(),
    };
    future::ready(Ok(tree))
  }

  fn begin_transaction(
    &self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + Send + '_ {
    let tx = StoreAdapterTransaction {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
      pending_ops: Vec::new(),
    };
    future::ready(Ok(tx))
  }
}

impl StoreAdapterTransaction {
  fn pending_lookup(&self, tree: &str, key: &EngineKey) -> Option<Option<EngineRow>> {
    for op in self.pending_ops.iter().rev() {
      match op {
        PendingOp::Insert {
          tree: op_tree,
          key: op_key,
          value,
        } if op_tree == tree && op_key == key => return Some(Some(value.clone())),
        PendingOp::Remove {
          tree: op_tree,
          key: op_key,
        } if op_tree == tree && op_key == key => return Some(None),
        _ => {}
      }
    }

    None
  }
}

impl NamedTreeTransaction<EngineKey, EngineRow> for StoreAdapterTransaction {
  fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> impl core::future::Future<Output = BTreeResult<Option<EngineRow>>> + Send + 'a
  where
    EngineKey: Ord,
  {
    let value = if let Some(value) = self.pending_lookup(tree, key) {
      Ok(value)
    } else {
      let adapter = StoreAdapterCallbacks {
        callbacks: self.callbacks.clone(),
        index_key_widths: self.index_key_widths.clone(),
      };
      adapter.callback_get(tree, key)
    };

    future::ready(value)
  }

  fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: EngineRow,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + Send + 'a
  where
    EngineKey: Ord,
  {
    self.pending_ops.push(PendingOp::Insert {
      tree: tree.to_string(),
      key,
      value,
    });
    future::ready(Ok(()))
  }

  fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> impl core::future::Future<Output = BTreeResult<Option<EngineRow>>> + Send + 'a
  where
    EngineKey: Ord,
  {
    let existing = if let Some(value) = self.pending_lookup(tree, key) {
      Ok(value)
    } else {
      let adapter = StoreAdapterCallbacks {
        callbacks: self.callbacks.clone(),
        index_key_widths: self.index_key_widths.clone(),
      };
      adapter.callback_get(tree, key)
    };

    let out = existing.inspect(|_| {
      self.pending_ops.push(PendingOp::Remove {
        tree: tree.to_string(),
        key: key.clone(),
      });
    });

    future::ready(out)
  }

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, EngineRow)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    let adapter = StoreAdapterCallbacks {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
    };
    let rows = adapter.callback_range(tree, &range).map(|callback_rows| {
      let mut map = std::collections::BTreeMap::new();

      for (key, value) in callback_rows {
        if key_in_range(&key, &range) {
          map.insert(key, value);
        }
      }

      for op in &self.pending_ops {
        match op {
          PendingOp::Insert {
            tree: op_tree,
            key,
            value,
          } if op_tree == tree && key_in_range(key, &range) => {
            map.insert(key.clone(), value.clone());
          }
          PendingOp::Remove { tree: op_tree, key }
            if op_tree == tree && key_in_range(key, &range) =>
          {
            let _ = map.remove(key);
          }
          _ => {}
        }
      }

      map.into_iter().map(Ok).collect::<Vec<_>>()
    });

    futures::stream::iter(rows.unwrap_or_else(|err| vec![Err(err)]))
  }

  fn commit(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    let adapter = StoreAdapterCallbacks {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
    };

    let out = if let Some(commit) = &self.callbacks.commit {
      to_js(&self.pending_ops).and_then(|ops_js| {
        let _ = call1(commit, &ops_js)?;
        Ok(())
      })
    } else {
      let mut result = Ok(());
      for op in &self.pending_ops {
        result = result.and_then(|_| match op {
          PendingOp::Insert { tree, key, value } => adapter.callback_insert(tree, key, value),
          PendingOp::Remove { tree, key } => adapter.callback_remove(tree, key).map(|_| ()),
        });
        if result.is_err() {
          break;
        }
      }
      result
    };

    future::ready(out)
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    let out = if let Some(rollback) = &self.callbacks.rollback {
      call0(rollback).map(|_| ())
    } else {
      Ok(())
    };
    future::ready(out)
  }
}

impl db_core::BTreeExecutor<EngineKey, EngineRow> for StoreAdapterTree {
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<EngineRow>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    let adapter = StoreAdapterCallbacks {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
    };
    future::ready(adapter.callback_get(&self.tree, key.borrow()))
  }

  fn insert<'a>(
    &'a mut self,
    key: EngineKey,
    value: EngineRow,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + Send + 'a
  where
    EngineKey: Ord,
  {
    let adapter = StoreAdapterCallbacks {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
    };
    future::ready(adapter.callback_insert(&self.tree, &key, &value))
  }

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<EngineRow>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    let adapter = StoreAdapterCallbacks {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
    };
    future::ready(adapter.callback_remove(&self.tree, key.borrow()))
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, EngineRow)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    let adapter = StoreAdapterCallbacks {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
    };
    let rows = adapter
      .callback_range(&self.tree, &range)
      .map(|pairs| {
        pairs
          .into_iter()
          .filter(|(key, _)| key_in_range(key, &range))
          .map(Ok)
          .collect::<Vec<_>>()
      })
      .unwrap_or_else(|err| vec![Err(err)]);
    futures::stream::iter(rows)
  }
}

impl db_core::BTreeExecutor<EngineKey, EngineRow> for StoreAdapterTransaction {
  fn get<'a, Q>(
    &'a self,
    _key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<EngineRow>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    future::ready(Err(BTreeError::UnsupportedOperation))
  }

  fn insert<'a>(
    &'a mut self,
    _key: EngineKey,
    _value: EngineRow,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + Send + 'a
  where
    EngineKey: Ord,
  {
    future::ready(Err(BTreeError::UnsupportedOperation))
  }

  fn remove<'a, Q>(
    &'a mut self,
    _key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<EngineRow>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    future::ready(Err(BTreeError::UnsupportedOperation))
  }

  fn range<'a, R>(
    &'a self,
    _range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, EngineRow)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    futures::stream::iter(Vec::new())
  }
}

impl db_core::BTreeTransaction<EngineKey, EngineRow> for StoreAdapterTransaction {
  fn commit(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    <Self as NamedTreeTransaction<EngineKey, EngineRow>>::commit(self)
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    <Self as NamedTreeTransaction<EngineKey, EngineRow>>::rollback(self)
  }
}

impl db_core::BTree<EngineKey, EngineRow> for StoreAdapterTree {
  type Transaction = StoreAdapterTransaction;

  fn transaction<'a>(
    &'a self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + Send + 'a {
    let tx = StoreAdapterTransaction {
      callbacks: self.callbacks.clone(),
      index_key_widths: self.index_key_widths.clone(),
      pending_ops: Vec::new(),
    };
    future::ready(Ok(tx))
  }
}
