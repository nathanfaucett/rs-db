use core::fmt;
use core::future;
use core::ops::{Bound, RangeBounds};
use db_core::{BTreeError, BTreeResult, NamedTreeProvider, NamedTreeTransaction, block_on};
use db_engine::{EngineKey, EngineRow};
use futures::Stream;
use js_sys::{Function, Promise, Reflect};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(typescript_custom_section)]
const STORE_ADAPTER_TS: &str = r#"
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

export interface StoreAdapter {
  get(tree: string, key: EngineKey): Promise<EngineValue[] | null | undefined>;
  insert(tree: string, key: EngineKey, value: EngineValue[]): Promise<void>;
  remove(tree: string, key: EngineKey): Promise<EngineValue[] | null | undefined>;
  range(tree: string, range: StoreAdapterRangeRequest): Promise<StoreAdapterEntry[]>;
  commit(ops: StoreAdapterCommitOp[]): Promise<void>;
  rollback(): Promise<void>;
}
"#;

#[wasm_bindgen]
extern "C" {
  #[wasm_bindgen(typescript_type = "StoreAdapter")]
  pub type StoreAdapter;
}

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

fn load_function(adapter: &JsValue, name: &str) -> Result<Function, String> {
  let key = JsValue::from_str(name);
  let value =
    Reflect::get(adapter, &key).map_err(|_| format!("missing store adapter method: {name}"))?;
  value
    .dyn_into::<Function>()
    .map_err(|_| format!("store adapter property is not a function: {name}"))
}

#[derive(Clone)]
struct CallbackRegistry {
  get: Function,
  insert: Function,
  remove: Function,
  range: Function,
  commit: Function,
  rollback: Function,
}

#[derive(Clone)]
pub struct StoreAdapterCallbacks {
  callbacks: CallbackRegistry,
}

impl TryFrom<JsValue> for StoreAdapterCallbacks {
  type Error = String;

  fn try_from(value: JsValue) -> Result<Self, Self::Error> {
    Ok(Self {
      callbacks: CallbackRegistry {
        get: load_function(&value, "get")?,
        insert: load_function(&value, "insert")?,
        remove: load_function(&value, "remove")?,
        range: load_function(&value, "range")?,
        commit: load_function(&value, "commit")?,
        rollback: load_function(&value, "rollback")?,
      },
    })
  }
}

#[derive(Clone)]
pub struct StoreAdapterTree {
  callbacks: CallbackRegistry,
  tree: String,
}

pub struct StoreAdapterTransaction {
  callbacks: CallbackRegistry,
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
  fn callback_get(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<EngineRow>> {
    let tree_js = JsValue::from_str(tree);
    let key_js = to_js(key)?;
    let value = call2(&self.callbacks.get, &tree_js, &key_js)?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  fn callback_insert(&self, tree: &str, key: &EngineKey, row: &EngineRow) -> BTreeResult<()> {
    let tree_js = JsValue::from_str(tree);
    let key_js = to_js(key)?;
    let row_js = to_js(row)?;
    let _ = call3(&self.callbacks.insert, &tree_js, &key_js, &row_js)?;
    Ok(())
  }

  fn callback_remove(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<EngineRow>> {
    let tree_js = JsValue::from_str(tree);
    let key_js = to_js(key)?;
    let value = call2(&self.callbacks.remove, &tree_js, &key_js)?;
    if value.is_null() || value.is_undefined() {
      return Ok(None);
    }
    from_js(value)
  }

  fn callback_range<R>(&self, tree: &str, range: &R) -> BTreeResult<Vec<(EngineKey, EngineRow)>>
  where
    R: RangeBounds<EngineKey>,
  {
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
    let value = call2(&self.callbacks.range, &tree_js, &req_js)?;
    let rows: Vec<KeyValuePair> = from_js(value)?;
    Ok(
      rows
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect(),
    )
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
      tree: name.to_string(),
    };
    future::ready(Ok(tree))
  }

  fn begin_transaction(
    &self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + Send + '_ {
    let tx = StoreAdapterTransaction {
      callbacks: self.callbacks.clone(),
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
    let out = to_js(&self.pending_ops).and_then(|ops_js| {
      let _ = call1(&self.callbacks.commit, &ops_js)?;
      Ok(())
    });

    future::ready(out)
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    let out = call0(&self.callbacks.rollback).map(|_| ());
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
      pending_ops: Vec::new(),
    };
    future::ready(Ok(tx))
  }
}
