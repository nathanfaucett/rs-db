use core::fmt;
use core::future;
use core::ops::{Bound, RangeBounds};
use db_core::{BTreeError, BTreeResult, NamedTreeProvider, NamedTreeTransaction, block_on};
use db_engine::{EngineKey, PrimaryKey};
use db_types::key_encoding::{DefaultEncoding, KeyEncoding, RowEncoding};
use db_types::persistence::decode_index_schema_row;
use futures::Stream;
use js_sys::{Function, Promise, Reflect};
use serde::de::{DeserializeOwned, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(typescript_custom_section)]
const STORE_ADAPTER_TS: &str = r#"
export type PrimaryKeyTuple = [
  number, number, number, number,
  number, number, number, number,
  number, number, number, number,
  number, number, number, number,
];

export type PrimaryKey = Uint8Array | PrimaryKeyTuple;

export interface PrimaryKeyRangeRequest {
  start?: PrimaryKey;
  startInclusive: boolean;
  end?: PrimaryKey;
  endInclusive: boolean;
}

export interface IndexRangeRequest {
  start?: EngineKey;
  startInclusive: boolean;
  end?: EngineKey;
  endInclusive: boolean;
}

export type RowBytes = Uint8Array;

export type PrimaryKeyEntry = {
  primaryKey: PrimaryKey;
  row: RowBytes;
};

export type IndexEntry = {
  indexKey: EngineKey;
  rowPrimaryKey: PrimaryKey;
};

export type TableSchemaEntry = {
  table: string;
  row: RowBytes;
};

export type IndexSchemaEntry = {
  index: string;
  row: RowBytes;
};

export interface DatabaseTransaction {
  getRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | null | undefined>;
  putRow(table: string, primaryKey: PrimaryKey, row: RowBytes): Promise<void>;
  deleteRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | null | undefined>;
  rangeRows(table: string, range: PrimaryKeyRangeRequest): Promise<PrimaryKeyEntry[]>;
  addIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void>;
  removeIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void>;
  rangeIndex(index: string, range: IndexRangeRequest): Promise<IndexEntry[]>;
  getTableSchema(table: string): Promise<RowBytes | null | undefined>;
  putTableSchema(table: string, row: RowBytes): Promise<void>;
  deleteTableSchema(table: string): Promise<RowBytes | null | undefined>;
  rangeTableSchemas(): Promise<TableSchemaEntry[]>;
  getIndexSchema(index: string): Promise<RowBytes | null | undefined>;
  putIndexSchema(index: string, row: RowBytes): Promise<void>;
  deleteIndexSchema(index: string): Promise<RowBytes | null | undefined>;
  rangeIndexSchemas(): Promise<IndexSchemaEntry[]>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
}

export interface DatabaseEngineOptions {
  beginTransaction(): Promise<DatabaseTransaction>;
}
"#;

#[wasm_bindgen]
extern "C" {
  #[wasm_bindgen(typescript_type = "DatabaseEngineOptions")]
  pub type DatabaseEngineOptions;
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

fn to_js<T: Serialize + ?Sized>(value: &T) -> BTreeResult<JsValue> {
  value
    .serialize(&serde_wasm_bindgen::Serializer::new().serialize_bytes_as_arrays(false))
    .map_err(serde_error)
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> BTreeResult<T> {
  serde_wasm_bindgen::from_value(value).map_err(serde_error)
}

fn resolve_js(value: JsValue) -> BTreeResult<JsValue> {
  let promise = Promise::resolve(&value);
  block_on(JsFuture::from(promise)).map_err(js_error)
}

fn call_method0(function: &Function, this: &JsValue) -> BTreeResult<JsValue> {
  let value = function.call0(this).map_err(js_error)?;
  resolve_js(value)
}

fn call_method1(function: &Function, this: &JsValue, arg0: &JsValue) -> BTreeResult<JsValue> {
  let value = function.call1(this, arg0).map_err(js_error)?;
  resolve_js(value)
}

fn call_method2(
  function: &Function,
  this: &JsValue,
  arg0: &JsValue,
  arg1: &JsValue,
) -> BTreeResult<JsValue> {
  let value = function.call2(this, arg0, arg1).map_err(js_error)?;
  resolve_js(value)
}

fn call_method3(
  function: &Function,
  this: &JsValue,
  arg0: &JsValue,
  arg1: &JsValue,
  arg2: &JsValue,
) -> BTreeResult<JsValue> {
  let value = function.call3(this, arg0, arg1, arg2).map_err(js_error)?;
  resolve_js(value)
}

fn load_required_function(adapter: &JsValue, name: &str) -> Result<Function, String> {
  let key = JsValue::from_str(name);
  let value =
    Reflect::get(adapter, &key).map_err(|_| format!("invalid adapter property: {name}"))?;
  if value.is_null() || value.is_undefined() {
    return Err(format!("missing required adapter function: {name}"));
  }
  value
    .dyn_into::<Function>()
    .map_err(|_| format!("adapter property is not a function: {name}"))
}

#[derive(Clone)]
struct CallbackRegistry {
  adapter: JsValue,
  begin_transaction: Function,
}

struct BackendTransaction {
  value: JsValue,
  get_row: Function,
  put_row: Function,
  delete_row: Function,
  range_rows: Function,
  add_index: Function,
  remove_index: Function,
  range_index: Function,
  get_table_schema: Function,
  put_table_schema: Function,
  delete_table_schema: Function,
  range_table_schemas: Function,
  get_index_schema: Function,
  put_index_schema: Function,
  delete_index_schema: Function,
  range_index_schemas: Function,
  commit: Function,
  rollback: Function,
}

impl BackendTransaction {
  fn commit(self) -> BTreeResult<()> {
    call_method0(&self.commit, &self.value).map(|_| ())
  }

  fn rollback(self) -> BTreeResult<()> {
    call_method0(&self.rollback, &self.value).map(|_| ())
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrimaryKeyRangeRequest {
  #[serde(deserialize_with = "deserialize_optional_primary_key")]
  start: Option<PrimaryKey>,
  start_inclusive: bool,
  #[serde(deserialize_with = "deserialize_optional_primary_key")]
  end: Option<PrimaryKey>,
  end_inclusive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexRangeRequest {
  start: Option<EngineKey>,
  start_inclusive: bool,
  end: Option<EngineKey>,
  end_inclusive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrimaryKeyEntry {
  #[serde(deserialize_with = "deserialize_primary_key")]
  primary_key: PrimaryKey,
  row: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexEntry {
  index_key: EngineKey,
  #[serde(deserialize_with = "deserialize_primary_key")]
  row_primary_key: PrimaryKey,
}

fn pk_from_bytes<E: serde::de::Error>(bytes: &[u8]) -> Result<PrimaryKey, E> {
  let array: [u8; 16] = bytes
    .try_into()
    .map_err(|_| E::custom("primary key must be exactly 16 bytes"))?;
  Ok(PrimaryKey::new(array))
}

fn deserialize_primary_key<'de, D>(deserializer: D) -> Result<PrimaryKey, D::Error>
where
  D: serde::Deserializer<'de>,
{
  struct PrimaryKeyVisitor;

  impl<'de> Visitor<'de> for PrimaryKeyVisitor {
    type Value = PrimaryKey;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
      formatter.write_str("a 16-byte primary key as Uint8Array or 16-number array")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      pk_from_bytes(v)
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      pk_from_bytes(&v)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
      A: SeqAccess<'de>,
    {
      let mut out = [0u8; 16];
      for (index, slot) in out.iter_mut().enumerate() {
        *slot = seq.next_element::<u8>()?.ok_or_else(|| {
          serde::de::Error::custom(format!("primary key missing byte at index {index}"))
        })?;
      }
      if seq.next_element::<u8>()?.is_some() {
        return Err(serde::de::Error::custom(
          "primary key must be exactly 16 bytes",
        ));
      }
      Ok(PrimaryKey::new(out))
    }
  }

  deserializer.deserialize_any(PrimaryKeyVisitor)
}

fn deserialize_optional_primary_key<'de, D>(deserializer: D) -> Result<Option<PrimaryKey>, D::Error>
where
  D: serde::Deserializer<'de>,
{
  struct OptionalPrimaryKeyVisitor;

  impl<'de> Visitor<'de> for OptionalPrimaryKeyVisitor {
    type Value = Option<PrimaryKey>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
      formatter.write_str("an optional primary key")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
      E: serde::de::Error,
    {
      Ok(None)
    }

    fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
    where
      D2: serde::Deserializer<'de>,
    {
      deserialize_primary_key(deserializer).map(Some)
    }
  }

  deserializer.deserialize_option(OptionalPrimaryKeyVisitor)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TableSchemaEntry {
  table: String,
  row: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexSchemaEntry {
  index: String,
  row: Vec<u8>,
}

#[derive(Clone)]
pub struct StoreAdapterCallbacks {
  callbacks: CallbackRegistry,
  index_key_widths: Arc<Mutex<HashMap<String, usize>>>,
}

impl TryFrom<JsValue> for StoreAdapterCallbacks {
  type Error = String;

  fn try_from(value: JsValue) -> Result<Self, Self::Error> {
    let callbacks = CallbackRegistry {
      begin_transaction: load_required_function(&value, "beginTransaction")?,
      adapter: value,
    };

    Ok(Self {
      callbacks,
      index_key_widths: Arc::new(Mutex::new(HashMap::new())),
    })
  }
}

#[derive(Clone)]
pub struct StoreAdapterTree {
  adapter: StoreAdapterCallbacks,
  tree: String,
}

pub struct StoreAdapterTransaction {
  adapter: StoreAdapterCallbacks,
  backend_tx: BackendTransaction,
}

const TABLE_SCHEMAS_TREE: &str = "sys:table_schemas";
const INDEX_SCHEMAS_TREE: &str = "sys:index_schemas";

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
    if self.index_name(tree).is_some() {
      None
    } else {
      Some(tree.strip_prefix("t:").unwrap_or(tree))
    }
  }

  fn index_name<'a>(&self, tree: &'a str) -> Option<&'a str> {
    tree.strip_prefix("i:")
  }

  fn table_schema_name_from_engine_key(&self, key: &EngineKey) -> BTreeResult<String> {
    let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
      .map_err(|e| serde_error(format!("decode key error: {e}")))?;
    match values.as_slice() {
      [db_engine::EngineValue::Text(name)] => Ok(name.clone()),
      _ => Err(serde_error("table schema key must be text scalar")),
    }
  }

  fn index_schema_name_from_engine_key(&self, key: &EngineKey) -> BTreeResult<String> {
    let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
      .map_err(|e| serde_error(format!("decode key error: {e}")))?;
    match values.as_slice() {
      [db_engine::EngineValue::Text(name)] => Ok(name.clone()),
      _ => Err(serde_error("index schema key must be text scalar")),
    }
  }

  fn primary_key_from_engine_key(&self, key: &EngineKey) -> BTreeResult<PrimaryKey> {
    let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
      .map_err(|e| serde_error(format!("decode key error: {e}")))?;
    match values.as_slice() {
      [db_engine::EngineValue::Uuid(bytes)] => Ok(PrimaryKey::new(*bytes)),
      _ => Err(serde_error("row primary key must be UUID scalar")),
    }
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

    let values = <DefaultEncoding as KeyEncoding>::decode_values(composite)
      .map_err(|e| serde_error(format!("decode composite key error: {e}")))?;
    if values.len() < width + 1 {
      return Err(serde_error("malformed composite index key"));
    }

    let index_key = <DefaultEncoding as KeyEncoding>::encode_values(&values[..width]);
    let row_pk_key = <DefaultEncoding as KeyEncoding>::encode_values(&values[width..]);
    let row_pk = self.primary_key_from_engine_key(&row_pk_key)?;
    Ok((index_key, row_pk))
  }

  fn compose_composite_index_key(&self, index_key: &EngineKey, row_pk: PrimaryKey) -> EngineKey {
    let mut values = <DefaultEncoding as KeyEncoding>::decode_values(index_key).unwrap_or_default();
    values.push(db_engine::EngineValue::Uuid(*row_pk.as_bytes()));
    <DefaultEncoding as KeyEncoding>::encode_values(&values)
  }

  fn maybe_update_index_schema_widths(&self, tree: &str, key: &EngineKey, row: &[u8]) {
    if tree != "sys:index_schemas" {
      return;
    }
    let decoded_key = <DefaultEncoding as KeyEncoding>::decode_values(key);
    let decoded_row = <DefaultEncoding as RowEncoding>::decode_values(row);
    if let (Ok([db_engine::EngineValue::Text(index_name)]), Ok(row_values)) =
      (decoded_key.as_deref(), decoded_row.as_deref())
      && let Ok(schema) = decode_index_schema_row(row_values)
      && let Ok(mut guard) = self.index_key_widths.lock()
    {
      guard.insert(index_name.clone(), schema.column_indices.len());
    }
  }

  fn maybe_remove_index_schema_width(&self, tree: &str, key: &EngineKey) {
    if tree != "sys:index_schemas" {
      return;
    }
    if let Ok([db_engine::EngineValue::Text(index_name)]) =
      <DefaultEncoding as KeyEncoding>::decode_values(key).as_deref()
      && let Ok(mut guard) = self.index_key_widths.lock()
    {
      let _ = guard.remove(index_name);
    }
  }

  fn begin_backend_transaction(&self) -> BTreeResult<BackendTransaction> {
    let value = call_method0(&self.callbacks.begin_transaction, &self.callbacks.adapter)?;

    if value.is_null() || value.is_undefined() {
      return Err(serde_error("beginTransaction returned null or undefined"));
    }

    Ok(BackendTransaction {
      get_row: load_required_function(&value, "getRow").map_err(serde_error)?,
      put_row: load_required_function(&value, "putRow").map_err(serde_error)?,
      delete_row: load_required_function(&value, "deleteRow").map_err(serde_error)?,
      range_rows: load_required_function(&value, "rangeRows").map_err(serde_error)?,
      add_index: load_required_function(&value, "addIndex").map_err(serde_error)?,
      remove_index: load_required_function(&value, "removeIndex").map_err(serde_error)?,
      range_index: load_required_function(&value, "rangeIndex").map_err(serde_error)?,
      get_table_schema: load_required_function(&value, "getTableSchema").map_err(serde_error)?,
      put_table_schema: load_required_function(&value, "putTableSchema").map_err(serde_error)?,
      delete_table_schema: load_required_function(&value, "deleteTableSchema")
        .map_err(serde_error)?,
      range_table_schemas: load_required_function(&value, "rangeTableSchemas")
        .map_err(serde_error)?,
      get_index_schema: load_required_function(&value, "getIndexSchema").map_err(serde_error)?,
      put_index_schema: load_required_function(&value, "putIndexSchema").map_err(serde_error)?,
      delete_index_schema: load_required_function(&value, "deleteIndexSchema")
        .map_err(serde_error)?,
      range_index_schemas: load_required_function(&value, "rangeIndexSchemas")
        .map_err(serde_error)?,
      commit: load_required_function(&value, "commit").map_err(serde_error)?,
      rollback: load_required_function(&value, "rollback").map_err(serde_error)?,
      value,
    })
  }

  fn with_ephemeral_tx<T, F>(&self, commit_on_success: bool, op: F) -> BTreeResult<T>
  where
    F: FnOnce(&StoreAdapterCallbacks, &BackendTransaction) -> BTreeResult<T>,
  {
    let tx = self.begin_backend_transaction()?;
    match op(self, &tx) {
      Ok(value) => {
        if commit_on_success {
          tx.commit()?;
        } else {
          tx.rollback()?;
        }
        Ok(value)
      }
      Err(err) => {
        let _ = tx.rollback();
        Err(err)
      }
    }
  }

  fn tx_get(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    if tree == TABLE_SCHEMAS_TREE {
      let table = self.table_schema_name_from_engine_key(key)?;
      let table_js = JsValue::from_str(&table);
      let value = call_method1(&tx.get_table_schema, &tx.value, &table_js)?;
      if value.is_null() || value.is_undefined() {
        return Ok(None);
      }
      return from_js(value);
    }

    if tree == INDEX_SCHEMAS_TREE {
      let index = self.index_schema_name_from_engine_key(key)?;
      let index_js = JsValue::from_str(&index);
      let value = call_method1(&tx.get_index_schema, &tx.value, &index_js)?;
      if value.is_null() || value.is_undefined() {
        return Ok(None);
      }
      return from_js(value);
    }

    if let Some(table) = self.row_table_name(tree) {
      let table_js = JsValue::from_str(table);
      let pk = self.primary_key_from_engine_key(key)?;
      let pk_js = to_js(&pk)?;
      let value = call_method2(&tx.get_row, &tx.value, &table_js, &pk_js)?;
      if value.is_null() || value.is_undefined() {
        return Ok(None);
      }
      return from_js(value);
    }

    if let Some(index_name) = self.index_name(tree) {
      let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
      let request = IndexRangeRequest {
        start: Some(index_key.clone()),
        start_inclusive: true,
        end: Some(index_key),
        end_inclusive: true,
      };
      let index_js = JsValue::from_str(index_name);
      let req_js = to_js(&request)?;
      let value = call_method2(&tx.range_index, &tx.value, &index_js, &req_js)?;
      let entries: Vec<IndexEntry> = from_js(value)?;
      let found = entries
        .into_iter()
        .any(|entry| entry.row_primary_key == row_pk);
      return Ok(found.then(Vec::new));
    }

    Err(serde_error(format!("unsupported tree name: {tree}")))
  }

  fn tx_insert(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
    row: &[u8],
  ) -> BTreeResult<()> {
    if tree == TABLE_SCHEMAS_TREE {
      let table = self.table_schema_name_from_engine_key(key)?;
      let table_js = JsValue::from_str(&table);
      let row_js = to_js(row)?;
      let _ = call_method2(&tx.put_table_schema, &tx.value, &table_js, &row_js)?;
      return Ok(());
    }

    if tree == INDEX_SCHEMAS_TREE {
      let index = self.index_schema_name_from_engine_key(key)?;
      let index_js = JsValue::from_str(&index);
      let row_js = to_js(row)?;
      let _ = call_method2(&tx.put_index_schema, &tx.value, &index_js, &row_js)?;
      self.maybe_update_index_schema_widths(tree, key, row);
      return Ok(());
    }

    if let Some(table) = self.row_table_name(tree) {
      let table_js = JsValue::from_str(table);
      let pk = self.primary_key_from_engine_key(key)?;
      let pk_js = to_js(&pk)?;
      let row_js = to_js(row)?;
      let _ = call_method3(&tx.put_row, &tx.value, &table_js, &pk_js, &row_js)?;
      self.maybe_update_index_schema_widths(tree, key, row);
      return Ok(());
    }

    if let Some(index_name) = self.index_name(tree) {
      let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
      let index_js = JsValue::from_str(index_name);
      let index_key_js = to_js(&index_key)?;
      let row_pk_js = to_js(&row_pk)?;
      let _ = call_method3(
        &tx.add_index,
        &tx.value,
        &index_js,
        &index_key_js,
        &row_pk_js,
      )?;
      return Ok(());
    }

    Err(serde_error(format!("unsupported tree name: {tree}")))
  }

  fn tx_remove(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    key: &EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>> {
    if tree == TABLE_SCHEMAS_TREE {
      let table = self.table_schema_name_from_engine_key(key)?;
      let table_js = JsValue::from_str(&table);
      let value = call_method1(&tx.delete_table_schema, &tx.value, &table_js)?;
      if value.is_null() || value.is_undefined() {
        return Ok(None);
      }
      return from_js(value);
    }

    if tree == INDEX_SCHEMAS_TREE {
      let index = self.index_schema_name_from_engine_key(key)?;
      let index_js = JsValue::from_str(&index);
      let value = call_method1(&tx.delete_index_schema, &tx.value, &index_js)?;
      self.maybe_remove_index_schema_width(tree, key);
      if value.is_null() || value.is_undefined() {
        return Ok(None);
      }
      return from_js(value);
    }

    if let Some(table) = self.row_table_name(tree) {
      let table_js = JsValue::from_str(table);
      let pk = self.primary_key_from_engine_key(key)?;
      let pk_js = to_js(&pk)?;
      let value = call_method2(&tx.delete_row, &tx.value, &table_js, &pk_js)?;
      if value.is_null() || value.is_undefined() {
        self.maybe_remove_index_schema_width(tree, key);
        return Ok(None);
      }
      self.maybe_remove_index_schema_width(tree, key);
      return from_js(value);
    }

    if let Some(index_name) = self.index_name(tree) {
      let (index_key, row_pk) = self.split_composite_index_key(index_name, key)?;
      let index_js = JsValue::from_str(index_name);
      let index_key_js = to_js(&index_key)?;
      let row_pk_js = to_js(&row_pk)?;
      let _ = call_method3(
        &tx.remove_index,
        &tx.value,
        &index_js,
        &index_key_js,
        &row_pk_js,
      )?;
      return Ok(None);
    }

    Err(serde_error(format!("unsupported tree name: {tree}")))
  }

  fn tx_range<R>(
    &self,
    tx: &BackendTransaction,
    tree: &str,
    range: &R,
  ) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>>
  where
    R: RangeBounds<EngineKey>,
  {
    if tree == TABLE_SCHEMAS_TREE {
      let value = call_method0(&tx.range_table_schemas, &tx.value)?;
      let entries: Vec<TableSchemaEntry> = from_js(value)?;
      return Ok(
        entries
          .into_iter()
          .map(|entry| {
            (
              <DefaultEncoding as KeyEncoding>::encode_values(&[db_engine::EngineValue::Text(
                entry.table,
              )]),
              entry.row,
            )
          })
          .collect(),
      );
    }

    if tree == INDEX_SCHEMAS_TREE {
      let value = call_method0(&tx.range_index_schemas, &tx.value)?;
      let entries: Vec<IndexSchemaEntry> = from_js(value)?;
      let out = entries
        .into_iter()
        .map(|entry| {
          (
            <DefaultEncoding as KeyEncoding>::encode_values(&[db_engine::EngineValue::Text(
              entry.index,
            )]),
            entry.row,
          )
        })
        .collect::<Vec<_>>();
      for (key, row) in &out {
        self.maybe_update_index_schema_widths(tree, key, row);
      }
      return Ok(out);
    }

    if let Some(table) = self.row_table_name(tree) {
      let request = PrimaryKeyRangeRequest {
        start: match range.start_bound() {
          Bound::Included(key) | Bound::Excluded(key) => {
            let pk = self.primary_key_from_engine_key(key)?;
            Some(pk)
          }
          Bound::Unbounded => None,
        },
        start_inclusive: matches!(range.start_bound(), Bound::Included(_)),
        end: match range.end_bound() {
          Bound::Included(key) | Bound::Excluded(key) => {
            let pk = self.primary_key_from_engine_key(key)?;
            Some(pk)
          }
          Bound::Unbounded => None,
        },
        end_inclusive: matches!(range.end_bound(), Bound::Included(_)),
      };

      let table_js = JsValue::from_str(table);
      let req_js = to_js(&request)?;
      let value = call_method2(&tx.range_rows, &tx.value, &table_js, &req_js)?;
      let rows: Vec<PrimaryKeyEntry> = from_js(value)?;
      let out = rows
        .into_iter()
        .map(|entry| (entry.primary_key.to_engine_key(), entry.row))
        .collect::<Vec<_>>();

      if tree == INDEX_SCHEMAS_TREE {
        for (key, row) in &out {
          self.maybe_update_index_schema_widths(tree, key, row);
        }
      }

      return Ok(out);
    }

    if let Some(index_name) = self.index_name(tree) {
      let request = IndexRangeRequest {
        start: match range.start_bound() {
          Bound::Included(key) | Bound::Excluded(key) => self
            .split_composite_index_key(index_name, key)
            .ok()
            .map(|(index_key, _)| index_key),
          Bound::Unbounded => None,
        },
        start_inclusive: matches!(range.start_bound(), Bound::Included(_)),
        end: match range.end_bound() {
          Bound::Included(key) | Bound::Excluded(key) => self
            .split_composite_index_key(index_name, key)
            .ok()
            .map(|(index_key, _)| index_key),
          Bound::Unbounded => None,
        },
        end_inclusive: matches!(range.end_bound(), Bound::Included(_)),
      };

      let index_js = JsValue::from_str(index_name);
      let req_js = to_js(&request)?;
      let value = call_method2(&tx.range_index, &tx.value, &index_js, &req_js)?;
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

    Err(serde_error(format!("unsupported tree name: {tree}")))
  }

  fn callback_get(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<Vec<u8>>> {
    self.with_ephemeral_tx(false, |adapter, tx| adapter.tx_get(tx, tree, key))
  }

  fn callback_insert(&self, tree: &str, key: &EngineKey, row: &[u8]) -> BTreeResult<()> {
    self.with_ephemeral_tx(true, |adapter, tx| adapter.tx_insert(tx, tree, key, row))
  }

  fn callback_remove(&self, tree: &str, key: &EngineKey) -> BTreeResult<Option<Vec<u8>>> {
    self.with_ephemeral_tx(true, |adapter, tx| adapter.tx_remove(tx, tree, key))
  }

  fn callback_range<R>(&self, tree: &str, range: &R) -> BTreeResult<Vec<(EngineKey, Vec<u8>)>>
  where
    R: RangeBounds<EngineKey>,
  {
    self.with_ephemeral_tx(false, |adapter, tx| adapter.tx_range(tx, tree, range))
  }
}

impl NamedTreeProvider<EngineKey, Vec<u8>> for StoreAdapterCallbacks {
  type Tree = StoreAdapterTree;
  type Transaction = StoreAdapterTransaction;

  fn get_tree(
    &self,
    name: &str,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Tree>> + Send + '_ {
    let tree = StoreAdapterTree {
      adapter: self.clone(),
      tree: name.to_string(),
    };
    future::ready(Ok(tree))
  }

  fn begin_transaction(
    &self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + Send + '_ {
    async move {
      let backend_tx = self.begin_backend_transaction()?;
      Ok(StoreAdapterTransaction {
        adapter: self.clone(),
        backend_tx,
      })
    }
  }
}

impl NamedTreeTransaction<EngineKey, Vec<u8>> for StoreAdapterTransaction {
  fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + Send + 'a
  where
    EngineKey: Ord,
  {
    future::ready(self.adapter.tx_get(&self.backend_tx, tree, key))
  }

  fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: Vec<u8>,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + Send + 'a
  where
    EngineKey: Ord,
  {
    future::ready(self.adapter.tx_insert(&self.backend_tx, tree, &key, &value))
  }

  fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + Send + 'a
  where
    EngineKey: Ord,
  {
    future::ready(self.adapter.tx_remove(&self.backend_tx, tree, key))
  }

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    let rows = self
      .adapter
      .tx_range(&self.backend_tx, tree, &range)
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

  fn commit(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    future::ready(self.backend_tx.commit())
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    future::ready(self.backend_tx.rollback())
  }
}

impl db_core::BTreeExecutor<EngineKey, Vec<u8>> for StoreAdapterTree {
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    future::ready(self.adapter.callback_get(&self.tree, key.borrow()))
  }

  fn insert<'a>(
    &'a mut self,
    key: EngineKey,
    value: Vec<u8>,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + Send + 'a
  where
    EngineKey: Ord,
  {
    future::ready(self.adapter.callback_insert(&self.tree, &key, &value))
  }

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    future::ready(self.adapter.callback_remove(&self.tree, key.borrow()))
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    let rows = self
      .adapter
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

impl db_core::BTreeExecutor<EngineKey, Vec<u8>> for StoreAdapterTransaction {
  fn get<'a, Q>(
    &'a self,
    _key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    future::ready(Err(BTreeError::UnsupportedOperation))
  }

  fn insert<'a>(
    &'a mut self,
    _key: EngineKey,
    _value: Vec<u8>,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + Send + 'a
  where
    EngineKey: Ord,
  {
    future::ready(Err(BTreeError::UnsupportedOperation))
  }

  fn remove<'a, Q>(
    &'a mut self,
    _key: Q,
  ) -> impl core::future::Future<Output = BTreeResult<Option<Vec<u8>>>> + Send + 'a
  where
    EngineKey: Ord,
    Q: core::borrow::Borrow<EngineKey> + Send + 'a,
  {
    future::ready(Err(BTreeError::UnsupportedOperation))
  }

  fn range<'a, R>(
    &'a self,
    _range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + Send + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + Send + 'a,
  {
    futures::stream::iter(Vec::new())
  }
}

impl db_core::BTreeTransaction<EngineKey, Vec<u8>> for StoreAdapterTransaction {
  fn commit(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    <Self as NamedTreeTransaction<EngineKey, Vec<u8>>>::commit(self)
  }

  fn rollback(self) -> impl core::future::Future<Output = BTreeResult<()>> + Send
  where
    Self: Sized,
  {
    <Self as NamedTreeTransaction<EngineKey, Vec<u8>>>::rollback(self)
  }
}

impl db_core::BTree<EngineKey, Vec<u8>> for StoreAdapterTree {
  type Transaction = StoreAdapterTransaction;

  fn transaction<'a>(
    &'a self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + Send + 'a {
    async move {
      let backend_tx = self.adapter.begin_backend_transaction()?;
      Ok(StoreAdapterTransaction {
        adapter: self.adapter.clone(),
        backend_tx,
      })
    }
  }
}
