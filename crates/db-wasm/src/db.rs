use db_engine::{EngineQuery, EngineResult, IndexSchema, Subscriber, SubscriptionId, TableSchema};
use db_facade::Database;
use db_in_memory::InMemoryNamedBTree;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

use crate::params::{parse_sql_params, to_js_error};
use crate::pluggable_store::PluggableBackendStore;
use crate::store_adapter::{DatabaseEngineOptions, StoreAdapterCallbacks};

/// Strongly-typed callback for query subscriptions.
/// TypeScript sees: `(error: Error | null, result: EngineResult | null) => void`
#[wasm_bindgen]
extern "C" {
  #[wasm_bindgen(typescript_type = "(error: Error | null, result: EngineResult | null) => void")]
  pub type SubscribeCallback;
}

#[wasm_bindgen]
pub struct BrowserDatabase {
  inner: Database<PluggableBackendStore>,
}

#[wasm_bindgen]
impl BrowserDatabase {
  #[wasm_bindgen(js_name = open)]
  pub fn open() -> BrowserDatabase {
    let store = PluggableBackendStore::InMemory(InMemoryNamedBTree::new());
    let inner = Database::from_store(store);

    BrowserDatabase { inner }
  }

  #[wasm_bindgen(js_name = openWithBackend)]
  pub async fn open_with_backend(
    options: DatabaseEngineOptions,
  ) -> Result<BrowserDatabase, JsValue> {
    let adapter_value: JsValue = options.into();
    let adapter = StoreAdapterCallbacks::try_from(adapter_value).map_err(to_js_error)?;
    let store = PluggableBackendStore::External(adapter);
    let inner = Database::open_with_store(store)
      .await
      .map_err(to_js_error)?;

    Ok(BrowserDatabase { inner })
  }

  #[wasm_bindgen(js_name = registerTable)]
  pub async fn register_table(&mut self, schema: TableSchema) -> Result<(), JsValue> {
    self.inner.register_table(schema).await.map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = dropTable)]
  pub async fn drop_table(&mut self, table_name: &str) -> Result<(), JsValue> {
    self.inner.drop_table(table_name).await.map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = registerIndex)]
  pub async fn register_index(&mut self, schema: IndexSchema) -> Result<(), JsValue> {
    self.inner.register_index(schema).await.map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = dropIndex)]
  pub async fn drop_index(&mut self, index_name: &str) -> Result<(), JsValue> {
    self.inner.drop_index(index_name).await.map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = describeTable)]
  pub fn describe_table(&self, table_name: &str) -> Option<TableSchema> {
    self.inner.describe_table(table_name)
  }

  #[wasm_bindgen(js_name = executeQuery)]
  pub async fn execute_query(&self, query: EngineQuery) -> Result<EngineResult, JsValue> {
    self.inner.execute_query(query).await.map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = executeSql)]
  pub async fn execute_sql(&mut self, sql: &str) -> Result<EngineResult, JsValue> {
    self.inner.execute_sql(sql).await.map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = executeSqlWithParams)]
  pub async fn execute_sql_with_params(
    &mut self,
    sql: &str,
    params: JsValue,
  ) -> Result<EngineResult, JsValue> {
    let params = parse_sql_params(params)?;
    self
      .inner
      .execute_sql_with_params(sql, &params)
      .await
      .map_err(to_js_error)
  }

  #[wasm_bindgen(js_name = selectSimple)]
  pub fn select_simple(
    table: String,
    projection: Vec<usize>,
    predicate: Option<db_engine::QualifiedPredicate>,
  ) -> EngineQuery {
    EngineQuery::select_simple(table, projection, predicate)
  }

  #[wasm_bindgen(js_name = subscribeQuery)]
  pub async fn subscribe_query(
    &self,
    query: EngineQuery,
    callback: SubscribeCallback,
  ) -> Result<SubscriptionId, JsValue> {
    let callback: js_sys::Function = callback.unchecked_into();
    let sub_id = self
      .inner
      .subscribe_query(query, Arc::new(WasmSubscriber { callback }), None)
      .await
      .map_err(to_js_error)?;
    Ok(sub_id)
  }

  #[wasm_bindgen(js_name = subscribeSql)]
  pub async fn subscribe_sql(
    &self,
    sql: &str,
    callback: SubscribeCallback,
  ) -> Result<SubscriptionId, JsValue> {
    let callback: js_sys::Function = callback.unchecked_into();
    let sub_id = self
      .inner
      .subscribe_sql(sql, Arc::new(WasmSubscriber { callback }), None)
      .await
      .map_err(to_js_error)?;
    Ok(sub_id)
  }

  #[wasm_bindgen(js_name = subscribeSqlWithParams)]
  pub async fn subscribe_sql_with_params(
    &self,
    sql: &str,
    params: JsValue,
    callback: SubscribeCallback,
  ) -> Result<SubscriptionId, JsValue> {
    let params = parse_sql_params(params)?;
    let callback: js_sys::Function = callback.unchecked_into();
    let sub_id = self
      .inner
      .subscribe_sql_with_params(sql, &params, Arc::new(WasmSubscriber { callback }), None)
      .await
      .map_err(to_js_error)?;
    Ok(sub_id)
  }

  #[wasm_bindgen(js_name = unsubscribe)]
  pub async fn unsubscribe(&self, id: SubscriptionId) -> Result<(), JsValue> {
    self.inner.unsubscribe(id).await.map_err(to_js_error)
  }
}

/// WASM subscriber wrapper that bridges Rust callbacks to JS functions
struct WasmSubscriber {
  callback: js_sys::Function,
}

impl Subscriber for WasmSubscriber {
  fn on_results(&self, result: Result<EngineResult, db_engine::EngineError>) {
    let this = JsValue::null();
    match result {
      Ok(results) => {
        let results_js: JsValue = results.into();
        let _ = self.callback.call2(&this, &JsValue::null(), &results_js);
      }
      Err(e) => {
        let error = js_sys::Error::new(&e.to_string());
        let _ = self.callback.call2(&this, &error.into(), &JsValue::null());
      }
    }
  }
}
