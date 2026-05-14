use db_engine::{EngineQuery, EngineResult, IndexSchema, TableSchema};
use db_facade::Database;
use db_in_memory::InMemoryNamedBTree;
use wasm_bindgen::prelude::*;

use crate::pluggable_store::PluggableBackendStore;
use crate::store_adapter::{DatabaseEngineOptions, StoreAdapterCallbacks};

fn to_js_error(message: impl core::fmt::Display) -> JsValue {
  js_sys::Error::new(&message.to_string()).into()
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

  #[wasm_bindgen(js_name = selectSimple)]
  pub fn select_simple(
    table: String,
    projection: Vec<usize>,
    predicate: Option<db_engine::QualifiedPredicate>,
  ) -> EngineQuery {
    EngineQuery::select_simple(table, projection, predicate)
  }
}
