use db_engine::{EngineQuery, EngineResult, IndexSchema, TableSchema};
use db_facade::{Database, InMemoryEngineStore};
use wasm_bindgen::prelude::*;

fn to_js_error(message: impl core::fmt::Display) -> JsValue {
  js_sys::Error::new(&message.to_string()).into()
}

#[wasm_bindgen]
pub struct BrowserDatabase {
  inner: Database<InMemoryEngineStore>,
}

#[wasm_bindgen]
impl BrowserDatabase {
  #[wasm_bindgen(js_name = open)]
  pub fn open() -> BrowserDatabase {
    let inner = Database::open_in_memory_sync();

    BrowserDatabase { inner }
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
