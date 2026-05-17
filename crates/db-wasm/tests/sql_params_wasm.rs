#![cfg(target_arch = "wasm32")]

use db_wasm::BrowserDatabase;
use db_wasm::types::EngineValue;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn execute_sql_with_positional_params_filters_rows() {
  let mut db = BrowserDatabase::open();

  db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
    .await
    .expect("create table should succeed");

  db.execute_sql(
    "INSERT INTO users (id, score) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 10);",
  )
  .await
  .expect("insert row should succeed");

  let params = js_sys::Array::new();
  params.push(&JsValue::from_f64(10.0));

  let result = db
    .execute_sql_with_params("SELECT score FROM users WHERE score = $1;", params.into())
    .await
    .expect("parameterized select should succeed");

  assert_eq!(result.rows, vec![vec![EngineValue::Integer(10)]]);
}
