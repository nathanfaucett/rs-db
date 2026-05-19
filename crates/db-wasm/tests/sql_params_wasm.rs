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

#[wasm_bindgen_test]
async fn execute_sql_with_named_json_array_param_inserts_json() {
  let mut db = BrowserDatabase::open();

  db.execute_sql("CREATE TABLE recipes (id TEXT PRIMARY KEY, ingredients JSON);")
    .await
    .expect("create table should succeed");

  let ingredients = js_sys::Array::new();
  ingredients.push(&JsValue::from_str("salt"));
  ingredients.push(&JsValue::from_str("pepper"));

  let params = js_sys::Object::new();
  js_sys::Reflect::set(&params, &JsValue::from_str("id"), &JsValue::from_str("r-1"))
    .expect("id param should be set");
  js_sys::Reflect::set(&params, &JsValue::from_str("ingredients"), &ingredients)
    .expect("ingredients param should be set");

  db.execute_sql_with_params(
    "INSERT INTO recipes (id, ingredients) VALUES (:id, :ingredients);",
    params.clone().into(),
  )
  .await
  .expect("insert with json array param should succeed");

  let result = db
    .execute_sql_with_params(
      "SELECT ingredients FROM recipes WHERE id = :id;",
      params.into(),
    )
    .await
    .expect("select with named param should succeed");

  assert_eq!(
    result.rows,
    vec![vec![EngineValue::Json("[\"salt\",\"pepper\"]".into())]]
  );
}

#[wasm_bindgen_test]
async fn execute_sql_with_named_u8_array_param_inserts_blob() {
  let mut db = BrowserDatabase::open();

  db.execute_sql("CREATE TABLE blobs (id TEXT PRIMARY KEY, payload BLOB);")
    .await
    .expect("create table should succeed");

  let payload = js_sys::Array::new();
  payload.push(&JsValue::from_f64(1.0));
  payload.push(&JsValue::from_f64(2.0));
  payload.push(&JsValue::from_f64(255.0));

  let params = js_sys::Object::new();
  js_sys::Reflect::set(&params, &JsValue::from_str("id"), &JsValue::from_str("b-1"))
    .expect("id param should be set");
  js_sys::Reflect::set(&params, &JsValue::from_str("payload"), &payload)
    .expect("payload param should be set");

  db.execute_sql_with_params(
    "INSERT INTO blobs (id, payload) VALUES (:id, :payload);",
    params.clone().into(),
  )
  .await
  .expect("insert with byte array param should succeed");

  let result = db
    .execute_sql_with_params("SELECT payload FROM blobs WHERE id = :id;", params.into())
    .await
    .expect("select with named param should succeed");

  assert_eq!(result.rows, vec![vec![EngineValue::Blob(vec![1, 2, 255])]]);
}
