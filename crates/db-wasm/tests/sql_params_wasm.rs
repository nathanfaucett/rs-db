#![cfg(target_arch = "wasm32")]

use db_wasm::BrowserDatabase;
use db_wasm::engine::EngineResult;
use db_wasm::types::EngineValue;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;

fn decode_engine_result(value: impl Into<JsValue>) -> EngineResult {
  serde_wasm_bindgen::from_value(value.into()).expect("engine result should deserialize")
}

#[wasm_bindgen_test]
async fn execute_sql_with_positional_params_filters_rows() {
  let mut db = BrowserDatabase::open();

  db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
    .await
    .expect("create table should succeed");

  assert!(db.describe_table("users").is_some());

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
  let result = decode_engine_result(result);

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
  let result = decode_engine_result(result);

  assert_eq!(
    result.rows,
    vec![vec![EngineValue::Json("[\"salt\",\"pepper\"]".into())]]
  );
}

#[wasm_bindgen_test]
async fn execute_sql_json_results_convert_to_js_values() {
  let mut db = BrowserDatabase::open();

  db.execute_sql("CREATE TABLE recipes (id TEXT PRIMARY KEY, ingredients JSON);")
    .await
    .expect("create table should succeed");

  db.execute_sql(r#"INSERT INTO recipes (id, ingredients) VALUES ('r-1', '["salt","pepper"]');"#)
    .await
    .expect("insert json should succeed");

  let result = db
    .execute_sql("SELECT ingredients FROM recipes WHERE id = 'r-1';")
    .await
    .expect("select should succeed");

  let result_js: JsValue = result.into();
  let rows =
    js_sys::Reflect::get(&result_js, &JsValue::from_str("rows")).expect("rows field should exist");
  let first_row = js_sys::Array::from(&rows).get(0);
  let first_value = js_sys::Array::from(&first_row).get(0);

  assert!(js_sys::Array::is_array(&first_value));
  assert!(!first_value.is_string());

  let values = js_sys::Array::from(&first_value);
  assert_eq!(values.get(0).as_string().as_deref(), Some("salt"));
  assert_eq!(values.get(1).as_string().as_deref(), Some("pepper"));
}

#[wasm_bindgen_test]
async fn execute_sql_nested_json_results_convert_to_js_values() {
  let mut db = BrowserDatabase::open();

  db.execute_sql("CREATE TABLE recipes (id TEXT PRIMARY KEY, data JSON);")
    .await
    .expect("create table should succeed");

  db.execute_sql(
    r#"INSERT INTO recipes (id, data) VALUES (
      'r-1',
      '{"sections":[[{"name":"dry","items":[{"label":"flour","qty":2},{"label":"salt","qty":1}]},{"name":"wet","items":[{"label":"water","qty":3}]}],[{"name":"toppings","items":[{"label":"sesame","qty":1}]}]],"meta":{"version":2}}'
    );"#,
  )
  .await
  .expect("insert json should succeed");

  let result = db
    .execute_sql("SELECT data FROM recipes WHERE id = 'r-1';")
    .await
    .expect("select should succeed");

  let result_js: JsValue = result.into();
  let rows =
    js_sys::Reflect::get(&result_js, &JsValue::from_str("rows")).expect("rows field should exist");
  let first_row = js_sys::Array::from(&rows).get(0);
  let data = js_sys::Array::from(&first_row).get(0);

  assert!(data.is_object());
  assert!(!data.is_string());

  let sections = js_sys::Reflect::get(&data, &JsValue::from_str("sections"))
    .expect("sections field should exist");
  assert!(js_sys::Array::is_array(&sections));

  let outer_sections = js_sys::Array::from(&sections);
  let first_group = outer_sections.get(0);
  let second_group = outer_sections.get(1);
  assert!(js_sys::Array::is_array(&first_group));
  assert!(js_sys::Array::is_array(&second_group));

  let first_section = js_sys::Array::from(&first_group).get(0);
  let first_name = js_sys::Reflect::get(&first_section, &JsValue::from_str("name"))
    .expect("name field should exist");
  assert_eq!(first_name.as_string().as_deref(), Some("dry"));

  let items = js_sys::Reflect::get(&first_section, &JsValue::from_str("items"))
    .expect("items field should exist");
  assert!(js_sys::Array::is_array(&items));

  let first_item = js_sys::Array::from(&items).get(0);
  let label = js_sys::Reflect::get(&first_item, &JsValue::from_str("label"))
    .expect("label field should exist");
  let qty =
    js_sys::Reflect::get(&first_item, &JsValue::from_str("qty")).expect("qty field should exist");
  assert_eq!(label.as_string().as_deref(), Some("flour"));
  assert_eq!(qty.as_f64(), Some(2.0));

  let meta =
    js_sys::Reflect::get(&data, &JsValue::from_str("meta")).expect("meta field should exist");
  let version =
    js_sys::Reflect::get(&meta, &JsValue::from_str("version")).expect("version field should exist");
  assert_eq!(version.as_f64(), Some(2.0));
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
  let result = decode_engine_result(result);

  assert_eq!(result.rows, vec![vec![EngineValue::Blob(vec![1, 2, 255])]]);
}

#[wasm_bindgen_test]
async fn execute_sql_with_uuid_string_param_inserts_uuid() {
  let mut db = BrowserDatabase::open();

  db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
    .await
    .expect("create table should succeed");

  let params = js_sys::Array::new();
  params.push(&JsValue::from_str("550e8400-e29b-41d4-a716-446655440000"));
  params.push(&JsValue::from_f64(10.0));

  db.execute_sql_with_params(
    "INSERT INTO users (id, score) VALUES ($1, $2);",
    params.into(),
  )
  .await
  .expect("insert with uuid param should succeed");

  let result = db
    .execute_sql("SELECT score FROM users;")
    .await
    .expect("select should succeed");
  let result = decode_engine_result(result);

  assert_eq!(result.rows, vec![vec![EngineValue::Integer(10)]]);
}
