use db_engine::EngineValue;
use db_facade::SqlParams;
use js_sys::Array;
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

pub fn to_js_error(message: impl core::fmt::Display) -> JsValue {
  js_sys::Error::new(&message.to_string()).into()
}

pub fn parse_sql_params(params: JsValue) -> Result<SqlParams, JsValue> {
  if Array::is_array(&params) {
    let positional: Vec<EngineValue> = serde_wasm_bindgen::from_value(params)
      .map_err(|e| to_js_error(format!("invalid positional params: {e}")))?;
    return Ok(SqlParams::from(positional));
  }

  let named: BTreeMap<String, EngineValue> = serde_wasm_bindgen::from_value(params)
    .map_err(|e| to_js_error(format!("invalid named params: {e}")))?;
  Ok(SqlParams::named(named))
}
