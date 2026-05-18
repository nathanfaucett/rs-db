use db_engine::EngineValue;
use db_facade::SqlParams;
use js_sys::{Array, Object, Reflect, Uint8Array};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

pub fn to_js_error(message: impl core::fmt::Display) -> JsValue {
  js_sys::Error::new(&message.to_string()).into()
}

fn number_to_engine_value(value: f64) -> EngineValue {
  if value.is_finite()
    && value.fract() == 0.0
    && value >= i64::MIN as f64
    && value <= i64::MAX as f64
  {
    EngineValue::Integer(value as i64)
  } else {
    EngineValue::Float(value)
  }
}

fn bytes_to_engine_value(bytes: Vec<u8>) -> EngineValue {
  if bytes.len() == 16 {
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&bytes);
    EngineValue::Uuid(uuid)
  } else {
    EngineValue::Blob(bytes)
  }
}

fn parse_byte_array(array: &Array, path: &str) -> Result<EngineValue, JsValue> {
  let mut bytes = Vec::with_capacity(array.length() as usize);
  for (index, entry) in array.iter().enumerate() {
    let Some(number) = entry.as_f64() else {
      return Err(to_js_error(format!(
        "invalid param at {path}[{index}]: expected byte number (0..=255)"
      )));
    };
    if !number.is_finite() || number.fract() != 0.0 || !(0.0..=255.0).contains(&number) {
      return Err(to_js_error(format!(
        "invalid param at {path}[{index}]: expected byte number (0..=255)"
      )));
    }
    bytes.push(number as u8);
  }
  Ok(bytes_to_engine_value(bytes))
}

fn parse_engine_value(value: JsValue, path: &str) -> Result<EngineValue, JsValue> {
  if value.is_null() || value.is_undefined() {
    return Ok(EngineValue::Null);
  }

  if let Some(number) = value.as_f64() {
    return Ok(number_to_engine_value(number));
  }

  if let Some(text) = value.as_string() {
    return Ok(EngineValue::Text(text));
  }

  if Array::is_array(&value) {
    let array = Array::from(&value);
    return parse_byte_array(&array, path);
  }

  if Uint8Array::instanceof(&value) {
    let bytes = Uint8Array::new(&value).to_vec();
    return Ok(bytes_to_engine_value(bytes));
  }

  Err(to_js_error(format!(
    "invalid param at {path}: expected number, string, null/undefined, byte array, or Uint8Array"
  )))
}

fn parse_named_params(params: JsValue) -> Result<BTreeMap<String, EngineValue>, JsValue> {
  if !params.is_object() || Array::is_array(&params) {
    return Err(to_js_error(
      "invalid named params: expected an object with key/value pairs",
    ));
  }

  let object = Object::from(params.clone());
  let keys = Object::keys(&object);
  let mut named = BTreeMap::new();

  for key in keys.iter() {
    let Some(name) = key.as_string() else {
      return Err(to_js_error(
        "invalid named params: found non-string object key",
      ));
    };
    let raw_value = Reflect::get(&object, &key)
      .map_err(|_| to_js_error(format!("invalid named params: cannot read key '{name}'")))?;
    let parsed = parse_engine_value(raw_value, &format!("named param '{name}'"))?;
    named.insert(name, parsed);
  }

  Ok(named)
}

pub fn parse_sql_params(params: JsValue) -> Result<SqlParams, JsValue> {
  if Array::is_array(&params) {
    let array = Array::from(&params);
    let mut positional = Vec::with_capacity(array.length() as usize);
    for (index, value) in array.iter().enumerate() {
      positional.push(parse_engine_value(
        value,
        &format!("positional param ${}", index + 1),
      )?);
    }
    return Ok(SqlParams::from(positional));
  }

  let named = parse_named_params(params)?;
  Ok(SqlParams::named(named))
}
