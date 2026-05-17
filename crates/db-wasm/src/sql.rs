use db_engine::{EngineQuery, SchemaResolver, TableSchema};
use db_sql_to_engine::{
  CanonicalStatement, parse_and_translate, parse_and_translate_statement,
  parse_and_translate_statement_with_params, parse_and_translate_with_params,
};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

use crate::params::{parse_sql_params, to_js_error};

#[derive(Default)]
struct StaticSchemaResolver {
  tables: BTreeMap<String, TableSchema>,
}

impl StaticSchemaResolver {
  fn from_tables(tables: Vec<TableSchema>) -> Self {
    let mapped = tables
      .into_iter()
      .map(|schema| (schema.name.clone(), schema))
      .collect();

    Self { tables: mapped }
  }
}

impl SchemaResolver for StaticSchemaResolver {
  fn describe_table(&self, name: &str) -> Option<TableSchema> {
    self.tables.get(name).cloned()
  }
}

#[wasm_bindgen]
pub fn translate_sql_to_query(
  sql: &str,
  schemas: Vec<TableSchema>,
) -> Result<EngineQuery, JsValue> {
  let resolver = StaticSchemaResolver::from_tables(schemas);
  parse_and_translate(sql, &resolver).map_err(to_js_error)
}

#[wasm_bindgen]
pub fn translate_sql_to_statement(
  sql: &str,
  schemas: Vec<TableSchema>,
) -> Result<CanonicalStatement, JsValue> {
  let resolver = StaticSchemaResolver::from_tables(schemas);
  parse_and_translate_statement(sql, &resolver).map_err(to_js_error)
}

#[wasm_bindgen]
pub fn translate_sql_to_query_with_params(
  sql: &str,
  schemas: Vec<TableSchema>,
  params: JsValue,
) -> Result<EngineQuery, JsValue> {
  let resolver = StaticSchemaResolver::from_tables(schemas);
  let params = parse_sql_params(params)?;
  parse_and_translate_with_params(sql, &resolver, &params).map_err(to_js_error)
}

#[wasm_bindgen]
pub fn translate_sql_to_statement_with_params(
  sql: &str,
  schemas: Vec<TableSchema>,
  params: JsValue,
) -> Result<CanonicalStatement, JsValue> {
  let resolver = StaticSchemaResolver::from_tables(schemas);
  let params = parse_sql_params(params)?;
  parse_and_translate_statement_with_params(sql, &resolver, &params).map_err(to_js_error)
}
