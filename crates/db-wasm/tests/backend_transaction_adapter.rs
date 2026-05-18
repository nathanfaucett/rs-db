#![cfg(target_arch = "wasm32")]

use db_wasm::BrowserDatabase;
use db_wasm::DatabaseEngineOptions;
use db_wasm::types::{ColumnSchema, EngineType, TableSchema};
use wasm_bindgen::JsCast;
use wasm_bindgen_test::wasm_bindgen_test;

const ADAPTER_SCRIPT: &str = r#"
(() => {
  const state = {
    rowsByTable: new Map(),
    indexEntries: new Map(),
    tableSchemas: new Map(),
    indexSchemas: new Map(),
  };

  const pkKey = (pk) => (Array.isArray(pk) ? pk.join(":") : String(pk));
  const indexEntryKey = (indexKey, rowPrimaryKey) => `${JSON.stringify(indexKey)}|${pkKey(rowPrimaryKey)}`;

  return {
    beginTransaction() {
      return Promise.resolve({
        getRow(table, primaryKey) {
          const tableRows = state.rowsByTable.get(table);
          const row = tableRows?.get(pkKey(primaryKey));
          return Promise.resolve(row ? [...row] : undefined);
        },
        putRow(table, primaryKey, row) {
          let tableRows = state.rowsByTable.get(table);
          if (!tableRows) {
            tableRows = new Map();
            state.rowsByTable.set(table, tableRows);
          }
          tableRows.set(pkKey(primaryKey), [...row]);
          return Promise.resolve();
        },
        deleteRow(table, primaryKey) {
          const tableRows = state.rowsByTable.get(table);
          const key = pkKey(primaryKey);
          const previous = tableRows?.get(key);
          tableRows?.delete(key);
          return Promise.resolve(previous ? [...previous] : undefined);
        },
        rangeRows(table, _range) {
          const tableRows = state.rowsByTable.get(table);
          if (!tableRows) {
            return Promise.resolve([]);
          }
          const out = Array.from(tableRows.entries()).map(([key, row]) => ({
            primaryKey: key.split(":").map((part) => Number.parseInt(part, 10)),
            row: [...row],
          }));
          return Promise.resolve(out);
        },
        addIndex(index, indexKey, rowPrimaryKey) {
          let entries = state.indexEntries.get(index);
          if (!entries) {
            entries = new Map();
            state.indexEntries.set(index, entries);
          }
          entries.set(indexEntryKey(indexKey, rowPrimaryKey), {
            indexKey,
            rowPrimaryKey,
          });
          return Promise.resolve();
        },
        removeIndex(index, indexKey, rowPrimaryKey) {
          const entries = state.indexEntries.get(index);
          entries?.delete(indexEntryKey(indexKey, rowPrimaryKey));
          return Promise.resolve();
        },
        rangeIndex(index, _range) {
          const entries = state.indexEntries.get(index);
          if (!entries) {
            return Promise.resolve([]);
          }
          return Promise.resolve(Array.from(entries.values()));
        },
        getTableSchema(table) {
          const row = state.tableSchemas.get(table);
          return Promise.resolve(row ? [...row] : undefined);
        },
        putTableSchema(table, row) {
          state.tableSchemas.set(table, [...row]);
          return Promise.resolve();
        },
        deleteTableSchema(table) {
          const previous = state.tableSchemas.get(table);
          state.tableSchemas.delete(table);
          return Promise.resolve(previous ? [...previous] : undefined);
        },
        rangeTableSchemas() {
          return Promise.resolve(
            Array.from(state.tableSchemas.entries()).map(([table, row]) => ({ table, row: [...row] })),
          );
        },
        getIndexSchema(index) {
          const row = state.indexSchemas.get(index);
          return Promise.resolve(row ? [...row] : undefined);
        },
        putIndexSchema(index, row) {
          state.indexSchemas.set(index, [...row]);
          return Promise.resolve();
        },
        deleteIndexSchema(index) {
          const previous = state.indexSchemas.get(index);
          state.indexSchemas.delete(index);
          return Promise.resolve(previous ? [...previous] : undefined);
        },
        rangeIndexSchemas() {
          return Promise.resolve(
            Array.from(state.indexSchemas.entries()).map(([index, row]) => ({ index, row: [...row] })),
          );
        },
        commit() {
          return Promise.resolve();
        },
        rollback() {
          return Promise.resolve();
        },
      });
    },
  };
})()
"#;

#[wasm_bindgen_test]
async fn open_with_strict_transaction_adapter_supports_schema_lifecycle() {
  let adapter_value = js_sys::eval(ADAPTER_SCRIPT).expect("adapter eval should succeed");
  let options: DatabaseEngineOptions = adapter_value.unchecked_into();
  let mut db = BrowserDatabase::open_with_backend(options)
    .await
    .expect("open_with_backend should succeed");

  let schema = TableSchema {
    name: "users".to_string(),
    columns: vec![
      ColumnSchema {
        name: "id".to_string(),
        data_type: EngineType::Uuid,
      },
      ColumnSchema {
        name: "name".to_string(),
        data_type: EngineType::Text,
      },
    ],
    primary_key: vec![0],
  };

  db.register_table(schema.clone(), false)
    .await
    .expect("register_table should succeed");

  let described = db.describe_table("users");
  assert_eq!(described, Some(schema));

  db.drop_table("users", false)
    .await
    .expect("drop_table should succeed");

  let described_after_drop = db.describe_table("users");
  assert_eq!(described_after_drop, None);
}
