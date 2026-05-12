# @athena/wasm

WASM bridge for Athena DB in browser environments.

## Build

```bash
just wasm
```

This runs `wasm-pack` and writes the generated package directly to `pkg/`.

## Imports

```ts
import init from "db-wasm";
import { BrowserDatabase, type StoreAdapter } from "db-wasm";
import { translate_sql_to_query, translate_sql_to_statement } from "db-wasm";
import type {
  EngineQuery,
  QualifiedPredicate,
  TableSchema,
  EngineValue,
} from "db-wasm";
```

## Minimal Usage

```ts
import init from "db-wasm";
import { BrowserDatabase } from "db-wasm";

await init();

const db = BrowserDatabase.open();

await db.executeSql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
await db.executeSql("INSERT INTO users (id, name) VALUES (1, 'Ada')");
const result = await db.executeSql("SELECT name FROM users WHERE id = 1");
console.log(result.rows);
```

## Notes

- This package exports a flat root API from the generated wasm bundle.
- `BrowserDatabase.open()` uses the built-in in-memory store.

## StoreAdapter

```ts
import { BrowserDatabase, type StoreAdapter } from "db-wasm";

const adapter: StoreAdapter = {
  async get(tree, key) {
    return undefined;
  },
  async insert(tree, key, value) {},
  async remove(tree, key) {
    return undefined;
  },
  async range(tree, range) {
    return [];
  },
  async commit(ops) {},
  async rollback() {},
};

const db = await BrowserDatabase.openWithBackend(adapter);
```
