# @athena/wasm

WASM bridge for Athena DB in browser environments.

## Build

```bash
just wasm
```

This runs `wasm-pack` and a postbuild step that writes subpath exports.

## Imports

```ts
import init from "@athena/wasm";
import { BrowserDatabase } from "@athena/wasm/db";
import {
  translate_sql_to_query,
  translate_sql_to_statement,
} from "@athena/wasm/sql";
import type { EngineQuery, QualifiedPredicate } from "@athena/wasm/engine";
import type { TableSchema, EngineValue } from "@athena/wasm/types";
```

## Minimal Usage

```ts
import init from "@athena/wasm";
import { BrowserDatabase } from "@athena/wasm/db";

await init();

const db = BrowserDatabase.open();

await db.registerTable({
  name: "users",
  columns: [
    { name: "id", data_type: "Integer" },
    { name: "name", data_type: "Text" },
  ],
  primary_key: [0],
});

await db.executeSql("INSERT INTO users (id, name) VALUES (1, 'Ada')");
const result = await db.executeSql("SELECT name FROM users WHERE id = 1");
console.log(result.rows);
```

## Notes

- This package exposes the in-memory engine-backed browser DB API.
- Backend implementations can be shipped in separate npm packages that consume this bridge.
