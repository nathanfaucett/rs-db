# @athena/wasm

WASM bridge for Athena DB in browser environments.

## Build

```bash
just wasm
```

This runs `wasm-pack` and writes the generated package directly to `pkg/`.

## Imports

```ts
import init from "@aicacia/db-wasm";
import { BrowserDatabase, type DatabaseEngineOptions } from "@aicacia/db-wasm";
import {
  type EngineKey,
  type EngineRow,
  type IndexStore,
  type PrimaryKeyEntry,
  type PrimaryKey,
  type PrimaryKeyRangeRequest,
  type PrimaryKeyStore,
  translate_sql_to_query,
  translate_sql_to_statement,
} from "@aicacia/db-wasm";
import type {
  EngineQuery,
  QualifiedPredicate,
  TableSchema,
  EngineValue,
} from "@aicacia/db-wasm";
```

## Minimal Usage

```ts
import init from "@aicacia/db-wasm";
import { BrowserDatabase } from "@aicacia/db-wasm";

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
import {
  BrowserDatabase,
  type DatabaseEngineOptions,
  type EngineKey,
  type EngineRow,
  type IndexEntry,
  type IndexStore,
  type PrimaryKey,
  type PrimaryKeyEntry,
  type PrimaryKeyRangeRequest,
  type PrimaryKeyStore,
} from "@aicacia/db-wasm";

const primaryKeyToString = (primaryKey: PrimaryKey): string => primaryKey.join(":");

class InMemoryPrimaryKeyStore implements PrimaryKeyStore {
  private readonly rowsByTable = new Map<string, Map<string, EngineRow>>();

  async get(table: string, primaryKey: PrimaryKey): Promise<EngineRow | undefined> {
    return this.rowsByTable.get(table)?.get(primaryKeyToString(primaryKey));
  }

  async put(table: string, primaryKey: PrimaryKey, row: EngineRow): Promise<void> {
    let tableRows = this.rowsByTable.get(table);
    if (!tableRows) {
      tableRows = new Map<string, EngineRow>();
      this.rowsByTable.set(table, tableRows);
    }
    tableRows.set(primaryKeyToString(primaryKey), row);
  }

  async delete(table: string, primaryKey: PrimaryKey): Promise<EngineRow | undefined> {
    const tableRows = this.rowsByTable.get(table);
    const key = primaryKeyToString(primaryKey);
    const previous = tableRows?.get(key);
    tableRows?.delete(key);
    return previous;
  }

  async range(table: string, _range: PrimaryKeyRangeRequest): Promise<PrimaryKeyEntry[]> {
    const tableRows = this.rowsByTable.get(table);
    if (!tableRows) {
      return [];
    }
    return Array.from(tableRows.entries()).map(([encodedPrimaryKey, row]) => ({
      primaryKey: encodedPrimaryKey.split(":").map((part) => Number.parseInt(part, 10)) as PrimaryKey,
      row,
    }));
  }
}

class InMemoryIndexStore implements IndexStore {
  private readonly entriesByIndex = new Map<string, IndexEntry[]>();

  async add(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> {
    const entries = this.entriesByIndex.get(index) ?? [];
    entries.push({ indexKey, rowPrimaryKey });
    this.entriesByIndex.set(index, entries);
  }

  async remove(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> {
    const entries = this.entriesByIndex.get(index) ?? [];
    this.entriesByIndex.set(
      index,
      entries.filter(
        (entry) =>
          JSON.stringify(entry.indexKey) !== JSON.stringify(indexKey) ||
          primaryKeyToString(entry.rowPrimaryKey) !== primaryKeyToString(rowPrimaryKey),
      ),
    );
  }

  async range(index: string): Promise<IndexEntry[]> {
    return this.entriesByIndex.get(index) ?? [];
  }
}

const options: DatabaseEngineOptions = {
  primaryKeyStore: new InMemoryPrimaryKeyStore(),
  indexStore: new InMemoryIndexStore(),
};

const db = await BrowserDatabase.openWithBackend(options);
```

### Recommended Adapter Shape

- Use `primaryKeyStore` for table row identity and row payload operations.
- Use `indexStore` for secondary index maintenance and lookups.
- Keep index keys as `EngineKey` (scalar for single-column indexes, tuple for composite indexes).

Use class-based stores as shown above for clearer ownership, testability, and strong typing.

### Legacy Compatibility

`DatabaseEngineOptions` still supports legacy tree-based callbacks (`get/insert/remove/range`) for backends that already implement them.
