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
  type DatabaseTransaction,
  type EngineKey,
  type IndexRangeRequest,
  type PrimaryKeyEntry,
  type PrimaryKey,
  type PrimaryKeyRangeRequest,
  type RowBytes,
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

`DatabaseEngineOptions` now requires a single ACID boundary: `beginTransaction()` must return a transaction object with row, index, commit, and rollback methods.

```ts
import {
  BrowserDatabase,
  type DatabaseEngineOptions,
  type DatabaseTransaction,
  type EngineKey,
  type IndexEntry,
  type IndexRangeRequest,
  type PrimaryKey,
  type PrimaryKeyEntry,
  type PrimaryKeyRangeRequest,
  type RowBytes,
} from "@aicacia/db-wasm";

const primaryKeyToString = (primaryKey: PrimaryKey): string => primaryKey.join(":");

class InMemoryTx implements DatabaseTransaction {
  constructor(
    private readonly rowsByTable: Map<string, Map<string, RowBytes>>,
    private readonly entriesByIndex: Map<string, IndexEntry[]>,
    private readonly tableSchemas: Map<string, RowBytes>,
    private readonly indexSchemas: Map<string, RowBytes>,
  ) {}

  async getRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | undefined> {
    return this.rowsByTable.get(table)?.get(primaryKeyToString(primaryKey));
  }

  async putRow(table: string, primaryKey: PrimaryKey, row: RowBytes): Promise<void> {
    let tableRows = this.rowsByTable.get(table);
    if (!tableRows) {
      tableRows = new Map<string, RowBytes>();
      this.rowsByTable.set(table, tableRows);
    }
    tableRows.set(primaryKeyToString(primaryKey), row);
  }

  async deleteRow(table: string, primaryKey: PrimaryKey): Promise<RowBytes | undefined> {
    const tableRows = this.rowsByTable.get(table);
    const encodedPk = primaryKeyToString(primaryKey);
    const previous = tableRows?.get(encodedPk);
    tableRows?.delete(encodedPk);
    return previous;
  }

  async rangeRows(table: string, _range: PrimaryKeyRangeRequest): Promise<PrimaryKeyEntry[]> {
    const tableRows = this.rowsByTable.get(table);
    if (!tableRows) {
      return [];
    }
    return Array.from(tableRows.entries()).map(([encodedPk, row]) => ({
      primaryKey: encodedPk.split(":").map((part) => Number.parseInt(part, 10)) as PrimaryKey,
      row,
    }));
  }

  async addIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> {
    const entries = this.entriesByIndex.get(index) ?? [];
    entries.push({ indexKey, rowPrimaryKey });
    this.entriesByIndex.set(index, entries);
  }

  async removeIndex(index: string, indexKey: EngineKey, rowPrimaryKey: PrimaryKey): Promise<void> {
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

  async rangeIndex(index: string, _range: IndexRangeRequest): Promise<IndexEntry[]> {
    return this.entriesByIndex.get(index) ?? [];
  }

  async getTableSchema(table: string): Promise<RowBytes | undefined> {
    return this.tableSchemas.get(table);
  }

  async putTableSchema(table: string, row: RowBytes): Promise<void> {
    this.tableSchemas.set(table, row);
  }

  async deleteTableSchema(table: string): Promise<RowBytes | undefined> {
    const previous = this.tableSchemas.get(table);
    this.tableSchemas.delete(table);
    return previous;
  }

  async rangeTableSchemas(): Promise<Array<{ table: string; row: RowBytes }>> {
    return Array.from(this.tableSchemas.entries()).map(([table, row]) => ({ table, row }));
  }

  async getIndexSchema(index: string): Promise<RowBytes | undefined> {
    return this.indexSchemas.get(index);
  }

  async putIndexSchema(index: string, row: RowBytes): Promise<void> {
    this.indexSchemas.set(index, row);
  }

  async deleteIndexSchema(index: string): Promise<RowBytes | undefined> {
    const previous = this.indexSchemas.get(index);
    this.indexSchemas.delete(index);
    return previous;
  }

  async rangeIndexSchemas(): Promise<Array<{ index: string; row: RowBytes }>> {
    return Array.from(this.indexSchemas.entries()).map(([index, row]) => ({ index, row }));
  }

  async commit(): Promise<void> {}

  async rollback(): Promise<void> {}
}

const rowsByTable = new Map<string, Map<string, RowBytes>>();
const entriesByIndex = new Map<string, IndexEntry[]>();
const tableSchemas = new Map<string, RowBytes>();
const indexSchemas = new Map<string, RowBytes>();

const options: DatabaseEngineOptions = {
  async beginTransaction() {
    return new InMemoryTx(rowsByTable, entriesByIndex, tableSchemas, indexSchemas);
  },
};

const db = await BrowserDatabase.openWithBackend(options);
```

### Required Guarantees

- All adapter methods execute through a transaction object returned by `beginTransaction()`.
- Schema operations are transaction-bound and mandatory.
- `commit()` and `rollback()` are mandatory.
- There are no legacy tree callbacks and no non-transactional fallback paths.
