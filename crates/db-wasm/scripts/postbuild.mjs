import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const pkgDir = join(process.cwd(), "pkg");
const pkgJsonPath = join(pkgDir, "package.json");

const pkgJson = JSON.parse(readFileSync(pkgJsonPath, "utf8"));

pkgJson.name = "@athena/wasm";
pkgJson.description = "Athena DB WASM bridge";

const files = new Set(pkgJson.files ?? []);
for (const file of [
  "db.d.ts",
  "db.js",
  "engine.d.ts",
  "engine.js",
  "sql.d.ts",
  "sql.js",
  "types.d.ts",
  "types.js",
]) {
  files.add(file);
}

pkgJson.files = Array.from(files);
pkgJson.exports = {
  ".": {
    types: "./db_wasm.d.ts",
    import: "./db_wasm.js",
  },
  "./db": {
    types: "./db.d.ts",
    import: "./db.js",
  },
  "./sql": {
    types: "./sql.d.ts",
    import: "./sql.js",
  },
  "./engine": {
    types: "./engine.d.ts",
    import: "./engine.js",
  },
  "./types": {
    types: "./types.d.ts",
    import: "./types.js",
  },
};

writeFileSync(pkgJsonPath, `${JSON.stringify(pkgJson, null, 2)}\n`, "utf8");

writeFileSync(
  join(pkgDir, "db.js"),
  'export { BrowserDatabase } from "./db_wasm.js";\nexport { default as init, initSync } from "./db_wasm.js";\n',
  "utf8",
);

writeFileSync(
  join(pkgDir, "sql.js"),
  'export {\n  translate_sql_to_query,\n  translate_sql_to_statement,\n} from "./db_wasm.js";\nexport { default as init, initSync } from "./db_wasm.js";\n',
  "utf8",
);

writeFileSync(
  join(pkgDir, "engine.js"),
  'export { default as init, initSync } from "./db_wasm.js";\n',
  "utf8",
);

writeFileSync(
  join(pkgDir, "types.js"),
  'export { default as init, initSync } from "./db_wasm.js";\n',
  "utf8",
);

writeFileSync(
  join(pkgDir, "db.d.ts"),
  'export { BrowserDatabase } from "./db_wasm";\nexport { default as init, initSync } from "./db_wasm";\nexport type {\n  EngineQuery,\n  EngineResult,\n  TableSchema,\n  IndexSchema,\n  QualifiedPredicate,\n} from "./db_wasm";\n',
  "utf8",
);

writeFileSync(
  join(pkgDir, "sql.d.ts"),
  'export {\n  translate_sql_to_query,\n  translate_sql_to_statement,\n} from "./db_wasm";\nexport { default as init, initSync } from "./db_wasm";\nexport type { CanonicalStatement, EngineQuery, TableSchema } from "./db_wasm";\n',
  "utf8",
);

writeFileSync(
  join(pkgDir, "engine.d.ts"),
  'export { default as init, initSync } from "./db_wasm";\nexport type {\n  Aggregate,\n  EngineQuery,\n  EngineResult,\n  HavingPredicate,\n  JoinClause,\n  JoinKind,\n  JoinOn,\n  OrderBy,\n  QualifiedColumn,\n  QualifiedOperand,\n  QualifiedPredicate,\n  RefOrAgg,\n  SelectOptions,\n  SortDirection,\n  UpdateAssignment,\n  UpdateValueExpr,\n} from "./db_wasm";\n',
  "utf8",
);

writeFileSync(
  join(pkgDir, "types.d.ts"),
  'export { default as init, initSync } from "./db_wasm";\nexport type {\n  ColumnSchema,\n  EngineKey,\n  EngineType,\n  EngineValue,\n  IndexSchema,\n  StoreKey,\n  TableSchema,\n} from "./db_wasm";\n',
  "utf8",
);
