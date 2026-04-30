# Examples Index

This file lists runnable examples and exact commands to run them.

## db-engine examples

- **engine_simple** — quickstart: register a table, insert a row, select it.

  ```bash
  cargo run -p db-engine --example engine_simple
  ```

- **engine_joins** — demo: register two tables, insert rows, perform an INNER JOIN and print results.

  ```bash
  cargo run -p db-engine --example engine_joins
  ```

- **codec_roundtrip** — demo: encode/decode a `StoreValue::Row` using the crate codecs.

  ```bash
  cargo run -p db-engine --example codec_roundtrip
  ```

## Notes

- `db-engine` enables the `std` feature by default. If you run examples directly in other crates that opt-out of `std`, you may need `--features std`.
- Examples are intentionally small; if test-only helpers are required later we can expose a small `examples` feature or move minimal helpers into the public API.
