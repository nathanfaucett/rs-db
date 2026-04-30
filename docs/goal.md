# Goal

Build a modular, async-first database foundation that cleanly separates logical storage adapters from byte-oriented persistent stores.

This project should make it easy to:

- define backend-agnostic table and index adapters as async traits,
- swap storage implementations without changing adapter logic,
- support both in-memory and durable persistence,
- expose minimal async transaction semantics for atomic operation sets.

The core idea is not to design a full SQL engine today, but to establish a flexible, composable storage architecture where adapters express data structure behavior and persistent stores provide raw byte-level durability. This foundation is intended to support a higher-level engine that can query and update data through those table and index abstractions.

Recent work has added a small, programmatic query layer in `crates/db-engine` that demonstrates how a relational surface can be layered on top of adapters. The initial implementation provides:

- a programmatic `EngineQuery::Select` API for qualified projections, joins, aggregates, `GROUP BY`, `ORDER BY`, and `LIMIT`/`OFFSET`;
- execution of inner, left, right, and full joins via a left-deep nested-loop executor;
- grouped aggregates (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) and explicit `NULL` semantics for outer joins;
- `ORDER BY` + `LIMIT` support and a set of unit tests that verify behavior.

This is intentionally a correctness-first, narrow surface: it is not a full SQL implementation and does not include cost-based planning, query rewriting, or advanced optimizations. Those capabilities can be added later by introducing a planner that emits logical plans which the engine can execute more efficiently.
