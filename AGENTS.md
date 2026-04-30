# AGENTS.md

## Overview

- Monorepo layout: crates/ (Rust libraries), docs/ (design, spec, examples).
- Core choices: default to `no_std` where possible; use GitHub Actions for CI.

## Build & Tooling

- Format: `cargo fmt --all -- --check`
- Lint: `cargo clippy -D warnings` (with and without default features)
- Build: `cargo build -p <crate> --no-default-features --all-targets`
- Build with alloc: `cargo build -p <crate> --no-default-features --features alloc --all-targets`
- Test: `cargo test -p <crate> --no-default-features`

## Formatting & Linting

- Run `cargo fmt --all` using `rustfmt.toml` formatting rules.
- Enforce `clippy` policy via `cargo clippy -D warnings` and `clippy.toml` as needed.

## Patterns & Conventions

- Default to `no_std`; enable `std` only when required (IO, threading, async runtimes).
- In `no_std` crates, use `hashbrown` for hash maps and BTree maps/sets.
- Group Rust imports by namespace and order: std/core/alloc → external → internal (`crate`, `super`).
- Prefer explicit imports and minimal dependencies.
- Avoid glob imports and hard-coded absolute paths.
- No non-essential comments; prefer refactoring over comments.

## Module Organization

- Keep modules minimal: `mod.rs` should only declare/re-export modules.
- `lib.rs` and `mod.rs` must stay thin: crate-level attributes, `extern crate`, `pub use`, and `mod` declarations only.
- Do not place implementation logic (structs, enums, `impl` blocks, free `fn`, constants, statics, type aliases, traits) directly in `lib.rs` or `mod.rs`.
- If functionality is missing, create/update a sibling module (for example, `src/node.rs`) and wire it from `lib.rs`/`mod.rs`.
- When adding behavior, create/update a dedicated module file (`src/<feature>.rs`) and re-export as needed.

## Dependencies & Versioning

- All new dependencies must use only `major.minor` in the version field.
- Specify dependencies in full table form with `default-features = false` and enable only required features.
- Group dependencies by logical category with a comment header, alphabetize within groups, and separate groups with a blank line.

## Public API & Documentation

- Public APIs must include doc comments or examples.
- Pass paths/config via environment/adapters; never hard-code paths or configuration.

## Testing & Examples

- Add or update tests/examples when behavior changes.
- Keep small unit tests with the implementation file (for example, `src/foo.rs` contains `#[cfg(test)] mod tests { ... }`).
- Integration tests should live under `tests/` (crate-level) or `crates/<crate>/tests/`.

## Contribution & PR Guidance

- PRs (including agent-generated changes) that place implementation code in `lib.rs`/`mod.rs` should be treated as non-compliant and rewritten before merge.
- Follow the Build & Tooling commands and formatting/lint checks in CI before submitting PRs.
