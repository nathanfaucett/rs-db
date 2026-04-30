// Canonical IR scaffold for the SQL translator.
// Currently this is a thin wrapper around the existing `EngineQuery` to
// make incremental refactors easier. Future work should replace this with
// a backend-agnostic canonical IR.

#[derive(Clone, Debug)]
pub struct CanonicalQuery {
  pub engine_query: db_engine::EngineQuery,
}

impl From<db_engine::EngineQuery> for CanonicalQuery {
  fn from(eq: db_engine::EngineQuery) -> Self {
    Self { engine_query: eq }
  }
}
