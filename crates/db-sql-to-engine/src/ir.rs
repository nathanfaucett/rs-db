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

#[derive(Clone, Debug)]
pub enum DdlOp {
  CreateTable(db_engine::TableSchema),
  DropTable(String),
  CreateIndex(db_engine::IndexSchema),
  DropIndex(String),
}

#[derive(Clone, Debug)]
pub enum CanonicalStatement {
  Query(db_engine::EngineQuery),
  Ddl(DdlOp),
}

impl From<db_engine::EngineQuery> for CanonicalStatement {
  fn from(eq: db_engine::EngineQuery) -> Self {
    CanonicalStatement::Query(eq)
  }
}

impl From<DdlOp> for CanonicalStatement {
  fn from(op: DdlOp) -> Self {
    CanonicalStatement::Ddl(op)
  }
}
