/// Adapter trait to lower a `CanonicalQuery` to a backend-specific representation.
pub trait EngineAdapter {
  type Output;
  fn lower(
    &self,
    cq: &crate::ir::CanonicalQuery,
  ) -> Result<Self::Output, crate::translate::TranslateError>;
}

#[derive(Clone, Debug)]
pub struct DbEngineAdapter;

impl EngineAdapter for DbEngineAdapter {
  type Output = db_engine::EngineQuery;

  fn lower(
    &self,
    cq: &crate::ir::CanonicalQuery,
  ) -> Result<Self::Output, crate::translate::TranslateError> {
    // For the initial scaffold the canonical query simply wraps an
    // `EngineQuery`; lowering returns the wrapped value.
    Ok(cq.engine_query.clone())
  }
}
