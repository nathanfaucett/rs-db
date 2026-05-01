/// Provides a minimal read-only view of table schemas used by SQL translation.
pub trait SchemaResolver {
  fn describe_table(&self, name: &str) -> Option<crate::TableSchema>;
}
