use db_engine::{EngineDatabase, EngineResult, StoreKey, StoreValue};
use db_in_memory::InMemoryBTree;
use db_sql_to_engine::{SchemaResolver, parse_and_translate};

pub type ExampleStore = InMemoryBTree<StoreKey, StoreValue>;
pub type ExampleDb = EngineDatabase<ExampleStore>;

pub async fn execute_sql(
  db: &mut ExampleDb,
  resolver: &dyn SchemaResolver,
  sql: &str,
) -> Result<EngineResult, String> {
  let q = parse_and_translate(sql, resolver).map_err(|e| e.to_string())?;
  db.execute(q).await.map_err(|e| format!("{:?}", e))
}
