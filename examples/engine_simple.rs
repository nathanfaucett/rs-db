// Example: register a table, insert a row (programmatic), then run a SQL SELECT
use futures::executor::block_on;
use std::collections::HashMap;

use db_engine::{ColumnSchema, EngineType, TableSchema};
use db_sql_to_engine::SchemaResolver;
use db::Database;

struct TestResolver {
  tables: HashMap<String, db_engine::TableSchema>,
}

impl SchemaResolver for TestResolver {
  fn describe_table(&self, name: &str) -> Option<db_engine::TableSchema> {
    self.tables.get(name).cloned()
  }
}

fn main() {
  block_on(async {
    let mut db = Database::open_in_memory().await.expect("open in-memory db");

    let schema = TableSchema {
      name: "items".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "name".into(),
          data_type: EngineType::Text,
        },
      ],
      primary_key: vec![0],
    };

    db.register_table(schema.clone())
      .await
      .expect("register table");

    // Build a resolver for the translator using the registered schema
    let mut tables = HashMap::new();
    tables.insert("items".to_string(), schema.clone());
    let resolver = TestResolver { tables };

    // Insert using SQL via the facade
    db.execute_sql(&resolver, "INSERT INTO items (id, name) VALUES (1, 'One');")
      .await
      .expect("insert");

    // Run SELECT via SQL using the facade
    let res = db
      .execute_sql(&resolver, "SELECT id, name FROM items WHERE id = 1;")
      .await
      .expect("execute select");

    println!("Found {} rows", res.rows.len());
    for row in res.rows {
      println!("row: {:?}", row);
    }
  });
}
