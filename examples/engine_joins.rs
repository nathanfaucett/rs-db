// Example: register two tables, insert rows, and run a SQL JOIN select
use futures::executor::block_on;
use std::collections::HashMap;

use db::Database;
use db_engine::{ColumnSchema, EngineType, TableSchema};
use db_sql_to_engine::SchemaResolver;

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

    let users = TableSchema {
      name: "users".into(),
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

    let orders = TableSchema {
      name: "orders".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "user_id".into(),
          data_type: EngineType::Integer,
        },
        ColumnSchema {
          name: "amount".into(),
          data_type: EngineType::Integer,
        },
      ],
      primary_key: vec![0],
    };

    db.register_table(users.clone())
      .await
      .expect("register users");
    db.register_table(orders.clone())
      .await
      .expect("register orders");

    // Build resolver up-front (used by the SQL helper)
    let mut tables = HashMap::new();
    tables.insert("users".to_string(), users.clone());
    tables.insert("orders".to_string(), orders.clone());
    let resolver = TestResolver { tables };

    // Insert some users via SQL using the facade
    db.execute_sql(
      &resolver,
      "INSERT INTO users (id, name) VALUES (1, 'Alice');",
    )
    .await
    .expect("insert user 1");
    db.execute_sql(&resolver, "INSERT INTO users (id, name) VALUES (2, 'Bob');")
      .await
      .expect("insert user 2");

    // Insert some orders via SQL using the facade
    db.execute_sql(
      &resolver,
      "INSERT INTO orders (id, user_id, amount) VALUES (1,1,100);",
    )
    .await
    .expect("insert order 1");
    db.execute_sql(
      &resolver,
      "INSERT INTO orders (id, user_id, amount) VALUES (2,2,200);",
    )
    .await
    .expect("insert order 2");

    // Build resolver
    let mut tables = HashMap::new();
    tables.insert("users".to_string(), users.clone());
    tables.insert("orders".to_string(), orders.clone());
    let resolver = TestResolver { tables };

    let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
    let res = db
      .execute_sql(&resolver, sql)
      .await
      .expect("execute select");

    println!("Joined rows: {}", res.rows.len());
    for row in res.rows {
      println!("row: {:?}", row);
    }
  });
}
