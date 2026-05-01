// Example: register a table, insert a row (programmatic), then run a SQL SELECT
use futures::executor::block_on;
use db_engine::{ColumnSchema, EngineType, TableSchema};
use db::Database;

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

    // Insert using SQL via the facade
    db.execute_sql("INSERT INTO items (id, name) VALUES (1, 'One');")
      .await
      .expect("insert");

    // Run SELECT via SQL using the facade
    let res = db
      .execute_sql("SELECT id, name FROM items WHERE id = 1;")
      .await
      .expect("execute select");

    println!("Found {} rows", res.rows.len());
    for row in res.rows {
      println!("row: {:?}", row);
    }
  });
}
