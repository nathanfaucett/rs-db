// Simple quickstart: register a table, insert a row, and select it.
use futures::executor::block_on;

use db_engine::{
  ColumnSchema, EngineDatabase, EnginePredicate, EngineQuery, EngineType, EngineValue, TableSchema,
};
use db_in_memory::InMemoryNamedBTree;

fn main() {
  block_on(async {
    let store: InMemoryNamedBTree<_, _> = InMemoryNamedBTree::new();
    let mut db = EngineDatabase::new(store);

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

    db.register_table(schema).await.expect("register table");

    db.execute(EngineQuery::Insert {
      table: "items".into(),
      row: vec![EngineValue::Integer(1), EngineValue::Text("One".into())],
    })
    .await
    .expect("insert");

    let res = db
      .execute(EngineQuery::select_simple(
        "items".into(),
        vec![1],
        Some(EnginePredicate::Equals(0, EngineValue::Integer(1))),
      ))
      .await
      .expect("select");

    println!("Found {} rows", res.rows.len());
    for row in res.rows {
      println!("row: {:?}", row);
    }
  });
}
