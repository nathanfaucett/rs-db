use futures::executor::block_on;

use db::Database;

#[test]
fn facade_smoke() {
  block_on(async {
    let mut db = Database::open_in_memory().await.expect("open in-memory");

    let schema = db_engine::TableSchema {
      name: "users".into(),
      columns: vec![
        db_engine::ColumnSchema {
          name: "id".into(),
          data_type: db_engine::EngineType::Integer,
        },
        db_engine::ColumnSchema {
          name: "name".into(),
          data_type: db_engine::EngineType::Text,
        },
      ],
      primary_key: vec![0],
    };

    db.register_table(schema).await.expect("register users");

    db.execute_query(db_engine::EngineQuery::Insert {
      table: "users".into(),
      row: vec![
        db_engine::EngineValue::Integer(1),
        db_engine::EngineValue::Text("Alice".into()),
      ],
    })
    .await
    .expect("execute insert");

    let result = db
      .execute_query(db_engine::EngineQuery::select_simple(
        "users".into(),
        vec![1],
        Some(db_engine::EnginePredicate::Equals(
          0,
          db_engine::EngineValue::Integer(1),
        )),
      ))
      .await
      .expect("execute select");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
      result.rows[0],
      vec![db_engine::EngineValue::Text("Alice".into())]
    );
  });
}
