use futures::executor::block_on;

use db_engine::{
  ColumnSchema, EngineDatabase, EngineQuery, EngineType, EngineValue, QualifiedColumn,
  QualifiedOperand, QualifiedPredicate, TableSchema,
};
use db_in_memory::InMemoryNamedBTree;

#[test]
fn engine_works_with_mock_btree() {
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
        Some(QualifiedPredicate::Equals(
          QualifiedOperand::Column(QualifiedColumn {
            table: "items".into(),
            column_index: 0,
          }),
          QualifiedOperand::Value(EngineValue::Integer(1)),
        )),
      ))
      .await
      .expect("select");

    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0], vec![EngineValue::Text("One".into())]);
  });
}
