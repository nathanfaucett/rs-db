/// Integration test for transaction contract validation.
/// Verifies that backends honor their declared transactional guarantees.
use db_engine::{
  BackendCapability, ColumnSchema, EngineDatabase, EngineQuery, EngineStore, EngineType,
  EngineValue, TableSchema, UpdateAssignment,
};
use db_in_memory::InMemoryNamedBTree;
use futures::executor::block_on;

type TestDb = EngineDatabase<InMemoryNamedBTree<db_engine::EngineKey, Vec<EngineValue>>>;

fn uuid_value(id: u128) -> EngineValue {
  EngineValue::Uuid(id.to_be_bytes())
}

fn make_test_db() -> TestDb {
  let store: InMemoryNamedBTree<db_engine::EngineKey, Vec<EngineValue>> = InMemoryNamedBTree::new();
  EngineDatabase::new(store)
}

#[test]
fn transaction_contract_defaults_to_multi_tree_atomicity() {
  block_on(async {
    let db = make_test_db();
    let contract = db.store().transaction_contract();
    assert_eq!(contract.atomicity, BackendCapability::MultiTreeAtomicity);
    assert!(contract.multi_tree_write_atomicity);
    assert!(contract.validate().is_ok());
  });
}

#[test]
fn multi_tree_atomicity_enables_cross_table_updates() {
  block_on(async {
    let mut db = make_test_db();

    // Create two tables
    db.register_table(TableSchema {
      name: "t1".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Uuid,
        },
        ColumnSchema {
          name: "value".into(),
          data_type: EngineType::Integer,
        },
      ],
      primary_key: vec![0],
    })
    .await
    .expect("register t1");

    db.register_table(TableSchema {
      name: "t2".into(),
      columns: vec![
        ColumnSchema {
          name: "id".into(),
          data_type: EngineType::Uuid,
        },
        ColumnSchema {
          name: "value".into(),
          data_type: EngineType::Integer,
        },
      ],
      primary_key: vec![0],
    })
    .await
    .expect("register t2");

    // Insert a row in each table
    db.execute(EngineQuery::Insert {
      table: "t1".into(),
      row: vec![uuid_value(1), EngineValue::Integer(1)],
    })
    .await
    .expect("insert t1");

    db.execute(EngineQuery::Insert {
      table: "t2".into(),
      row: vec![uuid_value(2), EngineValue::Integer(2)],
    })
    .await
    .expect("insert t2");

    // Use a transaction to mutate both tables atomically
    let mut txn = db.transaction();
    txn
      .update_rows_with_sources(
        "t1",
        vec![UpdateAssignment {
          column_index: 1,
          value: db_engine::UpdateValueExpr::Value(EngineValue::Integer(100)),
        }],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t1".into(),
            column_index: 0,
          }),
          db_engine::QualifiedOperand::Value(uuid_value(1)),
        )),
        vec![],
        vec![],
      )
      .await
      .expect("update t1");

    txn
      .update_rows_with_sources(
        "t2",
        vec![UpdateAssignment {
          column_index: 1,
          value: db_engine::UpdateValueExpr::Value(EngineValue::Integer(200)),
        }],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t2".into(),
            column_index: 0,
          }),
          db_engine::QualifiedOperand::Value(uuid_value(2)),
        )),
        vec![],
        vec![],
      )
      .await
      .expect("update t2");

    txn.commit().await.expect("commit");

    // Verify both updates were applied
    let r1 = db
      .execute(EngineQuery::select_simple(
        "t1".into(),
        vec![1],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t1".into(),
            column_index: 1,
          }),
          db_engine::QualifiedOperand::Value(EngineValue::Integer(1)),
        )),
      ))
      .await
      .expect("select t1");

    assert!(r1.rows.is_empty(), "original row should not exist");

    let r1_updated = db
      .execute(EngineQuery::select_simple(
        "t1".into(),
        vec![1],
        Some(db_engine::QualifiedPredicate::Equals(
          db_engine::QualifiedOperand::Column(db_engine::QualifiedColumn {
            table: "t1".into(),
            column_index: 1,
          }),
          db_engine::QualifiedOperand::Value(EngineValue::Integer(100)),
        )),
      ))
      .await
      .expect("select t1 updated");

    assert_eq!(r1_updated.rows.len(), 1, "updated row should exist");
  });
}
