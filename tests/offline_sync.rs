#[cfg(feature = "automerge")]
mod tests {
  use db::Database;
  use db_core::NamedTreeProvider;
  use db_engine::{
    EngineKey, EngineQuery, EngineRow, EngineValue, QualifiedColumn, QualifiedOperand,
    QualifiedPredicate, UpdateAssignment,
  };
  use futures::executor::block_on;

  #[cfg(feature = "redb")]
  use std::fs;
  #[cfg(feature = "redb")]
  use std::path::PathBuf;
  #[cfg(feature = "redb")]
  use std::sync::atomic::{AtomicU64, Ordering};
  #[cfg(feature = "redb")]
  use std::time::{SystemTime, UNIX_EPOCH};

  #[cfg(feature = "redb")]
  fn temp_redb_path(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system time after unix epoch")
      .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    path.push(format!(
      "aicacia_automerge_sync_{}_{}_{}.db",
      label, nanos, id
    ));
    path
  }

  fn expected_user_rows() -> Vec<Vec<EngineValue>> {
    vec![
      vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())],
      vec![EngineValue::Integer(2), EngineValue::Text("Bob".into())],
    ]
  }

  trait SyncStore: Clone + NamedTreeProvider<EngineKey, EngineRow> + Send + Sync + 'static {}

  impl<T> SyncStore for T where
    T: Clone + NamedTreeProvider<EngineKey, EngineRow> + Send + Sync + 'static
  {
  }

  fn sort_rows(rows: &mut [Vec<EngineValue>]) {
    rows.sort_by(|l, r| format!("{:?}", l).cmp(&format!("{:?}", r)));
  }

  async fn select_sorted<S>(db: &mut Database<S>, sql: &str) -> Vec<Vec<EngineValue>>
  where
    S: SyncStore,
  {
    let mut rows = db.execute_sql(sql).await.expect("select rows").rows;
    sort_rows(&mut rows);
    rows
  }

  async fn assert_query_rows_match<S>(
    left: &mut Database<S>,
    right: &mut Database<S>,
    sql: &str,
    expected: Vec<Vec<EngineValue>>,
  ) where
    S: SyncStore,
  {
    let left_rows = select_sorted(left, sql).await;
    let right_rows = select_sorted(right, sql).await;

    assert_eq!(left_rows, expected);
    assert_eq!(right_rows, expected);
    assert_eq!(left_rows, right_rows);
  }

  async fn assert_user_rows_match<S>(left: &mut Database<S>, right: &mut Database<S>)
  where
    S: SyncStore,
  {
    assert_query_rows_match(
      left,
      right,
      "SELECT id, name FROM users;",
      expected_user_rows(),
    )
    .await;
  }

  fn eq_pred(table: &str, column_index: usize, value: EngineValue) -> QualifiedPredicate {
    QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: table.into(),
        column_index,
      }),
      QualifiedOperand::Value(value),
    )
  }

  macro_rules! run_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users on left");

      $left.sync_with(&mut $right).await.expect("first sync");

      $right
        .execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
        .await
        .expect("insert bob on right");
      $left
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .await
        .expect("insert alice on left");

      $left.sync_with(&mut $right).await.expect("second sync");
      assert_user_rows_match(&mut $left, &mut $right).await;

      $left.sync_with(&mut $right).await.expect("third sync");
      assert_user_rows_match(&mut $left, &mut $right).await;
    }};
  }

  macro_rules! run_lifecycle_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, level INT);")
        .await
        .expect("create users on left");
      $left
        .execute_sql("CREATE TABLE teams (id INT PRIMARY KEY, title TEXT);")
        .await
        .expect("create teams on left");

      $left
        .sync_with(&mut $right)
        .await
        .expect("sync initial schema");

      $left
        .execute_sql("INSERT INTO users (id, name, level) VALUES (1, 'Alice', 10);")
        .await
        .expect("insert alice on left");
      $left
        .execute_sql("INSERT INTO teams (id, title) VALUES (1, 'Core');")
        .await
        .expect("insert core team on left");

      $right
        .execute_sql("INSERT INTO users (id, name, level) VALUES (2, 'Bob', 20);")
        .await
        .expect("insert bob on right");
      $right
        .execute_sql("INSERT INTO teams (id, title) VALUES (2, 'Infra');")
        .await
        .expect("insert infra team on right");

      $left
        .sync_with(&mut $right)
        .await
        .expect("sync divergent inserts");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, level FROM users;",
        vec![
          vec![
            EngineValue::Integer(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(10),
          ],
          vec![
            EngineValue::Integer(2),
            EngineValue::Text("Bob".into()),
            EngineValue::Integer(20),
          ],
        ],
      )
      .await;
      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, title FROM teams;",
        vec![
          vec![EngineValue::Integer(1), EngineValue::Text("Core".into())],
          vec![EngineValue::Integer(2), EngineValue::Text("Infra".into())],
        ],
      )
      .await;

      $left
        .execute_sql("INSERT INTO users (id, name, level) VALUES (3, 'Cara', 30);")
        .await
        .expect("insert cara on left");
      $left
        .execute_sql("INSERT INTO teams (id, title) VALUES (3, 'Ops');")
        .await
        .expect("insert ops team on left");
      $right
        .execute_sql("INSERT INTO users (id, name, level) VALUES (4, 'Dan', 40);")
        .await
        .expect("insert dan on right");

      $left
        .sync_with(&mut $right)
        .await
        .expect("sync second inserts");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, level FROM users;",
        vec![
          vec![
            EngineValue::Integer(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(10),
          ],
          vec![
            EngineValue::Integer(2),
            EngineValue::Text("Bob".into()),
            EngineValue::Integer(20),
          ],
          vec![
            EngineValue::Integer(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(30),
          ],
          vec![
            EngineValue::Integer(4),
            EngineValue::Text("Dan".into()),
            EngineValue::Integer(40),
          ],
        ],
      )
      .await;
      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, title FROM teams;",
        vec![
          vec![EngineValue::Integer(1), EngineValue::Text("Core".into())],
          vec![EngineValue::Integer(2), EngineValue::Text("Infra".into())],
          vec![EngineValue::Integer(3), EngineValue::Text("Ops".into())],
        ],
      )
      .await;

      $right
        .execute_sql("CREATE TABLE profiles (id INT PRIMARY KEY, alias TEXT, status TEXT);")
        .await
        .expect("create profiles on right");
      $right
        .sync_with(&mut $left)
        .await
        .expect("sync profiles schema");

      $left
        .execute_sql("INSERT INTO profiles (id, alias, status) VALUES (7, 'Dora', 'active');")
        .await
        .expect("insert dora on left");
      $right
        .execute_sql("INSERT INTO profiles (id, alias, status) VALUES (8, 'Eli', 'offline');")
        .await
        .expect("insert eli on right");
      $left
        .sync_with(&mut $right)
        .await
        .expect("sync profiles rows");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, alias, status FROM profiles;",
        vec![
          vec![
            EngineValue::Integer(7),
            EngineValue::Text("Dora".into()),
            EngineValue::Text("active".into()),
          ],
          vec![
            EngineValue::Integer(8),
            EngineValue::Text("Eli".into()),
            EngineValue::Text("offline".into()),
          ],
        ],
      )
      .await;

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT status, id FROM profiles;",
        vec![
          vec![EngineValue::Text("active".into()), EngineValue::Integer(7)],
          vec![EngineValue::Text("offline".into()), EngineValue::Integer(8)],
        ],
      )
      .await;

      $left.sync_with(&mut $right).await.expect("no-op sync one");
      $right.sync_with(&mut $left).await.expect("no-op sync two");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, alias, status FROM profiles;",
        vec![
          vec![
            EngineValue::Integer(7),
            EngineValue::Text("Dora".into()),
            EngineValue::Text("active".into()),
          ],
          vec![
            EngineValue::Integer(8),
            EngineValue::Text("Eli".into()),
            EngineValue::Text("offline".into()),
          ],
        ],
      )
      .await;
    }};
  }

  macro_rules! run_mutation_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);")
        .await
        .expect("create users on left");
      $left
        .sync_with(&mut $right)
        .await
        .expect("sync users schema");

      $left
        .execute_sql("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 10);")
        .await
        .expect("insert alice on left");
      $left
        .execute_sql("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 20);")
        .await
        .expect("insert bob on left");
      $left
        .execute_sql("INSERT INTO users (id, name, score) VALUES (3, 'Cara', 30);")
        .await
        .expect("insert cara on left");

      $left.sync_with(&mut $right).await.expect("sync seed rows");

      $left
        .execute_query(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(2, EngineValue::Integer(11))],
          predicate: Some(eq_pred("users", 0, EngineValue::Integer(1))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update score for alice on left");

      $left
        .sync_with(&mut $right)
        .await
        .expect("sync sequential update");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            EngineValue::Integer(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(11),
          ],
          vec![
            EngineValue::Integer(2),
            EngineValue::Text("Bob".into()),
            EngineValue::Integer(20),
          ],
          vec![
            EngineValue::Integer(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(30),
          ],
        ],
      )
      .await;

      $right
        .execute_query(EngineQuery::Delete {
          table: "users".into(),
          predicate: Some(eq_pred("users", 0, EngineValue::Integer(2))),
          returning: None,
        })
        .await
        .expect("delete bob on right");

      $right
        .sync_with(&mut $left)
        .await
        .expect("sync sequential delete");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            EngineValue::Integer(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(11),
          ],
          vec![
            EngineValue::Integer(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(30),
          ],
        ],
      )
      .await;

      $left
        .execute_query(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(
            1,
            EngineValue::Text("Alice L".into()),
          )],
          predicate: Some(eq_pred("users", 0, EngineValue::Integer(1))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update alice name on left");

      $right
        .execute_query(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(2, EngineValue::Integer(31))],
          predicate: Some(eq_pred("users", 0, EngineValue::Integer(3))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update cara score on right");

      $left
        .sync_with(&mut $right)
        .await
        .expect("sync divergent non-overlapping updates");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            EngineValue::Integer(1),
            EngineValue::Text("Alice L".into()),
            EngineValue::Integer(11),
          ],
          vec![
            EngineValue::Integer(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(31),
          ],
        ],
      )
      .await;

      $left
        .sync_with(&mut $right)
        .await
        .expect("mutation no-op sync one");
      $right
        .sync_with(&mut $left)
        .await
        .expect("mutation no-op sync two");

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            EngineValue::Integer(1),
            EngineValue::Text("Alice L".into()),
            EngineValue::Integer(11),
          ],
          vec![
            EngineValue::Integer(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(31),
          ],
        ],
      )
      .await;
    }};
  }

  macro_rules! run_efficiency_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users on left");

      for i in 1..=8 {
        $left
          .execute_sql(&format!(
            "INSERT INTO users (id, name) VALUES ({i}, 'user_{i}');"
          ))
          .await
          .expect("insert seed user on left");
      }

      $left
        .sync_with(&mut $right)
        .await
        .expect("sync seed dataset");

      let left_before = $left
        .automerge_sync_metrics()
        .await
        .expect("collect left metrics before no-op sync");
      let right_before = $right
        .automerge_sync_metrics()
        .await
        .expect("collect right metrics before no-op sync");

      $left
        .sync_with(&mut $right)
        .await
        .expect("first no-op sync");
      $right
        .sync_with(&mut $left)
        .await
        .expect("second no-op sync");

      let left_after = $left
        .automerge_sync_metrics()
        .await
        .expect("collect left metrics after no-op sync");
      let right_after = $right
        .automerge_sync_metrics()
        .await
        .expect("collect right metrics after no-op sync");

      assert_eq!(left_after.document_count, left_before.document_count);
      assert_eq!(right_after.document_count, right_before.document_count);
      assert!(left_after.total_document_bytes <= left_before.total_document_bytes + 128);
      assert!(right_after.total_document_bytes <= right_before.total_document_bytes + 128);

      $left
        .execute_sql("INSERT INTO users (id, name) VALUES (100, 'new_user');")
        .await
        .expect("insert one delta row on left");
      $left
        .sync_with(&mut $right)
        .await
        .expect("sync one-row delta");

      let left_delta = $left
        .automerge_sync_metrics()
        .await
        .expect("collect left metrics after delta sync");
      let right_delta = $right
        .automerge_sync_metrics()
        .await
        .expect("collect right metrics after delta sync");

      assert!(left_delta.document_count >= left_after.document_count);
      assert!(right_delta.document_count >= right_after.document_count);
      assert!(left_delta.total_document_bytes >= left_after.total_document_bytes);
      assert!(right_delta.total_document_bytes >= right_after.total_document_bytes);
    }};
  }

  async fn offline_sync_in_memory_scenario() {
    let mut left = Database::open_automerge_in_memory()
      .await
      .expect("open left in-memory db");
    let mut right = Database::open_automerge_in_memory()
      .await
      .expect("open right in-memory db");

    run_sync_scenario!(left, right);
  }

  #[test]
  fn automerge_in_memory_offline_sync_converges() {
    block_on(async {
      offline_sync_in_memory_scenario().await;
    });
  }

  #[test]
  fn automerge_in_memory_offline_sync_lifecycle_converges() {
    block_on(async {
      let mut left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let mut right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      run_lifecycle_sync_scenario!(left, right);
    });
  }

  #[test]
  fn automerge_in_memory_offline_sync_mutations_converge() {
    block_on(async {
      let mut left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let mut right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      run_mutation_sync_scenario!(left, right);
    });
  }

  #[cfg(feature = "redb")]
  async fn offline_sync_redb_scenario() {
    let left_path = temp_redb_path("left");
    let right_path = temp_redb_path("right");

    let mut left = Database::open_automerge_with_redb(&left_path, "automerge_store")
      .await
      .expect("open left redb db");
    let mut right = Database::open_automerge_with_redb(&right_path, "automerge_store")
      .await
      .expect("open right redb db");

    run_sync_scenario!(left, right);

    let _ = fs::remove_file(left_path);
    let _ = fs::remove_file(right_path);
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_converges() {
    block_on(async {
      offline_sync_redb_scenario().await;
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_lifecycle_converges() {
    block_on(async {
      let left_path = temp_redb_path("left_lifecycle");
      let right_path = temp_redb_path("right_lifecycle");

      let mut left = Database::open_automerge_with_redb(&left_path, "automerge_store")
        .await
        .expect("open left redb db");
      let mut right = Database::open_automerge_with_redb(&right_path, "automerge_store")
        .await
        .expect("open right redb db");

      run_lifecycle_sync_scenario!(left, right);

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_mutations_converge() {
    block_on(async {
      let left_path = temp_redb_path("left_mutations");
      let right_path = temp_redb_path("right_mutations");

      let mut left = Database::open_automerge_with_redb(&left_path, "automerge_store")
        .await
        .expect("open left redb db");
      let mut right = Database::open_automerge_with_redb(&right_path, "automerge_store")
        .await
        .expect("open right redb db");

      run_mutation_sync_scenario!(left, right);

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }

  #[test]
  fn automerge_in_memory_offline_sync_noop_is_data_efficient() {
    block_on(async {
      let mut left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let mut right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      run_efficiency_sync_scenario!(left, right);
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_noop_is_data_efficient() {
    block_on(async {
      let left_path = temp_redb_path("left_efficiency");
      let right_path = temp_redb_path("right_efficiency");

      let mut left = Database::open_automerge_with_redb(&left_path, "automerge_store")
        .await
        .expect("open left redb db");
      let mut right = Database::open_automerge_with_redb(&right_path, "automerge_store")
        .await
        .expect("open right redb db");

      run_efficiency_sync_scenario!(left, right);

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }
}
