#[cfg(feature = "automerge")]
mod tests {
  use db::Database;
  use db_core::NamedTreeProvider;
  use db_engine::{EngineKey, EngineRow, EngineValue};
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

  async fn select_sorted_users<S>(db: &mut Database<S>) -> Vec<Vec<EngineValue>>
  where
    S: SyncStore,
  {
    let mut rows = db
      .execute_sql("SELECT id, name FROM users;")
      .await
      .expect("select users")
      .rows;
    sort_rows(&mut rows);
    rows
  }

  async fn assert_user_rows_match<S>(left: &mut Database<S>, right: &mut Database<S>)
  where
    S: SyncStore,
  {
    let left_rows = select_sorted_users(left).await;
    let right_rows = select_sorted_users(right).await;
    let expected = expected_user_rows();
    assert_eq!(left_rows, expected);
    assert_eq!(right_rows, expected);
    assert_eq!(left_rows, right_rows);
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
}
