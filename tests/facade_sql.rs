#[cfg(feature = "automerge")]
mod tests {
  use db::Database;
  use db_engine::EngineValue;
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
    path.push(format!("aicacia_facade_sql_{}_{}_{}.db", label, nanos, id));
    path
  }

  #[test]
  fn automerge_two_create_tables() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);")
        .await
        .expect("create orders");
    });
  }

  #[test]
  fn automerge_in_memory_join_returns_two_rows() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);")
        .await
        .expect("create orders");

      db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .await
        .expect("insert user 1");
      db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
        .await
        .expect("insert user 2");

      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (1,1,100);")
        .await
        .expect("insert order 1");
      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (2,2,200);")
        .await
        .expect("insert order 2");

      let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
      let res = db.execute_sql(sql).await.expect("execute select");
      assert_eq!(res.rows.len(), 2);
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Alice".into()),
        EngineValue::Integer(100),
      ]));
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Bob".into()),
        EngineValue::Integer(200),
      ]));
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_join_returns_two_rows() {
    block_on(async {
      let path = temp_redb_path("join");
      let _ = fs::remove_file(&path);

      let mut db = Database::open_automerge_with_redb(&path, "automerge_store")
        .await
        .expect("open automerge redb");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);")
        .await
        .expect("create orders");

      db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
        .await
        .expect("insert user 1");
      db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
        .await
        .expect("insert user 2");

      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (1,1,100);")
        .await
        .expect("insert order 1");
      db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (2,2,200);")
        .await
        .expect("insert order 2");

      let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
      let res = db.execute_sql(sql).await.expect("execute select");
      assert_eq!(res.rows.len(), 2);
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Alice".into()),
        EngineValue::Integer(100),
      ]));
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Bob".into()),
        EngineValue::Integer(200),
      ]));

      let _ = fs::remove_file(path);
    });
  }
}
