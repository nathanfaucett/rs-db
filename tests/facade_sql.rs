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

  #[test]
  fn automerge_in_memory_update_delete_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score INT);")
        .await
        .expect("create users");

      db.execute_sql("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 10);")
        .await
        .expect("insert alice");
      db.execute_sql("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 20);")
        .await
        .expect("insert bob");

      db.execute_sql("UPDATE users SET score = 11 WHERE id = 1;")
        .await
        .expect("update alice score");
      db.execute_sql("DELETE FROM users WHERE id = 2;")
        .await
        .expect("delete bob");

      let result = db
        .execute_sql("SELECT id, name, score FROM users;")
        .await
        .expect("select users");

      assert_eq!(result.rows.len(), 1);
      assert_eq!(
        result.rows[0],
        vec![
          EngineValue::Integer(1),
          EngineValue::Text("Alice".into()),
          EngineValue::Integer(11),
        ]
      );
    });
  }

  #[test]
  fn automerge_in_memory_update_join_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, team_id INT, score INT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE teams (id INT PRIMARY KEY, bonus INT);")
        .await
        .expect("create teams");

      db.execute_sql("INSERT INTO users (id, team_id, score) VALUES (1, 10, 5);")
        .await
        .expect("insert user 1");
      db.execute_sql("INSERT INTO users (id, team_id, score) VALUES (2, 20, 7);")
        .await
        .expect("insert user 2");
      db.execute_sql("INSERT INTO teams (id, bonus) VALUES (10, 3);")
        .await
        .expect("insert team 10");
      db.execute_sql("INSERT INTO teams (id, bonus) VALUES (20, 4);")
        .await
        .expect("insert team 20");

      db.execute_sql(
        "UPDATE users u JOIN teams t ON u.team_id = t.id SET score = score + t.bonus WHERE u.id = 1;",
      )
      .await
      .expect("update joined user score");

      let result = db
        .execute_sql("SELECT id, score FROM users;")
        .await
        .expect("select users");

      assert_eq!(result.rows.len(), 2);
      assert!(
        result
          .rows
          .contains(&vec![EngineValue::Integer(1), EngineValue::Integer(8),])
      );
      assert!(
        result
          .rows
          .contains(&vec![EngineValue::Integer(2), EngineValue::Integer(7),])
      );
    });
  }

  #[test]
  fn automerge_in_memory_update_from_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, team_id INT, score INT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE teams (id INT PRIMARY KEY, bonus INT);")
        .await
        .expect("create teams");

      db.execute_sql("INSERT INTO users (id, team_id, score) VALUES (1, 10, 5);")
        .await
        .expect("insert user 1");
      db.execute_sql("INSERT INTO users (id, team_id, score) VALUES (2, 20, 7);")
        .await
        .expect("insert user 2");
      db.execute_sql("INSERT INTO teams (id, bonus) VALUES (10, 3);")
        .await
        .expect("insert team 10");
      db.execute_sql("INSERT INTO teams (id, bonus) VALUES (20, 4);")
        .await
        .expect("insert team 20");

      db.execute_sql(
        "UPDATE users SET score = score + teams.bonus FROM teams WHERE users.team_id = teams.id AND users.id = 1;",
      )
      .await
      .expect("update from user score");

      let result = db
        .execute_sql("SELECT id, score FROM users;")
        .await
        .expect("select users");

      assert_eq!(result.rows.len(), 2);
      assert!(
        result
          .rows
          .contains(&vec![EngineValue::Integer(1), EngineValue::Integer(8),])
      );
      assert!(
        result
          .rows
          .contains(&vec![EngineValue::Integer(2), EngineValue::Integer(7),])
      );
    });
  }

  #[test]
  fn automerge_in_memory_update_returning_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql("INSERT INTO users (id, score) VALUES (1, 10);")
        .await
        .expect("insert user");

      let result = db
        .execute_sql("UPDATE users SET score = score + 2 WHERE id = 1 RETURNING id, score;")
        .await
        .expect("update returning");

      assert_eq!(
        result.rows,
        vec![vec![EngineValue::Integer(1), EngineValue::Integer(12)]],
      );
    });
  }

  #[test]
  fn automerge_in_memory_delete_returning_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql("INSERT INTO users (id, score) VALUES (1, 10);")
        .await
        .expect("insert user");

      let result = db
        .execute_sql("DELETE FROM users WHERE id = 1 RETURNING id, score;")
        .await
        .expect("delete returning");

      assert_eq!(
        result.rows,
        vec![vec![EngineValue::Integer(1), EngineValue::Integer(10)]],
      );

      let remaining = db
        .execute_sql("SELECT id, score FROM users WHERE id = 1;")
        .await
        .expect("select users");
      assert!(remaining.rows.is_empty());
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
