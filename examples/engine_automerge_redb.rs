// Example: register two tables, insert rows, and run a SQL JOIN select
// Uses an Automerge-backed store persisted in `redb` when built with the
// `automerge` and `redb` features. When those features are not enabled, the
// dummy `main` prints an informational message so the example crate still
// compiles.
#[cfg(all(feature = "automerge", feature = "redb"))]
use futures::executor::block_on;

#[cfg(all(feature = "automerge", feature = "redb"))]
use db::Database;
#[cfg(all(feature = "automerge", feature = "redb"))]
use std::path::PathBuf;

#[cfg(all(feature = "automerge", feature = "redb"))]
fn main() {
  block_on(async {
    let mut path = std::env::temp_dir();
    path.push("aicacia_automerge_redb.db");

    let mut db = Database::open_automerge_with_redb(path, "automerge_store")
      .await
      .expect("open automerge redb");

    // Create tables via SQL using the facade.
    db.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
      .await
      .expect("create users");
    db.execute_sql("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);")
      .await
      .expect("create orders");

    // Insert some users via SQL using the facade
    db.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');")
      .await
      .expect("insert user 1");
    db.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
      .await
      .expect("insert user 2");

    // Insert some orders via SQL using the facade
    db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (1,1,100);")
      .await
      .expect("insert order 1");
    db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (2,2,200);")
      .await
      .expect("insert order 2");

    let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
    let res = db.execute_sql(sql).await.expect("execute select");

    println!("Joined rows: {}", res.rows.len());
    for row in res.rows {
      println!("row: {:?}", row);
    }
  });
}

#[cfg(not(all(feature = "automerge", feature = "redb")))]
fn main() {
  eprintln!("example requires features: automerge + redb; build with --features 'automerge redb'");
}
