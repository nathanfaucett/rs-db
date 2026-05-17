#[cfg(feature = "automerge")]
mod tests {
  use db::Database;

  use futures::executor::block_on;
  use serde::Deserialize;

  #[derive(Deserialize, Debug, PartialEq)]
  struct User {
    id: Vec<u8>,
    name: String,
  }

  #[derive(Deserialize, Debug, PartialEq)]
  struct UserWithAge {
    id: Vec<u8>,
    name: String,
    age: Option<i64>,
  }

  #[test]
  fn automerge_typed_query_basic() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      db.execute_sql(
        "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'Alice');",
      )
      .await
      .expect("insert 1");

      db.execute_sql(
        "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000002'::uuid, 'Bob');",
      )
      .await
      .expect("insert 2");

      let query_sql = "SELECT id, name FROM users;";
      let result = db.execute_sql(query_sql).await.expect("execute query");

      let schema = db.engine.describe_table("users").expect("get schema");

      let users: Vec<User> = result.into_typed::<User>(&schema).expect("deserialize");

      assert_eq!(users.len(), 2);
      assert_eq!(users[0].name, "Alice");
      assert_eq!(users[1].name, "Bob");
    });
  }

  #[test]
  fn automerge_typed_query_with_null() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, age INTEGER);")
        .await
        .expect("create table");

      db.execute_sql(
        "INSERT INTO users (id, name, age) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'Alice', 30);",
      )
      .await
      .expect("insert 1");

      db.execute_sql(
        "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000002'::uuid, 'Bob');",
      )
      .await
      .expect("insert 2");

      let result = db
        .execute_sql("SELECT id, name, age FROM users;")
        .await
        .expect("execute query");

      let schema = db.engine.describe_table("users").expect("get schema");

      let users: Vec<UserWithAge> = result
        .into_typed::<UserWithAge>(&schema)
        .expect("deserialize");

      assert_eq!(users.len(), 2);
      assert_eq!(users[0].name, "Alice");
      assert_eq!(users[0].age, Some(30));
      assert_eq!(users[1].name, "Bob");
      assert_eq!(users[1].age, None);
    });
  }
}
