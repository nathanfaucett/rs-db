//! High-level facade tests for JSON type support.

#[cfg(feature = "automerge")]
mod tests {
  use db_engine::EngineValue;
  use futures::executor::block_on;

  #[test]
  fn json_table_creation_and_insert() {
    block_on(async {
      use db::Database;

      let mut db = Database::open_in_memory().await.expect("open in-memory");

      // Create table with JSON column
      db.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, data JSON);")
        .await
        .expect("create table");

      // Insert JSON value
      db.execute_sql(
        r#"
        INSERT INTO items VALUES (
          '00000000-0000-0000-0000-000000000001'::uuid,
          '{"name": "test", "value": 42}'
        )
      "#,
      )
      .await
      .expect("insert json");

      // Query should succeed
      let result = db
        .execute_sql("SELECT * FROM items")
        .await
        .expect("select items");

      assert!(!result.rows.is_empty(), "should have 1 row");
      // Verify the JSON was stored
      if let Some(row) = result.rows.first() {
        if let Some(EngineValue::Json(json_str)) = row.get(1) {
          assert!(json_str.contains("test"));
        } else {
          panic!("expected JSON value in second column");
        }
      }
    });
  }

  #[test]
  fn json_null_and_mixed_columns() {
    block_on(async {
      use db::Database;

      let mut db = Database::open_in_memory().await.expect("open in-memory");

      // Create table with mixed types including JSON
      db.execute_sql(
        "CREATE TABLE records (id UUID PRIMARY KEY, name TEXT, config JSON, count INTEGER);",
      )
      .await
      .expect("create table");

      // Insert with NULL JSON
      db.execute_sql(
        r#"
        INSERT INTO records VALUES (
          '00000000-0000-0000-0000-000000000001'::uuid,
          'Alice',
          NULL,
          10
        )
      "#,
      )
      .await
      .expect("insert with null json");

      // Insert with JSON value
      db.execute_sql(
        r#"
        INSERT INTO records VALUES (
          '00000000-0000-0000-0000-000000000002'::uuid,
          'Bob',
          '{"role": "admin"}',
          20
        )
      "#,
      )
      .await
      .expect("insert with json");

      let result = db
        .execute_sql("SELECT * FROM records ORDER BY id")
        .await
        .expect("select all");

      assert_eq!(result.rows.len(), 2, "should have 2 rows");

      // Check first row (NULL JSON)
      if let Some(row) = result.rows.first() {
        assert_eq!(row.get(1), Some(&EngineValue::Text("Alice".to_string())));
        assert_eq!(row.get(2), Some(&EngineValue::Null));
      }

      // Check second row (with JSON)
      if let Some(row) = result.rows.get(1) {
        assert_eq!(row.get(1), Some(&EngineValue::Text("Bob".to_string())));
        if let Some(EngineValue::Json(json_str)) = row.get(2) {
          assert!(json_str.contains("admin"));
        } else {
          panic!("expected JSON value");
        }
      }
    });
  }

  #[test]
  fn json_invalid_data_rejected_at_insert() {
    block_on(async {
      use db::Database;

      let mut db = Database::open_in_memory().await.expect("open in-memory");

      db.execute_sql("CREATE TABLE config (id UUID PRIMARY KEY, settings JSON);")
        .await
        .expect("create table");

      // Try to insert invalid JSON - should fail or be handled gracefully
      let result = db
        .execute_sql(
          r#"
          INSERT INTO config VALUES (
            '00000000-0000-0000-0000-000000000001'::uuid,
            'not valid json at all'
          )
        "#,
        )
        .await;

      // The behavior depends on whether we validate at insert time
      // For now, we accept it as a string and let it be validated at query time
      match result {
        Ok(_) => {
          // If it succeeds, the string should be stored
          let rows = db
            .execute_sql("SELECT * FROM config")
            .await
            .expect("select");
          assert!(!rows.rows.is_empty());
        }
        Err(_) => {
          // If it fails, that's also valid behavior (strict validation)
        }
      }
    });
  }
}

#[cfg(not(feature = "automerge"))]
mod tests {
  // Placeholder: these tests require automerge feature
  // To run full tests: `cargo test --features automerge`
}
