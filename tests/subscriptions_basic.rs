#[cfg(feature = "std")]
mod tests {
  use db::Database;
  use db_engine::{EngineQuery, EngineResult, Subscriber, SyncScope};
  use futures::executor::block_on;
  use std::sync::{Arc, Mutex};

  #[derive(Clone)]
  struct TestSubscriber {
    results: Arc<Mutex<Vec<EngineResult>>>,
    call_count: Arc<Mutex<usize>>,
  }

  impl TestSubscriber {
    fn new() -> Self {
      Self {
        results: Arc::new(Mutex::new(Vec::new())),
        call_count: Arc::new(Mutex::new(0)),
      }
    }

    fn get_results(&self) -> Vec<EngineResult> {
      self.results.lock().unwrap().clone()
    }

    fn get_call_count(&self) -> usize {
      *self.call_count.lock().unwrap()
    }
  }

  impl Subscriber for TestSubscriber {
    fn on_results(&self, results: EngineResult) {
      self.results.lock().unwrap().push(results);
      *self.call_count.lock().unwrap() += 1;
    }
  }

  #[test]
  fn test_subscription_basic() {
    block_on(async {
      // Create an in-memory database
      let mut db = Database::open_in_memory().await.expect("open in-memory db");

      // Create a simple table via SQL
      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      // Create a subscriber
      let subscriber = TestSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      // Create a simple select query
      let query = EngineQuery::select_simple("users".to_string(), vec![0, 1], None);

      // Subscribe (empty table initially)
      let scope = SyncScope::default(); // unrestricted
      let _sub_id = db
        .engine
        .subscribe(query.clone(), &scope, subscriber_arc.clone())
        .await
        .expect("subscribe");

      // Verify subscriber was called with initial (empty) results
      assert_eq!(
        subscriber.get_call_count(),
        1,
        "should be called once initially"
      );
      let initial_results = subscriber.get_results();
      assert_eq!(initial_results.len(), 1, "should have one result set");
      assert_eq!(
        initial_results[0].rows.len(),
        0,
        "should start with empty rows"
      );

      println!(
        "✓ Subscription created successfully with call_count={}",
        subscriber.get_call_count()
      );
    });
  }

  #[test]
  fn test_scope_access_control() {
    // Test restricted scope
    let restricted_scope = SyncScope::new(vec!["users".to_string()].into_iter().collect());

    // Verify can_access works
    assert!(restricted_scope.can_access("users"));
    assert!(!restricted_scope.can_access("orders"));

    println!("✓ SyncScope access control works");
  }
}
