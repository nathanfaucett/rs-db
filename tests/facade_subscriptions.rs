#[cfg(feature = "std")]
mod tests {
  use db::Database;
  use db_engine::{EngineQuery, EngineResult, Subscriber, SyncScope};
  use futures::executor::block_on;
  use std::sync::{Arc, Mutex};

  #[derive(Clone)]
  struct CountingSubscriber {
    call_count: Arc<Mutex<usize>>,
    last_row_count: Arc<Mutex<usize>>,
  }

  impl CountingSubscriber {
    fn new() -> Self {
      Self {
        call_count: Arc::new(Mutex::new(0)),
        last_row_count: Arc::new(Mutex::new(0)),
      }
    }

    fn get_call_count(&self) -> usize {
      *self.call_count.lock().unwrap()
    }

    fn get_last_row_count(&self) -> usize {
      *self.last_row_count.lock().unwrap()
    }
  }

  impl Subscriber for CountingSubscriber {
    fn on_results(&self, results: EngineResult) {
      *self.call_count.lock().unwrap() += 1;
      *self.last_row_count.lock().unwrap() = results.rows.len();
    }
  }

  #[test]
  fn test_facade_subscription() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE events (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      let query = EngineQuery::select_simple("events".to_string(), vec![0, 1], None);

      // Subscribe through facade
      let _id = db
        .subscribe(query, subscriber_arc.clone(), None)
        .await
        .expect("subscribe via facade");

      // Should have initial callback
      assert_eq!(subscriber.get_call_count(), 1);
      assert_eq!(subscriber.get_last_row_count(), 0);

      println!("✓ Facade layer subscriptions work");
    });
  }

  #[test]
  fn test_facade_subscription_with_scope() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE data (id UUID PRIMARY KEY, value TEXT);")
        .await
        .expect("create table");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      let query = EngineQuery::select_simple("data".to_string(), vec![0, 1], None);
      let scope = SyncScope::new(vec!["data".to_string()].into_iter().collect());

      // Subscribe with explicit scope
      let _id = db
        .subscribe(query, subscriber_arc.clone(), Some(scope))
        .await
        .expect("subscribe with scope");

      assert_eq!(subscriber.get_call_count(), 1);

      println!("✓ Facade subscriptions with scope work");
    });
  }

  #[test]
  fn test_facade_execute_with_scope() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      let query = EngineQuery::select_simple("users".to_string(), vec![0, 1], None);
      let scope = SyncScope::default();

      // Execute with scope
      let results = db
        .execute_with_scope(query, &scope)
        .await
        .expect("execute with scope");

      assert_eq!(results.rows.len(), 0);

      println!("✓ execute_with_scope on facade works");
    });
  }
}
