use crate::{ChangeEvent, EngineQuery, EngineResult, SyncScope};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Unique identifier for a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct SubscriptionId(u64);

impl SubscriptionId {
  pub(crate) fn next() -> Self {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    SubscriptionId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
  }
}

/// Trait for objects that want to receive subscription updates.
/// Called when a subscribed query's results change.
pub trait Subscriber: Send + Sync {
  /// Called with new query results.
  fn on_results(&self, results: EngineResult);
}

/// Internal representation of a subscription.
pub(crate) struct QuerySubscription {
  pub(crate) id: SubscriptionId,
  pub(crate) query: EngineQuery,
  pub(crate) scope: SyncScope,
  pub(crate) subscriber: Arc<dyn Subscriber>,
  pub(crate) last_results: RwLock<Option<EngineResult>>,
}

impl std::fmt::Debug for QuerySubscription {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("QuerySubscription")
      .field("id", &self.id)
      .field("query", &self.query)
      .field("scope", &self.scope)
      .field("subscriber", &"<dyn Subscriber>")
      .field("last_results", &"<stored>")
      .finish()
  }
}

impl QuerySubscription {
  /// Check if this subscription is affected by a change event.
  pub(crate) fn is_affected_by(&self, event: &ChangeEvent) -> bool {
    let table = event.table();
    // Check if the query references this table
    self.query.tables().contains(&table.to_string())
  }

  /// Check if the change event is visible to this subscription's scope.
  pub(crate) fn matches_scope(&self, event: &ChangeEvent) -> bool {
    self.scope.matches(event)
  }
}

/// Internal registry managing active subscriptions.
#[derive(Debug)]
pub(crate) struct SubscriptionRegistry {
  subscriptions: RwLock<HashMap<SubscriptionId, Arc<QuerySubscription>>>,
  /// Map from table name to subscription IDs that reference it.
  /// Used for efficient lookup when a change event occurs.
  table_to_subscriptions: RwLock<HashMap<String, Vec<SubscriptionId>>>,
}

impl SubscriptionRegistry {
  pub(crate) fn new() -> Self {
    Self {
      subscriptions: RwLock::new(HashMap::new()),
      table_to_subscriptions: RwLock::new(HashMap::new()),
    }
  }

  /// Register a subscription.
  pub(crate) fn register(&self, subscription: Arc<QuerySubscription>) {
    let id = subscription.id;

    // Add to subscriptions map
    {
      let mut subs = self.subscriptions.write().unwrap();
      subs.insert(id, subscription.clone());
    }

    // Add to table-to-subscriptions index
    {
      let mut table_map = self.table_to_subscriptions.write().unwrap();
      for table in subscription.query.tables() {
        table_map.entry(table).or_default().push(id);
      }
    }
  }

  /// Unregister a subscription.
  pub(crate) fn unregister(&self, id: SubscriptionId) {
    let mut subs = self.subscriptions.write().unwrap();
    if let Some(sub) = subs.remove(&id) {
      // Remove from table index
      let mut table_map = self.table_to_subscriptions.write().unwrap();
      for table in sub.query.tables() {
        if let Some(ids) = table_map.get_mut(&table) {
          ids.retain(|&sid| sid != id);
          if ids.is_empty() {
            table_map.remove(&table);
          }
        }
      }
    }
  }

  /// Get all subscriptions affected by a change to a specific table.
  pub(crate) fn subscriptions_for_table(&self, table: &str) -> Vec<Arc<QuerySubscription>> {
    let table_map = self.table_to_subscriptions.read().unwrap();
    let subs = self.subscriptions.read().unwrap();

    if let Some(ids) = table_map.get(table) {
      ids.iter().filter_map(|id| subs.get(id).cloned()).collect()
    } else {
      Vec::new()
    }
  }

  /// Update the last results for a subscription.
  pub(crate) fn update_last_results(&self, id: SubscriptionId, results: EngineResult) {
    let subs = self.subscriptions.read().unwrap();
    if let Some(sub) = subs.get(&id) {
      *sub.last_results.write().unwrap() = Some(results);
    }
  }

  /// Get the last results for a subscription.
  pub(crate) fn get_last_results(&self, id: SubscriptionId) -> Option<EngineResult> {
    let subs = self.subscriptions.read().unwrap();
    if let Some(sub) = subs.get(&id) {
      sub.last_results.read().unwrap().clone()
    } else {
      None
    }
  }

  /// Clear all subscriptions.
  pub(crate) fn clear(&self) {
    let mut subs = self.subscriptions.write().unwrap();
    subs.clear();

    let mut table_map = self.table_to_subscriptions.write().unwrap();
    table_map.clear();
  }

  /// Collect all subscriptions affected by a change event.
  /// Returns subscriptions that should be recomputed.
  pub(crate) fn affected_by_change(&self, event: &ChangeEvent) -> Vec<Arc<QuerySubscription>> {
    let table = event.table();
    let mut affected = Vec::new();

    // Get all subscriptions that reference this table
    for sub in self.subscriptions_for_table(table) {
      // Check if this subscription's scope matches the change
      if sub.matches_scope(event) {
        affected.push(sub);
      }
    }

    affected
  }
}

/// Helper for batching subscription updates during sync.
pub(crate) struct SubscriptionBatch {
  invalidated: RwLock<Vec<SubscriptionId>>,
}

impl SubscriptionBatch {
  pub(crate) fn new() -> Self {
    Self {
      invalidated: RwLock::new(Vec::new()),
    }
  }

  pub(crate) fn invalidate(&self, id: SubscriptionId) {
    let mut inv = self.invalidated.write().unwrap();
    if !inv.contains(&id) {
      inv.push(id);
    }
  }

  pub(crate) fn take_invalidated(&self) -> Vec<SubscriptionId> {
    let mut inv = self.invalidated.write().unwrap();
    std::mem::take(&mut *inv)
  }
}
