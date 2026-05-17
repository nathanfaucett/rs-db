use crate::{EngineRow, PrimaryKey};
use std::sync::Arc;
use std::sync::RwLock;

/// A change event emitted when data in the engine mutates.
/// Subscribers listen to these events and recompute affected queries.
#[derive(Debug, Clone)]
pub enum ChangeEvent {
  /// A row was inserted into a table.
  RowInserted {
    table: String,
    pk: PrimaryKey,
    row: EngineRow,
  },
  /// A row was deleted from a table.
  RowDeleted {
    table: String,
    pk: PrimaryKey,
    row: EngineRow,
  },
  /// A row was updated in a table.
  RowUpdated {
    table: String,
    pk: PrimaryKey,
    old_row: EngineRow,
    new_row: EngineRow,
  },
}

impl ChangeEvent {
  /// Get the table name for this change.
  pub fn table(&self) -> &str {
    match self {
      Self::RowInserted { table, .. } => table,
      Self::RowDeleted { table, .. } => table,
      Self::RowUpdated { table, .. } => table,
    }
  }

  /// Get the primary key for this change.
  pub fn pk(&self) -> &PrimaryKey {
    match self {
      Self::RowInserted { pk, .. } => pk,
      Self::RowDeleted { pk, .. } => pk,
      Self::RowUpdated { pk, .. } => pk,
    }
  }
}

/// Trait for objects that want to listen to change events.
pub trait ChangeListener: Send + Sync {
  /// Called when a change event occurs.
  fn on_change(&self, event: ChangeEvent);
}

/// Internal registry that maintains listeners and broadcasts change events.
pub(crate) struct ChangeListenerRegistry {
  listeners: RwLock<Vec<Arc<dyn ChangeListener>>>,
}

impl std::fmt::Debug for ChangeListenerRegistry {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ChangeListenerRegistry")
      .field(
        "listener_count",
        &self.listeners.read().map(|l| l.len()).unwrap_or(0),
      )
      .finish()
  }
}

impl ChangeListenerRegistry {
  pub(crate) fn new() -> Self {
    Self {
      listeners: RwLock::new(Vec::new()),
    }
  }

  /// Register a change listener.
  pub(crate) fn register(&self, listener: Arc<dyn ChangeListener>) {
    let mut listeners = self.listeners.write().unwrap();
    listeners.push(listener);
  }

  /// Emit a change event to all registered listeners.
  pub(crate) fn emit(&self, event: ChangeEvent) {
    let listeners = self.listeners.read().unwrap();
    for listener in listeners.iter() {
      listener.on_change(event.clone());
    }
  }

  /// Clear all listeners.
  pub(crate) fn clear(&self) {
    let mut listeners = self.listeners.write().unwrap();
    listeners.clear();
  }
}
