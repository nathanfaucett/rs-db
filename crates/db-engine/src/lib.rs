mod access_control;
mod change_event;
mod engine;
mod engine_kernel;
mod predicate;
mod query;
mod schema_resolver;
mod store_adapter;
mod subscriptions;
mod types;

pub use access_control::SyncScope;
pub use change_event::{ChangeEvent, ChangeListener};
pub use db_types::schema::{ColumnSchema, IndexSchema, TableSchema};
pub use engine::{EngineDatabase, EngineTransaction};
pub use query::{
  Aggregate, HavingPredicate, JoinClause, JoinKind, JoinOn, OrderBy, QualifiedColumn,
  QualifiedOperand, QualifiedPredicate, RefOrAgg, SelectOptions, SortDirection, UpdateAssignment,
  UpdateValueExpr,
};
pub use query::{EngineQuery, EngineResult};
pub use schema_resolver::SchemaResolver;
pub use store_adapter::{
  BackendCapability, EngineStore, EngineStoreTransaction, IndexStore, RowStore, SchemaStore,
  TransactionContract, TransactionControl, fetch_rows_by_primary_keys,
  lookup_primary_keys_by_index_predicate,
};
pub use subscriptions::{Subscriber, SubscriptionId};
pub use types::{EngineError, EngineKey, EngineRow, EngineType, EngineValue, PrimaryKey};

// Internal exports for engine module
pub(crate) use change_event::ChangeListenerRegistry;
