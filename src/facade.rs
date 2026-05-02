#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

#[cfg(feature = "automerge")]
use db_automerge::{AutomergeEngineStore, AutomergeEntry, DocumentChangeKey};
use db_engine::{EngineDatabase, EngineKey, EngineQuery, EngineResult, EngineRow, EngineValue};
#[cfg(feature = "redb")]
use db_engine::{EngineKeyCodec, EngineRowCodec};
use db_in_memory::{InMemoryBTree, InMemoryNamedBTree};
#[cfg(feature = "redb")]
use db_redb::{REDBBTree, REDBNamedBTree};
#[cfg(feature = "redb")]
use std::path::Path;

use db_sql_to_engine::{
  CanonicalStatement, DdlOp, SchemaResolver, parse_and_translate, parse_and_translate_statement,
};

/// Public facade error type.
#[derive(Debug)]
pub enum DatabaseError {
  Engine(String),
  Other(String),
}

impl fmt::Display for DatabaseError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DatabaseError::Engine(s) => write!(f, "engine error: {s}"),
      DatabaseError::Other(s) => write!(f, "{s}"),
    }
  }
}

impl From<db_engine::EngineError> for DatabaseError {
  fn from(e: db_engine::EngineError) -> Self {
    DatabaseError::Engine(format!("{e}"))
  }
}

/// Simple row type reusing EngineValue
pub type Row = Vec<EngineValue>;

type InMemoryEngineStore = InMemoryNamedBTree<EngineKey, EngineRow>;
#[cfg(feature = "redb")]
type RedbEngineStore = REDBNamedBTree<EngineKey, EngineRow, EngineKeyCodec, EngineRowCodec>;
#[cfg(all(feature = "automerge", feature = "redb"))]
type RedbAutomergeStore = AutomergeEngineStore<
  REDBBTree<DocumentChangeKey, AutomergeEntry, DocumentChangeKeyCodec, VecBytesCodec>,
>;

/// Opaque database handle.
pub enum Database {
  InMemory(EngineDatabase<InMemoryEngineStore>),
  #[cfg(feature = "automerge")]
  AutomergeInMemory(
    EngineDatabase<AutomergeEngineStore<InMemoryBTree<DocumentChangeKey, AutomergeEntry>>>,
  ),
  #[cfg(all(feature = "automerge", feature = "redb"))]
  AutomergeRedb(EngineDatabase<RedbAutomergeStore>),
  #[cfg(feature = "redb")]
  Redb(EngineDatabase<RedbEngineStore>),
}

impl SchemaResolver for Database {
  fn describe_table(&self, name: &str) -> Option<db_engine::TableSchema> {
    match self {
      Database::InMemory(engine) => engine.describe_table(name),
      #[cfg(feature = "automerge")]
      Database::AutomergeInMemory(engine) => engine.describe_table(name),
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Database::AutomergeRedb(engine) => engine.describe_table(name),
      #[cfg(feature = "redb")]
      Database::Redb(engine) => engine.describe_table(name),
    }
  }
}

impl Database {
  /// Open an in-memory database (dev/test convenience).
  pub async fn open_in_memory() -> Result<Self, DatabaseError> {
    let store = InMemoryNamedBTree::new();
    let engine = EngineDatabase::new(store);
    Ok(Self::InMemory(engine))
  }

  /// Open an Automerge-backed database (feature-gated).
  #[cfg(feature = "automerge")]
  pub async fn open_automerge_in_memory() -> Result<Self, DatabaseError> {
    let backend = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store);
    Ok(Self::AutomergeInMemory(engine))
  }

  #[cfg(all(feature = "automerge", feature = "redb"))]
  pub async fn open_automerge_with_redb(
    path: impl AsRef<Path>,
    table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let backend = REDBBTree::<
      DocumentChangeKey,
      AutomergeEntry,
      DocumentChangeKeyCodec,
      VecBytesCodec,
    >::open_with_codecs(path, table_name)
    .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store);
    Ok(Self::AutomergeRedb(engine))
  }

  #[cfg(feature = "redb")]
  pub async fn open_in_redb(
    path: impl AsRef<Path>,
    _table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let store =
      REDBNamedBTree::<EngineKey, EngineRow, EngineKeyCodec, EngineRowCodec>::open_with_codecs(
        path,
      )
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let engine = EngineDatabase::new(store);
    Ok(Self::Redb(engine))
  }

  /// Register a table schema with the engine.
  pub async fn register_table(
    &mut self,
    schema: db_engine::TableSchema,
  ) -> Result<(), DatabaseError> {
    match self {
      Database::InMemory(engine) => engine.register_table(schema).await?,
      #[cfg(feature = "automerge")]
      Database::AutomergeInMemory(engine) => engine.register_table(schema).await?,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Database::AutomergeRedb(engine) => engine.register_table(schema).await?,
      #[cfg(feature = "redb")]
      Database::Redb(engine) => engine.register_table(schema).await?,
    }
    Ok(())
  }

  /// Execute an `EngineQuery` directly against the engine.
  pub async fn execute_query(&self, query: EngineQuery) -> Result<EngineResult, DatabaseError> {
    let res = match self {
      Database::InMemory(engine) => engine.execute(query).await?,
      #[cfg(feature = "automerge")]
      Database::AutomergeInMemory(engine) => engine.execute(query).await?,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Database::AutomergeRedb(engine) => engine.execute(query).await?,
      #[cfg(feature = "redb")]
      Database::Redb(engine) => engine.execute(query).await?,
    };
    Ok(res)
  }

  /// Execute a SQL string using the database schema catalog.
  pub async fn execute_sql(&mut self, sql: &str) -> Result<EngineResult, DatabaseError> {
    match parse_and_translate_statement(sql, self) {
      Ok(CanonicalStatement::Query(q)) => self.execute_query(q).await,
      Ok(CanonicalStatement::Ddl(DdlOp::CreateTable(schema))) => {
        match self {
          Database::InMemory(engine) => engine.register_table(schema).await?,
          #[cfg(feature = "automerge")]
          Database::AutomergeInMemory(engine) => engine.register_table(schema).await?,
          #[cfg(all(feature = "automerge", feature = "redb"))]
          Database::AutomergeRedb(engine) => engine.register_table(schema).await?,
          #[cfg(feature = "redb")]
          Database::Redb(engine) => engine.register_table(schema).await?,
        }
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropTable(name))) => {
        match self {
          Database::InMemory(engine) => engine.drop_table(&name).await?,
          #[cfg(feature = "automerge")]
          Database::AutomergeInMemory(engine) => engine.drop_table(&name).await?,
          #[cfg(all(feature = "automerge", feature = "redb"))]
          Database::AutomergeRedb(engine) => engine.drop_table(&name).await?,
          #[cfg(feature = "redb")]
          Database::Redb(engine) => engine.drop_table(&name).await?,
        }
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::CreateIndex(schema))) => {
        match self {
          Database::InMemory(engine) => engine.register_index(schema).await?,
          #[cfg(feature = "automerge")]
          Database::AutomergeInMemory(engine) => engine.register_index(schema).await?,
          #[cfg(all(feature = "automerge", feature = "redb"))]
          Database::AutomergeRedb(engine) => engine.register_index(schema).await?,
          #[cfg(feature = "redb")]
          Database::Redb(engine) => engine.register_index(schema).await?,
        }
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropIndex(name))) => {
        match self {
          Database::InMemory(engine) => engine.drop_index(&name).await?,
          #[cfg(feature = "automerge")]
          Database::AutomergeInMemory(engine) => engine.drop_index(&name).await?,
          #[cfg(all(feature = "automerge", feature = "redb"))]
          Database::AutomergeRedb(engine) => engine.drop_index(&name).await?,
          #[cfg(feature = "redb")]
          Database::Redb(engine) => engine.drop_index(&name).await?,
        }
        Ok(EngineResult::new(Vec::new()))
      }
      Err(e) => Err(DatabaseError::Other(format!("{e}"))),
    }
  }

  /// Convenience: run a closure in a transaction context.
  pub async fn transaction<F, Fut, T>(&self, f: F) -> Result<T, DatabaseError>
  where
    F: FnOnce(&mut Transaction<'_>) -> Fut,
    Fut: core::future::Future<Output = Result<T, DatabaseError>>,
  {
    let mut tx = match self {
      Database::InMemory(engine) => Transaction::InMemory(engine.transaction()),
      #[cfg(feature = "automerge")]
      Database::AutomergeInMemory(engine) => Transaction::AutomergeInMemory(engine.transaction()),
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Database::AutomergeRedb(engine) => Transaction::AutomergeRedb(engine.transaction()),
      #[cfg(feature = "redb")]
      Database::Redb(engine) => Transaction::Redb(engine.transaction()),
    };
    let out = f(&mut tx).await;
    match out {
      Ok(v) => {
        tx.commit().await?;
        Ok(v)
      }
      Err(e) => {
        let _ = tx.rollback().await;
        Err(e)
      }
    }
  }
}

/// Transaction wrapper delegating to EngineTransaction
pub enum Transaction<'db> {
  InMemory(db_engine::EngineTransaction<'db, InMemoryEngineStore>),
  #[cfg(feature = "automerge")]
  AutomergeInMemory(
    db_engine::EngineTransaction<
      'db,
      AutomergeEngineStore<InMemoryBTree<DocumentChangeKey, AutomergeEntry>>,
    >,
  ),
  #[cfg(all(feature = "automerge", feature = "redb"))]
  AutomergeRedb(db_engine::EngineTransaction<'db, RedbAutomergeStore>),
  #[cfg(feature = "redb")]
  Redb(db_engine::EngineTransaction<'db, RedbEngineStore>),
}

impl<'db> Transaction<'db> {
  pub async fn insert_row(&mut self, table: &str, row: Row) -> Result<(), DatabaseError> {
    match self {
      Transaction::InMemory(inner) => inner.insert_row(table, row).await?,
      #[cfg(feature = "automerge")]
      Transaction::AutomergeInMemory(inner) => inner.insert_row(table, row).await?,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Transaction::AutomergeRedb(inner) => inner.insert_row(table, row).await?,
      #[cfg(feature = "redb")]
      Transaction::Redb(inner) => inner.insert_row(table, row).await?,
    }
    Ok(())
  }

  /// Execute a SQL string inside this transaction. Supports INSERT/UPDATE/DELETE.
  /// SELECT is not supported in a write transaction — use `Database::execute_sql`.
  pub async fn execute_sql(
    &mut self,
    resolver: &dyn SchemaResolver,
    sql: &str,
  ) -> Result<EngineResult, DatabaseError> {
    let q = parse_and_translate(sql, resolver).map_err(|e| DatabaseError::Other(format!("{e}")))?;
    match q {
      EngineQuery::Insert { table, row } => match self {
        Transaction::InMemory(inner) => {
          inner.insert_row(&table, row).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(feature = "automerge")]
        Transaction::AutomergeInMemory(inner) => {
          inner.insert_row(&table, row).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(all(feature = "automerge", feature = "redb"))]
        Transaction::AutomergeRedb(inner) => {
          inner.insert_row(&table, row).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(feature = "redb")]
        Transaction::Redb(inner) => {
          inner.insert_row(&table, row).await?;
          Ok(EngineResult::new(Vec::new()))
        }
      },
      EngineQuery::Update {
        table,
        assignments,
        predicate,
      } => match self {
        Transaction::InMemory(inner) => {
          inner.update_rows(&table, assignments, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(feature = "automerge")]
        Transaction::AutomergeInMemory(inner) => {
          inner.update_rows(&table, assignments, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(all(feature = "automerge", feature = "redb"))]
        Transaction::AutomergeRedb(inner) => {
          inner.update_rows(&table, assignments, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(feature = "redb")]
        Transaction::Redb(inner) => {
          inner.update_rows(&table, assignments, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
      },
      EngineQuery::Delete { table, predicate } => match self {
        Transaction::InMemory(inner) => {
          inner.delete_rows(&table, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(feature = "automerge")]
        Transaction::AutomergeInMemory(inner) => {
          inner.delete_rows(&table, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(all(feature = "automerge", feature = "redb"))]
        Transaction::AutomergeRedb(inner) => {
          inner.delete_rows(&table, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
        #[cfg(feature = "redb")]
        Transaction::Redb(inner) => {
          inner.delete_rows(&table, predicate).await?;
          Ok(EngineResult::new(Vec::new()))
        }
      },
      EngineQuery::Select { .. } => Err(DatabaseError::Other(
        "SELECT inside transaction not supported; use Database::execute_sql instead".into(),
      )),
    }
  }

  pub async fn commit(self) -> Result<(), DatabaseError> {
    match self {
      Transaction::InMemory(inner) => inner.commit().await?,
      #[cfg(feature = "automerge")]
      Transaction::AutomergeInMemory(inner) => inner.commit().await?,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Transaction::AutomergeRedb(inner) => inner.commit().await?,
      #[cfg(feature = "redb")]
      Transaction::Redb(inner) => inner.commit().await?,
    }
    Ok(())
  }

  pub async fn rollback(self) -> Result<(), DatabaseError> {
    match self {
      Transaction::InMemory(inner) => inner.rollback().await?,
      #[cfg(feature = "automerge")]
      Transaction::AutomergeInMemory(inner) => inner.rollback().await?,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Transaction::AutomergeRedb(inner) => inner.rollback().await?,
      #[cfg(feature = "redb")]
      Transaction::Redb(inner) => inner.rollback().await?,
    }
    Ok(())
  }
}
