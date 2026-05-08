#[cfg(not(feature = "std"))]
extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use db_engine::{EngineQuery, EngineResult, TableSchema};
use db_sql_to_engine::{
  CanonicalStatement, DdlOp, SchemaResolver, parse_and_translate, parse_and_translate_statement,
};

use super::types::{Database, DatabaseError, Row, Transaction};

macro_rules! with_database {
  ($database:expr, |$engine:ident| $body:expr) => {
    match $database {
      Database::InMemory($engine) => $body,
      #[cfg(feature = "automerge")]
      Database::AutomergeInMemory($engine) => $body,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Database::AutomergeRedb($engine) => $body,
      #[cfg(feature = "redb")]
      Database::Redb($engine) => $body,
    }
  };
}

macro_rules! with_database_mut {
  ($database:expr, |$engine:ident| $body:expr) => {
    match $database {
      Database::InMemory($engine) => $body,
      #[cfg(feature = "automerge")]
      Database::AutomergeInMemory($engine) => $body,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Database::AutomergeRedb($engine) => $body,
      #[cfg(feature = "redb")]
      Database::Redb($engine) => $body,
    }
  };
}

macro_rules! with_transaction_mut {
  ($transaction:expr, |$inner:ident| $body:expr) => {
    match $transaction {
      Transaction::InMemory($inner) => $body,
      #[cfg(feature = "automerge")]
      Transaction::AutomergeInMemory($inner) => $body,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Transaction::AutomergeRedb($inner) => $body,
      #[cfg(feature = "redb")]
      Transaction::Redb($inner) => $body,
    }
  };
}

macro_rules! with_transaction_owned {
  ($transaction:expr, |$inner:ident| $body:expr) => {
    match $transaction {
      Transaction::InMemory($inner) => $body,
      #[cfg(feature = "automerge")]
      Transaction::AutomergeInMemory($inner) => $body,
      #[cfg(all(feature = "automerge", feature = "redb"))]
      Transaction::AutomergeRedb($inner) => $body,
      #[cfg(feature = "redb")]
      Transaction::Redb($inner) => $body,
    }
  };
}

pub(crate) fn describe_table(database: &Database, name: &str) -> Option<TableSchema> {
  with_database!(database, |engine| engine.describe_table(name))
}

pub(crate) async fn register_table(
  database: &mut Database,
  schema: TableSchema,
) -> Result<(), DatabaseError> {
  with_database_mut!(database, |engine| engine.register_table(schema).await?);
  Ok(())
}

pub(crate) async fn execute_query(
  database: &Database,
  query: EngineQuery,
) -> Result<EngineResult, DatabaseError> {
  let result = with_database!(database, |engine| engine.execute(query).await?);
  Ok(result)
}

async fn drop_table(database: &mut Database, name: &str) -> Result<(), DatabaseError> {
  with_database_mut!(database, |engine| engine.drop_table(name).await?);
  Ok(())
}

async fn register_index(
  database: &mut Database,
  schema: db_engine::IndexSchema,
) -> Result<(), DatabaseError> {
  with_database_mut!(database, |engine| engine.register_index(schema).await?);
  Ok(())
}

async fn drop_index(database: &mut Database, name: &str) -> Result<(), DatabaseError> {
  with_database_mut!(database, |engine| engine.drop_index(name).await?);
  Ok(())
}

pub(crate) async fn execute_sql(
  database: &mut Database,
  sql: &str,
) -> Result<EngineResult, DatabaseError> {
  match parse_and_translate_statement(sql, database) {
    Ok(CanonicalStatement::Query(query)) => execute_query(database, query).await,
    Ok(CanonicalStatement::Ddl(DdlOp::CreateTable(schema))) => {
      register_table(database, schema).await?;
      Ok(EngineResult::new(Vec::new()))
    }
    Ok(CanonicalStatement::Ddl(DdlOp::DropTable(name))) => {
      drop_table(database, &name).await?;
      Ok(EngineResult::new(Vec::new()))
    }
    Ok(CanonicalStatement::Ddl(DdlOp::CreateIndex(schema))) => {
      register_index(database, schema).await?;
      Ok(EngineResult::new(Vec::new()))
    }
    Ok(CanonicalStatement::Ddl(DdlOp::DropIndex(name))) => {
      drop_index(database, &name).await?;
      Ok(EngineResult::new(Vec::new()))
    }
    Err(e) => Err(DatabaseError::Other(format!("{e}"))),
  }
}

pub(crate) fn begin_transaction(database: &Database) -> Transaction<'_> {
  match database {
    Database::InMemory(engine) => Transaction::InMemory(engine.transaction()),
    #[cfg(feature = "automerge")]
    Database::AutomergeInMemory(engine) => Transaction::AutomergeInMemory(engine.transaction()),
    #[cfg(all(feature = "automerge", feature = "redb"))]
    Database::AutomergeRedb(engine) => Transaction::AutomergeRedb(engine.transaction()),
    #[cfg(feature = "redb")]
    Database::Redb(engine) => Transaction::Redb(engine.transaction()),
  }
}

pub(crate) async fn transaction_insert_row(
  transaction: &mut Transaction<'_>,
  table: &str,
  row: Row,
) -> Result<(), DatabaseError> {
  with_transaction_mut!(transaction, |inner| inner.insert_row(table, row).await?);
  Ok(())
}

async fn transaction_update_rows(
  transaction: &mut Transaction<'_>,
  table: &str,
  assignments: Vec<(usize, db_engine::EngineValue)>,
  predicate: Option<db_engine::QualifiedPredicate>,
) -> Result<(), DatabaseError> {
  with_transaction_mut!(transaction, |inner| {
    inner.update_rows(table, assignments, predicate).await?
  });
  Ok(())
}

async fn transaction_delete_rows(
  transaction: &mut Transaction<'_>,
  table: &str,
  predicate: Option<db_engine::QualifiedPredicate>,
) -> Result<(), DatabaseError> {
  with_transaction_mut!(transaction, |inner| {
    inner.delete_rows(table, predicate).await?
  });
  Ok(())
}

pub(crate) async fn transaction_execute_sql(
  transaction: &mut Transaction<'_>,
  resolver: &dyn SchemaResolver,
  sql: &str,
) -> Result<EngineResult, DatabaseError> {
  let query =
    parse_and_translate(sql, resolver).map_err(|e| DatabaseError::Other(format!("{e}")))?;
  match query {
    EngineQuery::Insert { table, row } => {
      transaction_insert_row(transaction, &table, row).await?;
      Ok(EngineResult::new(Vec::new()))
    }
    EngineQuery::Update {
      table,
      assignments,
      predicate,
    } => {
      transaction_update_rows(transaction, &table, assignments, predicate).await?;
      Ok(EngineResult::new(Vec::new()))
    }
    EngineQuery::Delete { table, predicate } => {
      transaction_delete_rows(transaction, &table, predicate).await?;
      Ok(EngineResult::new(Vec::new()))
    }
    EngineQuery::Select { .. } => Err(DatabaseError::Other(
      "SELECT inside transaction not supported; use Database::execute_sql instead".into(),
    )),
  }
}

pub(crate) async fn transaction_commit(transaction: Transaction<'_>) -> Result<(), DatabaseError> {
  with_transaction_owned!(transaction, |inner| inner.commit().await?);
  Ok(())
}

pub(crate) async fn transaction_rollback(
  transaction: Transaction<'_>,
) -> Result<(), DatabaseError> {
  with_transaction_owned!(transaction, |inner| inner.rollback().await?);
  Ok(())
}
