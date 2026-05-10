#[cfg(not(feature = "std"))]
extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use db_engine::{EngineQuery, EngineResult, TableSchema};
use db_sql_to_engine::{
  CanonicalStatement, DdlOp, SchemaResolver, parse_and_translate, parse_and_translate_statement,
};

use super::types::{Database, DatabaseError, FacadeStore, Row, Transaction};

pub(crate) fn describe_table<S>(database: &Database<S>, name: &str) -> Option<TableSchema>
where
  S: FacadeStore,
{
  database.engine.describe_table(name)
}

pub(crate) async fn register_table(
  database: &mut Database<impl FacadeStore>,
  schema: TableSchema,
) -> Result<(), DatabaseError> {
  database.engine.register_table(schema).await?;
  Ok(())
}

pub(crate) async fn execute_query(
  database: &Database<impl FacadeStore>,
  query: EngineQuery,
) -> Result<EngineResult, DatabaseError> {
  let result = database.engine.execute(query).await?;
  Ok(result)
}

async fn drop_table(
  database: &mut Database<impl FacadeStore>,
  name: &str,
) -> Result<(), DatabaseError> {
  database.engine.drop_table(name).await?;
  Ok(())
}

async fn register_index(
  database: &mut Database<impl FacadeStore>,
  schema: db_engine::IndexSchema,
) -> Result<(), DatabaseError> {
  database.engine.register_index(schema).await?;
  Ok(())
}

async fn drop_index(
  database: &mut Database<impl FacadeStore>,
  name: &str,
) -> Result<(), DatabaseError> {
  database.engine.drop_index(name).await?;
  Ok(())
}

pub(crate) async fn execute_sql(
  database: &mut Database<impl FacadeStore>,
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

pub(crate) fn begin_transaction<S>(database: &Database<S>) -> Transaction<'_, S>
where
  S: FacadeStore,
{
  Transaction {
    inner: database.engine.transaction(),
  }
}

pub(crate) async fn transaction_insert_row(
  transaction: &mut Transaction<'_, impl FacadeStore>,
  table: &str,
  row: Row,
) -> Result<(), DatabaseError> {
  transaction.inner.insert_row(table, row).await?;
  Ok(())
}

async fn transaction_update_rows(
  transaction: &mut Transaction<'_, impl FacadeStore>,
  table: &str,
  assignments: Vec<(usize, db_engine::EngineValue)>,
  predicate: Option<db_engine::QualifiedPredicate>,
) -> Result<(), DatabaseError> {
  transaction
    .inner
    .update_rows(table, assignments, predicate)
    .await?;
  Ok(())
}

async fn transaction_delete_rows(
  transaction: &mut Transaction<'_, impl FacadeStore>,
  table: &str,
  predicate: Option<db_engine::QualifiedPredicate>,
) -> Result<(), DatabaseError> {
  transaction.inner.delete_rows(table, predicate).await?;
  Ok(())
}

pub(crate) async fn transaction_execute_sql(
  transaction: &mut Transaction<'_, impl FacadeStore>,
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

pub(crate) async fn transaction_commit(
  transaction: Transaction<'_, impl FacadeStore>,
) -> Result<(), DatabaseError> {
  transaction.inner.commit().await?;
  Ok(())
}

pub(crate) async fn transaction_rollback(
  transaction: Transaction<'_, impl FacadeStore>,
) -> Result<(), DatabaseError> {
  transaction.inner.rollback().await?;
  Ok(())
}
