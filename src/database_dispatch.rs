#[cfg(not(feature = "std"))]
extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use db_engine::{EngineQuery, EngineResult, TableSchema};
use db_sql_to_engine::{
  CanonicalStatement, DdlOp, SchemaResolver, parse_and_translate, parse_and_translate_statement,
};

use crate::database_types::{Database, DatabaseError, Row, Transaction};

pub(crate) fn describe_table(database: &Database, name: &str) -> Option<TableSchema> {
  match database {
    Database::InMemory(engine) => engine.describe_table(name),
    #[cfg(feature = "automerge")]
    Database::AutomergeInMemory(engine) => engine.describe_table(name),
    #[cfg(all(feature = "automerge", feature = "redb"))]
    Database::AutomergeRedb(engine) => engine.describe_table(name),
    #[cfg(feature = "redb")]
    Database::Redb(engine) => engine.describe_table(name),
  }
}

pub(crate) async fn register_table(
  database: &mut Database,
  schema: TableSchema,
) -> Result<(), DatabaseError> {
  match database {
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

pub(crate) async fn execute_query(
  database: &Database,
  query: EngineQuery,
) -> Result<EngineResult, DatabaseError> {
  let result = match database {
    Database::InMemory(engine) => engine.execute(query).await?,
    #[cfg(feature = "automerge")]
    Database::AutomergeInMemory(engine) => engine.execute(query).await?,
    #[cfg(all(feature = "automerge", feature = "redb"))]
    Database::AutomergeRedb(engine) => engine.execute(query).await?,
    #[cfg(feature = "redb")]
    Database::Redb(engine) => engine.execute(query).await?,
  };
  Ok(result)
}

async fn drop_table(database: &mut Database, name: &str) -> Result<(), DatabaseError> {
  match database {
    Database::InMemory(engine) => engine.drop_table(name).await?,
    #[cfg(feature = "automerge")]
    Database::AutomergeInMemory(engine) => engine.drop_table(name).await?,
    #[cfg(all(feature = "automerge", feature = "redb"))]
    Database::AutomergeRedb(engine) => engine.drop_table(name).await?,
    #[cfg(feature = "redb")]
    Database::Redb(engine) => engine.drop_table(name).await?,
  }
  Ok(())
}

async fn register_index(
  database: &mut Database,
  schema: db_engine::IndexSchema,
) -> Result<(), DatabaseError> {
  match database {
    Database::InMemory(engine) => engine.register_index(schema).await?,
    #[cfg(feature = "automerge")]
    Database::AutomergeInMemory(engine) => engine.register_index(schema).await?,
    #[cfg(all(feature = "automerge", feature = "redb"))]
    Database::AutomergeRedb(engine) => engine.register_index(schema).await?,
    #[cfg(feature = "redb")]
    Database::Redb(engine) => engine.register_index(schema).await?,
  }
  Ok(())
}

async fn drop_index(database: &mut Database, name: &str) -> Result<(), DatabaseError> {
  match database {
    Database::InMemory(engine) => engine.drop_index(name).await?,
    #[cfg(feature = "automerge")]
    Database::AutomergeInMemory(engine) => engine.drop_index(name).await?,
    #[cfg(all(feature = "automerge", feature = "redb"))]
    Database::AutomergeRedb(engine) => engine.drop_index(name).await?,
    #[cfg(feature = "redb")]
    Database::Redb(engine) => engine.drop_index(name).await?,
  }
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
  match transaction {
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
    } => match transaction {
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
    EngineQuery::Delete { table, predicate } => match transaction {
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

pub(crate) async fn transaction_commit(transaction: Transaction<'_>) -> Result<(), DatabaseError> {
  match transaction {
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

pub(crate) async fn transaction_rollback(
  transaction: Transaction<'_>,
) -> Result<(), DatabaseError> {
  match transaction {
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
