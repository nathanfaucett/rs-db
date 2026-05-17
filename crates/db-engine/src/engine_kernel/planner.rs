use futures::future::FutureExt;

use crate::store_adapter::{
  EngineStore, collect_table_rows, lookup_index_row_pks, materialize_rows_by_primary_keys,
};
use crate::{
  EngineError, IndexSchema, TableSchema, query::EngineQuery, query::EngineResult,
  query::QualifiedColumn, query::QualifiedPredicate, query::SelectOptions,
};

use super::catalog::EngineCatalog;
use super::executor::EngineWriteTxn;
use super::select_orchestrator::{
  SelectStageOutput, execute_select_pipeline, finalize_grouped_result,
};
use super::transaction_lifecycle::TransactionLifecycle;
use crate::ChangeListenerRegistry;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct EngineKernel<S> {
  store: S,
  catalog: EngineCatalog,
  change_listener_registry: Arc<ChangeListenerRegistry>,
}

impl<S> EngineKernel<S>
where
  S: EngineStore,
{
  pub(crate) fn new(store: S, change_listener_registry: Arc<ChangeListenerRegistry>) -> Self {
    Self {
      store,
      catalog: EngineCatalog::new(),
      change_listener_registry,
    }
  }

  pub(crate) async fn open(
    store: S,
    change_listener_registry: Arc<ChangeListenerRegistry>,
  ) -> Result<Self, EngineError> {
    let mut kernel = Self::new(store, change_listener_registry);
    kernel.load_schema().await?;
    Ok(kernel)
  }

  pub(crate) async fn load_schema(&mut self) -> Result<(), EngineError> {
    self.catalog.load_from_store(&self.store).await
  }

  pub(crate) fn store(&self) -> &S {
    &self.store
  }

  pub(crate) fn table(&self, table_name: &str) -> Result<&TableSchema, EngineError> {
    self.catalog.table(table_name)
  }

  pub(crate) async fn register_table(&mut self, schema: TableSchema) -> Result<(), EngineError> {
    self.catalog.register_table(&self.store, schema).await
  }

  pub(crate) async fn drop_table(&mut self, table_name: &str) -> Result<(), EngineError> {
    self.catalog.drop_table(&self.store, table_name).await
  }

  pub(crate) async fn register_index(&mut self, schema: IndexSchema) -> Result<(), EngineError> {
    self.catalog.register_index(&self.store, schema).await
  }

  pub(crate) async fn drop_index(&mut self, index_name: &str) -> Result<(), EngineError> {
    self.catalog.drop_index(&self.store, index_name).await
  }

  pub(crate) fn writer(&self) -> EngineWriteTxn<'_, S> {
    EngineWriteTxn {
      store: &self.store,
      catalog: &self.catalog,
      lifecycle: TransactionLifecycle::new(),
      change_listener_registry: self.change_listener_registry.clone(),
    }
  }

  pub(crate) async fn read(
    &self,
    table_name: &str,
    projection: &[usize],
    predicate: Option<QualifiedPredicate>,
  ) -> Result<EngineResult, EngineError> {
    self.table(table_name)?;

    let mut writer = self.writer();
    let tx = writer.transaction().await?;

    if let Some(predicate) = &predicate
      && let Some(index) = self.catalog.find_index_for_predicate(table_name, predicate)
    {
      let row_pks = lookup_index_row_pks(tx, &index, predicate).await?;
      let rows = materialize_rows_by_primary_keys(tx, table_name, row_pks).await?;

      if !rows.is_empty() {
        return Ok(EngineResult::new(
          rows
            .into_iter()
            .map(|row| self.catalog.project_row(&row, projection))
            .collect::<Result<Vec<_>, _>>()?,
        ));
      }
    }

    let rows = collect_table_rows(tx, table_name, predicate).await?;
    Ok(EngineResult::new(
      rows
        .into_iter()
        .map(|(_primary_key, row)| self.catalog.project_row(&row, projection))
        .collect::<Result<Vec<_>, _>>()?,
    ))
  }

  pub(crate) async fn read_extended(
    &self,
    base_table: &str,
    projection: &[QualifiedColumn],
    predicate: Option<QualifiedPredicate>,
    options: &SelectOptions,
  ) -> Result<EngineResult, EngineError> {
    let mut writer = self.writer();
    let tx = writer.transaction().await?;
    match execute_select_pipeline::<S, _, _>(tx, base_table, projection, predicate, options, |q| {
      self.run(q)
    })
    .await?
    {
      SelectStageOutput::Final(result) => Ok(result),
      SelectStageOutput::Joined(partial_results) => {
        finalize_grouped_result(partial_results, options)
      }
    }
  }

  pub(crate) async fn run(&self, query: EngineQuery) -> Result<EngineResult, EngineError> {
    match query {
      EngineQuery::Insert { table, row } => {
        let mut writer = self.writer();
        match writer.insert(&table, row).await {
          Ok(()) => {
            writer.commit().await?;
            Ok(EngineResult::default())
          }
          Err(error) => {
            let _ = writer.rollback().await;
            Err(error)
          }
        }
      }
      EngineQuery::Select {
        table,
        projection,
        predicate,
        options,
      } => {
        return self
          .read_extended(&table, &projection, predicate, &options)
          .boxed_local()
          .await;
      }
      EngineQuery::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => {
        let mut writer = self.writer();
        match writer
          .update(
            &table,
            assignments,
            predicate,
            joins,
            from_tables,
            returning,
          )
          .await
        {
          Ok(rows) => {
            writer.commit().await?;
            Ok(EngineResult::new(rows))
          }
          Err(error) => {
            let _ = writer.rollback().await;
            Err(error)
          }
        }
      }
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => {
        let mut writer = self.writer();
        match writer.delete(&table, predicate, returning).await {
          Ok(rows) => {
            writer.commit().await?;
            Ok(EngineResult::new(rows))
          }
          Err(error) => {
            let _ = writer.rollback().await;
            Err(error)
          }
        }
      }
    }
  }
}
