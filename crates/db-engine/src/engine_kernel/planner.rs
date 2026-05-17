use futures::future::FutureExt;

use crate::store_adapter::{
  EngineStore, collect_table_rows, lookup_index_row_pks, materialize_rows_by_primary_keys,
};
use crate::{
  EngineError, IndexSchema, TableSchema, query::Aggregate, query::EngineQuery, query::EngineResult,
  query::QualifiedColumn, query::QualifiedPredicate, query::ResultColumn, query::SelectOptions,
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
  fn dedupe_result_column_names(columns: &mut [ResultColumn]) {
    use std::collections::HashMap;

    let mut counts: HashMap<String, usize> = HashMap::new();
    for column in columns.iter() {
      *counts.entry(column.name.clone()).or_insert(0) += 1;
    }

    for column in columns.iter_mut() {
      if counts.get(&column.name).copied().unwrap_or(0) > 1
        && let Some(table) = &column.source_table
      {
        column.name = format!("{}.{}", table, column.name);
      }
    }

    let mut seen: HashMap<String, usize> = HashMap::new();
    for column in columns.iter_mut() {
      let entry = seen.entry(column.name.clone()).or_insert(0);
      if *entry > 0 {
        column.name = format!("{}_{}", column.name, *entry + 1);
      }
      *entry += 1;
    }
  }

  fn projection_columns_for_table(
    &self,
    table_name: &str,
    projection: &[usize],
  ) -> Result<Vec<ResultColumn>, EngineError> {
    let schema = self.table(table_name)?;
    let mut columns = Vec::new();

    if projection.is_empty() {
      for (column_index, column) in schema.columns.iter().enumerate() {
        columns.push(ResultColumn::new(
          column.name.clone(),
          Some(table_name.to_string()),
          Some(column_index),
        ));
      }
    } else {
      for column_index in projection {
        let column = schema.columns.get(*column_index).ok_or_else(|| {
          EngineError::SchemaMismatch(format!(
            "projection index {} is out of bounds",
            column_index
          ))
        })?;
        columns.push(ResultColumn::new(
          column.name.clone(),
          Some(table_name.to_string()),
          Some(*column_index),
        ));
      }
    }

    Self::dedupe_result_column_names(&mut columns);
    Ok(columns)
  }

  fn projection_columns_for_qualified(
    &self,
    projection: &[QualifiedColumn],
  ) -> Result<Vec<ResultColumn>, EngineError> {
    let mut columns = Vec::with_capacity(projection.len());

    for column_ref in projection {
      let schema = self.table(&column_ref.table)?;
      let column = schema.columns.get(column_ref.column_index).ok_or_else(|| {
        EngineError::SchemaMismatch(format!(
          "projection index {} is out of bounds for table {}",
          column_ref.column_index, column_ref.table
        ))
      })?;
      columns.push(ResultColumn::new(
        column.name.clone(),
        Some(column_ref.table.clone()),
        Some(column_ref.column_index),
      ));
    }

    Self::dedupe_result_column_names(&mut columns);
    Ok(columns)
  }

  fn aggregate_label(aggregate: &Aggregate) -> String {
    match aggregate {
      Aggregate::Count(None) => "count_all".to_string(),
      Aggregate::Count(Some(column)) => {
        format!("count_{}_{}", column.table, column.column_index)
      }
      Aggregate::Sum(column) => format!("sum_{}_{}", column.table, column.column_index),
      Aggregate::Min(column) => format!("min_{}_{}", column.table, column.column_index),
      Aggregate::Max(column) => format!("max_{}_{}", column.table, column.column_index),
      Aggregate::Avg(column) => format!("avg_{}_{}", column.table, column.column_index),
    }
  }

  fn output_columns_for_select(
    &self,
    projection: &[QualifiedColumn],
    options: &SelectOptions,
  ) -> Result<Vec<ResultColumn>, EngineError> {
    let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();
    if !needs_grouping {
      return self.projection_columns_for_qualified(projection);
    }

    let mut columns = Vec::with_capacity(options.group_by.len() + options.aggregates.len());
    for group_column in &options.group_by {
      let schema = self.table(&group_column.table)?;
      let column = schema
        .columns
        .get(group_column.column_index)
        .ok_or_else(|| {
          EngineError::SchemaMismatch(format!(
            "GROUP BY index {} is out of bounds for table {}",
            group_column.column_index, group_column.table
          ))
        })?;
      columns.push(ResultColumn::new(
        column.name.clone(),
        Some(group_column.table.clone()),
        Some(group_column.column_index),
      ));
    }

    for aggregate in &options.aggregates {
      columns.push(ResultColumn::new(
        Self::aggregate_label(aggregate),
        None,
        None,
      ));
    }

    Self::dedupe_result_column_names(&mut columns);
    Ok(columns)
  }

  fn output_columns_for_returning(
    &self,
    table_name: &str,
    returning: &[QualifiedColumn],
  ) -> Result<Vec<ResultColumn>, EngineError> {
    let schema = self.table(table_name)?;
    let mut columns = Vec::with_capacity(returning.len());

    for column_ref in returning {
      if column_ref.table != table_name {
        return Err(EngineError::SchemaMismatch(format!(
          "RETURNING column {} must reference target table {}",
          column_ref.table, table_name
        )));
      }

      let column = schema.columns.get(column_ref.column_index).ok_or_else(|| {
        EngineError::SchemaMismatch(format!(
          "RETURNING index {} is out of bounds for table {}",
          column_ref.column_index, table_name
        ))
      })?;

      columns.push(ResultColumn::new(
        column.name.clone(),
        Some(table_name.to_string()),
        Some(column_ref.column_index),
      ));
    }

    Self::dedupe_result_column_names(&mut columns);
    Ok(columns)
  }

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
      pending_events: Vec::new(),
    }
  }

  pub(crate) async fn read(
    &self,
    table_name: &str,
    projection: &[usize],
    predicate: Option<QualifiedPredicate>,
  ) -> Result<EngineResult, EngineError> {
    self.table(table_name)?;
    let columns = self.projection_columns_for_table(table_name, projection)?;

    let mut writer = self.writer();
    let tx = writer.transaction().await?;

    if let Some(predicate) = &predicate
      && let Some(index) = self.catalog.find_index_for_predicate(table_name, predicate)
    {
      let row_pks = lookup_index_row_pks(tx, &index, predicate).await?;
      let rows = materialize_rows_by_primary_keys(tx, table_name, row_pks).await?;

      if !rows.is_empty() {
        return Ok(EngineResult::new_with_columns(
          rows
            .into_iter()
            .map(|row| self.catalog.project_row(&row, projection))
            .collect::<Result<Vec<_>, _>>()?,
          columns.clone(),
        ));
      }
    }

    let rows = collect_table_rows(tx, table_name, predicate).await?;
    Ok(EngineResult::new_with_columns(
      rows
        .into_iter()
        .map(|(_primary_key, row)| self.catalog.project_row(&row, projection))
        .collect::<Result<Vec<_>, _>>()?,
      columns,
    ))
  }

  pub(crate) async fn read_extended(
    &self,
    base_table: &str,
    projection: &[QualifiedColumn],
    predicate: Option<QualifiedPredicate>,
    options: &SelectOptions,
  ) -> Result<EngineResult, EngineError> {
    let output_columns = self.output_columns_for_select(projection, options)?;
    let mut writer = self.writer();
    let tx = writer.transaction().await?;
    match execute_select_pipeline::<S, _, _>(
      tx,
      base_table,
      projection,
      predicate,
      options,
      output_columns.clone(),
      |q| self.run(q),
    )
    .await?
    {
      SelectStageOutput::Final(result) => Ok(result),
      SelectStageOutput::Joined(partial_results) => {
        finalize_grouped_result(partial_results, options, output_columns)
      }
    }
  }

  pub(crate) async fn run(&self, query: EngineQuery) -> Result<EngineResult, EngineError> {
    match query {
      EngineQuery::Insert { table, row } => {
        let mut writer = self.writer();
        match writer.insert(&table, row).await {
          Ok(()) => {
            let _ = writer.commit().await?;
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
        let returning_columns = match &returning {
          Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
          None => None,
        };
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
            let _ = writer.commit().await?;
            Ok(match returning_columns {
              Some(columns) => EngineResult::new_with_columns(rows, columns),
              None => EngineResult::new(rows),
            })
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
        let returning_columns = match &returning {
          Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
          None => None,
        };
        match writer.delete(&table, predicate, returning).await {
          Ok(rows) => {
            let _ = writer.commit().await?;
            Ok(match returning_columns {
              Some(columns) => EngineResult::new_with_columns(rows, columns),
              None => EngineResult::new(rows),
            })
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
