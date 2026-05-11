use futures::future::FutureExt;

use crate::store_adapter::{EngineStore, collect_table_rows, lookup_index_rows};
use crate::{
  EngineError, IndexSchema, TableSchema, query::EngineQuery, query::EngineResult,
  query::QualifiedColumn, query::QualifiedPredicate, query::SelectOptions,
};

use super::catalog::EngineCatalog;
use super::executor::EngineWriteTxn;
use super::select_pipeline::{
  build_sorted_projection_rows, filter_joined_rows, materialize_joined_rows,
};

#[derive(Debug, Clone)]
pub(crate) struct EngineKernel<S> {
  store: S,
  catalog: EngineCatalog,
}

impl<S> EngineKernel<S>
where
  S: EngineStore,
{
  pub(crate) fn new(store: S) -> Self {
    Self {
      store,
      catalog: EngineCatalog::new(),
    }
  }

  pub(crate) async fn open(store: S) -> Result<Self, EngineError> {
    let mut kernel = Self::new(store);
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
      tx: None,
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
      let rows = lookup_index_rows(tx, table_name, &index, predicate).await?;

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

    let mut partial_results = materialize_joined_rows::<S>(tx, base_table, options).await?;

    // If no grouping/aggregation requested, build output rows and apply ORDER/LIMIT
    let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();

    if let Some(qpred) = &predicate {
      filter_joined_rows(&mut partial_results, qpred, |q| self.run(q)).await?;
    }

    if !needs_grouping {
      let keyed = build_sorted_projection_rows(&partial_results, projection, options)?;

      let out_rows = super::operators::Sorter::new(
        options.order_by.clone(),
        options.distinct,
        options.limit,
        options.offset,
        keyed,
      )
      .execute();

      return Ok(EngineResult::new(out_rows));
    }

    // Aggregation path: delegate to Aggregator operator.
    let aggregator = super::operators::Aggregator::new(
      options.group_by.clone(),
      options.aggregates.clone(),
      options.having.clone(),
      options.order_by.clone(),
      options.limit,
      options.offset,
      partial_results,
    );
    Ok(EngineResult::new(aggregator.execute()?))
  }

  pub(crate) async fn run(&self, query: EngineQuery) -> Result<EngineResult, EngineError> {
    match query {
      EngineQuery::Insert { table, row } => {
        let mut writer = self.writer();
        writer.insert(&table, row).await?;
        writer.commit().await?;
        Ok(EngineResult::default())
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
        let rows = writer
          .update(
            &table,
            assignments,
            predicate,
            joins,
            from_tables,
            returning,
          )
          .await?;
        writer.commit().await?;
        Ok(EngineResult::new(rows))
      }
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => {
        let mut writer = self.writer();
        let rows = writer.delete(&table, predicate, returning).await?;
        writer.commit().await?;
        Ok(EngineResult::new(rows))
      }
    }
  }
}
