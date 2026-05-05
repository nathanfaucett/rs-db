use std::collections::{HashMap, HashSet};

use futures::future::FutureExt;

use crate::predicate::{JoinedRowContext, eval_predicate};
use crate::store_adapter::{EngineStore, EngineStoreTransaction};
use crate::{
  EngineError, EngineRow, EngineValue, IndexSchema, TableSchema, query::EngineQuery,
  query::EngineResult, query::JoinKind, query::JoinOn, query::QualifiedColumn,
  query::QualifiedPredicate, query::SelectOptions,
};

use super::catalog::EngineCatalog;
use super::executor::EngineWriteTxn;
use super::operators::nested_loop_join::{
  apply_full_join, apply_inner_join, apply_left_join, apply_right_join,
};

fn collect_subqueries(pred: &QualifiedPredicate, acc: &mut Vec<crate::EngineQuery>) {
  match pred {
    QualifiedPredicate::InSubquery { subquery, .. } => acc.push((**subquery).clone()),
    QualifiedPredicate::And(l, r) | QualifiedPredicate::Or(l, r) => {
      collect_subqueries(l, acc);
      collect_subqueries(r, acc);
    }
    QualifiedPredicate::Not(p) => collect_subqueries(p, acc),
    _ => {}
  }
}

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
      let rows = tx.lookup_index_rows(table_name, &index, predicate).await?;

      if !rows.is_empty() {
        return Ok(EngineResult::new(
          rows
            .into_iter()
            .map(|row| self.catalog.project_row(&row, projection))
            .collect::<Result<Vec<_>, _>>()?,
        ));
      }
    }

    let rows = EngineWriteTxn::<S>::collect_table_rows(tx, table_name, predicate).await?;
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
    // Acquire a transaction and collect rows for all referenced tables
    let mut writer = self.writer();
    let tx = writer.transaction().await?;

    // Build set of tables referenced
    let mut tables: HashSet<String> = HashSet::new();
    tables.insert(base_table.to_string());
    for j in &options.joins {
      tables.insert(j.left_table.clone());
      tables.insert(j.right_table.clone());
    }

    // Collect rows per table
    let mut table_rows_map: HashMap<String, Vec<EngineRow>> = HashMap::new();
    for table in &tables {
      let rows_with_pk = EngineWriteTxn::<S>::collect_table_rows(tx, table, None).await?;
      let rows = rows_with_pk
        .into_iter()
        .map(|(_pk, row)| row)
        .collect::<Vec<_>>();
      table_rows_map.insert(table.clone(), rows);
    }

    // Template partial with all tables present (None)
    let mut template: HashMap<String, Option<EngineRow>> = HashMap::new();
    for table in &tables {
      template.insert(table.clone(), None);
    }

    // Initialize partial results from base_table rows (or single null entry when base_table empty)
    let mut partial_results: Vec<HashMap<String, Option<EngineRow>>> = Vec::new();
    if let Some(base_rows) = table_rows_map.get(base_table) {
      if !base_rows.is_empty() {
        for row in base_rows {
          let mut m = template.clone();
          m.insert(base_table.to_string(), Some(row.clone()));
          partial_results.push(m);
        }
      } else {
        partial_results.push(template.clone());
      }
    } else {
      partial_results.push(template.clone());
    }

    // Apply joins in provided order using a left-deep nested loop strategy
    for join in &options.joins {
      let _left_table = &join.left_table;
      let right_table = &join.right_table;

      let (left_qc, right_qc) = match &join.on {
        JoinOn::ColumnEq { left, right } => (left, right),
      };

      let right_rows = table_rows_map.get(right_table).cloned().unwrap_or_default();

      partial_results = match join.kind {
        JoinKind::Inner => apply_inner_join(
          &partial_results,
          &right_rows,
          right_table,
          left_qc,
          right_qc,
        ),
        JoinKind::Left => apply_left_join(
          &partial_results,
          &right_rows,
          right_table,
          left_qc,
          right_qc,
        ),
        JoinKind::Right => apply_right_join(
          &partial_results,
          &right_rows,
          right_table,
          left_qc,
          right_qc,
          &template,
        ),
        JoinKind::Full => apply_full_join(
          &partial_results,
          &right_rows,
          right_table,
          left_qc,
          right_qc,
          &template,
        ),
      };
    }

    // If no grouping/aggregation requested, build output rows and apply ORDER/LIMIT
    let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();

    // If a qualified predicate (WHERE) was provided, evaluate it against joined partials now
    if let Some(qpred) = &predicate {
      // gather subqueries to pre-execute
      let mut subquery_keys: Vec<crate::EngineQuery> = Vec::new();
      collect_subqueries(qpred, &mut subquery_keys);

      // execute unique subqueries and cache results keyed by their Debug string
      let mut subquery_cache: HashMap<String, HashSet<EngineValue>> = HashMap::new();
      for q in subquery_keys {
        let key = format!("{:?}", q);
        if subquery_cache.contains_key(&key) {
          continue;
        }
        let res = self.run(q.clone()).await?;
        let mut set: HashSet<EngineValue> = HashSet::new();
        for row in res.rows {
          if let Some(v) = row.first() {
            set.insert(v.clone());
          }
        }
        subquery_cache.insert(key, set);
      }

      let eval_ctx = crate::predicate::EvalContext::with_cache(subquery_cache);
      partial_results.retain(|p| {
        let ctx = JoinedRowContext { partial: p };
        eval_predicate(qpred, &ctx, &eval_ctx)
      });
    }

    if !needs_grouping {
      let mut keyed: Vec<(Vec<EngineValue>, EngineRow)> = Vec::new();

      for partial in &partial_results {
        // Build projection
        let mut out_row: EngineRow = Vec::with_capacity(projection.len());
        for proj in projection {
          match partial.get(&proj.table) {
            Some(Some(row)) => out_row.push(
              row
                .get(proj.column_index)
                .cloned()
                .unwrap_or(EngineValue::Null),
            ),
            Some(None) => out_row.push(EngineValue::Null),
            None => {
              return Err(EngineError::SchemaMismatch(format!(
                "projection references unknown table {}",
                proj.table
              )));
            }
          }
        }

        // Compute order keys
        let mut keys: Vec<EngineValue> = Vec::with_capacity(options.order_by.len());
        for ord in &options.order_by {
          let qc = &ord.expr;
          match partial.get(&qc.table) {
            Some(Some(row)) => keys.push(
              row
                .get(qc.column_index)
                .cloned()
                .unwrap_or(EngineValue::Null),
            ),
            Some(None) => keys.push(EngineValue::Null),
            None => {
              return Err(EngineError::SchemaMismatch(format!(
                "ORDER BY references unknown table {}",
                qc.table
              )));
            }
          }
        }

        keyed.push((keys, out_row));
      }

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
      } => {
        let mut writer = self.writer();
        writer.update(&table, assignments, predicate).await?;
        writer.commit().await?;
        Ok(EngineResult::default())
      }
      EngineQuery::Delete { table, predicate } => {
        let mut writer = self.writer();
        writer.delete(&table, predicate).await?;
        writer.commit().await?;
        Ok(EngineResult::default())
      }
    }
  }
}
