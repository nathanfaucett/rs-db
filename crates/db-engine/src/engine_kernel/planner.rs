use std::collections::{HashMap, HashSet};

use futures::future::FutureExt;

use crate::predicate::{JoinedRowContext, eval_predicate};
use crate::store_adapter::{EngineStore, collect_table_rows, lookup_index_rows};
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

type JoinedRowState = HashMap<String, Option<EngineRow>>;
type JoinedRowStates = Vec<JoinedRowState>;

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

fn collect_joined_tables(base_table: &str, options: &SelectOptions) -> HashSet<String> {
  let mut tables: HashSet<String> = HashSet::new();
  tables.insert(base_table.to_string());
  for join in &options.joins {
    tables.insert(join.left_table.clone());
    tables.insert(join.right_table.clone());
  }
  tables
}

fn build_join_template(tables: &HashSet<String>) -> JoinedRowState {
  let mut template: JoinedRowState = HashMap::new();
  for table in tables {
    template.insert(table.clone(), None);
  }
  template
}

fn seed_joined_row_states(
  base_table: &str,
  table_rows_map: &HashMap<String, Vec<EngineRow>>,
  template: &JoinedRowState,
) -> JoinedRowStates {
  let mut partial_results: JoinedRowStates = Vec::new();
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
  partial_results
}

fn apply_joins(
  options: &SelectOptions,
  table_rows_map: &HashMap<String, Vec<EngineRow>>,
  template: &JoinedRowState,
  mut partial_results: JoinedRowStates,
) -> JoinedRowStates {
  for join in &options.joins {
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
        template,
      ),
      JoinKind::Full => apply_full_join(
        &partial_results,
        &right_rows,
        right_table,
        left_qc,
        right_qc,
        template,
      ),
    };
  }

  partial_results
}

fn build_sorted_projection_rows(
  partial_results: &[JoinedRowState],
  projection: &[QualifiedColumn],
  options: &SelectOptions,
) -> Result<Vec<(Vec<EngineValue>, EngineRow)>, EngineError> {
  let mut keyed: Vec<(Vec<EngineValue>, EngineRow)> = Vec::new();

  for partial in partial_results {
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

  Ok(keyed)
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
  async fn collect_table_rows_map(
    tx: &mut S::Transaction,
    tables: &HashSet<String>,
  ) -> Result<HashMap<String, Vec<EngineRow>>, EngineError> {
    let mut table_rows_map: HashMap<String, Vec<EngineRow>> = HashMap::new();
    for table in tables {
      let rows_with_pk = collect_table_rows(tx, table, None).await?;
      let rows = rows_with_pk
        .into_iter()
        .map(|(_pk, row)| row)
        .collect::<Vec<_>>();
      table_rows_map.insert(table.clone(), rows);
    }
    Ok(table_rows_map)
  }

  async fn filter_joined_rows(
    &self,
    partial_results: &mut JoinedRowStates,
    predicate: &QualifiedPredicate,
  ) -> Result<(), EngineError> {
    let mut subquery_keys: Vec<crate::EngineQuery> = Vec::new();
    collect_subqueries(predicate, &mut subquery_keys);

    let mut subquery_cache: HashMap<String, HashSet<EngineValue>> = HashMap::new();
    for query in subquery_keys {
      let key = format!("{:?}", query);
      if subquery_cache.contains_key(&key) {
        continue;
      }
      let res = self.run(query.clone()).await?;
      let mut set: HashSet<EngineValue> = HashSet::new();
      for row in res.rows {
        if let Some(v) = row.first() {
          set.insert(v.clone());
        }
      }
      subquery_cache.insert(key, set);
    }

    let eval_ctx = crate::predicate::EvalContext::with_cache(subquery_cache);
    partial_results.retain(|partial| {
      let ctx = JoinedRowContext { partial };
      eval_predicate(predicate, &ctx, &eval_ctx)
    });
    Ok(())
  }

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

    let tables = collect_joined_tables(base_table, options);
    let table_rows_map = Self::collect_table_rows_map(tx, &tables).await?;
    let template = build_join_template(&tables);
    let mut partial_results = seed_joined_row_states(base_table, &table_rows_map, &template);
    partial_results = apply_joins(options, &table_rows_map, &template, partial_results);

    // If no grouping/aggregation requested, build output rows and apply ORDER/LIMIT
    let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();

    if let Some(qpred) = &predicate {
      self.filter_joined_rows(&mut partial_results, qpred).await?;
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
