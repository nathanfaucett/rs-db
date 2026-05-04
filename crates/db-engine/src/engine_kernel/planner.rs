use std::collections::{HashMap, HashSet};

use futures::future::FutureExt;

use crate::predicate::{GroupRowContext, JoinedRowContext, eval_having_predicate, eval_predicate};
use crate::store_adapter::{EngineStore, EngineStoreTransaction};
use crate::{
  EngineError, EngineKey, EngineRow, EngineValue, IndexSchema, TableSchema, query::EngineQuery,
  query::EngineResult, query::JoinKind, query::JoinOn, query::QualifiedColumn,
  query::QualifiedPredicate, query::SelectOptions,
};

use super::catalog::EngineCatalog;
use super::executor::{EngineWriteTxn, Executor};
use super::operators::nested_loop_join::{
  apply_full_join, apply_inner_join, apply_left_join, apply_right_join,
};
use super::plan::LogicalPlan;

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

fn is_simple_select(options: &crate::query::SelectOptions) -> bool {
  options.joins.is_empty()
    && options.aggregates.is_empty()
    && options.group_by.is_empty()
    && options.order_by.is_empty()
    && options.limit.is_none()
    && options.offset.is_none()
    && !options.distinct
    && options.having.is_none()
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

      partial_results.retain(|p| {
        let ctx = JoinedRowContext { partial: p };
        eval_predicate(qpred, &ctx, &subquery_cache)
      });
    }

    if !needs_grouping {
      struct KeyedRow {
        keys: Vec<EngineValue>,
        row: EngineRow,
      }

      let mut keyed: Vec<KeyedRow> = Vec::new();

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

        keyed.push(KeyedRow { keys, row: out_row });
      }

      // Apply DISTINCT if requested (dedupe by row before sorting)
      if options.distinct {
        let mut seen: HashSet<EngineRow> = HashSet::new();
        keyed.retain(|kr| seen.insert(kr.row.clone()));
      }

      // Sort if requested
      if !options.order_by.is_empty() {
        let orders = options.order_by.clone();
        keyed.sort_by(|a, b| {
          for (i, ord) in orders.iter().enumerate() {
            let av = a.keys.get(i).unwrap_or(&EngineValue::Null);
            let bv = b.keys.get(i).unwrap_or(&EngineValue::Null);
            let cmp = av.cmp(bv);
            let cmp = match ord.direction {
              crate::query::SortDirection::Asc => cmp,
              crate::query::SortDirection::Desc => cmp.reverse(),
            };
            if cmp != std::cmp::Ordering::Equal {
              return cmp;
            }
          }
          std::cmp::Ordering::Equal
        });
      }

      // Apply offset/limit and extract rows
      let mut out_rows: Vec<EngineRow> = Vec::new();
      let offset = options.offset.unwrap_or(0);
      let mut taken = 0usize;
      for (idx, kr) in keyed.into_iter().enumerate() {
        if idx < offset {
          continue;
        }
        out_rows.push(kr.row);
        taken += 1;
        if let Some(limit) = options.limit
          && taken >= limit
        {
          break;
        }
      }

      return Ok(EngineResult::new(out_rows));
    }

    // Aggregation path: group rows by the requested group_by columns and maintain aggregator state.

    #[derive(Clone)]
    enum AggState {
      Count(u64),
      Sum(f64),
      Min(Option<EngineValue>),
      Max(Option<EngineValue>),
      Avg { sum: f64, count: u64 },
    }

    impl AggState {
      fn new_for(agg: &crate::query::Aggregate) -> Self {
        match agg {
          crate::query::Aggregate::Count(_) => AggState::Count(0),
          crate::query::Aggregate::Sum(_) => AggState::Sum(0.0),
          crate::query::Aggregate::Min(_) => AggState::Min(None),
          crate::query::Aggregate::Max(_) => AggState::Max(None),
          crate::query::Aggregate::Avg(_) => AggState::Avg { sum: 0.0, count: 0 },
        }
      }

      fn update(&mut self, agg: &crate::query::Aggregate, value: Option<EngineValue>) {
        match agg {
          crate::query::Aggregate::Count(col) => {
            if let AggState::Count(c) = self {
              if col.is_none() {
                *c += 1;
              } else if let Some(v) = value
                && !matches!(v, EngineValue::Null)
              {
                *c += 1;
              }
            }
          }
          crate::query::Aggregate::Sum(_) => {
            if let AggState::Sum(s) = self
              && let Some(v) = value
              && let Some(n) = to_f64(&v)
            {
              *s += n;
            }
          }
          crate::query::Aggregate::Min(_) => {
            if let AggState::Min(opt) = self
              && let Some(v) = value
              && (opt.is_none() || v < opt.as_ref().unwrap().clone())
            {
              *opt = Some(v);
            }
          }
          crate::query::Aggregate::Max(_) => {
            if let AggState::Max(opt) = self
              && let Some(v) = value
              && (opt.is_none() || v > opt.as_ref().unwrap().clone())
            {
              *opt = Some(v);
            }
          }
          crate::query::Aggregate::Avg(_) => {
            if let AggState::Avg { sum, count } = self
              && let Some(v) = value
              && let Some(n) = to_f64(&v)
            {
              *sum += n;
              *count += 1;
            }
          }
        }
      }

      fn finish(&self) -> EngineValue {
        match self {
          AggState::Count(c) => EngineValue::Integer(*c as i64),
          AggState::Sum(s) => EngineValue::Float(*s),
          AggState::Min(Some(v)) => v.clone(),
          AggState::Min(None) => EngineValue::Null,
          AggState::Max(Some(v)) => v.clone(),
          AggState::Max(None) => EngineValue::Null,
          AggState::Avg { sum, count } => {
            if *count == 0 {
              EngineValue::Null
            } else {
              EngineValue::Float(*sum / (*count as f64))
            }
          }
        }
      }
    }

    fn to_f64(value: &EngineValue) -> Option<f64> {
      match value {
        EngineValue::Integer(i) => Some(*i as f64),
        EngineValue::Float(f) => Some(*f),
        _ => None,
      }
    }

    let mut groups: HashMap<EngineKey, Vec<AggState>> = HashMap::new();

    for partial in &partial_results {
      // Build group key values
      let mut key_vals: Vec<EngineValue> = Vec::with_capacity(options.group_by.len());
      for gc in &options.group_by {
        match partial.get(&gc.table) {
          Some(Some(row)) => key_vals.push(
            row
              .get(gc.column_index)
              .cloned()
              .unwrap_or(EngineValue::Null),
          ),
          Some(None) => key_vals.push(EngineValue::Null),
          None => {
            return Err(EngineError::SchemaMismatch(format!(
              "group_by references unknown table {}",
              gc.table
            )));
          }
        }
      }

      let key = EngineKey::from_values(key_vals);

      let entry = groups.entry(key).or_insert_with(|| {
        options
          .aggregates
          .iter()
          .map(AggState::new_for)
          .collect::<Vec<_>>()
      });

      // Update aggregates for this partial row
      for (i, agg) in options.aggregates.iter().enumerate() {
        let val = match agg {
          crate::query::Aggregate::Count(Some(qc))
          | crate::query::Aggregate::Sum(qc)
          | crate::query::Aggregate::Min(qc)
          | crate::query::Aggregate::Max(qc)
          | crate::query::Aggregate::Avg(qc) => match partial.get(&qc.table) {
            Some(Some(row)) => row.get(qc.column_index).cloned(),
            Some(None) => None,
            None => None,
          },
          crate::query::Aggregate::Count(None) => None,
        };

        entry[i].update(agg, val);
      }
    }

    // Build final rows: group key values + aggregate values in order
    let mut out_rows: Vec<EngineRow> = Vec::with_capacity(groups.len());
    for (key, agg_states) in groups {
      let mut row: EngineRow = key.values().to_vec();
      for st in agg_states {
        row.push(st.finish());
      }
      out_rows.push(row);
    }

    // Apply HAVING filter if present
    if let Some(having) = &options.having {
      out_rows.retain(|r| {
        let ctx = GroupRowContext {
          row: r,
          group_by: &options.group_by,
        };
        eval_having_predicate(having, &ctx)
      });
    }

    // If ORDER BY is requested in a grouped query, only support ordering by group keys or by aggregates
    if !options.order_by.is_empty() {
      // Map each OrderBy to an index in out_rows
      let mut orders_idx: Vec<(usize, crate::query::SortDirection)> = Vec::new();
      for ord in &options.order_by {
        // try to find in group_by
        if let Some(pos) = options.group_by.iter().position(|gc| gc == &ord.expr) {
          orders_idx.push((pos, ord.direction.clone()));
          continue;
        }

        // try to find in aggregates
        if let Some(pos) = options.aggregates.iter().position(|agg| match agg {
          crate::query::Aggregate::Count(None) => false,
          crate::query::Aggregate::Count(Some(qc)) => qc == &ord.expr,
          crate::query::Aggregate::Sum(qc) => qc == &ord.expr,
          crate::query::Aggregate::Min(qc) => qc == &ord.expr,
          crate::query::Aggregate::Max(qc) => qc == &ord.expr,
          crate::query::Aggregate::Avg(qc) => qc == &ord.expr,
        }) {
          // aggregate columns appear after group keys
          orders_idx.push((options.group_by.len() + pos, ord.direction.clone()));
          continue;
        }

        return Err(EngineError::SchemaMismatch(
          "ORDER BY references unknown group or aggregate column".into(),
        ));
      }

      out_rows.sort_by(|a, b| {
        for (idx, dir) in &orders_idx {
          let av = a.get(*idx).unwrap_or(&EngineValue::Null);
          let bv = b.get(*idx).unwrap_or(&EngineValue::Null);
          let cmp = av.cmp(bv);
          let cmp = match dir {
            crate::query::SortDirection::Asc => cmp,
            crate::query::SortDirection::Desc => cmp.reverse(),
          };
          if cmp != std::cmp::Ordering::Equal {
            return cmp;
          }
        }
        std::cmp::Ordering::Equal
      });
    }

    // Apply offset/limit
    let offset = options.offset.unwrap_or(0);
    let mut limited: Vec<EngineRow> = Vec::new();
    for (i, row) in out_rows.into_iter().enumerate() {
      if i < offset {
        continue;
      }
      limited.push(row);
      if let Some(limit) = options.limit
        && limited.len() >= limit
      {
        break;
      }
    }

    Ok(EngineResult::new(limited))
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
        let plan = LogicalPlan::Select {
          table: table.clone(),
          projection: projection.clone(),
          predicate: predicate.clone(),
          options: options.clone(),
        };

        if is_simple_select(&options) {
          let mut writer = self.writer();
          let tx = writer.transaction().await?;
          let mut executor: Executor<'_, S> = Executor::new(tx, &self.catalog);
          return executor.execute_plan(&plan).await;
        }

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
