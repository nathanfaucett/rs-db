use std::collections::{HashMap, HashSet};
use std::future::Future;

use crate::predicate::{JoinedRowContext, eval_predicate};
use crate::store_adapter::{EngineStore, collect_table_rows};
use crate::{
  EngineError, EngineRow, EngineValue,
  query::{
    EngineQuery, EngineResult, JoinKind, JoinOn, QualifiedColumn, QualifiedPredicate, SelectOptions,
  },
};

use super::operators::nested_loop_join::{
  apply_full_join, apply_inner_join, apply_left_join, apply_right_join,
};

pub(crate) type JoinedRowState = HashMap<String, Option<EngineRow>>;
pub(crate) type JoinedRowStates = Vec<JoinedRowState>;

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

async fn collect_table_rows_map<S>(
  tx: &mut S::Transaction,
  tables: &HashSet<String>,
) -> Result<HashMap<String, Vec<EngineRow>>, EngineError>
where
  S: EngineStore,
{
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

pub(crate) async fn materialize_joined_rows<S>(
  tx: &mut S::Transaction,
  base_table: &str,
  options: &SelectOptions,
) -> Result<JoinedRowStates, EngineError>
where
  S: EngineStore,
{
  let tables = collect_joined_tables(base_table, options);
  let table_rows_map = collect_table_rows_map::<S>(tx, &tables).await?;
  let template = build_join_template(&tables);
  let partial_results = seed_joined_row_states(base_table, &table_rows_map, &template);
  Ok(apply_joins(
    options,
    &table_rows_map,
    &template,
    partial_results,
  ))
}

pub(crate) fn build_sorted_projection_rows(
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

fn collect_subqueries(pred: &QualifiedPredicate, acc: &mut Vec<EngineQuery>) {
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

/// Filter `partial_results` by `predicate`, executing any subqueries via `run_subquery`.
///
/// Keeping this inside the select pipeline seam ensures all join-state mutation
/// lives behind one interface rather than leaking into planner orchestration.
pub(crate) async fn filter_joined_rows<F, Fut>(
  partial_results: &mut JoinedRowStates,
  predicate: &QualifiedPredicate,
  run_subquery: F,
) -> Result<(), EngineError>
where
  F: Fn(EngineQuery) -> Fut,
  Fut: Future<Output = Result<EngineResult, EngineError>>,
{
  let mut subquery_list: Vec<EngineQuery> = Vec::new();
  collect_subqueries(predicate, &mut subquery_list);

  let mut subquery_cache: HashMap<String, HashSet<EngineValue>> = HashMap::new();
  for query in subquery_list {
    let key = format!("{:?}", query);
    if subquery_cache.contains_key(&key) {
      continue;
    }
    let res = run_subquery(query).await?;
    let set: HashSet<EngineValue> = res
      .rows
      .into_iter()
      .filter_map(|row| row.into_iter().next())
      .collect();
    subquery_cache.insert(key, set);
  }

  let eval_ctx = crate::predicate::EvalContext::with_cache(subquery_cache);
  partial_results.retain(|partial| {
    let ctx = JoinedRowContext { partial };
    eval_predicate(predicate, &ctx, &eval_ctx)
  });
  Ok(())
}
