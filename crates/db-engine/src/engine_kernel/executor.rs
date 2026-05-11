use super::catalog::EngineCatalog;
use super::operators::nested_loop_join::{
  apply_full_join, apply_inner_join, apply_left_join, apply_right_join,
};
use super::select_pipeline::JoinedRowState;
use crate::predicate::{EvalContext, JoinedRowContext, eval_predicate};
use crate::store_adapter::{
  EngineStore, EngineStoreTransaction, collect_table_rows, delete_row, find_conflicting_index_entry,
};
use crate::{
  EngineError, EngineKey, EngineRow, EngineValue, IndexSchema, query::JoinClause,
  query::QualifiedColumn, query::QualifiedPredicate, query::UpdateAssignment,
  query::UpdateValueExpr,
};
use std::collections::{HashMap, HashSet};

async fn ensure_indexes_unique<TX>(
  tx: &mut TX,
  indexes: &[IndexSchema],
  row: &EngineRow,
  row_pk: &EngineKey,
) -> Result<(), EngineError>
where
  TX: crate::store_adapter::EngineStoreTransaction,
{
  for index in indexes.iter().filter(|index| index.unique) {
    let index_key = index.key_for(row)?;
    if find_conflicting_index_entry(tx, index, &index_key, row_pk)
      .await?
      .is_some()
    {
      return Err(EngineError::UniqueIndexViolation(index.name.clone()));
    }
  }
  Ok(())
}

async fn insert_all_index_entries<TX>(
  tx: &mut TX,
  indexes: &[IndexSchema],
  row: &EngineRow,
  primary_key: &EngineKey,
) -> Result<(), EngineError>
where
  TX: crate::store_adapter::EngineStoreTransaction,
{
  for index in indexes {
    let index_key = index.key_for(row)?;
    tx.insert_index_entry(index, &index_key, primary_key)
      .await?;
  }
  Ok(())
}

#[derive(Debug)]
pub(crate) struct EngineWriteTxn<'db, S>
where
  S: EngineStore,
  S::Transaction: EngineStoreTransaction,
{
  pub(crate) store: &'db S,
  pub(crate) catalog: &'db EngineCatalog,
  pub(crate) tx: Option<S::Transaction>,
}

impl<'db, S> EngineWriteTxn<'db, S>
where
  S: EngineStore,
  S::Transaction: EngineStoreTransaction,
{
  pub(crate) async fn transaction(&mut self) -> Result<&mut S::Transaction, EngineError> {
    if self.tx.is_none() {
      let tx = self.store.engine_transaction().await?;
      self.tx = Some(tx);
    }

    Ok(self.tx.as_mut().expect("transaction should be initialized"))
  }

  pub(crate) async fn insert(
    &mut self,
    table_name: &str,
    row: EngineRow,
  ) -> Result<(), EngineError> {
    let table = self.catalog.table(table_name)?;
    table.validate_row(&row)?;
    let pk = table.primary_key(&row)?;
    let indexes = self.catalog.indexes_for_table(table_name);

    let tx = self.transaction().await?;
    if tx.get_table_row(table_name, &pk).await?.is_some() {
      return Err(EngineError::DuplicatePrimaryKey(pk));
    }

    ensure_indexes_unique(tx, &indexes, &row, &pk).await?;

    tx.insert_table_row(table_name, pk.clone(), row.clone())
      .await?;

    insert_all_index_entries(tx, &indexes, &row, &pk).await?;

    Ok(())
  }

  pub(crate) async fn delete(
    &mut self,
    table_name: &str,
    predicate: Option<QualifiedPredicate>,
    returning: Option<Vec<QualifiedColumn>>,
  ) -> Result<Vec<EngineRow>, EngineError> {
    self.catalog.table(table_name)?;
    let indexes = self.catalog.indexes_for_table(table_name);
    let tx = self.transaction().await?;
    let rows = collect_table_rows(tx, table_name, predicate).await?;
    let mut returning_rows: Vec<EngineRow> = Vec::new();

    for (primary_key, row) in rows {
      if let Some(projection) = returning.as_ref() {
        returning_rows.push(Self::project_returning_row(table_name, &row, projection)?);
      }
      delete_row(tx, table_name, &primary_key, &row, &indexes).await?;
    }

    Ok(returning_rows)
  }

  pub(crate) async fn update(
    &mut self,
    table_name: &str,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
    joins: Vec<JoinClause>,
    from_tables: Vec<String>,
    returning: Option<Vec<QualifiedColumn>>,
  ) -> Result<Vec<EngineRow>, EngineError> {
    let table = self.catalog.table(table_name)?.clone();
    if assignments.is_empty() {
      return Ok(Vec::new());
    }

    let indexes = self.catalog.indexes_for_table(table_name);
    let tx = self.transaction().await?;

    let rows = if joins.is_empty() && from_tables.is_empty() {
      collect_table_rows(tx, table_name, predicate)
        .await?
        .into_iter()
        .map(|(pk, row)| (pk, row, None))
        .collect::<Vec<_>>()
    } else {
      Self::collect_join_update_rows(
        tx,
        &table,
        table_name,
        &joins,
        &from_tables,
        predicate.as_ref(),
      )
      .await?
    };

    let mut returning_rows: Vec<EngineRow> = Vec::new();
    for (old_pk, row, joined_state) in rows {
      let updated_row =
        Self::apply_assignments(&row, &assignments, table_name, joined_state.as_ref())?;
      table.validate_row(&updated_row)?;
      let new_pk = table.primary_key(&updated_row)?;

      if new_pk != old_pk && tx.get_table_row(table_name, &new_pk).await?.is_some() {
        return Err(EngineError::DuplicatePrimaryKey(new_pk));
      }

      delete_row(tx, table_name, &old_pk, &row, &indexes).await?;
      ensure_indexes_unique(tx, &indexes, &updated_row, &new_pk).await?;

      tx.insert_table_row(table_name, new_pk.clone(), updated_row.clone())
        .await?;

      insert_all_index_entries(tx, &indexes, &updated_row, &new_pk).await?;

      if let Some(projection) = returning.as_ref() {
        returning_rows.push(Self::project_returning_row(
          table_name,
          &updated_row,
          projection,
        )?);
      }
    }

    Ok(returning_rows)
  }

  fn project_returning_row(
    table_name: &str,
    row: &EngineRow,
    projection: &[QualifiedColumn],
  ) -> Result<EngineRow, EngineError> {
    let mut output: EngineRow = Vec::with_capacity(projection.len());
    for column in projection {
      if column.table != table_name {
        return Err(EngineError::SchemaMismatch(format!(
          "RETURNING column {} must reference target table {}",
          column.table, table_name
        )));
      }
      let value = row.get(column.column_index).cloned().ok_or_else(|| {
        EngineError::SchemaMismatch(format!(
          "RETURNING index {} out of bounds",
          column.column_index
        ))
      })?;
      output.push(value);
    }
    Ok(output)
  }

  fn apply_assignments(
    row: &EngineRow,
    assignments: &[UpdateAssignment],
    target_table: &str,
    joined_state: Option<&JoinedRowState>,
  ) -> Result<EngineRow, EngineError> {
    let mut updated = row.clone();

    for assignment in assignments {
      let value = Self::evaluate_update_expr(row, &assignment.value, target_table, joined_state)?;
      let cell = updated.get_mut(assignment.column_index).ok_or_else(|| {
        EngineError::SchemaMismatch(format!(
          "update index {} is out of bounds",
          assignment.column_index
        ))
      })?;
      *cell = value;
    }

    Ok(updated)
  }

  fn evaluate_update_expr(
    row: &EngineRow,
    expr: &UpdateValueExpr,
    target_table: &str,
    joined_state: Option<&JoinedRowState>,
  ) -> Result<EngineValue, EngineError> {
    match expr {
      UpdateValueExpr::Value(value) => Ok(value.clone()),
      UpdateValueExpr::Column(column) => {
        if column.table == target_table {
          return row.get(column.column_index).cloned().ok_or_else(|| {
            EngineError::SchemaMismatch(format!(
              "column index {} is out of bounds",
              column.column_index
            ))
          });
        }

        let state = joined_state.ok_or_else(|| {
          EngineError::SchemaMismatch(format!(
            "column {}.{} requires join context",
            column.table, column.column_index
          ))
        })?;

        let joined_row = state
          .get(&column.table)
          .and_then(|entry| entry.as_ref())
          .ok_or_else(|| {
            EngineError::SchemaMismatch(format!(
              "join table {} has no row for expression",
              column.table
            ))
          })?;

        joined_row.get(column.column_index).cloned().ok_or_else(|| {
          EngineError::SchemaMismatch(format!(
            "column index {} is out of bounds",
            column.column_index
          ))
        })
      }
      UpdateValueExpr::Add(left, right) => Self::evaluate_numeric_binary(
        row,
        left,
        right,
        "+",
        |l, r| l + r,
        target_table,
        joined_state,
      ),
      UpdateValueExpr::Subtract(left, right) => Self::evaluate_numeric_binary(
        row,
        left,
        right,
        "-",
        |l, r| l - r,
        target_table,
        joined_state,
      ),
      UpdateValueExpr::Multiply(left, right) => Self::evaluate_numeric_binary(
        row,
        left,
        right,
        "*",
        |l, r| l * r,
        target_table,
        joined_state,
      ),
      UpdateValueExpr::Divide(left, right) => {
        let left_value = Self::evaluate_update_expr(row, left, target_table, joined_state)?;
        let right_value = Self::evaluate_update_expr(row, right, target_table, joined_state)?;
        if matches!(&right_value, EngineValue::Integer(0)) {
          return Err(EngineError::TypeMismatch("division by zero".into()));
        }
        if matches!(&right_value, EngineValue::Float(v) if *v == 0.0) {
          return Err(EngineError::TypeMismatch("division by zero".into()));
        }
        let left_number = Self::engine_value_to_f64(&left_value, "/")?;
        let right_number = Self::engine_value_to_f64(&right_value, "/")?;
        Ok(EngineValue::Float(left_number / right_number))
      }
    }
  }

  fn evaluate_numeric_binary<F>(
    row: &EngineRow,
    left: &UpdateValueExpr,
    right: &UpdateValueExpr,
    op: &str,
    apply: F,
    target_table: &str,
    joined_state: Option<&JoinedRowState>,
  ) -> Result<EngineValue, EngineError>
  where
    F: Fn(f64, f64) -> f64,
  {
    let left_value = Self::evaluate_update_expr(row, left, target_table, joined_state)?;
    let right_value = Self::evaluate_update_expr(row, right, target_table, joined_state)?;

    if left_value == EngineValue::Null || right_value == EngineValue::Null {
      return Ok(EngineValue::Null);
    }

    let left_number = Self::engine_value_to_f64(&left_value, op)?;
    let right_number = Self::engine_value_to_f64(&right_value, op)?;
    let output = apply(left_number, right_number);

    if let (EngineValue::Integer(_), EngineValue::Integer(_)) = (&left_value, &right_value)
      && op != "/"
    {
      return Ok(EngineValue::Integer(output as i64));
    }

    Ok(EngineValue::Float(output))
  }

  fn engine_value_to_f64(value: &EngineValue, op: &str) -> Result<f64, EngineError> {
    match value {
      EngineValue::Integer(number) => Ok(*number as f64),
      EngineValue::Float(number) => Ok(*number),
      other => Err(EngineError::TypeMismatch(format!(
        "operator {op} requires numeric values, got {other:?}"
      ))),
    }
  }

  async fn collect_join_update_rows(
    tx: &mut S::Transaction,
    table: &crate::TableSchema,
    table_name: &str,
    joins: &[JoinClause],
    from_tables: &[String],
    predicate: Option<&QualifiedPredicate>,
  ) -> Result<Vec<(EngineKey, EngineRow, Option<JoinedRowState>)>, EngineError> {
    let mut all_tables: HashSet<String> = HashSet::new();
    all_tables.insert(table_name.to_string());
    for from_table in from_tables {
      all_tables.insert(from_table.clone());
    }
    for join in joins {
      all_tables.insert(join.left_table.clone());
      all_tables.insert(join.right_table.clone());
    }

    let mut table_rows_map: HashMap<String, Vec<EngineRow>> = HashMap::new();
    for table_name in &all_tables {
      let rows_with_pk = collect_table_rows(tx, table_name, None).await?;
      let rows = rows_with_pk
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();
      table_rows_map.insert(table_name.clone(), rows);
    }

    let mut template: JoinedRowState = HashMap::new();
    for table_name in &all_tables {
      template.insert(table_name.clone(), None);
    }

    let base_rows = table_rows_map.get(table_name).cloned().unwrap_or_default();
    let mut partial_results: Vec<JoinedRowState> = base_rows
      .iter()
      .map(|row| {
        let mut partial = template.clone();
        partial.insert(table_name.to_string(), Some(row.clone()));
        partial
      })
      .collect();

    for from_table in from_tables {
      let source_rows = table_rows_map.get(from_table).cloned().unwrap_or_default();
      if source_rows.is_empty() {
        partial_results.clear();
        break;
      }

      let mut expanded: Vec<JoinedRowState> = Vec::new();
      for partial in &partial_results {
        for source_row in &source_rows {
          let mut next = partial.clone();
          next.insert(from_table.clone(), Some(source_row.clone()));
          expanded.push(next);
        }
      }
      partial_results = expanded;
    }

    for join in joins {
      let right_rows = table_rows_map
        .get(&join.right_table)
        .cloned()
        .unwrap_or_default();

      let (left_qc, right_qc) = match &join.on {
        crate::query::JoinOn::ColumnEq { left, right } => (left, right),
      };

      partial_results = match join.kind {
        crate::query::JoinKind::Inner => apply_inner_join(
          &partial_results,
          &right_rows,
          &join.right_table,
          left_qc,
          right_qc,
        ),
        crate::query::JoinKind::Left => apply_left_join(
          &partial_results,
          &right_rows,
          &join.right_table,
          left_qc,
          right_qc,
        ),
        crate::query::JoinKind::Right => apply_right_join(
          &partial_results,
          &right_rows,
          &join.right_table,
          left_qc,
          right_qc,
          &template,
        ),
        crate::query::JoinKind::Full => apply_full_join(
          &partial_results,
          &right_rows,
          &join.right_table,
          left_qc,
          right_qc,
          &template,
        ),
      };
    }

    if let Some(pred) = predicate {
      let eval_ctx = EvalContext::empty();
      partial_results.retain(|partial| {
        let ctx = JoinedRowContext { partial };
        eval_predicate(pred, &ctx, &eval_ctx)
      });
    }

    let mut matched: HashMap<EngineKey, (EngineRow, JoinedRowState)> = HashMap::new();
    let mut seen: HashSet<EngineKey> = HashSet::new();

    for partial in partial_results {
      let Some(Some(base_row)) = partial.get(table_name) else {
        continue;
      };

      let pk = table.primary_key(base_row)?;
      if seen.contains(&pk) {
        return Err(EngineError::SchemaMismatch(
          "UPDATE JOIN matched target row more than once".into(),
        ));
      }
      seen.insert(pk.clone());
      matched.insert(pk, (base_row.clone(), partial));
    }

    Ok(
      matched
        .into_iter()
        .map(|(pk, (row, partial))| (pk, row, Some(partial)))
        .collect(),
    )
  }

  pub(crate) async fn commit(mut self) -> Result<(), EngineError> {
    if let Some(tx) = self.tx.take() {
      tx.commit().await?;
    }
    Ok(())
  }

  pub(crate) async fn rollback(mut self) -> Result<(), EngineError> {
    if let Some(tx) = self.tx.take() {
      tx.rollback().await?;
    }
    Ok(())
  }
}
