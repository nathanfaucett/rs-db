use crate::{
  EngineError, EngineRow, EngineValue,
  query::{QualifiedColumn, QualifiedPredicate},
};

pub struct Scan {
  table: String,
  rows: Vec<EngineRow>,
  projection: Vec<QualifiedColumn>,
  predicate: Option<QualifiedPredicate>,
  pos: usize,
}

impl Scan {
  pub fn new(
    table: String,
    rows: Vec<EngineRow>,
    projection: Vec<QualifiedColumn>,
    predicate: Option<QualifiedPredicate>,
  ) -> Self {
    Self {
      table,
      rows,
      projection,
      predicate,
      pos: 0,
    }
  }

  pub fn next(&mut self) -> Option<Result<EngineRow, EngineError>> {
    while let Some(row) = self.rows.get(self.pos).cloned() {
      self.pos += 1;
      if !self.matches_row(&row) {
        continue;
      }
      return Some(self.project_row(&row));
    }

    None
  }

  fn project_row(&self, row: &EngineRow) -> Result<EngineRow, EngineError> {
    let mut projected = Vec::with_capacity(self.projection.len());

    for qc in &self.projection {
      if qc.table != self.table {
        return Err(EngineError::SchemaMismatch(format!(
          "projection references unknown table {}",
          qc.table
        )));
      }

      projected.push(
        row
          .get(qc.column_index)
          .cloned()
          .unwrap_or(EngineValue::Null),
      );
    }

    Ok(projected)
  }

  fn matches_row(&self, row: &EngineRow) -> bool {
    if let Some(pred) = &self.predicate {
      self.eval_predicate(pred, row)
    } else {
      true
    }
  }

  fn eval_predicate(&self, pred: &QualifiedPredicate, row: &EngineRow) -> bool {
    use crate::predicate::{SingleRowContext, eval_predicate as do_eval};
    use std::collections::HashMap;
    let ctx = SingleRowContext {
      table: &self.table,
      row,
    };
    do_eval(pred, &ctx, &HashMap::new())
  }
}

impl crate::engine_kernel::operators::Operator for Scan {
  fn next(&mut self) -> Option<Result<EngineRow, EngineError>> {
    Scan::next(self)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{EngineValue, query::QualifiedOperand};

  #[test]
  fn scan_filters_and_projects_single_table_rows() {
    let rows = vec![
      vec![EngineValue::Integer(1), EngineValue::Text("Alice".into())],
      vec![EngineValue::Integer(2), EngineValue::Text("Bob".into())],
    ];

    let projection = vec![QualifiedColumn {
      table: "users".into(),
      column_index: 1,
    }];

    let predicate = Some(QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      }),
      QualifiedOperand::Value(EngineValue::Integer(1)),
    ));

    let mut scan = Scan::new("users".into(), rows, projection, predicate);

    let mut results = Vec::new();
    while let Some(result) = scan.next() {
      results.push(result.expect("scan should not fail"));
    }

    assert_eq!(results, vec![vec![EngineValue::Text("Alice".into())]]);
  }
}
