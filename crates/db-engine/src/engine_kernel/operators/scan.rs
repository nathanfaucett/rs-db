use crate::{
  EngineError, EngineRow, EngineValue,
  query::{QualifiedColumn, QualifiedOperand, QualifiedPredicate},
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
    let operand_value = |op: &QualifiedOperand| match op {
      QualifiedOperand::Value(v) => Some(v.clone()),
      QualifiedOperand::Column(qc) => {
        if qc.table != self.table {
          return None;
        }
        row.get(qc.column_index).cloned()
      }
    };

    match pred {
      QualifiedPredicate::Equals(left, right) => {
        let lv = operand_value(left);
        let rv = operand_value(right);
        matches!((lv, rv), (Some(lv), Some(rv)) if lv == rv)
      }
      QualifiedPredicate::NotEquals(left, right) => {
        let lv = operand_value(left);
        let rv = operand_value(right);
        matches!((lv, rv), (Some(lv), Some(rv)) if lv != rv)
      }
      QualifiedPredicate::LessThan(left, right) => {
        let lv = operand_value(left);
        let rv = operand_value(right);
        matches!((lv, rv), (Some(lv), Some(rv)) if lv < rv)
      }
      QualifiedPredicate::LessThanOrEquals(left, right) => {
        let lv = operand_value(left);
        let rv = operand_value(right);
        matches!((lv, rv), (Some(lv), Some(rv)) if lv <= rv)
      }
      QualifiedPredicate::GreaterThan(left, right) => {
        let lv = operand_value(left);
        let rv = operand_value(right);
        matches!((lv, rv), (Some(lv), Some(rv)) if lv > rv)
      }
      QualifiedPredicate::GreaterThanOrEquals(left, right) => {
        let lv = operand_value(left);
        let rv = operand_value(right);
        matches!((lv, rv), (Some(lv), Some(rv)) if lv >= rv)
      }
      QualifiedPredicate::IsNull(qc) => {
        match operand_value(&QualifiedOperand::Column(qc.clone())) {
          Some(EngineValue::Null) => true,
          _ => false,
        }
      }
      QualifiedPredicate::IsNotNull(qc) => {
        match operand_value(&QualifiedOperand::Column(qc.clone())) {
          Some(EngineValue::Null) => false,
          Some(_) => true,
          None => false,
        }
      }
      QualifiedPredicate::InList {
        expr,
        list,
        negated,
      } => {
        let found = match operand_value(&QualifiedOperand::Column(expr.clone())) {
          Some(v) => list.iter().any(|x| x == &v),
          None => false,
        };
        if *negated { !found } else { found }
      }
      QualifiedPredicate::InSubquery { .. } => false,
      QualifiedPredicate::And(left, right) => {
        self.eval_predicate(left, row) && self.eval_predicate(right, row)
      }
      QualifiedPredicate::Or(left, right) => {
        self.eval_predicate(left, row) || self.eval_predicate(right, row)
      }
      QualifiedPredicate::Not(inner) => !self.eval_predicate(inner, row),
    }
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
