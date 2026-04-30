use crate::{EngineKey, EngineRow, EngineValue, IndexSchema};

#[derive(Debug, Clone)]
pub enum EnginePredicate {
  Equals(usize, EngineValue),
  NotEquals(usize, EngineValue),
  LessThan(usize, EngineValue),
  LessThanOrEquals(usize, EngineValue),
  GreaterThan(usize, EngineValue),
  GreaterThanOrEquals(usize, EngineValue),
  IsNull(usize),
  IsNotNull(usize),
  And(Box<EnginePredicate>, Box<EnginePredicate>),
  Or(Box<EnginePredicate>, Box<EnginePredicate>),
  Not(Box<EnginePredicate>),
}

impl EnginePredicate {
  pub fn matches(&self, row: &EngineRow) -> bool {
    match self {
      EnginePredicate::Equals(index, value) => row
        .get(*index)
        .map(|column| column == value)
        .unwrap_or(false),
      EnginePredicate::NotEquals(index, value) => row
        .get(*index)
        .map(|column| column != value)
        .unwrap_or(false),
      EnginePredicate::LessThan(index, value) => row
        .get(*index)
        .map(|column| column < value)
        .unwrap_or(false),
      EnginePredicate::LessThanOrEquals(index, value) => row
        .get(*index)
        .map(|column| column <= value)
        .unwrap_or(false),
      EnginePredicate::GreaterThan(index, value) => row
        .get(*index)
        .map(|column| column > value)
        .unwrap_or(false),
      EnginePredicate::GreaterThanOrEquals(index, value) => row
        .get(*index)
        .map(|column| column >= value)
        .unwrap_or(false),
      EnginePredicate::IsNull(index) => row
        .get(*index)
        .map(|column| matches!(column, EngineValue::Null))
        .unwrap_or(false),
      EnginePredicate::IsNotNull(index) => row
        .get(*index)
        .map(|column| !matches!(column, EngineValue::Null))
        .unwrap_or(false),
      EnginePredicate::And(left, right) => left.matches(row) && right.matches(row),
      EnginePredicate::Or(left, right) => left.matches(row) || right.matches(row),
      EnginePredicate::Not(predicate) => !predicate.matches(row),
    }
  }

  pub fn index_key_for(&self, index: &IndexSchema) -> Option<EngineKey> {
    let mut values = vec![None; index.column_indices.len()];
    self.fill_index_key_values(index, &mut values)?;
    if values.iter().all(Option::is_some) {
      let values = values.into_iter().map(Option::unwrap).collect();
      Some(EngineKey::from_values(values))
    } else {
      None
    }
  }

  fn fill_index_key_values(
    &self,
    index: &IndexSchema,
    values: &mut [Option<EngineValue>],
  ) -> Option<()> {
    match self {
      EnginePredicate::Equals(position, value) => {
        if let Some((slot, _)) = index
          .column_indices
          .iter()
          .enumerate()
          .find(|(_, index)| *index == position)
        {
          if let Some(existing) = &values[slot]
            && existing != value
          {
            return None;
          }
          values[slot] = Some(value.clone());
        }
        Some(())
      }
      EnginePredicate::And(left, right) => {
        left.fill_index_key_values(index, values)?;
        right.fill_index_key_values(index, values)?;
        Some(())
      }
      _ => None,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedColumn {
  pub table: String,
  pub column_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
  Inner,
  Left,
  Right,
  Full,
}

#[derive(Debug, Clone)]
pub enum JoinOn {
  ColumnEq {
    left: QualifiedColumn,
    right: QualifiedColumn,
  },
}

#[derive(Debug, Clone)]
pub struct JoinClause {
  pub kind: JoinKind,
  pub left_table: String,
  pub right_table: String,
  pub on: JoinOn,
}

#[derive(Debug, Clone)]
pub enum Aggregate {
  Count(Option<QualifiedColumn>),
  Sum(QualifiedColumn),
  Min(QualifiedColumn),
  Max(QualifiedColumn),
  Avg(QualifiedColumn),
}

#[derive(Debug, Clone)]
pub enum SortDirection {
  Asc,
  Desc,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
  pub expr: QualifiedColumn,
  pub direction: SortDirection,
}
#[derive(Debug, Clone)]
pub enum QualifiedOperand {
  Column(QualifiedColumn),
  Value(crate::EngineValue),
}

#[derive(Debug, Clone)]
pub enum QualifiedPredicate {
  Equals(QualifiedOperand, QualifiedOperand),
  NotEquals(QualifiedOperand, QualifiedOperand),
  LessThan(QualifiedOperand, QualifiedOperand),
  LessThanOrEquals(QualifiedOperand, QualifiedOperand),
  GreaterThan(QualifiedOperand, QualifiedOperand),
  GreaterThanOrEquals(QualifiedOperand, QualifiedOperand),
  IsNull(QualifiedColumn),
  IsNotNull(QualifiedColumn),
  InList {
    expr: QualifiedColumn,
    list: Vec<crate::EngineValue>,
    negated: bool,
  },
  InSubquery {
    expr: QualifiedColumn,
    subquery: Box<crate::EngineQuery>,
    negated: bool,
  },
  And(Box<QualifiedPredicate>, Box<QualifiedPredicate>),
  Or(Box<QualifiedPredicate>, Box<QualifiedPredicate>),
  Not(Box<QualifiedPredicate>),
}

#[derive(Debug, Clone)]
pub enum RefOrAgg {
  Column(QualifiedColumn),
  AggregateIndex(usize),
}

#[derive(Debug, Clone)]
pub enum HavingPredicate {
  Equals(RefOrAgg, crate::EngineValue),
  NotEquals(RefOrAgg, crate::EngineValue),
  LessThan(RefOrAgg, crate::EngineValue),
  LessThanOrEquals(RefOrAgg, crate::EngineValue),
  GreaterThan(RefOrAgg, crate::EngineValue),
  GreaterThanOrEquals(RefOrAgg, crate::EngineValue),
  IsNull(RefOrAgg),
  IsNotNull(RefOrAgg),
  And(Box<HavingPredicate>, Box<HavingPredicate>),
  Or(Box<HavingPredicate>, Box<HavingPredicate>),
  Not(Box<HavingPredicate>),
}

#[derive(Debug, Clone, Default)]
pub struct SelectOptions {
  pub joins: Vec<JoinClause>,
  pub aggregates: Vec<Aggregate>,
  pub group_by: Vec<QualifiedColumn>,
  pub order_by: Vec<OrderBy>,
  pub limit: Option<usize>,
  pub offset: Option<usize>,
  pub distinct: bool,
  pub having: Option<HavingPredicate>,
}

#[derive(Debug, Clone)]
pub enum EngineQuery {
  Select {
    table: String,
    projection: Vec<QualifiedColumn>,
    predicate: Option<QualifiedPredicate>,
    options: Box<SelectOptions>,
  },
  Insert {
    table: String,
    row: EngineRow,
  },
  Update {
    table: String,
    assignments: Vec<(usize, EngineValue)>,
    predicate: Option<EnginePredicate>,
  },
  Delete {
    table: String,
    predicate: Option<EnginePredicate>,
  },
}

#[derive(Debug, Clone, Default)]
pub struct EngineResult {
  pub rows: Vec<EngineRow>,
}

impl EngineResult {
  pub fn new(rows: Vec<EngineRow>) -> Self {
    Self { rows }
  }
}

impl EngineQuery {
  fn qualified_column(table: &str, index: usize) -> QualifiedColumn {
    QualifiedColumn {
      table: table.into(),
      column_index: index,
    }
  }

  fn column_operand(table: &str, index: usize) -> QualifiedOperand {
    QualifiedOperand::Column(Self::qualified_column(table, index))
  }

  fn value_operand(value: EngineValue) -> QualifiedOperand {
    QualifiedOperand::Value(value)
  }

  fn binary_engine_pred(
    table: &str,
    index: usize,
    value: EngineValue,
    ctor: fn(QualifiedOperand, QualifiedOperand) -> QualifiedPredicate,
  ) -> QualifiedPredicate {
    ctor(
      Self::column_operand(table, index),
      Self::value_operand(value),
    )
  }

  pub fn select_simple(
    table: String,
    projection: Vec<usize>,
    predicate: Option<EnginePredicate>,
  ) -> Self {
    let proj = projection
      .into_iter()
      .map(|i| QualifiedColumn {
        table: table.clone(),
        column_index: i,
      })
      .collect();

    let qpred = predicate.map(|p| Self::engine_pred_to_qualified(p, &table));

    EngineQuery::Select {
      table,
      projection: proj,
      predicate: qpred,
      options: Box::new(SelectOptions::default()),
    }
  }

  fn engine_pred_to_qualified(pred: EnginePredicate, table: &str) -> QualifiedPredicate {
    match pred {
      EnginePredicate::Equals(i, v) => {
        Self::binary_engine_pred(table, i, v, QualifiedPredicate::Equals)
      }
      EnginePredicate::NotEquals(i, v) => {
        Self::binary_engine_pred(table, i, v, QualifiedPredicate::NotEquals)
      }
      EnginePredicate::LessThan(i, v) => {
        Self::binary_engine_pred(table, i, v, QualifiedPredicate::LessThan)
      }
      EnginePredicate::LessThanOrEquals(i, v) => {
        Self::binary_engine_pred(table, i, v, QualifiedPredicate::LessThanOrEquals)
      }
      EnginePredicate::GreaterThan(i, v) => {
        Self::binary_engine_pred(table, i, v, QualifiedPredicate::GreaterThan)
      }
      EnginePredicate::GreaterThanOrEquals(i, v) => {
        Self::binary_engine_pred(table, i, v, QualifiedPredicate::GreaterThanOrEquals)
      }
      EnginePredicate::IsNull(i) => QualifiedPredicate::IsNull(Self::qualified_column(table, i)),
      EnginePredicate::IsNotNull(i) => {
        QualifiedPredicate::IsNotNull(Self::qualified_column(table, i))
      }
      EnginePredicate::And(l, r) => QualifiedPredicate::And(
        Box::new(Self::engine_pred_to_qualified(*l, table)),
        Box::new(Self::engine_pred_to_qualified(*r, table)),
      ),
      EnginePredicate::Or(l, r) => QualifiedPredicate::Or(
        Box::new(Self::engine_pred_to_qualified(*l, table)),
        Box::new(Self::engine_pred_to_qualified(*r, table)),
      ),
      EnginePredicate::Not(p) => {
        QualifiedPredicate::Not(Box::new(Self::engine_pred_to_qualified(*p, table)))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn build_select_ex_shape() {
    let left_col = QualifiedColumn {
      table: "users".into(),
      column_index: 0,
    };
    let right_col = QualifiedColumn {
      table: "orders".into(),
      column_index: 0,
    };

    let join = JoinClause {
      kind: JoinKind::Inner,
      left_table: "users".into(),
      right_table: "orders".into(),
      on: JoinOn::ColumnEq {
        left: left_col.clone(),
        right: right_col.clone(),
      },
    };

    let options = SelectOptions {
      joins: vec![join],
      aggregates: vec![Aggregate::Count(None)],
      group_by: vec![left_col.clone()],
      order_by: vec![OrderBy {
        expr: left_col.clone(),
        direction: SortDirection::Asc,
      }],
      limit: Some(10),
      offset: Some(0),
      distinct: false,
      having: None,
    };

    let q = EngineQuery::Select {
      table: "users".into(),
      projection: vec![left_col],
      predicate: None,
      options: Box::new(options),
    };

    match q {
      EngineQuery::Select { .. } => {}
      _ => panic!("expected Select variant"),
    }
  }
}
