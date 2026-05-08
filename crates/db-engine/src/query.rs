use crate::{EngineRow, EngineValue};

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
    predicate: Option<QualifiedPredicate>,
  },
  Delete {
    table: String,
    predicate: Option<QualifiedPredicate>,
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
  pub fn select_simple(
    table: String,
    projection: Vec<usize>,
    predicate: Option<QualifiedPredicate>,
  ) -> Self {
    let proj = projection
      .into_iter()
      .map(|i| QualifiedColumn {
        table: table.clone(),
        column_index: i,
      })
      .collect();

    EngineQuery::Select {
      table,
      projection: proj,
      predicate,
      options: Box::new(SelectOptions::default()),
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
