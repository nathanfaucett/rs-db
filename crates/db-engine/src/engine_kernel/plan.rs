use crate::{
  EngineRow, query::EngineQuery, query::JoinClause, query::QualifiedColumn,
  query::QualifiedPredicate, query::SelectOptions, query::UpdateAssignment,
};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum LogicalPlan {
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
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
    joins: Vec<JoinClause>,
    from_tables: Vec<String>,
    returning: Option<Vec<QualifiedColumn>>,
  },
  Delete {
    table: String,
    predicate: Option<QualifiedPredicate>,
    returning: Option<Vec<QualifiedColumn>>,
  },
}

#[allow(dead_code)]
impl LogicalPlan {
  pub fn from_query(query: EngineQuery) -> Self {
    match query {
      EngineQuery::Select {
        table,
        projection,
        predicate,
        options,
      } => LogicalPlan::Select {
        table,
        projection,
        predicate,
        options,
      },
      EngineQuery::Insert { table, row } => LogicalPlan::Insert { table, row },
      EngineQuery::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => LogicalPlan::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      },
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => LogicalPlan::Delete {
        table,
        predicate,
        returning,
      },
    }
  }

  pub fn is_simple_select(&self) -> bool {
    match self {
      LogicalPlan::Select { options, .. } => {
        options.joins.is_empty()
          && options.aggregates.is_empty()
          && options.group_by.is_empty()
          && options.order_by.is_empty()
          && options.limit.is_none()
          && options.offset.is_none()
          && !options.distinct
          && options.having.is_none()
      }
      _ => false,
    }
  }
}
