use hashbrown::HashMap;
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, UnaryOperator};

use super::TranslateError;

fn resolve_operand(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedOperand, TranslateError> {
  match expr {
    SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
      Ok(db_engine::QualifiedOperand::Column(
        crate::translate::helpers::resolve_column_local(expr, alias_map, table_schemas)?,
      ))
    }
    SqlExpr::Value(_) => Ok(db_engine::QualifiedOperand::Value(
      mapper.map_sql_value(expr)?,
    )),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported operand in comparison".into(),
    )),
  }
}

fn binary_predicate(
  op: &BinaryOperator,
  left: db_engine::QualifiedOperand,
  right: db_engine::QualifiedOperand,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  Ok(match op {
    BinaryOperator::Eq => db_engine::QualifiedPredicate::Equals(left, right),
    BinaryOperator::NotEq => db_engine::QualifiedPredicate::NotEquals(left, right),
    BinaryOperator::Lt => db_engine::QualifiedPredicate::LessThan(left, right),
    BinaryOperator::LtEq => db_engine::QualifiedPredicate::LessThanOrEquals(left, right),
    BinaryOperator::Gt => db_engine::QualifiedPredicate::GreaterThan(left, right),
    BinaryOperator::GtEq => db_engine::QualifiedPredicate::GreaterThanOrEquals(left, right),
    _ => {
      return Err(TranslateError::UnsupportedFeature(
        "unsupported binary operator in WHERE".into(),
      ));
    }
  })
}

fn extract_column_value(
  left: &db_engine::QualifiedOperand,
  right: &db_engine::QualifiedOperand,
  base_table: &str,
) -> Result<(usize, db_engine::EngineValue), TranslateError> {
  match (left, right) {
    (db_engine::QualifiedOperand::Column(qc), db_engine::QualifiedOperand::Value(v))
    | (db_engine::QualifiedOperand::Value(v), db_engine::QualifiedOperand::Column(qc)) => {
      if qc.table != base_table {
        return Err(TranslateError::UnsupportedFeature(
          "predicate references non-base table".into(),
        ));
      }
      Ok((qc.column_index, v.clone()))
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "cannot convert predicate to engine predicate".into(),
    )),
  }
}

/// Convert a sqlparser expression into a qualified predicate (used for JOINs, HAVING, etc.).
pub fn expr_to_qualified_predicate(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  // helper to resolve identifier -> QualifiedColumn
  let resolve_qc_local = |expr: &SqlExpr| -> Result<db_engine::QualifiedColumn, TranslateError> {
    crate::translate::helpers::resolve_column_local(expr, alias_map, table_schemas)
  };

  match expr {
    SqlExpr::BinaryOp { left, op, right } => {
      use sqlparser::ast::BinaryOperator;
      match op {
        BinaryOperator::And => Ok(db_engine::QualifiedPredicate::And(
          Box::new(expr_to_qualified_predicate(
            left,
            alias_map,
            table_schemas,
            resolver,
            mapper,
          )?),
          Box::new(expr_to_qualified_predicate(
            right,
            alias_map,
            table_schemas,
            resolver,
            mapper,
          )?),
        )),
        BinaryOperator::Or => Ok(db_engine::QualifiedPredicate::Or(
          Box::new(expr_to_qualified_predicate(
            left,
            alias_map,
            table_schemas,
            resolver,
            mapper,
          )?),
          Box::new(expr_to_qualified_predicate(
            right,
            alias_map,
            table_schemas,
            resolver,
            mapper,
          )?),
        )),
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => {
          let left_op = resolve_operand(left, alias_map, table_schemas, mapper)?;
          let right_op = resolve_operand(right, alias_map, table_schemas, mapper)?;
          binary_predicate(op, left_op, right_op)
        }
        _ => Err(TranslateError::UnsupportedFeature(
          "unsupported binary operator in WHERE".into(),
        )),
      }
    }
    SqlExpr::UnaryOp { op, expr } => match op {
      UnaryOperator::Not => Ok(db_engine::QualifiedPredicate::Not(Box::new(
        expr_to_qualified_predicate(expr, alias_map, table_schemas, resolver, mapper)?,
      ))),
      _ => Err(TranslateError::UnsupportedFeature(
        "unsupported unary operator in WHERE".into(),
      )),
    },
    SqlExpr::InList {
      expr: in_expr,
      list,
      negated,
    } => {
      let qc = resolve_qc_local(in_expr)?;
      let mut values: Vec<db_engine::EngineValue> = Vec::new();
      for item in list {
        match item {
          SqlExpr::Value(_) => values.push(mapper.map_sql_value(item)?),
          _ => {
            return Err(TranslateError::UnsupportedFeature(
              "IN list only supports literals in v1".into(),
            ));
          }
        }
      }
      Ok(db_engine::QualifiedPredicate::InList {
        expr: qc,
        list: values,
        negated: *negated,
      })
    }
    SqlExpr::InSubquery {
      expr: in_expr,
      subquery,
      negated,
    } => {
      let qc = resolve_qc_local(in_expr)?;
      // translate subquery to EngineQuery; reject correlated queries via resolver errors
      let sub_sql = format!("{}", subquery);
      let sub_q = crate::translate::parse_and_translate_with_mapper(&sub_sql, resolver, mapper)?;
      Ok(db_engine::QualifiedPredicate::InSubquery {
        expr: qc,
        subquery: Box::new(sub_q),
        negated: *negated,
      })
    }
    SqlExpr::IsNull(inner) => Ok(db_engine::QualifiedPredicate::IsNull(resolve_qc_local(
      inner,
    )?)),
    SqlExpr::IsNotNull(inner) => Ok(db_engine::QualifiedPredicate::IsNotNull(resolve_qc_local(
      inner,
    )?)),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported WHERE expression".into(),
    )),
  }
}

pub fn qualified_to_engine_pred(
  pred: &db_engine::QualifiedPredicate,
  base_table: &str,
) -> Result<db_engine::EnginePredicate, TranslateError> {
  match pred {
    db_engine::QualifiedPredicate::Equals(l, r) => {
      let (index, value) = extract_column_value(l, r, base_table)?;
      Ok(db_engine::EnginePredicate::Equals(index, value))
    }
    db_engine::QualifiedPredicate::NotEquals(l, r) => {
      let (index, value) = extract_column_value(l, r, base_table)?;
      Ok(db_engine::EnginePredicate::NotEquals(index, value))
    }
    db_engine::QualifiedPredicate::And(l, r) => Ok(db_engine::EnginePredicate::And(
      Box::new(qualified_to_engine_pred(l, base_table)?),
      Box::new(qualified_to_engine_pred(r, base_table)?),
    )),
    db_engine::QualifiedPredicate::Or(l, r) => Ok(db_engine::EnginePredicate::Or(
      Box::new(qualified_to_engine_pred(l, base_table)?),
      Box::new(qualified_to_engine_pred(r, base_table)?),
    )),
    db_engine::QualifiedPredicate::Not(p) => Ok(db_engine::EnginePredicate::Not(Box::new(
      qualified_to_engine_pred(p, base_table)?,
    ))),
    db_engine::QualifiedPredicate::InList {
      expr,
      list,
      negated,
    } => {
      if expr.table != base_table {
        return Err(TranslateError::UnsupportedFeature(
          "IN list references non-base table".into(),
        ));
      }
      // convert to OR of equals
      let mut it = list.iter();
      if let Some(first) = it.next() {
        let mut pred = if *negated {
          db_engine::EnginePredicate::Not(Box::new(db_engine::EnginePredicate::Equals(
            expr.column_index,
            first.clone(),
          )))
        } else {
          db_engine::EnginePredicate::Equals(expr.column_index, first.clone())
        };
        for v in it {
          let p = db_engine::EnginePredicate::Equals(expr.column_index, v.clone());
          pred = db_engine::EnginePredicate::Or(Box::new(pred), Box::new(p));
        }
        Ok(pred)
      } else {
        Err(TranslateError::UnsupportedFeature("empty IN list".into()))
      }
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported qualification for engine predicate".into(),
    )),
  }
}
