use hashbrown::HashMap;
use sqlparser::ast::{
  Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator,
};

// helper to resolve qualified column for HAVING
fn resolve_qc_for_having(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<db_engine::QualifiedColumn, TranslateError> {
  crate::translate::helpers::resolve_column_local(expr, alias_map, table_schemas)
}

use super::TranslateError;

pub fn expr_to_having_predicate(
  expr: &SqlExpr,
  _group_by: &Vec<db_engine::QualifiedColumn>,
  aggregates: &Vec<db_engine::Aggregate>,
  proj_alias_map: &HashMap<String, db_engine::QualifiedColumn>,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  _resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::HavingPredicate, TranslateError> {
  use sqlparser::ast::BinaryOperator;

  // helper to resolve an aggregate reference to index
  let find_agg_index =
    |func_name: &str, arg_qc: Option<db_engine::QualifiedColumn>| -> Option<usize> {
      for (i, ag) in aggregates.iter().enumerate() {
        match (ag, func_name) {
          (db_engine::Aggregate::Count(opt), "count") => match (opt, &arg_qc) {
            (None, None) => return Some(i),
            (Some(a), Some(q)) if a == q => return Some(i),
            _ => {}
          },
          (db_engine::Aggregate::Sum(a), "sum") if Some(a) == arg_qc.as_ref() => return Some(i),
          (db_engine::Aggregate::Min(a), "min") if Some(a) == arg_qc.as_ref() => return Some(i),
          (db_engine::Aggregate::Max(a), "max") if Some(a) == arg_qc.as_ref() => return Some(i),
          (db_engine::Aggregate::Avg(a), "avg") if Some(a) == arg_qc.as_ref() => return Some(i),
          _ => {}
        }
      }
      None
    };

  // helper to build RefOrAgg from expression (aggregate func or column)
  let resolve_ref = |e: &SqlExpr| -> Result<db_engine::RefOrAgg, TranslateError> {
    match e {
      SqlExpr::Function(func) => {
        let fname = func.name.to_string().to_lowercase();
        let first = fname.split('.').next().unwrap_or("");
        let args = match &func.args {
          FunctionArguments::List(list) => &list.args[..],
          FunctionArguments::None => &[],
          FunctionArguments::Subquery(_) => {
            return Err(TranslateError::UnsupportedFeature(
              "aggregate with subquery arg in HAVING unsupported".into(),
            ));
          }
        };
        if args.len() != 1 && !(first == "count" && args.len() == 1) {
          return Err(TranslateError::UnsupportedFeature(
            "aggregate in HAVING must take one argument".into(),
          ));
        }
        let arg_opt = match &args[0] {
          FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) => {
            Some(resolve_qc_for_having(arg_expr, alias_map, table_schemas)?)
          }
          FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
          _ => {
            return Err(TranslateError::UnsupportedFeature(
              "unsupported aggregate arg in HAVING".into(),
            ));
          }
        };
        if let Some(idx) = find_agg_index(first, arg_opt.clone()) {
          Ok(db_engine::RefOrAgg::AggregateIndex(idx))
        } else {
          Err(TranslateError::UnsupportedFeature(
            "unknown aggregate in HAVING".into(),
          ))
        }
      }
      SqlExpr::Identifier(ident) => {
        // may be alias referencing projection/aggregate
        if let Some(qc) = proj_alias_map.get(&ident.value) {
          // see if this alias corresponds to an aggregate by comparing against aggregates
          for (i, ag) in aggregates.iter().enumerate() {
            match ag {
              db_engine::Aggregate::Count(opt) => {
                if let Some(arg) = opt
                  && arg == qc
                {
                  return Ok(db_engine::RefOrAgg::AggregateIndex(i));
                }
              }
              db_engine::Aggregate::Sum(a)
              | db_engine::Aggregate::Min(a)
              | db_engine::Aggregate::Max(a)
              | db_engine::Aggregate::Avg(a) => {
                if a == qc {
                  return Ok(db_engine::RefOrAgg::AggregateIndex(i));
                }
              }
            }
          }
          // otherwise treat as column reference (group key)
          Ok(db_engine::RefOrAgg::Column(qc.clone()))
        } else {
          // treat as qualified column (resolve against table schemas)
          let qc = resolve_qc_for_having(e, alias_map, table_schemas)?;
          Ok(db_engine::RefOrAgg::Column(qc))
        }
      }
      SqlExpr::CompoundIdentifier(_) => {
        let qc = resolve_qc_for_having(e, alias_map, table_schemas)?;
        Ok(db_engine::RefOrAgg::Column(qc))
      }
      _ => Err(TranslateError::UnsupportedFeature(
        "unsupported HAVING operand".into(),
      )),
    }
  };

  match expr {
    SqlExpr::BinaryOp { left, op, right } => match op {
      BinaryOperator::And => Ok(db_engine::HavingPredicate::And(
        Box::new(expr_to_having_predicate(
          left,
          _group_by,
          aggregates,
          proj_alias_map,
          alias_map,
          table_schemas,
          _resolver,
          mapper,
        )?),
        Box::new(expr_to_having_predicate(
          right,
          _group_by,
          aggregates,
          proj_alias_map,
          alias_map,
          table_schemas,
          _resolver,
          mapper,
        )?),
      )),
      BinaryOperator::Or => Ok(db_engine::HavingPredicate::Or(
        Box::new(expr_to_having_predicate(
          left,
          _group_by,
          aggregates,
          proj_alias_map,
          alias_map,
          table_schemas,
          _resolver,
          mapper,
        )?),
        Box::new(expr_to_having_predicate(
          right,
          _group_by,
          aggregates,
          proj_alias_map,
          alias_map,
          table_schemas,
          _resolver,
          mapper,
        )?),
      )),
      BinaryOperator::Eq
      | BinaryOperator::NotEq
      | BinaryOperator::Lt
      | BinaryOperator::LtEq
      | BinaryOperator::Gt
      | BinaryOperator::GtEq => {
        let lref = resolve_ref(left)?;
        let rval = match &**right {
          SqlExpr::Value(_) => mapper.map_sql_value(right)?,
          _ => {
            return Err(TranslateError::UnsupportedFeature(
              "HAVING RHS must be literal value in v1".into(),
            ));
          }
        };
        let pred = match op {
          BinaryOperator::Eq => db_engine::HavingPredicate::Equals(lref, rval),
          BinaryOperator::NotEq => db_engine::HavingPredicate::NotEquals(lref, rval),
          BinaryOperator::Lt => db_engine::HavingPredicate::LessThan(lref, rval),
          BinaryOperator::LtEq => db_engine::HavingPredicate::LessThanOrEquals(lref, rval),
          BinaryOperator::Gt => db_engine::HavingPredicate::GreaterThan(lref, rval),
          BinaryOperator::GtEq => db_engine::HavingPredicate::GreaterThanOrEquals(lref, rval),
          _ => unreachable!(),
        };
        Ok(pred)
      }
      _ => Err(TranslateError::UnsupportedFeature(
        "unsupported binary operator in HAVING".into(),
      )),
    },
    SqlExpr::UnaryOp { op, expr } => match op {
      UnaryOperator::Not => Ok(db_engine::HavingPredicate::Not(Box::new(
        expr_to_having_predicate(
          expr,
          _group_by,
          aggregates,
          proj_alias_map,
          alias_map,
          table_schemas,
          _resolver,
          mapper,
        )?,
      ))),
      _ => Err(TranslateError::UnsupportedFeature(
        "unsupported unary operator in HAVING".into(),
      )),
    },
    SqlExpr::IsNull(inner) => Ok(db_engine::HavingPredicate::IsNull(resolve_ref(inner)?)),
    SqlExpr::IsNotNull(inner) => Ok(db_engine::HavingPredicate::IsNotNull(resolve_ref(inner)?)),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported HAVING expression".into(),
    )),
  }
}
