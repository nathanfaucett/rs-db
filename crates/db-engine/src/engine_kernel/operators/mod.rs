pub mod aggregate;
pub mod index_join;
pub(crate) mod join_algorithm;
pub mod nested_loop_join;
pub mod scan;
pub mod sorter;

pub use aggregate::Aggregator;
pub(crate) use join_algorithm::{
  JoinAlgorithm, NestedLoopFull, NestedLoopInner, NestedLoopLeft, NestedLoopRight,
};
pub use sorter::Sorter;

#[allow(dead_code)]
pub trait Operator {
  fn next(&mut self) -> Option<Result<crate::EngineRow, crate::EngineError>>;
}
