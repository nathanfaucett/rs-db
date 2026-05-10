pub mod aggregate;
pub mod index_join;
pub mod nested_loop_join;
pub mod scan;
pub mod sorter;

pub use aggregate::Aggregator;
pub use sorter::Sorter;

#[allow(dead_code)]
pub trait Operator {
  fn next(&mut self) -> Option<Result<crate::EngineRow, crate::EngineError>>;
}
