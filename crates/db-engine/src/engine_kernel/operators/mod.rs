pub mod aggregator;
pub mod index_join;
pub mod nested_loop_join;
pub mod scan;
pub mod sorter;

pub use scan::Scan;

#[allow(dead_code)]
pub trait Operator {
  fn next(&mut self) -> Option<Result<crate::EngineRow, crate::EngineError>>;
}
