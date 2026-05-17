#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};
#[cfg(feature = "std")]
use std::{string::String, vec::Vec};

use db_engine::EngineValue;
use hashbrown::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub enum SqlParams {
  Positional(Vec<EngineValue>),
  Named(HashMap<String, EngineValue>),
}

impl Default for SqlParams {
  fn default() -> Self {
    Self::Positional(Vec::new())
  }
}

impl SqlParams {
  pub fn positional(values: Vec<EngineValue>) -> Self {
    Self::Positional(values)
  }

  pub fn named<I, K>(entries: I) -> Self
  where
    I: IntoIterator<Item = (K, EngineValue)>,
    K: Into<String>,
  {
    let values = entries
      .into_iter()
      .map(|(k, v)| (k.into(), v))
      .collect::<HashMap<_, _>>();
    Self::Named(values)
  }

  pub(crate) fn get_positional(&self, one_based: usize) -> Option<&EngineValue> {
    match self {
      Self::Positional(values) => one_based.checked_sub(1).and_then(|index| values.get(index)),
      Self::Named(_) => None,
    }
  }

  pub(crate) fn get_named(&self, name: &str) -> Option<&EngineValue> {
    match self {
      Self::Named(values) => values.get(name),
      Self::Positional(_) => None,
    }
  }
}

impl From<Vec<EngineValue>> for SqlParams {
  fn from(values: Vec<EngineValue>) -> Self {
    Self::Positional(values)
  }
}

impl From<HashMap<String, EngineValue>> for SqlParams {
  fn from(values: HashMap<String, EngineValue>) -> Self {
    Self::Named(values)
  }
}
