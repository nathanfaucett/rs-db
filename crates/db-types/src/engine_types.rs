use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "std")]
use std::string::String;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "ts",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub enum EngineType {
  Integer,
  Float,
  Text,
  Blob,
}

#[derive(Debug, Clone)]
pub enum EngineValue {
  Integer(i64),
  Float(f64),
  Text(String),
  Blob(Vec<u8>),
  Null,
}

impl PartialEq for EngineValue {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (EngineValue::Null, EngineValue::Null) => true,
      (EngineValue::Integer(left), EngineValue::Integer(right)) => left == right,
      (EngineValue::Float(left), EngineValue::Float(right)) => {
        if left == right {
          true
        } else {
          left.is_nan() && right.is_nan()
        }
      }
      (EngineValue::Text(left), EngineValue::Text(right)) => left == right,
      (EngineValue::Blob(left), EngineValue::Blob(right)) => left == right,
      _ => false,
    }
  }
}

impl Eq for EngineValue {}

impl Hash for EngineValue {
  fn hash<H: Hasher>(&self, state: &mut H) {
    match self {
      EngineValue::Null => {
        state.write_u8(0);
      }
      EngineValue::Integer(value) => {
        state.write_u8(1);
        state.write_i64(*value);
      }
      EngineValue::Float(value) => {
        state.write_u8(2);
        let canonical = if *value == 0.0 {
          0_u64
        } else if value.is_nan() {
          f64::NAN.to_bits()
        } else {
          value.to_bits()
        };
        state.write_u64(canonical);
      }
      EngineValue::Text(value) => {
        state.write_u8(3);
        value.hash(state);
      }
      EngineValue::Blob(value) => {
        state.write_u8(4);
        value.hash(state);
      }
    }
  }
}

impl PartialOrd for EngineValue {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

fn compare_floats(left: f64, right: f64) -> Ordering {
  if left == right {
    Ordering::Equal
  } else if left.is_nan() {
    if right.is_nan() {
      Ordering::Equal
    } else {
      Ordering::Greater
    }
  } else if right.is_nan() {
    Ordering::Less
  } else {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
  }
}

impl Ord for EngineValue {
  fn cmp(&self, other: &Self) -> Ordering {
    match (self, other) {
      (EngineValue::Null, EngineValue::Null) => Ordering::Equal,
      (EngineValue::Null, _) => Ordering::Less,
      (_, EngineValue::Null) => Ordering::Greater,
      (EngineValue::Integer(left), EngineValue::Integer(right)) => left.cmp(right),
      (EngineValue::Float(left), EngineValue::Float(right)) => compare_floats(*left, *right),
      (EngineValue::Integer(left), EngineValue::Float(right)) => {
        compare_floats(*left as f64, *right)
      }
      (EngineValue::Float(left), EngineValue::Integer(right)) => {
        compare_floats(*left, *right as f64)
      }
      (EngineValue::Text(left), EngineValue::Text(right)) => left.cmp(right),
      (EngineValue::Blob(left), EngineValue::Blob(right)) => left.cmp(right),
      (EngineValue::Integer(_), EngineValue::Text(_)) => Ordering::Less,
      (EngineValue::Integer(_), EngineValue::Blob(_)) => Ordering::Less,
      (EngineValue::Float(_), EngineValue::Text(_)) => Ordering::Less,
      (EngineValue::Float(_), EngineValue::Blob(_)) => Ordering::Less,
      (EngineValue::Text(_), EngineValue::Integer(_)) => Ordering::Greater,
      (EngineValue::Text(_), EngineValue::Float(_)) => Ordering::Greater,
      (EngineValue::Text(_), EngineValue::Blob(_)) => Ordering::Less,
      (EngineValue::Blob(_), _) => Ordering::Greater,
    }
  }
}

impl fmt::Display for EngineValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      EngineValue::Integer(value) => write!(f, "{}", value),
      EngineValue::Float(value) => write!(f, "{}", value),
      EngineValue::Text(value) => write!(f, "{}", value),
      EngineValue::Blob(value) => {
        write!(f, "0x")?;
        for byte in value {
          write!(f, "{:02x}", byte)?;
        }
        Ok(())
      }
      EngineValue::Null => write!(f, "NULL"),
    }
  }
}

impl From<i64> for EngineValue {
  fn from(value: i64) -> Self {
    EngineValue::Integer(value)
  }
}

impl From<f64> for EngineValue {
  fn from(value: f64) -> Self {
    EngineValue::Float(value)
  }
}

impl From<String> for EngineValue {
  fn from(value: String) -> Self {
    EngineValue::Text(value)
  }
}

impl From<&str> for EngineValue {
  fn from(value: &str) -> Self {
    EngineValue::Text(value.to_string())
  }
}

impl From<Vec<u8>> for EngineValue {
  fn from(value: Vec<u8>) -> Self {
    EngineValue::Blob(value)
  }
}

impl From<&[u8]> for EngineValue {
  fn from(value: &[u8]) -> Self {
    EngineValue::Blob(value.to_vec())
  }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EngineKey {
  Scalar(EngineValue),
  Tuple(Vec<EngineValue>),
}

impl From<EngineValue> for EngineKey {
  fn from(value: EngineValue) -> Self {
    EngineKey::Scalar(value)
  }
}

impl From<Vec<EngineValue>> for EngineKey {
  fn from(values: Vec<EngineValue>) -> Self {
    if values.len() == 1 {
      EngineKey::Scalar(values.into_iter().next().expect("expected one value"))
    } else {
      EngineKey::Tuple(values)
    }
  }
}

impl EngineKey {
  pub fn from_values(values: Vec<EngineValue>) -> Self {
    values.into()
  }

  pub fn values(&self) -> &[EngineValue] {
    match self {
      EngineKey::Scalar(value) => core::slice::from_ref(value),
      EngineKey::Tuple(values) => values,
    }
  }
}

pub type EngineRow = Vec<EngineValue>;
