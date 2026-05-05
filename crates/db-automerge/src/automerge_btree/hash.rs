use sha2::{Digest, Sha256};

pub fn hash_hashes<I>(hashes: I) -> [u8; 32]
where
  I: IntoIterator<Item = [u8; 32]>,
{
  let mut hasher = Sha256::new();
  for hash in hashes {
    hasher.update(hash);
  }
  let result = hasher.finalize();
  let mut out = [0u8; 32];
  out.copy_from_slice(&result);
  out
}

pub fn hash_heads(heads: &[automerge::ChangeHash]) -> [u8; 32] {
  hash_hashes(heads.iter().map(|head| head.0))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn hash_hashes_produces_different_output_for_different_input() {
    let h1 = hash_hashes([[1u8; 32], [2u8; 32]]);
    let h2 = hash_hashes([[1u8; 32], [3u8; 32]]);
    assert_ne!(h1, h2);
  }

  #[test]
  fn hash_hashes_produces_same_output_for_same_input() {
    let h1 = hash_hashes([[1u8; 32], [2u8; 32]]);
    let h2 = hash_hashes([[1u8; 32], [2u8; 32]]);
    assert_eq!(h1, h2);
  }

  #[test]
  fn hash_hashes_produces_32_bytes() {
    let h = hash_hashes([[1u8; 32], [2u8; 32]]);
    assert_eq!(h.len(), 32);
  }

  #[test]
  fn hash_hashes_is_stable() {
    let h1 = hash_hashes([[1u8; 32], [2u8; 32]]);
    let h2 = hash_hashes([[1u8; 32], [2u8; 32]]);
    assert_eq!(h1, h2);
  }
}
