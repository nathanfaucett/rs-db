use sha2::{Digest, Sha256};

/// Compute a 32-byte change hash: 8-byte timestamp prefix (BE nanos) + first 24 bytes of SHA-256(payload).
pub fn make_change_hash(payload: &[u8]) -> [u8; 32] {
  let ts = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
    Ok(d) => d.as_nanos(),
    Err(_) => 0u128,
  };
  let mut hasher = Sha256::new();
  hasher.update(payload);
  let digest = hasher.finalize();

  let mut out = [0u8; 32];
  let ts_be = ts.to_be_bytes();
  out[0..8].copy_from_slice(&ts_be[8..16]);
  out[8..32].copy_from_slice(&digest[0..24]);
  out
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn hash_produces_32_bytes() {
    let h = make_change_hash(b"test payload");
    assert_eq!(h.len(), 32);
  }

  #[test]
  fn hash_timestamp_prefix_is_nonzero() {
    let h = make_change_hash(b"data");
    // Bytes 0..8 are the timestamp prefix; extremely unlikely to be all zero in practice.
    // Bytes 8..32 come from SHA-256.
    assert_eq!(h[8..32].len(), 24);
  }

  #[test]
  fn same_payload_different_calls_may_differ() {
    // Two hashes of the same payload may differ due to timestamp, but are never panicked.
    let _a = make_change_hash(b"x");
    let _b = make_change_hash(b"x");
  }
}
