use alloc::vec::Vec;

/// Reconstruct a document state by applying `deltas` on top of `snapshot`.
pub fn reconstruct(snapshot: &[u8], deltas: &[Vec<u8>]) -> Vec<u8> {
  let mut state = snapshot.to_vec();
  for d in deltas {
    state.extend_from_slice(d);
  }
  state
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn reconstruct_appends_deltas() {
    let snapshot = b"base".to_vec();
    let deltas = vec![b"a".to_vec(), b"b".to_vec()];
    let got = reconstruct(&snapshot, &deltas);
    assert_eq!(got, b"baseab".to_vec());
  }
}
