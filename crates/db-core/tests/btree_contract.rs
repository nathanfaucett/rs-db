use core::{
  future::Future,
  pin::pin,
  task::{Context, Poll, Waker},
};
use db_core::{BTree, BTreeExecutor, BTreeTransaction};
use futures::{StreamExt, pin_mut};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

fn block_on<F: Future>(future: F) -> F::Output {
  let waker = Waker::noop();
  let mut context = Context::from_waker(waker);
  let mut future = pin!(future);

  loop {
    match future.as_mut().poll(&mut context) {
      Poll::Ready(output) => return output,
      Poll::Pending => core::hint::spin_loop(),
    }
  }
}

async fn commit_and_rollback_contract<S>(mut store: S)
where
  S: BTree<u64, u64>,
{
  store.insert(1, 100).await.expect("insert initial value");

  let mut tx = store.transaction().await.expect("start tx");
  tx.insert(2, 200).await.expect("insert in tx");
  let _ = tx.remove(&1).await;
  tx.commit().await.expect("commit");

  assert_eq!(store.get(&1).await.expect("get failed"), None);
  assert_eq!(store.get(&2).await.expect("get failed"), Some(200));
}

async fn transaction_range_merges_contract<S>(mut store: S)
where
  S: BTree<u64, u64>,
{
  store.insert(1, 100).await.expect("insert initial");
  store.insert(3, 300).await.expect("insert second");

  let mut tx = store.transaction().await.expect("start tx");
  tx.insert(2, 200).await.expect("insert in tx");
  tx.remove(&3).await.expect("remove failed");

  let mut values = Vec::new();
  let stream = tx.range(0..10);
  pin_mut!(stream);
  while let Some(item) = stream.next().await {
    let (k, v) = item.expect("range item failed");
    values.push((k, v));
  }

  assert_eq!(values, Vec::from([(1u64, 100u64), (2u64, 200u64)]));
}

#[test]
fn inmemory_transaction_commit_and_rollback_contract() {
  block_on(async {
    let store = db_in_memory::InMemoryBTree::<u64, u64>::new();
    commit_and_rollback_contract(store).await;
  });
}

#[test]
fn inmemory_transaction_range_merges_contract() {
  block_on(async {
    let store = db_in_memory::InMemoryBTree::<u64, u64>::new();
    transaction_range_merges_contract(store).await;
  });
}

#[test]
fn redb_transaction_commit_and_rollback_contract() {
  block_on(async {
    // Use a temp file path under std::env::temp_dir()
    let mut path = std::env::temp_dir();
    path.push(format!(
      "aicacia_btree_contract_{}.db",
      std::time::SystemTime::now().elapsed().unwrap().as_nanos()
    ));

    let store = db_redb::REDBBTree::<u64, u64>::open(&path, "contract_table").expect("open redb");
    let _ = std::fs::remove_file(&path);
    commit_and_rollback_contract(store).await;

    let _ = std::fs::remove_file(path);
  });
}

#[test]
fn redb_transaction_range_merges_contract() {
  block_on(async {
    let mut path = std::env::temp_dir();
    path.push(format!(
      "aicacia_btree_contract_{}.db",
      std::time::SystemTime::now().elapsed().unwrap().as_nanos()
    ));

    let store = db_redb::REDBBTree::<u64, u64>::open(&path, "contract_table").expect("open redb");
    let _ = std::fs::remove_file(&path);
    transaction_range_merges_contract(store).await;

    let _ = std::fs::remove_file(path);
  });
}
