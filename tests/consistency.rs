//! Restore correctness under CONCURRENT writes. A writer inserts `seq-0, seq-1, …` strictly in
//! order while we capture. A consistent cut of sequential inserts is a contiguous prefix `0..k` — so
//! if the restored db has a hole (e.g. seq-7 present but seq-5 missing), the capture was torn. This
//! is the test the stress suite was missing: it checks the restore is *correct*, not just that a
//! static baseline survived.

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::restore::restore_to;
use fjallstream::types::RestoreTarget;
use fjallstream::{Generation, LocalObjectStore};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

#[tokio::test]
async fn concurrent_writes_restore_is_a_consistent_prefix() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let db = Database::builder(src.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();

    // Writer inserts strictly in order, in a background thread.
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU32::new(0));
    let writer = {
        let ks = ks.clone();
        let stop = stop.clone();
        let written = written.clone();
        std::thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                ks.insert(format!("seq-{i:08}"), b"x".as_ref()).unwrap();
                written.store(i + 1, Ordering::Relaxed);
                i += 1;
            }
        })
    };

    // Let the writer get well underway, then capture mid-flight.
    while written.load(Ordering::Relaxed) < 3_000 {
        std::thread::yield_now();
    }
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());
    let cap = capture(&db, src.path(), &[&ks], None).unwrap();
    repl.replicate_once(&cap.version).await.unwrap();
    drop(cap);

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // Restore + open.
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    restore_to(&store, &layout, RestoreTarget::Latest, dst.path()).await.unwrap();
    let rdb = Database::builder(dst.path()).open().unwrap();
    let rks = rdb.keyspace("data", KeyspaceCreateOptions::default).unwrap();

    // The restored seq-keys must form a contiguous prefix 0..k — a torn cut would leave a hole.
    let total = written.load(Ordering::Relaxed) + 50;
    let mut present = 0u32;
    let mut max_present = None;
    for i in 0..total {
        if rks.get(format!("seq-{i:08}")).unwrap().is_some() {
            present += 1;
            max_present = Some(i);
        }
    }
    assert!(present > 0, "expected some keys to be restored");
    let max = max_present.unwrap();
    assert_eq!(
        present,
        max + 1,
        "restored cut has a hole: {present} keys present but highest index is {max} — not a consistent prefix"
    );
}
