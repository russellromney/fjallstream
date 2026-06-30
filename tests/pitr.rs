//! Point-in-time recovery on real fjall: two captures separated in seqno and time, restored by both
//! seqno and wall-clock, asserting the exact state at each point.

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::restore::{resolve_version, restore_to};
use fjallstream::types::RestoreTarget;
use fjallstream::{Generation, LocalObjectStore};

const N: u32 = 200;

fn layout() -> Layout {
    Layout::new("db", Generation("g".into()))
}

/// Restore the given target into a fresh dir and return (has_A, has_B) for the two batches.
async fn restore_and_probe(bucket: &std::path::Path, target: RestoreTarget) -> (bool, bool) {
    let dst = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(bucket);
    restore_to(&store, &layout(), target, dst.path()).await.unwrap();
    let db = Database::builder(dst.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    let has_a = ks.get("a-00000").unwrap().is_some();
    let has_b = ks.get("b-00000").unwrap().is_some();
    (has_a, has_b)
}

#[tokio::test]
async fn pitr_by_seqno_and_time() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();

    let db = Database::builder(src.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    let store = LocalObjectStore::new(bucket.path());
    let mut repl = Replicator::new(store, layout(), ReplicateConfig::default());

    // Version A: batch a.
    for i in 0..N {
        ks.insert(format!("a-{i:05}"), format!("av-{i:05}")).unwrap();
    }
    let ca = capture(&db, src.path(), &[&ks], None).unwrap();
    let sa = ca.version.seqno;
    repl.replicate_once(&ca.version).await.unwrap();
    drop(ca);
    let rec_a = resolve_version(&LocalObjectStore::new(bucket.path()), &layout(), &RestoreTarget::Latest)
        .await
        .unwrap();
    let ta = rec_a.ts_millis;

    // Version B: batch b (higher seqnos, later, monotonic ts).
    for i in 0..N {
        ks.insert(format!("b-{i:05}"), format!("bv-{i:05}")).unwrap();
    }
    let cb = capture(&db, src.path(), &[&ks], Some(sa)).unwrap();
    let sb = cb.version.seqno;
    repl.replicate_once(&cb.version).await.unwrap();
    drop(cb);
    let rec_b = resolve_version(&LocalObjectStore::new(bucket.path()), &layout(), &RestoreTarget::Latest)
        .await
        .unwrap();
    let tb = rec_b.ts_millis;

    assert!(sa < sb, "seqnos must advance ({sa} !< {sb})");
    assert!(ta < tb, "timestamps must advance monotonically ({ta} !< {tb})");

    // By seqno.
    assert_eq!(restore_and_probe(bucket.path(), RestoreTarget::Seqno(sa)).await, (true, false), "at sA: only A");
    assert_eq!(restore_and_probe(bucket.path(), RestoreTarget::Seqno(sb)).await, (true, true), "at sB: A and B");
    assert_eq!(restore_and_probe(bucket.path(), RestoreTarget::Latest).await, (true, true), "latest: A and B");

    // By wall-clock.
    assert_eq!(restore_and_probe(bucket.path(), RestoreTarget::TimestampMillis(ta)).await, (true, false), "at tA: only A");
    assert_eq!(restore_and_probe(bucket.path(), RestoreTarget::TimestampMillis(tb)).await, (true, true), "at tB: A and B");

    // Before the first version: nothing resolves.
    let dst = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(bucket.path());
    let before = restore_to(&store, &layout(), RestoreTarget::TimestampMillis(ta - 1), dst.path()).await;
    assert!(before.is_err(), "restoring before the first version must find nothing");
}
