//! Crash tests. A crash is simulated by a fault-injecting store that fails partway through. The
//! invariants: a crash mid-upload leaves no version record (record written last); a crash mid
//! restore leaves the target absent (staged then renamed atomically).

mod common;
use common::FaultyStore;

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::restore::{resolve_version, restore_to};
use fjallstream::types::RestoreTarget;
use fjallstream::{Generation, LocalObjectStore};

#[tokio::test]
async fn crash_mid_upload_leaves_no_version_record() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();

    let db = Database::builder(src.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    for i in 0..2_000u32 {
        ks.insert(format!("k{i:06}"), format!("v{i:06}")).unwrap();
    }

    // Fail on the 3rd put — well before the version record (written last, after all files).
    let store = FaultyStore::new(LocalObjectStore::new(bucket.path())).fail_put_after(2);
    let layout = Layout::new("db", Generation("g".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

    let cap = capture(&db, src.path(), &[&ks], None).unwrap();
    let result = repl.replicate_once(&cap.version).await;
    assert!(result.is_err(), "the injected upload failure must surface");
    drop(cap);

    // The bucket must contain NO version record — a partial upload is invisible to readers.
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let resolved = resolve_version(&store, &layout, &RestoreTarget::Latest).await;
    assert!(
        resolved.is_err(),
        "a crash before the record was written must leave no resolvable version"
    );
}

#[tokio::test]
async fn crash_mid_restore_leaves_target_absent() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dst_parent = tempfile::tempdir().unwrap();
    let dst = dst_parent.path().join("restore-target"); // deliberately does NOT exist yet

    // A good backup first.
    {
        let db = Database::builder(src.path()).open().unwrap();
        let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
        for i in 0..2_000u32 {
            ks.insert(format!("k{i:06}"), format!("v{i:06}")).unwrap();
        }
        let store = LocalObjectStore::new(bucket.path());
        let layout = Layout::new("db", Generation("g".into()));
        let mut repl = Replicator::new(store, layout, ReplicateConfig::default());
        let cap = capture(&db, src.path(), &[&ks], None).unwrap();
        repl.replicate_once(&cap.version).await.unwrap();
        drop(cap);
    }

    // Restore with a store that fails on the 2nd get (1st get is the version record; the 2nd is the
    // first file) — i.e. a crash partway through downloading files.
    let store = FaultyStore::new(LocalObjectStore::new(bucket.path())).fail_get_after(1);
    let layout = Layout::new("db", Generation("g".into()));
    let result = restore_to(&store, &layout, RestoreTarget::Latest, &dst).await;
    assert!(result.is_err(), "the injected download failure must surface");

    // The target was never renamed into place — it must not exist (no half-built db).
    assert!(!dst.exists(), "a crash mid-restore must leave the target absent, never partial");
}
