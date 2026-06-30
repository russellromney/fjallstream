//! Hot follower: a primary replicates, a `Follower` polls the bucket, restores into a local copy,
//! and serves reads — staying caught up as new versions land.

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::{FollowConfig, Follower, Generation, LocalObjectStore};
use std::time::Duration;

fn layout() -> Layout {
    Layout::new("db", Generation("g".into()))
}

#[tokio::test]
async fn follower_catches_up_and_serves_reads() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let follower_home = tempfile::tempdir().unwrap();

    let db = Database::builder(src.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    let mut repl = Replicator::new(LocalObjectStore::new(bucket.path()), layout(), ReplicateConfig::default());

    // Version A.
    for i in 0..500u32 {
        ks.insert(format!("a-{i:05}"), format!("av-{i:05}")).unwrap();
    }
    let ca = capture(&db, src.path(), &[&ks], None).unwrap();
    let sa = ca.version.seqno;
    repl.replicate_once(&ca.version).await.unwrap();
    drop(ca);

    let follower = Follower::new(
        LocalObjectStore::new(bucket.path()),
        layout(),
        FollowConfig { poll_interval: Duration::from_millis(50), local_dir: follower_home.path().to_path_buf() },
    );

    // First catch-up: follower advances and can read batch A.
    assert!(follower.poll_once().await.unwrap(), "follower should advance to version A");
    {
        let fdb = follower.database().expect("follower has a db");
        let fks = fdb.keyspace("data", KeyspaceCreateOptions::default).unwrap();
        assert_eq!(fks.len().unwrap(), 500, "follower must see all of A (via iteration)");
        assert_eq!(fks.get("a-00042").unwrap().as_deref(), Some(b"av-00042".as_ref()));
    }

    // No new version => no advance.
    assert!(!follower.poll_once().await.unwrap(), "no new version, follower should not advance");

    // Primary writes batch B and replicates.
    for i in 0..500u32 {
        ks.insert(format!("b-{i:05}"), format!("bv-{i:05}")).unwrap();
    }
    let cb = capture(&db, src.path(), &[&ks], Some(sa)).unwrap();
    repl.replicate_once(&cb.version).await.unwrap();
    drop(cb);

    // Second catch-up: follower advances and now sees A+B.
    assert!(follower.poll_once().await.unwrap(), "follower should advance to version B");
    let fdb = follower.database().expect("follower has a db");
    let fks = fdb.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    assert_eq!(fks.len().unwrap(), 1000, "follower must see A+B");
    assert_eq!(fks.get("b-00007").unwrap().as_deref(), Some(b"bv-00007".as_ref()));
}
