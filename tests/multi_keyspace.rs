//! Multiple keyspaces, including an empty one — verifies the whole-database consistency boundary and
//! that empty directories (a fresh keyspace's `tables/`) survive a round-trip. fjall's recovery does
//! not create `tables/`, so the restore must reproduce it.

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::restore::restore_to;
use fjallstream::types::RestoreTarget;
use fjallstream::{Generation, LocalObjectStore};

#[tokio::test]
async fn two_keyspaces_one_empty_roundtrip() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    {
        let db = Database::builder(src.path()).open().unwrap();
        let users = db.keyspace("users", KeyspaceCreateOptions::default).unwrap();
        let empty = db.keyspace("empty", KeyspaceCreateOptions::default).unwrap();

        for i in 0..1_000u32 {
            users.insert(format!("u-{i:05}"), format!("name-{i:05}")).unwrap();
        }
        // `empty` keyspace gets created but never written — its tables/ stays empty.

        let store = LocalObjectStore::new(bucket.path());
        let layout = Layout::new("db", Generation("g".into()));
        let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

        let cap = capture(&db, src.path(), &[&users, &empty], None).unwrap();
        repl.replicate_once(&cap.version).await.unwrap();
        drop(cap);
    }

    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    restore_to(&store, &layout, RestoreTarget::Latest, dst.path())
        .await
        .unwrap();

    let db2 = Database::builder(dst.path()).open().unwrap();
    assert!(db2.keyspace_exists("users"), "users keyspace must survive");
    assert!(db2.keyspace_exists("empty"), "empty keyspace must survive");

    let users2 = db2.keyspace("users", KeyspaceCreateOptions::default).unwrap();
    for i in 0..1_000u32 {
        assert_eq!(
            users2.get(format!("u-{i:05}")).unwrap().as_deref(),
            Some(format!("name-{i:05}").as_bytes()),
            "users key {i} missing"
        );
    }

    // The empty keyspace opens and is genuinely empty.
    let empty2 = db2.keyspace("empty", KeyspaceCreateOptions::default).unwrap();
    assert_eq!(empty2.len().unwrap(), 0, "empty keyspace should have no rows");
}
