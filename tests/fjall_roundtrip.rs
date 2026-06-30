//! The decisive test (PLAN M3): a real fjall database → capture → replicate to an object store →
//! restore into a fresh directory → **open it and read every key back**. If this passes, the core
//! works end to end against real fjall. It also settles whether fjall opens a restored dir with no
//! journal and with the meta keyspace intact.

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::restore::restore_to;
use fjallstream::types::RestoreTarget;
use fjallstream::{Generation, LocalObjectStore};

const N: u32 = 5_000;

#[tokio::test]
async fn fjall_capture_replicate_restore_open() {
    let _ = env_logger::builder().is_test(true).try_init();
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    // --- primary: write a real fjall database, then capture + replicate it ---
    {
        let db = Database::builder(src.path()).open().unwrap();
        let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
        for i in 0..N {
            ks.insert(format!("key-{i:08}"), format!("value-{i:08}")).unwrap();
        }

        let store = LocalObjectStore::new(bucket.path());
        let layout = Layout::new("db", Generation("g1".into()));
        let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

        let cap = capture(&db, src.path(), &[&ks], None).unwrap();
        println!(
            "captured: seqno={}, {} immutable files, {} pointer files",
            cap.version.seqno,
            cap.version.files.len(),
            cap.version.pointers.len()
        );
        repl.replicate_once(&cap.version).await.unwrap();
        drop(cap); // release the snapshot
    } // db dropped/closed here

    // --- restore into a clean directory ---
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g1".into()));
    let rec = restore_to(&store, &layout, RestoreTarget::Latest, dst.path())
        .await
        .unwrap();
    println!(
        "restored: seqno={}, {} files, {} pointers",
        rec.seqno,
        rec.file_ids.len(),
        rec.pointers.len()
    );

    // --- open the restored database and read every key back ---
    let db2 = Database::builder(dst.path()).open().unwrap();
    assert!(
        db2.keyspace_exists("data"),
        "restored db must know the 'data' keyspace (meta keyspace survived)"
    );
    let ks2 = db2.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    for i in 0..N {
        let got = ks2.get(format!("key-{i:08}")).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(format!("value-{i:08}").as_bytes()),
            "key {i} mismatch after restore"
        );
    }
    println!("all {N} keys verified after restore");
}
