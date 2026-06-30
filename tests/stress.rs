//! Hammers capture under concurrent writes + background compaction. This is what actually exercises
//! the dead-file tolerance (C1) and the capture consistency guard: while a writer thread floods the
//! keyspace (driving flushes and compaction), we capture → replicate → restore → open → verify in a
//! loop. Every iteration's restore must open cleanly and contain the durable baseline keys.

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::restore::restore_to;
use fjallstream::types::RestoreTarget;
use fjallstream::{Generation, LocalObjectStore};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const BASE: u32 = 1_000;
const ITERS: usize = 20;

#[tokio::test]
async fn capture_survives_concurrent_writes_and_compaction() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();

    let db = Database::builder(src.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();

    // Durable baseline: present in every later capture.
    for i in 0..BASE {
        ks.insert(format!("base-{i:06}"), format!("v-{i:06}")).unwrap();
    }
    ks.rotate_memtable_and_wait().unwrap();

    // Background writer: floods the keyspace to force flushes + compaction during captures.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let ks = ks.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                ks.insert(format!("hot-{i:08}"), format!("x-{i:08}-{}", "p".repeat(80)))
                    .unwrap();
                i = i.wrapping_add(1);
            }
        })
    };

    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

    for iter in 0..ITERS {
        // Capture + replicate while the writer churns. Must not error (C1: no NotFound on a
        // file GC'd mid-upload; guard: no torn manifest).
        let cap = capture(&db, src.path(), &[&ks], None)
            .unwrap_or_else(|e| panic!("iter {iter}: capture failed: {e}"));
        repl.replicate_once(&cap.version)
            .await
            .unwrap_or_else(|e| panic!("iter {iter}: replicate failed: {e}"));
        drop(cap);

        // Restore the latest version into a fresh dir and open it.
        let dst = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(bucket.path());
        let layout = Layout::new("db", Generation("g".into()));
        restore_to(&store, &layout, RestoreTarget::Latest, dst.path())
            .await
            .unwrap_or_else(|e| panic!("iter {iter}: restore failed: {e}"));

        let rdb = Database::builder(dst.path())
            .open()
            .unwrap_or_else(|e| panic!("iter {iter}: open restored failed: {e}"));
        let rks = rdb.keyspace("data", KeyspaceCreateOptions::default).unwrap();
        for i in 0..BASE {
            let got = rks.get(format!("base-{i:06}")).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(format!("v-{i:06}").as_bytes()),
                "iter {iter}: baseline key {i} missing after restore"
            );
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}
