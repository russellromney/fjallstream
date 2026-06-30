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
const ITERS: usize = 12;

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

    // Background writer: floods the keyspace to force flushes + compaction during captures, but
    // BOUNDED — without a cap it writes millions of keys over the run, the db grows unbounded, and
    // each capture's flush/walk gets slower (runaway). A bounded flood keeps the db modest while
    // still racing compaction against captures.
    const FLOOD: u32 = 80_000;
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let ks = ks.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                if i < FLOOD {
                    ks.insert(format!("hot-{i:08}"), format!("x-{i:08}-{}", "p".repeat(40)))
                        .unwrap();
                    i += 1;
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(2)); // done; idle until stop
                }
            }
        })
    };

    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

    for iter in 0..ITERS {
        // Capture + replicate every iteration while the writer churns. This is the thing under test
        // (C1: no NotFound on a file GC'd mid-upload; guard: no torn manifest). Must not error.
        let cap = capture(&db, src.path(), &[&ks], None)
            .unwrap_or_else(|e| panic!("iter {iter}: capture failed: {e}"));
        repl.replicate_once(&cap.version)
            .await
            .unwrap_or_else(|e| panic!("iter {iter}: replicate failed: {e}"));
        drop(cap);

        // Restore + open + verify only occasionally — restore (gunzip + write a 64 MiB journal) is
        // expensive and already proven elsewhere; here we just confirm the churned captures are
        // valid. Always check the last iteration.
        if iter % 4 == 0 || iter == ITERS - 1 {
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
            // len() (iteration) confirms the restored seqno watermark, not just point gets.
            assert!(rks.len().unwrap() >= BASE as usize, "iter {iter}: restored rows < baseline");
            for i in 0..BASE {
                let got = rks.get(format!("base-{i:06}")).unwrap();
                assert_eq!(
                    got.as_deref(),
                    Some(format!("v-{i:06}").as_bytes()),
                    "iter {iter}: baseline key {i} missing after restore"
                );
            }
        }
    }

    // Confirm the C1 vanished-file guard had a chance to fire (it may legitimately be 0 if no file
    // was GC'd mid-upload, but this surfaces the counter for observability).
    eprintln!("stress: files_dropped by C1 guard = {}", repl.files_dropped());

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}
