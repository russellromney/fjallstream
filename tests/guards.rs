//! Guard-firing + deterministic negative tests. These prove the correctness guards actually execute
//! and that failure paths fail loudly instead of silently corrupting.

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{LocalVersion, ReplicateConfig, Replicator};
use fjallstream::restore::{resolve_version, restore_to};
use fjallstream::types::{FileId, RestoreTarget};
use fjallstream::{Generation, LocalObjectStore};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ---- C1: the vanished-file guard fires and drops only the missing file ----

#[tokio::test]
async fn c1_drops_vanished_file_and_counts_it() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();

    // One real file on disk, one "ghost" path that doesn't exist (an obsolete file GC'd after
    // capture). replicate_once must upload the real one, drop the ghost, and count the drop.
    let real = src.path().join("real.sst");
    tokio::fs::write(&real, b"hello").await.unwrap();
    let ghost = src.path().join("ghost.sst");

    let version = LocalVersion {
        seqno: 1,
        parent: None,
        files: vec![
            (FileId("real.sst".into()), real),
            (FileId("ghost.sst".into()), ghost),
        ],
        dirs: vec![],
        journals: vec![],
        pointers: vec![],
    };

    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

    repl.replicate_once(&version).await.unwrap();
    assert_eq!(repl.files_dropped(), 1, "the vanished-file guard must have fired exactly once");

    // The committed record must reference only the file that actually exists.
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let rec = resolve_version(&store, &layout, &RestoreTarget::Latest).await.unwrap();
    assert_eq!(rec.file_ids, vec![FileId("real.sst".into())]);
}

// ---- C2: restore refuses a non-empty target ----

#[tokio::test]
async fn restore_into_nonempty_dir_errors() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    {
        let db = Database::builder(src.path()).open().unwrap();
        let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
        ks.insert("k", "v").unwrap();
        let store = LocalObjectStore::new(bucket.path());
        let layout = Layout::new("db", Generation("g".into()));
        let mut repl = Replicator::new(store, layout, ReplicateConfig::default());
        let cap = capture(&db, src.path(), &[&ks], None).unwrap();
        repl.replicate_once(&cap.version).await.unwrap();
        drop(cap);
    }

    // Put something in the target, then attempt to restore into it.
    tokio::fs::write(dst.path().join("stale-file"), b"junk").await.unwrap();
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let err = restore_to(&store, &layout, RestoreTarget::Latest, dst.path()).await;
    assert!(err.is_err(), "restore into a non-empty dir must error, not clobber");
}

// ---- Anti-masking: a referenced file missing from the bucket fails loudly ----

#[tokio::test]
async fn missing_referenced_file_fails_loud() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    {
        let db = Database::builder(src.path()).open().unwrap();
        let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
        for i in 0..500u32 {
            ks.insert(format!("k{i:05}"), format!("v{i:05}")).unwrap();
        }
        let store = LocalObjectStore::new(bucket.path());
        let layout = Layout::new("db", Generation("g".into()));
        let mut repl = Replicator::new(store, layout, ReplicateConfig::default());
        let cap = capture(&db, src.path(), &[&ks], None).unwrap();
        repl.replicate_once(&cap.version).await.unwrap();
        drop(cap);
    }

    // Delete one referenced file from the bucket (simulating loss/incomplete upload).
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let rec = resolve_version(&store, &layout, &RestoreTarget::Latest).await.unwrap();
    let victim = rec.file_ids.first().expect("at least one file");
    fjallstream::ObjectStore::delete(&store, &layout.file(victim)).await.unwrap();

    // Restore must fail loudly, not silently produce a partial database.
    let err = restore_to(&store, &layout, RestoreTarget::Latest, dst.path()).await;
    assert!(err.is_err(), "restore with a missing referenced file must fail loudly");
}

// ---- Corruption is detected (by fjall's checksums), never silently served ----

#[tokio::test]
async fn corrupted_file_is_detected_not_silently_served() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

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

    // Corrupt every SST table file in the bucket (overwrite with same-length garbage).
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let rec = resolve_version(&store, &layout, &RestoreTarget::Latest).await.unwrap();
    let mut corrupted = 0;
    for id in &rec.file_ids {
        if id.0.contains("/tables/") {
            let key = layout.file(id);
            let bytes = fjallstream::ObjectStore::get(&store, &key).await.unwrap();
            let garbage = vec![0xABu8; bytes.len()];
            fjallstream::ObjectStore::put(&store, &key, bytes::Bytes::from(garbage)).await.unwrap();
            corrupted += 1;
        }
    }
    assert!(corrupted > 0, "expected at least one table file to corrupt");

    restore_to(&store, &layout, RestoreTarget::Latest, dst.path()).await.unwrap();

    // Open + read everything. fjall's XXH3 checksums must surface the corruption: either open fails,
    // or a read errors. The one thing that must NOT happen is silently returning wrong/missing data
    // as if it were correct.
    let detected = match Database::builder(dst.path()).open() {
        Err(_) => true,
        Ok(db) => {
            let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
            let mut bad = false;
            for i in 0..2_000u32 {
                match ks.get(format!("k{i:06}")) {
                    Err(_) => {
                        bad = true;
                        break;
                    }
                    Ok(v) if v.as_deref() != Some(format!("v{i:06}").as_bytes()) => {
                        bad = true;
                        break;
                    }
                    Ok(_) => {}
                }
            }
            bad
        }
    };
    assert!(detected, "corruption must be detected (open or read error), never silently served");
}

// ---- C6: the keyspace-set guard fires under concurrent keyspace creation ----

#[tokio::test]
async fn c6_retry_fires_under_keyspace_churn() {
    let src = tempfile::tempdir().unwrap();
    let db = Database::builder(src.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    for i in 0..2_000u32 {
        ks.insert(format!("k{i:06}"), format!("v{i:06}")).unwrap();
    }

    // Thread that creates keyspaces intermittently (a brief gap between, like real usage — not a
    // tight fsync loop). Each creation is a persisting count increment a capture's walk can observe.
    let stop = Arc::new(AtomicBool::new(false));
    let churner = {
        let db = db.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = db.keyspace(&format!("churn-{i}"), KeyspaceCreateOptions::default);
                i += 1;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        })
    };

    // Capture repeatedly; a walk overlapping a creation forces a retry. Proof the guard fired is
    // EITHER a successful capture that reports retries > 0, OR a capture that exhausted its retries
    // (which only happens by retrying). Either way the retry path executed.
    let mut retried = false;
    for _ in 0..3_000 {
        match capture(&db, src.path(), &[&ks], None) {
            Ok(cap) if cap.retries > 0 => retried = true,
            Ok(_) => {}
            Err(_) => retried = true, // exhausted CAPTURE_RETRIES => definitely retried
        }
        if retried {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();

    assert!(
        retried,
        "the C6 keyspace-set guard never retried despite continuous keyspace creation"
    );
}
