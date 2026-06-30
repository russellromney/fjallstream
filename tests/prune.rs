//! Retention/prune: keep the newest N version records, delete older ones plus the files only they
//! reference — never a file a retained version still needs. Synthetic versions (prune is pure
//! object-store logic).

use fjallstream::layout::Layout;
use fjallstream::replicator::{LocalVersion, ReplicateConfig, Replicator};
use fjallstream::restore::restore_to;
use fjallstream::types::{FileId, RestoreTarget};
use fjallstream::{Generation, LocalObjectStore, ObjectStore};
use std::path::PathBuf;
use std::time::Duration;

async fn make_file(dir: &std::path::Path, name: &str) -> (FileId, PathBuf) {
    let path = dir.join(name);
    tokio::fs::write(&path, format!("content-of-{name}")).await.unwrap();
    (FileId(name.to_string()), path)
}

fn layout() -> Layout {
    Layout::new("db", Generation("g".into()))
}

#[tokio::test]
async fn prune_keeps_window_and_deletes_only_unreferenced_files() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();

    let cfg = ReplicateConfig { interval: Duration::from_secs(1), retention_versions: 2 };
    let mut repl = Replicator::new(LocalObjectStore::new(bucket.path()), layout(), cfg);

    let mut f = Vec::new();
    for n in ["a", "b", "c", "d", "e", "f"] {
        f.push(make_file(src.path(), n).await);
    }
    let by = |idxs: &[usize]| -> Vec<_> { idxs.iter().map(|&i| f[i].clone()).collect() };

    // Overlapping file sets, increasing seqnos.
    let versions = [
        (10u64, by(&[0, 1])), // a,b
        (20, by(&[0, 2])),    // a,c
        (30, by(&[2, 3])),    // c,d
        (40, by(&[3, 4])),    // d,e
        (50, by(&[4, 5])),    // e,f
    ];
    for (seqno, fl) in versions {
        let v = LocalVersion { seqno, parent: None, files: fl, dirs: vec![], journals: vec![], pointers: vec![] };
        repl.replicate_once(&v).await.unwrap();
    }

    repl.prune().await.unwrap();

    let store = LocalObjectStore::new(bucket.path());
    let l = layout();

    // Only the newest 2 version records survive.
    let versions = store.list(&l.versions_prefix()).await.unwrap();
    assert_eq!(versions.len(), 2, "retention window must keep exactly 2 versions");

    // Retained set is {d,e,f} (v40={d,e}, v50={e,f}); a,b,c are referenced only by pruned versions.
    for gone in ["a", "b", "c"] {
        assert!(
            !store.exists(&l.file(&FileId(gone.into()))).await.unwrap(),
            "file {gone} should be deleted"
        );
    }
    for kept in ["d", "e", "f"] {
        assert!(
            store.exists(&l.file(&FileId(kept.into()))).await.unwrap(),
            "file {kept} is still referenced by a retained version and must survive"
        );
    }

    // The retained versions are still fully restorable.
    let dst = tempfile::tempdir().unwrap();
    let rec = restore_to(&store, &l, RestoreTarget::Latest, dst.path()).await.unwrap();
    assert_eq!(rec.seqno, 50);
}
