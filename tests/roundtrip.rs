//! Exercises the fjall-free core: capture some `LocalVersion`s, replicate them to a local
//! object store, then restore the latest and an earlier point. This proves the upload/record/
//! resolve/restore path independent of any live fjall instance.

use bytes::Bytes;
use fjallstream::layout::Layout;
use fjallstream::replicator::{LocalVersion, ReplicateConfig, Replicator};
use fjallstream::restore::{resolve_version, restore_to};
use fjallstream::types::{FileId, RestoreTarget};
use fjallstream::{Generation, LocalObjectStore};
use std::path::PathBuf;

/// Write `content` to a fresh file under `dir` and return its (id, path).
async fn make_file(dir: &std::path::Path, name: &str, content: &[u8]) -> (FileId, PathBuf) {
    let path = dir.join(name);
    tokio::fs::write(&path, content).await.unwrap();
    (FileId(name.to_string()), path)
}

#[tokio::test]
async fn replicate_then_restore_roundtrip() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();

    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("testdb", Generation("gen-1".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

    // Version 1: two files.
    let f_a = make_file(src.path(), "0001.sst", b"alpha").await;
    let f_b = make_file(src.path(), "0002.blob", b"bravo").await;
    let v1 = LocalVersion {
        seqno: 100,
        parent: None,
        files: vec![f_a.clone(), f_b.clone()],
        pointers: vec![],
    };
    assert_eq!(repl.replicate_once(&v1).await.unwrap(), 100);

    // Version 2: f_a is unchanged (immutable, must dedup), f_b compacted into a new file.
    let f_c = make_file(src.path(), "0003.sst", b"charlie").await;
    let v2 = LocalVersion {
        seqno: 200,
        parent: Some(100),
        files: vec![f_a.clone(), f_c.clone()],
        pointers: vec![],
    };
    assert_eq!(repl.replicate_once(&v2).await.unwrap(), 200);

    // Restore latest: should land f_a + f_c at seqno 200.
    let store2 = LocalObjectStore::new(bucket.path());
    let layout2 = Layout::new("testdb", Generation("gen-1".into()));
    let rec = restore_to(&store2, &layout2, RestoreTarget::Latest, restored.path())
        .await
        .unwrap();
    assert_eq!(rec.seqno, 200);
    assert_eq!(
        tokio::fs::read(restored.path().join("0001.sst")).await.unwrap(),
        b"alpha"
    );
    assert_eq!(
        tokio::fs::read(restored.path().join("0003.sst")).await.unwrap(),
        b"charlie"
    );
    // f_b was not part of version 2, so it must not be restored.
    assert!(!restored.path().join("0002.blob").exists());

    // Point-in-time: resolve at seqno 150 should pick version 100.
    let at_150 = resolve_version(&store2, &layout2, &RestoreTarget::Seqno(150))
        .await
        .unwrap();
    assert_eq!(at_150.seqno, 100);
    assert_eq!(at_150.file_ids.len(), 2);
}

#[tokio::test]
async fn immutable_files_upload_once() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

    let f = make_file(src.path(), "same.sst", b"x").await;
    let v1 = LocalVersion { seqno: 1, parent: None, files: vec![f.clone()], pointers: vec![] };
    let v2 = LocalVersion { seqno: 2, parent: Some(1), files: vec![f.clone()], pointers: vec![] };
    repl.replicate_once(&v1).await.unwrap();

    // Mutate the on-disk file after first upload. Because the file id is the same and the store
    // already has it, replicate_once must NOT re-read or re-upload — the bucket keeps the original.
    tokio::fs::write(src.path().join("same.sst"), b"TAMPERED").await.unwrap();
    repl.replicate_once(&v2).await.unwrap();

    let check = LocalObjectStore::new(bucket.path());
    let layout2 = Layout::new("db", Generation("g".into()));
    let got = fjallstream::ObjectStore::get(&check, &layout2.file(&f.0)).await.unwrap();
    assert_eq!(got, Bytes::from_static(b"x"), "immutable file must be uploaded once, not overwritten");
}
