//! Shared conformance suite every `ObjectStore` backend must pass — pins the contract (especially
//! that `list` is recursive) so Local and Mem (and later S3) behave identically.

use bytes::Bytes;
use fjallstream::{Error, LocalObjectStore, MemObjectStore, ObjectStore};

async fn conformance<S: ObjectStore>(store: S) {
    // put / get round-trip, including nested keys.
    store.put("a/b/c.txt", Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(store.get("a/b/c.txt").await.unwrap(), Bytes::from_static(b"hello"));

    // exists.
    assert!(store.exists("a/b/c.txt").await.unwrap());
    assert!(!store.exists("a/b/missing").await.unwrap());

    // not found is a typed error.
    assert!(matches!(store.get("nope").await, Err(Error::NotFound(_))));

    // list is RECURSIVE and returns full flat keys under the prefix, sorted.
    store.put("p/x/1", Bytes::from_static(b"1")).await.unwrap();
    store.put("p/y/2", Bytes::from_static(b"2")).await.unwrap();
    store.put("p/3", Bytes::from_static(b"3")).await.unwrap();
    let keys = store.list("p").await.unwrap();
    assert_eq!(keys, vec!["p/3".to_string(), "p/x/1".to_string(), "p/y/2".to_string()]);

    // a sibling prefix must not leak in.
    store.put("pother/9", Bytes::from_static(b"9")).await.unwrap();
    assert_eq!(store.list("p").await.unwrap().len(), 3, "list('p') must not match 'pother/...'");

    // overwrite.
    store.put("p/3", Bytes::from_static(b"33")).await.unwrap();
    assert_eq!(store.get("p/3").await.unwrap(), Bytes::from_static(b"33"));

    // delete is idempotent; listing a missing prefix is empty.
    store.delete("p/3").await.unwrap();
    assert!(!store.exists("p/3").await.unwrap());
    store.delete("p/3").await.unwrap();
    assert!(store.list("does/not/exist").await.unwrap().is_empty());
}

#[tokio::test]
async fn local_object_store_conformance() {
    let dir = tempfile::tempdir().unwrap();
    conformance(LocalObjectStore::new(dir.path())).await;
}

#[tokio::test]
async fn mem_object_store_conformance() {
    conformance(MemObjectStore::new()).await;
}
