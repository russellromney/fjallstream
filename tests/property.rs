//! Model-based oracle: drive a real fjall keyspace with a random sequence of inserts and deletes,
//! mirroring every op into an in-memory model. At random points, capture → replicate → restore →
//! open, and assert the restored database EXACTLY equals the model. Deterministic (seeded PRNG), so
//! any failure reproduces.

mod common;
use common::Rng;

use fjall::{Database, KeyspaceCreateOptions};
use fjallstream::capture::capture;
use fjallstream::layout::Layout;
use fjallstream::replicator::{ReplicateConfig, Replicator};
use fjallstream::restore::restore_to;
use fjallstream::types::RestoreTarget;
use fjallstream::{Generation, LocalObjectStore};
use std::collections::HashMap;

const KEY_SPACE: u32 = 150;
const ROUNDS: u32 = 40;
const OPS_PER_ROUND: u32 = 25;

#[tokio::test]
async fn random_ops_restore_equals_model() {
    let src = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();

    let db = Database::builder(src.path()).open().unwrap();
    let ks = db.keyspace("data", KeyspaceCreateOptions::default).unwrap();
    let store = LocalObjectStore::new(bucket.path());
    let layout = Layout::new("db", Generation("g".into()));
    let mut repl = Replicator::new(store, layout, ReplicateConfig::default());

    let mut model: HashMap<String, String> = HashMap::new();
    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_F00D);
    let mut parent: Option<u64> = None;

    for round in 0..ROUNDS {
        // Apply a random batch of ops, mirroring into the model.
        for _ in 0..OPS_PER_ROUND {
            let key = format!("k{:04}", rng.below(KEY_SPACE));
            if rng.below(100) < 30 {
                // delete
                ks.remove(&key).unwrap();
                model.remove(&key);
            } else {
                let val = format!("r{round}-{key}");
                ks.insert(&key, &val).unwrap();
                model.insert(key, val);
            }
        }

        // Every few rounds, capture and verify a restore matches the model exactly.
        if round % 6 == 5 || round == ROUNDS - 1 {
            let cap = capture(&db, src.path(), &[&ks], parent).unwrap();
            parent = Some(cap.version.seqno);
            repl.replicate_once(&cap.version).await.unwrap();
            drop(cap);

            let dst = tempfile::tempdir().unwrap();
            let store = LocalObjectStore::new(bucket.path());
            let layout = Layout::new("db", Generation("g".into()));
            restore_to(&store, &layout, RestoreTarget::Latest, dst.path()).await.unwrap();

            let rdb = Database::builder(dst.path()).open().unwrap();
            let rks = rdb.keyspace("data", KeyspaceCreateOptions::default).unwrap();

            // Same number of live rows, and every model entry present with the right value ⇒ equal.
            // len() iterates at visible_seqno, so this also proves the restored seqno watermark is
            // correct (iterators/snapshots work), not just point gets.
            let restored_len = rks.len().unwrap();
            assert_eq!(
                restored_len,
                model.len(),
                "round {round}: restored row count {restored_len} != model {}",
                model.len()
            );
            for (k, v) in &model {
                assert_eq!(
                    rks.get(k).unwrap().as_deref(),
                    Some(v.as_bytes()),
                    "round {round}: key {k} mismatch after restore"
                );
            }
        }
    }
}
