//! Spike (throwaway): learn fjall 3.1.5's real on-disk layout and the seqno/snapshot API, so the
//! M2 capture adapter is built against reality instead of guesses. Answers:
//!   - Is the database directory a flat bag of files or a tree? (=> what FileId must be)
//!   - What do SST / blob / manifest / journal files look like and where do they live?
//!   - Does `db.seqno()` / `visible_seqno()` behave as a usable replication watermark?
//!   - Does holding a Snapshot keep files on disk across a flush+compaction?
//!
//! Run: `cargo run --example spike_layout`

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use std::path::Path;

fn dump_tree(root: &Path) {
    fn walk(base: &Path, dir: &Path, depth: usize) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        entries.sort_by_key(|e| e.path());
        for e in entries {
            let path = e.path();
            let rel = path.strip_prefix(base).unwrap();
            let meta = e.metadata().unwrap();
            let indent = "  ".repeat(depth);
            if meta.is_dir() {
                println!("{indent}{}/", rel.display());
                walk(base, &path, depth + 1);
            } else {
                println!("{indent}{}  ({} bytes)", rel.display(), meta.len());
            }
        }
    }
    walk(root, root, 0);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path();
    println!("db path: {}\n", path.display());

    // Smallest allowed write buffer (1 MiB) so inserts flush to SST files we can observe.
    #[allow(deprecated)]
    let db = Database::builder(path)
        .max_write_buffer_size(Some(1024 * 1024))
        .open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;

    // Round 1: write well past the 1 MiB buffer to force several flushes to SST.
    let big = "x".repeat(120);
    for i in 0..40_000u32 {
        ks.insert(format!("key-{i:08}"), format!("value-{i:08}-{big}"))?;
    }
    db.persist(PersistMode::SyncAll)?;
    println!("after 5000 inserts + persist:");
    println!("  db.seqno()         = {}", db.seqno());
    println!("  db.visible_seqno() = {}", db.visible_seqno());
    println!("  outstanding_flushes= {}", db.outstanding_flushes());
    println!("  journal_count      = {}", db.journal_count());
    println!("\n=== DIRECTORY TREE (round 1) ===");
    dump_tree(path);

    // Hold a snapshot, capture the file set, then churn + compact and see if the captured files
    // survive (the GC-pin property the whole design leans on).
    let snap = db.snapshot();
    let captured: Vec<String> = collect_files(path);
    println!("\nheld snapshot at visible_seqno = {}", db.visible_seqno());
    println!("captured {} files under snapshot", captured.len());

    // Round 2: overwrite the same keys (creates new versions => compaction churn).
    for i in 0..40_000u32 {
        ks.insert(format!("key-{i:08}"), format!("v2-{i:08}-{big}"))?;
    }
    db.persist(PersistMode::SyncAll)?;
    // Give background compaction a chance.
    std::thread::sleep(std::time::Duration::from_millis(500));

    println!("\n=== DIRECTORY TREE (round 2, after churn) ===");
    dump_tree(path);

    let still_present = captured.iter().filter(|f| path.join(f).exists()).count();
    println!(
        "\nof {} captured files, {} still on disk while snapshot held",
        captured.len(),
        still_present
    );
    drop(snap);
    println!("snapshot dropped.");

    Ok(())
}

/// Relative paths of every regular file under `root`.
fn collect_files(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).unwrap().filter_map(|e| e.ok()) {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                out.push(p.strip_prefix(base).unwrap().display().to_string());
            }
        }
    }
    walk(root, root, &mut out);
    out.sort();
    out
}
