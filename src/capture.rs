//! The fjall seam. Captures a consistent, complete file set from a live `fjall::Database` — the
//! only module that touches fjall. Verified against fjall 3.1.5 (see `examples/spike_layout.rs`).
//!
//! Strategy (DESIGN.md "Capture strategy"):
//!   1. `rotate_memtable_and_wait()` on each keyspace — push committed data out of the journal into
//!      immutable SSTs, so the captured set is complete without shipping the 64 MiB journal.
//!   2. `db.snapshot()` — pin GC so nothing we're about to read is deleted under us.
//!   3. Walk the db dir: immutable files become content-addressed `files`; the per-keyspace
//!      `current` HEAD pointers are captured inline; `*.jnl` and `lock` are skipped.
//!
//! The returned [`Captured`] holds the snapshot, so GC stays pinned until the caller has finished
//! uploading and drops it.

use crate::error::{Error, Result};
use crate::replicator::LocalVersion;
use crate::types::{FileId, PointerFile};
use fjall::{Database, Keyspace};
use std::path::Path;

/// A captured version plus the held snapshot that pins its files from GC. Keep it alive until the
/// upload finishes, then drop it.
pub struct Captured {
    pub version: LocalVersion,
    // Held to keep GC from deleting captured files mid-upload. Dropped with `Captured`.
    _snapshot: fjall::Snapshot,
}

/// How many times to retry capture if background compaction churns the manifest mid-walk.
const CAPTURE_RETRIES: usize = 8;

/// Capture the current state of `db` (rooted at `db_path`) for replication.
///
/// `keyspaces` must list every user keyspace whose data should be flushed before capture. `parent`
/// is the seqno of the previous replicated version, or `None` for the first.
///
/// Consistency: a held `Snapshot` pins SST *data* from GC, but background compaction still rewrites
/// the on-disk manifest (`v<N>` files) and the `current` HEAD pointer while we walk the directory.
/// A torn read of that set would produce a backup that won't open. We guard against it by capturing,
/// then re-reading every `current` pointer: if none changed across the walk, the manifest was stable
/// the whole time, so each `current` points at a fully-written `v<N>` we captured — a consistent set.
/// If a `current` moved, we retry. lsm-tree writes a new `v<N>` fully *before* flipping `current` to
/// it, which is what makes the stable-`current` check sufficient.
pub fn capture(
    db: &Database,
    db_path: &Path,
    keyspaces: &[&Keyspace],
    parent: Option<u64>,
) -> Result<Captured> {
    // Force all committed data into immutable files (once; cheap to leave flushed).
    for ks in keyspaces {
        ks.rotate_memtable_and_wait()
            .map_err(|e| Error::Fjall(format!("flush keyspace {:?}: {e}", ks.name())))?;
    }

    for _ in 0..CAPTURE_RETRIES {
        // Pin GC. Held inside `Captured` until the caller drops it.
        let snapshot = db.snapshot();
        let seqno: u64 = snapshot.seqno();

        // Watch the keyspace set: a keyspace created or dropped mid-walk would be captured torn,
        // and the pointer check wouldn't catch it (its `current` isn't in our list yet).
        let keyspaces_before = db.keyspace_count();

        // Walk the directory into immutable files, directories, and inline pointer files.
        let mut files = Vec::new();
        let mut dirs = Vec::new();
        let mut pointers = Vec::new();
        walk(db_path, db_path, &mut files, &mut dirs, &mut pointers)?;

        // Consistent iff the manifest was stable (no `current` moved) AND the keyspace set didn't
        // change across the walk.
        if db.keyspace_count() == keyspaces_before && pointers_unchanged(db_path, &pointers)? {
            let version = LocalVersion { seqno, parent, files, dirs, pointers };
            return Ok(Captured { version, _snapshot: snapshot });
        }
        // Something moved under us — drop the snapshot and try again.
        drop(snapshot);
    }

    Err(Error::Fjall(format!(
        "could not capture a stable manifest after {CAPTURE_RETRIES} tries (compaction churn)"
    )))
}

/// True if every captured `current` pointer still holds the bytes we recorded. A changed or missing
/// pointer means compaction advanced the manifest during the walk, so the capture may be torn.
fn pointers_unchanged(db_path: &Path, pointers: &[PointerFile]) -> Result<bool> {
    for p in pointers {
        match std::fs::read(db_path.join(&p.path)) {
            Ok(now) if now == p.bytes => {}
            Ok(_) => return Ok(false),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => return Err(Error::Io { path: db_path.join(&p.path), source }),
        }
    }
    Ok(true)
}

fn walk(
    base: &Path,
    dir: &Path,
    files: &mut Vec<(FileId, std::path::PathBuf)>,
    dirs: &mut Vec<String>,
    pointers: &mut Vec<PointerFile>,
) -> Result<()> {
    let rd = std::fs::read_dir(dir).map_err(|source| Error::Io { path: dir.to_path_buf(), source })?;
    for entry in rd {
        let entry = entry.map_err(|source| Error::Io { path: dir.to_path_buf(), source })?;
        let path = entry.path();
        let ft = entry
            .file_type()
            .map_err(|source| Error::Io { path: path.clone(), source })?;

        // Relative path with forward slashes, used as the file id / dir / pointer path.
        let rel = path
            .strip_prefix(base)
            .expect("walked path is under base")
            .to_string_lossy()
            .replace('\\', "/");

        if ft.is_dir() {
            dirs.push(rel);
            walk(base, &path, files, dirs, pointers)?;
            continue;
        }

        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // Never replicate the lock, and skip the journal entirely (force-flush makes it unneeded).
        if name == "lock" || rel.ends_with(".jnl") {
            continue;
        }

        if name == "current" {
            // Mutable HEAD pointer — rewritten in place, so it can't be content-addressed.
            let bytes = std::fs::read(&path).map_err(|source| Error::Io { path: path.clone(), source })?;
            pointers.push(PointerFile { path: rel, bytes });
        } else {
            // Immutable: SST table, blob, version manifest, or the top-level format marker.
            files.push((FileId(rel), path));
        }
    }
    Ok(())
}
