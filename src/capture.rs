//! The fjall seam. Captures a consistent, complete file set from a live `fjall::Database` — the
//! only module that touches fjall. Verified against fjall 3.1.5 (see `examples/spike_layout.rs`).
//!
//! Strategy (DESIGN.md "Capture strategy"):
//!   1. `rotate_memtable_and_wait()` on each keyspace — push committed data into immutable SSTs,
//!      minimizing the journal's live content (we still ship the journal; it carries the seqno).
//!   2. `db.snapshot()` — pin SST GC so nothing we're about to read is deleted under us.
//!   3. Walk the db dir: immutable files become content-addressed `files`; the per-keyspace
//!      `current` HEAD pointers are captured inline; `*.jnl` are captured as per-version journals
//!      (read consistently, since they race background maintenance); `lock` is skipped.
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
    /// How many times capture retried before getting a consistent set (0 = first try). Exposed so
    /// tests can confirm the C6 consistency guard actually fired under churn.
    pub retries: u32,
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

    for attempt in 0..CAPTURE_RETRIES {
        // Pin GC. Held inside `Captured` until the caller drops it.
        let snapshot = db.snapshot();

        // Watch the keyspace set: a keyspace created or dropped mid-walk would be captured torn,
        // and the pointer check wouldn't catch it (its `current` isn't in our list yet).
        let keyspaces_before = db.keyspace_count();

        // Walk the directory into immutable files, directories, journals, and inline pointer files.
        // A file or directory vanishing mid-walk (racing compaction or keyspace change) surfaces as
        // NotFound; treat that as "inconsistent, retry" rather than a hard error.
        let mut files = Vec::new();
        let mut dirs = Vec::new();
        let mut journals = Vec::new();
        let mut pointers = Vec::new();
        match walk(db_path, db_path, &mut files, &mut dirs, &mut journals, &mut pointers) {
            Ok(()) => {}
            Err(Error::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                drop(snapshot);
                continue;
            }
            Err(e) => return Err(e),
        }

        // Consistent iff the manifest was stable (no `current` moved), the keyspace set didn't change
        // across the walk, AND the journals can be read without racing background maintenance
        // (rotation/truncation), which would ship a torn journal that won't recover.
        if db.keyspace_count() == keyspaces_before && pointers_unchanged(db_path, &pointers)? {
            if let Some(journal_bytes) = read_journals_stable(db_path, &journals)? {
                // Record an upper-bound seqno read *after* the journal: `db.seqno()` is the next
                // sequence number, so it exceeds every write the journal contains. This makes the
                // recorded seqno an honest upper bound — `restore at-or-before X` then never returns
                // content past X, even when writes land concurrently during capture.
                let seqno: u64 = db.seqno();
                let version =
                    LocalVersion { seqno, parent, files, dirs, journals: journal_bytes, pointers };
                return Ok(Captured { version, retries: attempt as u32, _snapshot: snapshot });
            }
        }
        // Something moved under us — drop the snapshot and try again.
        drop(snapshot);
    }

    Err(Error::Fjall(format!(
        "could not capture a stable manifest after {CAPTURE_RETRIES} tries (compaction churn)"
    )))
}

/// Read the journal files consistently, returning `None` (retry) if any raced background
/// maintenance. Journals aren't pinned by the snapshot and are truncated/rotated when a flush's
/// entries are reclaimed, so we read each one bracketed by length checks and confirm the set of
/// `*.jnl` files in the db root is exactly what we captured. A torn journal makes recovery fail with
/// EINVAL, so this must be airtight.
fn read_journals_stable(db_path: &Path, journals: &[NamedPath]) -> Result<Option<Vec<NamedBytes>>> {
    // The set of journal files must match what the walk saw (no rotation added/removed one).
    let mut on_disk = std::collections::BTreeSet::new();
    let rd = std::fs::read_dir(db_path).map_err(|source| Error::Io { path: db_path.to_path_buf(), source })?;
    for entry in rd {
        let entry = entry.map_err(|source| Error::Io { path: db_path.to_path_buf(), source })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".jnl") {
            on_disk.insert(name);
        }
    }
    let captured: std::collections::BTreeSet<String> = journals.iter().map(|(n, _)| n.clone()).collect();
    if on_disk != captured {
        return Ok(None);
    }

    let mut out = Vec::with_capacity(journals.len());
    for (name, path) in journals {
        let len_before = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::Io { path: path.clone(), source }),
        };
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::Io { path: path.clone(), source }),
        };
        let len_after = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::Io { path: path.clone(), source }),
        };
        // If the file's length moved across the read (truncation/rotation in flight), the bytes may
        // be torn — retry.
        if len_before != len_after || bytes.len() as u64 != len_after {
            return Ok(None);
        }
        out.push((name.clone(), bytes));
    }
    Ok(Some(out))
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

type NamedPath = (String, std::path::PathBuf);
type NamedBytes = (String, Vec<u8>);

fn walk(
    base: &Path,
    dir: &Path,
    files: &mut Vec<(FileId, std::path::PathBuf)>,
    dirs: &mut Vec<String>,
    journals: &mut Vec<NamedPath>,
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
            walk(base, &path, files, dirs, journals, pointers)?;
            continue;
        }

        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // Never replicate the lock (a 0-byte runtime artifact restore recreates).
        if name == "lock" {
            continue;
        }

        // Journals are mutable; ship them per-version (they carry the seqno watermark).
        if rel.ends_with(".jnl") {
            journals.push((rel, path));
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
