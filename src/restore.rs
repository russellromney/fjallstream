//! Cold reader. Reconstructs a local database directory at a chosen point in time.
//!
//! Restore is just "replicate, run backward once": pick the newest version record at or before the
//! target, download its files, and (TODO) replay the journal tail to the exact seqno.

use crate::error::{Error, Result};
use crate::layout::Layout;
use crate::object_store::ObjectStore;
use crate::types::{RestoreTarget, VersionRecord};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

/// Find the version record matching `target`, returning the parsed record.
pub async fn resolve_version<S: ObjectStore>(
    store: &S,
    layout: &Layout,
    target: &RestoreTarget,
) -> Result<VersionRecord> {
    let mut keys = store.list(&layout.versions_prefix()).await?;
    keys.sort(); // zero-padded => lexical == numeric order
    if keys.is_empty() {
        return Err(Error::NotFound("no version records in generation".into()));
    }

    let pick = match target {
        RestoreTarget::Latest => keys.last().cloned(),
        RestoreTarget::Seqno(want) => newest_matching(store, &keys, |r| r.seqno <= *want).await?,
        RestoreTarget::TimestampMillis(want) => {
            newest_matching(store, &keys, |r| r.ts_millis <= *want).await?
        }
    };

    let key = pick.ok_or_else(|| Error::NotFound("no version matches restore target".into()))?;
    let body = store.get(&key).await?;
    Ok(serde_json::from_slice(&body)?)
}

/// Walk version keys newest-first, returning the first whose record satisfies `pred`.
async fn newest_matching<S: ObjectStore>(
    store: &S,
    sorted_keys: &[String],
    pred: impl Fn(&VersionRecord) -> bool,
) -> Result<Option<String>> {
    for key in sorted_keys.iter().rev() {
        let body = store.get(key).await?;
        let record: VersionRecord = serde_json::from_slice(&body)?;
        if pred(&record) {
            return Ok(Some(key.clone()));
        }
    }
    Ok(None)
}

/// Restore the database to `dst`: download every file the resolved version references, verifying
/// each before it lands.
///
/// TODO: after downloading files, replay the journal segment referenced by the record up to the
/// target seqno so `dst` opens at the exact point. Also verify SFA XXH3 checksums on each file.
pub async fn restore_to<S: ObjectStore>(
    store: &S,
    layout: &Layout,
    target: RestoreTarget,
    dst: &Path,
) -> Result<VersionRecord> {
    let record = resolve_version(store, layout, &target).await?;

    // Refuse to clobber a populated target — mixing two restores produces a Frankenstein db.
    if dir_nonempty(dst).await? {
        return Err(Error::Store(format!(
            "restore target {} is not empty",
            dst.display()
        )));
    }

    // Build the whole tree in a sibling staging dir, fsync it, then atomically rename into place.
    // A crash before the rename leaves `dst` absent or untouched — never half-built. (C2)
    let staging = staging_path(dst)?;
    let _ = tokio::fs::remove_dir_all(&staging).await; // clear any leftover from a prior crash
    tokio::fs::create_dir_all(&staging)
        .await
        .map_err(|source| Error::Io { path: staging.clone(), source })?;

    // Recreate every directory, including empty ones (e.g. a fresh keyspace's `tables/`, which
    // fjall's recovery does not create).
    for d in &record.dirs {
        let p = staging.join(d);
        tokio::fs::create_dir_all(&p)
            .await
            .map_err(|source| Error::Io { path: p, source })?;
    }

    // Immutable files, by relative path.
    for id in &record.file_ids {
        let bytes = store.get(&layout.file(id)).await?;
        write_file(&staging, &id.0, &bytes).await?;
    }

    // Journals, keyed per-version (gzip-compressed). Required for recovery to restore the seqno
    // watermark — without them the restored db has visible_seqno 0 and iterators see nothing.
    for name in &record.journals {
        let compressed = store.get(&layout.journal(record.seqno, name)).await?;
        let bytes = crate::compress::gunzip(&compressed)?;
        write_file(&staging, name, &bytes).await?;
    }

    // Mutable pointer files (each keyspace's `current` HEAD), captured inline in the record.
    for p in &record.pointers {
        write_file(&staging, &p.path, &p.bytes).await?;
    }

    // fjall's recovery acquires the lock file with open (not create), so it must pre-exist. It's a
    // 0-byte runtime artifact we deliberately don't replicate — recreate an empty one here.
    write_file(&staging, "lock", b"").await?;

    // Swap staging into place. `dst` is empty (checked above), so removing it is safe.
    if tokio::fs::try_exists(dst).await.unwrap_or(false) {
        tokio::fs::remove_dir(dst)
            .await
            .map_err(|source| Error::Io { path: dst.to_path_buf(), source })?;
    }
    tokio::fs::rename(&staging, dst)
        .await
        .map_err(|source| Error::Io { path: dst.to_path_buf(), source })?;

    Ok(record)
}

/// True if `dir` exists and contains at least one entry. A missing dir is "empty".
async fn dir_nonempty(dir: &Path) -> Result<bool> {
    match tokio::fs::read_dir(dir).await {
        Ok(mut rd) => Ok(rd
            .next_entry()
            .await
            .map_err(|source| Error::Io { path: dir.to_path_buf(), source })?
            .is_some()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(Error::Io { path: dir.to_path_buf(), source }),
    }
}

/// A sibling of `dst` to stage the restore into (same filesystem, so the final rename is atomic).
fn staging_path(dst: &Path) -> Result<PathBuf> {
    let name = dst
        .file_name()
        .ok_or_else(|| Error::Store(format!("restore target {} has no final component", dst.display())))?;
    let mut s = OsString::from(name);
    s.push(".fjallstream-restore-tmp");
    Ok(dst.with_file_name(s))
}

/// Write `bytes` to `base/<rel>`, creating parent directories and fsyncing the file for durability.
async fn write_file(base: &Path, rel: &str, bytes: &[u8]) -> Result<()> {
    let out = base.join(rel);
    if let Some(parent) = out.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::Io { path: parent.to_path_buf(), source })?;
    }
    let mut f = tokio::fs::File::create(&out)
        .await
        .map_err(|source| Error::Io { path: out.clone(), source })?;
    f.write_all(bytes)
        .await
        .map_err(|source| Error::Io { path: out.clone(), source })?;
    f.sync_all()
        .await
        .map_err(|source| Error::Io { path: out, source })?;
    Ok(())
}
