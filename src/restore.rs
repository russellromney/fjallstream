//! Cold reader. Reconstructs a local database directory at a chosen point in time.
//!
//! Restore is just "replicate, run backward once": pick the newest version record at or before the
//! target, download its files, and (TODO) replay the journal tail to the exact seqno.

use crate::error::{Error, Result};
use crate::layout::Layout;
use crate::object_store::ObjectStore;
use crate::types::{RestoreTarget, VersionRecord};
use std::path::Path;

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
    tokio::fs::create_dir_all(dst)
        .await
        .map_err(|source| Error::Io { path: dst.to_path_buf(), source })?;

    // Immutable files, by relative path.
    for id in &record.file_ids {
        let bytes = store.get(&layout.file(id)).await?;
        write_relative(dst, &id.0, &bytes).await?;
    }

    // Mutable pointer files (each keyspace's `current` HEAD), captured inline in the record.
    for p in &record.pointers {
        write_relative(dst, &p.path, &p.bytes).await?;
    }

    // fjall's recovery acquires the lock file with open (not create), so it must pre-exist. It's a
    // 0-byte runtime artifact we deliberately don't replicate — recreate an empty one here.
    write_relative(dst, "lock", b"").await?;

    Ok(record)
}

/// Write `bytes` to `dst/<rel>`, creating parent directories.
async fn write_relative(dst: &Path, rel: &str, bytes: &[u8]) -> Result<()> {
    let out = dst.join(rel);
    if let Some(parent) = out.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::Io { path: parent.to_path_buf(), source })?;
    }
    tokio::fs::write(&out, bytes)
        .await
        .map_err(|source| Error::Io { path: out, source })
}
