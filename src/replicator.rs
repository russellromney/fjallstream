//! The writer. Mirrors fjall's version history forward into the object store.
//!
//! The fjall-native loop is:
//!   1. hold a `Snapshot` (pins the current version's files from GC),
//!   2. enumerate the immutable files on disk + the current seqno watermark,
//!   3. upload every file not already present (immutable => upload once),
//!   4. write a version record naming the full file set,
//!   5. ship the journal tail,
//!   6. drop the snapshot, prune anything past the retention window.
//!
//! Steps 3-4 are pure object-store logic and live here, fully testable against
//! [`crate::object_store::LocalObjectStore`]. Steps 1-2-5 are the fjall coupling; a real adapter
//! that produces a [`LocalVersion`] from a live `fjall::Database` lands once the API is pinned
//! (tracked in DESIGN.md "Pragmatic v1 vs upstream-later").

use crate::error::Result;
use crate::layout::Layout;
use crate::object_store::ObjectStore;
use crate::types::{FileId, PointerFile, VersionRecord};
use bytes::Bytes;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A point-in-time view of the local database: the immutable file set at `seqno`, captured while a
/// fjall `Snapshot` is held so the files cannot be GC'd before we upload them.
///
/// This is the seam between fjall and fjallstream. The replicator only ever sees this struct, so
/// the writer logic can be tested with no fjall instance at all.
#[derive(Debug, Clone)]
pub struct LocalVersion {
    pub seqno: u64,
    pub parent: Option<u64>,
    /// Every immutable file the database references right now, as `(file id, path on disk)`.
    pub files: Vec<(FileId, PathBuf)>,
    /// The mutable pointer files (each keyspace's `current` HEAD), captured by value.
    pub pointers: Vec<PointerFile>,
}

#[derive(Debug, Clone)]
pub struct ReplicateConfig {
    /// How often the writer captures and ships a new version.
    pub interval: Duration,
    /// Force a full re-base (fresh snapshot record) every this many versions, so old files can GC
    /// out of the bucket. `None` disables forced re-basing.
    pub snapshot_every: Option<u64>,
    /// Keep version records + their files for at least this many versions. Older ones may be
    /// pruned. A follower lagging past this window must re-bootstrap.
    pub retention_versions: u64,
}

impl Default for ReplicateConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            snapshot_every: Some(64),
            retention_versions: 256,
        }
    }
}

pub struct Replicator<S: ObjectStore> {
    store: S,
    layout: Layout,
    cfg: ReplicateConfig,
    versions_since_snapshot: u64,
}

impl<S: ObjectStore> Replicator<S> {
    pub fn new(store: S, layout: Layout, cfg: ReplicateConfig) -> Self {
        Self { store, layout, cfg, versions_since_snapshot: 0 }
    }

    /// Replicate one captured version: upload its not-yet-present files, ship its journal tail, and
    /// write the version record. Returns the seqno that was recorded.
    ///
    /// Idempotent on files (immutable => skip if already present). Writing the version record last
    /// means a crash mid-upload leaves an unreferenced partial file set, never a dangling record.
    pub async fn replicate_once(&mut self, version: &LocalVersion) -> Result<u64> {
        // 1. Upload immutable files we don't already have.
        for (id, path) in &version.files {
            let key = self.layout.file(id);
            if self.store.exists(&key).await? {
                continue;
            }
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|source| crate::error::Error::Io { path: path.clone(), source })?;
            self.store.put(&key, Bytes::from(bytes)).await?;
        }

        // 2. Decide whether this is a forced re-base point.
        let is_snapshot =
            matches!(self.cfg.snapshot_every, Some(n) if self.versions_since_snapshot + 1 >= n);

        // 3. Write the version record last (the commit point). The mutable pointer files ride
        //    inline; there is no journal in 0.1 (capture force-flushes first).
        let ts_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let record = VersionRecord {
            seqno: version.seqno,
            parent: version.parent,
            file_ids: version.files.iter().map(|(id, _)| id.clone()).collect(),
            pointers: version.pointers.clone(),
            is_snapshot,
            ts_millis,
        };
        let body = Bytes::from(serde_json::to_vec(&record)?);
        self.store.put(&self.layout.version(version.seqno), body.clone()).await?;
        if is_snapshot {
            self.store.put(&self.layout.snapshot(version.seqno), body).await?;
            self.versions_since_snapshot = 0;
        } else {
            self.versions_since_snapshot += 1;
        }

        Ok(version.seqno)
    }

    /// Prune version records (and their now-unreferenced files) older than the retention window.
    ///
    /// TODO: implement once `replicate_once` is exercised end-to-end. Must never delete a file
    /// still referenced by a retained version record; compute the live file set across the retained
    /// window first, then delete only files outside it.
    pub async fn prune(&self) -> Result<()> {
        Ok(())
    }

    /// The long-running writer loop: capture a `LocalVersion` from the live database every
    /// `interval`, replicate it, and prune.
    ///
    /// TODO: wire the fjall adapter that produces `LocalVersion` (hold `Snapshot`, list files,
    /// read seqno + journal tail). Until then `replicate_once` is driven directly by tests.
    pub async fn run(self) -> Result<()> {
        todo!("wire fjall Snapshot capture -> LocalVersion, then loop replicate_once + prune")
    }
}
