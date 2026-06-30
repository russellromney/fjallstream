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
    /// Relative paths of every directory in the db tree (so restore can recreate empty ones).
    pub dirs: Vec<String>,
    /// Journal files as `(name, bytes)` — read consistently at capture time (journals race
    /// background maintenance, so capture reads + verifies them rather than leaving a late read).
    pub journals: Vec<(String, Vec<u8>)>,
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
    /// Last `ts_millis` we wrote, so version timestamps stay monotonic even if the wall clock steps
    /// backwards (NTP). Time-based restore targets rely on this.
    last_ts_millis: u64,
    /// Cumulative count of files dropped by the C1 vanished-file guard. Exposed so tests (and
    /// operators) can confirm the guard actually fired rather than trusting it silently.
    files_dropped: u64,
}

impl<S: ObjectStore> Replicator<S> {
    pub fn new(store: S, layout: Layout, cfg: ReplicateConfig) -> Self {
        Self { store, layout, cfg, versions_since_snapshot: 0, last_ts_millis: 0, files_dropped: 0 }
    }

    /// Total files dropped by the C1 vanished-file guard across all `replicate_once` calls.
    pub fn files_dropped(&self) -> u64 {
        self.files_dropped
    }

    /// Replicate one captured version: upload its not-yet-present files, ship its journal tail, and
    /// write the version record. Returns the seqno that was recorded.
    ///
    /// Idempotent on files (immutable => skip if already present). Writing the version record last
    /// means a crash mid-upload leaves an unreferenced partial file set, never a dangling record.
    pub async fn replicate_once(&mut self, version: &LocalVersion) -> Result<u64> {
        // 1. Upload immutable files we don't already have. A file that vanished between capture and
        //    now (obsolete, GC'd after a compaction) is dropped from the record: the held snapshot
        //    pins everything reachable from `current`, so anything that disappeared was not part of
        //    the live set and is safe to omit. (C1)
        let mut present_ids = Vec::with_capacity(version.files.len());
        for (id, path) in &version.files {
            let key = self.layout.file(id);
            if self.store.exists(&key).await? {
                present_ids.push(id.clone());
                continue;
            }
            let bytes = match tokio::fs::read(path).await {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.files_dropped += 1;
                    continue;
                }
                Err(source) => return Err(crate::error::Error::Io { path: path.clone(), source }),
            };
            self.store.put(&key, Bytes::from(bytes)).await?;
            present_ids.push(id.clone());
        }

        // 2. Upload this version's journal files, keyed per-version (mutable, never deduped). The
        //    bytes were read consistently at capture; here we just compress and ship them. Journals
        //    are ~64 MiB of mostly zeros after a force-flush, so gzip shrinks them to KB.
        let mut journal_names = Vec::with_capacity(version.journals.len());
        for (name, bytes) in &version.journals {
            let compressed = crate::compress::gzip(bytes)?;
            self.store
                .put(&self.layout.journal(version.seqno, name), Bytes::from(compressed))
                .await?;
            journal_names.push(name.clone());
        }

        // 3. Decide whether this is a forced re-base point.
        let is_snapshot =
            matches!(self.cfg.snapshot_every, Some(n) if self.versions_since_snapshot + 1 >= n);

        // 4. Write the version record last (the commit point). The mutable pointer files ride
        //    inline; journals are referenced per-version. The timestamp is kept monotonic across
        //    versions so a backward clock step can't reorder time-based restore. (C7)
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let ts_millis = now_millis.max(self.last_ts_millis.saturating_add(1));
        self.last_ts_millis = ts_millis;
        let record = VersionRecord {
            seqno: version.seqno,
            parent: version.parent,
            file_ids: present_ids,
            dirs: version.dirs.clone(),
            pointers: version.pointers.clone(),
            journals: journal_names,
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
