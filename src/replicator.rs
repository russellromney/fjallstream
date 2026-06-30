//! The writer. Captures the live database and mirrors each version forward into the object store.
//!
//! [`replicate_once`](Replicator::replicate_once) is pure object-store logic over a [`LocalVersion`]
//! — fully testable with no fjall instance (see `tests/roundtrip.rs`). [`run`](Replicator::run) is
//! the fjall-coupled loop: capture every `interval`, replicate, prune. [`capture`](crate::capture)
//! produces the `LocalVersion`.

use crate::error::{Error, Result};
use crate::layout::Layout;
use crate::object_store::ObjectStore;
use crate::types::{FileId, PointerFile, VersionRecord};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A point-in-time view of the local database produced by [`crate::capture::capture`]: the immutable
/// file set, directory tree, per-version journals, and inline pointer files, all captured while a
/// fjall `Snapshot` is held. The replicator only ever sees this struct.
#[derive(Debug, Clone)]
pub struct LocalVersion {
    /// Upper-bound sequence number: exceeds every write the captured journal contains.
    pub seqno: u64,
    pub parent: Option<u64>,
    /// Every immutable file the database references, as `(file id, path on disk)`.
    pub files: Vec<(FileId, PathBuf)>,
    /// Relative paths of every directory in the db tree (so restore can recreate empty ones).
    pub dirs: Vec<String>,
    /// Journal files as `(name, bytes)` — read consistently at capture time.
    pub journals: Vec<(String, Vec<u8>)>,
    /// The mutable pointer files (each keyspace's `current` HEAD), captured by value.
    pub pointers: Vec<PointerFile>,
}

#[derive(Debug, Clone)]
pub struct ReplicateConfig {
    /// How often the writer captures and ships a new version.
    pub interval: Duration,
    /// Keep at least this many of the newest version records; older ones (and the files only they
    /// reference) are pruned.
    pub retention_versions: u64,
}

impl Default for ReplicateConfig {
    fn default() -> Self {
        Self { interval: Duration::from_secs(10), retention_versions: 256 }
    }
}

pub struct Replicator<S: ObjectStore> {
    store: S,
    layout: Layout,
    cfg: ReplicateConfig,
    /// Last `ts_millis` we wrote, kept monotonic so a backward wall-clock step can't reorder
    /// time-based restore targets.
    last_ts_millis: u64,
    /// Cumulative count of files dropped by the C1 vanished-file guard, exposed for observability.
    files_dropped: u64,
    /// `file_id -> content hash`, so a file already handled this run isn't re-read just to checksum it.
    checksum_cache: HashMap<FileId, u64>,
}

impl<S: ObjectStore> Replicator<S> {
    pub fn new(store: S, layout: Layout, cfg: ReplicateConfig) -> Self {
        Self { store, layout, cfg, last_ts_millis: 0, files_dropped: 0, checksum_cache: HashMap::new() }
    }

    /// Total files dropped by the C1 vanished-file guard across all `replicate_once` calls.
    pub fn files_dropped(&self) -> u64 {
        self.files_dropped
    }

    /// Replicate one captured version: upload its not-yet-present files and journals (recording a
    /// content hash for each), then write the version record. Returns the seqno that was recorded.
    ///
    /// The version record is written **last** — a crash mid-upload leaves orphaned files but never a
    /// record pointing at a missing file.
    pub async fn replicate_once(&mut self, version: &LocalVersion) -> Result<u64> {
        // 1. Upload immutable files we don't already have, recording each one's content hash. A file
        //    that vanished between capture and now (obsolete, GC'd after a compaction) is dropped: the
        //    held snapshot pins everything reachable from `current`, so anything gone wasn't live. (C1)
        let mut present_ids = Vec::with_capacity(version.files.len());
        let mut file_checksums = Vec::with_capacity(version.files.len());
        for (id, path) in &version.files {
            let checksum = match self.checksum_cache.get(id) {
                Some(c) => *c,
                None => {
                    let bytes = match tokio::fs::read(path).await {
                        Ok(b) => b,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            self.files_dropped += 1;
                            continue;
                        }
                        Err(source) => return Err(Error::Io { path: path.clone(), source }),
                    };
                    let c = crate::checksum::hash64(&bytes);
                    let key = self.layout.file(id);
                    if !self.store.exists(&key).await? {
                        self.store.put(&key, Bytes::from(bytes)).await?;
                    }
                    self.checksum_cache.insert(id.clone(), c);
                    c
                }
            };
            present_ids.push(id.clone());
            file_checksums.push(checksum);
        }

        // 2. Upload this version's journals, keyed per-version (mutable, never deduped). Bytes were
        //    read consistently at capture; gzip shrinks the ~64 MiB-of-zeros journal to KB.
        let mut journal_names = Vec::with_capacity(version.journals.len());
        let mut journal_checksums = Vec::with_capacity(version.journals.len());
        for (name, bytes) in &version.journals {
            journal_checksums.push(crate::checksum::hash64(bytes));
            let compressed = crate::compress::gzip(bytes)?;
            self.store
                .put(&self.layout.journal(version.seqno, name), Bytes::from(compressed))
                .await?;
            journal_names.push(name.clone());
        }

        // 3. Write the version record last (the commit point). Timestamp kept monotonic. (C7)
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
            file_checksums,
            dirs: version.dirs.clone(),
            pointers: version.pointers.clone(),
            journals: journal_names,
            journal_checksums,
            ts_millis,
        };
        let body = Bytes::from(serde_json::to_vec(&record)?);
        self.store.put(&self.layout.version(version.seqno), body).await?;
        Ok(version.seqno)
    }

    /// Delete version records older than the retention window, plus their journals and any files no
    /// retained version still references. Every version record is self-contained, so keeping the
    /// newest `retention_versions` is always safe.
    pub async fn prune(&self) -> Result<()> {
        let mut version_keys = self.store.list(&self.layout.versions_prefix()).await?;
        version_keys.sort();
        let retain = self.cfg.retention_versions as usize;
        if version_keys.len() <= retain {
            return Ok(());
        }
        let cut = version_keys.len() - retain;
        let (to_delete, to_keep) = version_keys.split_at(cut);

        // Files referenced by any retained version must not be deleted.
        let mut live_files: HashSet<String> = HashSet::new();
        for key in to_keep {
            let rec = self.read_record(key).await?;
            for id in &rec.file_ids {
                live_files.insert(id.0.clone());
            }
        }

        for key in to_delete {
            let rec = self.read_record(key).await?;
            // Journals (per-version) and files not referenced by any retained version. Delete the
            // record last so a crash mid-prune leaves a still-resolvable (if orphan-heavy) version.
            for jkey in self.store.list(&self.layout.journals_prefix(rec.seqno)).await? {
                self.store.delete(&jkey).await?;
            }
            for id in &rec.file_ids {
                if !live_files.contains(&id.0) {
                    self.store.delete(&self.layout.file(id)).await?;
                }
            }
            self.store.delete(key).await?;
        }
        Ok(())
    }

    async fn read_record(&self, key: &str) -> Result<VersionRecord> {
        let body = self.store.get(key).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// The long-running writer loop: capture the live database every `interval`, replicate it, prune.
    /// Returns on the first error (capture/replicate/store failure) so a supervisor can log + restart.
    ///
    /// `keyspaces` are the user keyspaces to flush before each capture. The journal covers anything
    /// not flushed, so this is an optimization, but passing the active keyspaces keeps the journal
    /// small.
    pub async fn run(
        mut self,
        db: fjall::Database,
        db_path: PathBuf,
        keyspaces: Vec<fjall::Keyspace>,
    ) -> Result<()> {
        let mut parent = None;
        loop {
            let refs: Vec<&fjall::Keyspace> = keyspaces.iter().collect();
            let captured = crate::capture::capture(&db, &db_path, &refs, parent)?;
            let seqno = self.replicate_once(&captured.version).await?;
            drop(captured); // release the snapshot before sleeping
            parent = Some(seqno);
            self.prune().await?;
            tokio::time::sleep(self.cfg.interval).await;
        }
    }
}
