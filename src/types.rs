use serde::{Deserialize, Serialize};

/// A lineage id. A fresh generation is started whenever a database is restored and then
/// diverges, so two histories can never be confused in the same bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Generation(pub String);

impl std::fmt::Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifier for an immutable file (SST or blob) in the content-addressed store.
///
/// fjall's files are immutable once written, so a file id maps to exactly one byte sequence
/// forever. We key the bucket's `files/` store by this and upload each file at most once.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FileId(pub String);

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One point in the replicated version history.
///
/// The exact set of immutable files that make up the database at sequence number `seqno`, plus the
/// bytes of the few *mutable* pointer files (fjall's per-keyspace `current` HEAD) captured inline —
/// those can't be content-addressed because they're rewritten in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionRecord {
    /// Monotonic sequence-number watermark this version is flushed up to.
    pub seqno: u64,
    /// The version this one descends from, if any. `None` for the first record in a generation.
    pub parent: Option<u64>,
    /// Every immutable file the database references at this version (keyed by relative path).
    pub file_ids: Vec<FileId>,
    /// Relative paths of every directory in the database tree, so restore can recreate empty ones
    /// (e.g. a freshly created keyspace's `tables/`, which fjall's recovery does not create).
    #[serde(default)]
    pub dirs: Vec<String>,
    /// The mutable pointer files (e.g. each keyspace's `current` HEAD), captured inline.
    pub pointers: Vec<PointerFile>,
    /// True if this record is a full re-base point (a fresh snapshot) rather than an incremental
    /// version. Re-base points let old files GC out of the bucket by breaking dependency chains.
    pub is_snapshot: bool,
    /// Wall-clock time the version was recorded, milliseconds since the Unix epoch. Used only for
    /// human-facing point-in-time restore targets, never for ordering.
    pub ts_millis: u64,
}

/// A mutable file captured by value inside a [`VersionRecord`]. `path` is relative to the database
/// root (e.g. `keyspaces/1/current`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointerFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

/// A reader's position in the replication stream. This is a log position, never a per-key seqno
/// (fjall rewrites per-key seqnos to 0 during bottom-level compaction).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub version_seqno: u64,
    pub journal_offset: u64,
}

/// What point in time to restore to.
#[derive(Debug, Clone)]
pub enum RestoreTarget {
    /// The newest version available.
    Latest,
    /// The newest version at or before this seqno watermark.
    Seqno(u64),
    /// The newest version at or before this wall-clock time (millis since Unix epoch).
    TimestampMillis(u64),
}
