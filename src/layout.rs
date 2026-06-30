//! Bucket key layout. Centralizes the object-store key scheme so the writer, reader, and pruner
//! all agree on where things live.
//!
//! ```text
//! <db>/generations/<gen>/
//!     files/<relpath>            immutable files (SSTs, blobs, manifests), keyed by db-relative path
//!     journals/<seqno>/<name>    per-version journal files (mutable, not content-addressed)
//!     versions/<seqno>.json      version records (each self-contained — a full file set)
//! ```

use crate::types::{FileId, Generation};

pub struct Layout {
    pub db: String,
    pub generation: Generation,
}

impl Layout {
    pub fn new(db: impl Into<String>, generation: Generation) -> Self {
        Self { db: db.into(), generation }
    }

    fn base(&self) -> String {
        format!("{}/generations/{}", self.db, self.generation)
    }

    pub fn file(&self, id: &FileId) -> String {
        format!("{}/files/{}", self.base(), id)
    }

    pub fn files_prefix(&self) -> String {
        format!("{}/files", self.base())
    }

    pub fn version(&self, seqno: u64) -> String {
        // Zero-pad so lexical list order matches numeric order.
        format!("{}/versions/{:020}.json", self.base(), seqno)
    }

    pub fn versions_prefix(&self) -> String {
        format!("{}/versions", self.base())
    }

    /// A journal file for one version. Journals are mutable (rewritten in place), so they are keyed
    /// per-version rather than content-addressed — each capture ships its own copy.
    pub fn journal(&self, seqno: u64, name: &str) -> String {
        format!("{}/journals/{:020}/{}", self.base(), seqno, name)
    }

    /// The prefix under which a version's journals live, for pruning.
    pub fn journals_prefix(&self, seqno: u64) -> String {
        format!("{}/journals/{:020}", self.base(), seqno)
    }

    /// Parse a `versions/<seqno>.json` key back to its seqno. Returns `None` if the key isn't a
    /// version record.
    pub fn seqno_from_version_key(key: &str) -> Option<u64> {
        let name = key.rsplit('/').next()?;
        let stem = name.strip_suffix(".json")?;
        stem.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_keys_sort_numerically() {
        let l = Layout::new("db", Generation("g1".into()));
        let k9 = l.version(9);
        let k10 = l.version(10);
        assert!(k9 < k10, "zero-padding must keep lexical order == numeric order");
        assert_eq!(Layout::seqno_from_version_key(&k10), Some(10));
    }
}
