//! Bucket key layout. Centralizes the object-store key scheme so the writer, reader, and pruner
//! all agree on where things live.
//!
//! ```text
//! <db>/generations/<gen>/
//!     files/<file-id>            immutable SSTs + blobs, uploaded once
//!     versions/<seqno>.json      incremental version records
//!     snapshots/<seqno>.json     version records flagged as full re-base points
//!     journal/<from>-<to>.seg    journal tail per version
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

    pub fn snapshot(&self, seqno: u64) -> String {
        format!("{}/snapshots/{:020}.json", self.base(), seqno)
    }

    /// A journal file for one version. Journals are mutable (rewritten in place), so they are keyed
    /// per-version rather than content-addressed — each capture ships its own copy.
    pub fn journal(&self, seqno: u64, name: &str) -> String {
        format!("{}/journals/{:020}/{}", self.base(), seqno, name)
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
