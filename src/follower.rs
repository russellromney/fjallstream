//! Hot reader. A read-only copy of the database that tails the bucket and stays caught up — "restore
//! that never stops."
//!
//! Local-copy follower: poll for a newer version, restore it into a fresh local directory, open it
//! read-only, and atomically swap the handle readers get from [`Follower::read`]. All methods take
//! `&self`, so a `Follower` can be wrapped in an `Arc` and shared between a poll loop and readers.
//!
//! A restored directory is reference-counted ([`DirHandle`]): it is deleted only once the follower
//! has swapped past it *and* every [`ReadHandle`] over it has dropped. So a reader holding a
//! `ReadHandle` across any number of polls can never have its directory removed underneath it — as
//! long as it reads through the handle and doesn't clone the inner `Database` back out.
//!
//! The VFS / lazy-block follower (read blocks on demand instead of a full local copy) is the same
//! interface with a harder backend; see ROADMAP.md.

use crate::error::{Error, Result};
use crate::layout::Layout;
use crate::object_store::ObjectStore;
use crate::restore::{resolve_version, restore_to};
use crate::types::RestoreTarget;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct FollowConfig {
    /// How often to poll the bucket for a newer version record.
    pub poll_interval: Duration,
    /// Local directory the follower materializes downloaded versions into.
    pub local_dir: PathBuf,
}

/// Owns a restored version directory; removes it when the last reference drops.
struct DirHandle {
    path: PathBuf,
}

impl Drop for DirHandle {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct State {
    db: fjall::Database,
    dir: Arc<DirHandle>,
    seqno: u64,
}

/// A read view over the follower's current database. Keeps the underlying directory alive while held
/// (read through it; don't clone the inner `Database` out, or that guarantee is lost). Derefs to
/// [`fjall::Database`].
pub struct ReadHandle {
    db: fjall::Database,
    _dir: Arc<DirHandle>,
}

impl std::ops::Deref for ReadHandle {
    type Target = fjall::Database;
    fn deref(&self) -> &fjall::Database {
        &self.db
    }
}

pub struct Follower<S: ObjectStore> {
    store: S,
    layout: Layout,
    cfg: FollowConfig,
    current: Mutex<Option<State>>,
}

impl<S: ObjectStore> Follower<S> {
    pub fn new(store: S, layout: Layout, cfg: FollowConfig) -> Self {
        Self { store, layout, cfg, current: Mutex::new(None) }
    }

    /// Seqno the follower currently serves, for lag measurement against the primary's latest version.
    pub fn applied_seqno(&self) -> Option<u64> {
        self.current.lock().expect("poisoned").as_ref().map(|s| s.seqno)
    }

    /// A read view over the currently-applied database, or `None` before the first catch-up. Safe to
    /// hold across polls — its directory won't be deleted while the handle lives.
    pub fn read(&self) -> Option<ReadHandle> {
        self.current
            .lock()
            .expect("poisoned")
            .as_ref()
            .map(|s| ReadHandle { db: s.db.clone(), _dir: s.dir.clone() })
    }

    /// One catch-up step. If a newer version exists, restore it into a fresh local directory, open it
    /// read-only, and swap it in. Returns whether it advanced. Call serially (one poll loop).
    pub async fn poll_once(&self) -> Result<bool> {
        let rec = resolve_version(&self.store, &self.layout, &RestoreTarget::Latest).await?;
        if matches!(self.applied_seqno(), Some(a) if a >= rec.seqno) {
            return Ok(false);
        }

        let dir = self.cfg.local_dir.join(format!("v{:020}", rec.seqno));
        let _ = tokio::fs::remove_dir_all(&dir).await; // clear any leftover from a crash
        restore_to(&self.store, &self.layout, RestoreTarget::Seqno(rec.seqno), &dir).await?;
        let db = fjall::Database::builder(&dir)
            .open()
            .map_err(|e| Error::Fjall(format!("open restored follower db: {e}")))?;

        // Swap in the new state. The previous `State` drops here, releasing its `Arc<DirHandle>` — the
        // directory is removed only once no `ReadHandle` over it remains, so in-flight readers are safe.
        *self.current.lock().expect("poisoned") =
            Some(State { db, dir: Arc::new(DirHandle { path: dir }), seqno: rec.seqno });
        Ok(true)
    }

    /// Run the catch-up loop forever, polling every `poll_interval`.
    pub async fn run(&self) -> Result<()> {
        loop {
            self.poll_once().await?;
            tokio::time::sleep(self.cfg.poll_interval).await;
        }
    }
}
