//! Hot reader. A read-only copy of the database that tails the bucket and stays caught up — "restore
//! that never stops."
//!
//! This is the local-copy follower: poll for a newer version, restore it into a fresh local
//! directory, open it read-only, and atomically swap the handle readers get from [`Follower::database`].
//! All methods take `&self`, so a `Follower` can be wrapped in an `Arc` and shared between a poll loop
//! and readers.
//!
//! Limitations (v1): it keeps the current + previous restored directories so an in-flight reader has a
//! grace window, then deletes older ones — a reader that holds a `database()` clone across two polls
//! can still observe its directory removed. The VFS / lazy-block follower (read blocks on demand
//! instead of a full local copy) is the same interface with a harder backend; see ROADMAP.md.

use crate::error::{Error, Result};
use crate::layout::Layout;
use crate::object_store::ObjectStore;
use crate::restore::{resolve_version, restore_to};
use crate::types::RestoreTarget;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct FollowConfig {
    /// How often to poll the bucket for a newer version record.
    pub poll_interval: Duration,
    /// Local directory the follower materializes downloaded versions into.
    pub local_dir: PathBuf,
}

struct State {
    db: fjall::Database,
    dir: PathBuf,
    seqno: u64,
}

pub struct Follower<S: ObjectStore> {
    store: S,
    layout: Layout,
    cfg: FollowConfig,
    current: Mutex<Option<State>>,
    /// The previous version's directory, kept one generation for in-flight readers, deleted on the
    /// next swap.
    previous_dir: Mutex<Option<PathBuf>>,
}

impl<S: ObjectStore> Follower<S> {
    pub fn new(store: S, layout: Layout, cfg: FollowConfig) -> Self {
        Self {
            store,
            layout,
            cfg,
            current: Mutex::new(None),
            previous_dir: Mutex::new(None),
        }
    }

    /// Seqno the follower currently serves, for lag measurement against the primary's latest version.
    pub fn applied_seqno(&self) -> Option<u64> {
        self.current.lock().expect("poisoned").as_ref().map(|s| s.seqno)
    }

    /// The currently-applied read-only database (a cheap `Arc` clone), or `None` before the first
    /// catch-up. Re-fetch after each poll rather than holding the clone across polls.
    pub fn database(&self) -> Option<fjall::Database> {
        self.current.lock().expect("poisoned").as_ref().map(|s| s.db.clone())
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

        // Swap in the new state. Retire the directory from two generations ago; keep the previous one
        // as a grace window for in-flight readers.
        let prev = self
            .current
            .lock()
            .expect("poisoned")
            .replace(State { db, dir, seqno: rec.seqno });
        let two_ago = self.previous_dir.lock().expect("poisoned").take();
        if let Some(old) = two_ago {
            let _ = tokio::fs::remove_dir_all(old).await;
        }
        if let Some(p) = prev {
            drop(p.db); // release our handle to the old db
            *self.previous_dir.lock().expect("poisoned") = Some(p.dir);
        }
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
