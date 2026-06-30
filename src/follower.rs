//! Hot reader. A read-only database that tails the bucket and stays caught up — "restore that never
//! stops."
//!
//! v1 is the local-copy follower: poll for new version records, download their files, open a
//! read-only `fjall::Database` at the new file set, and atomically swap the handle readers use.
//! fjall's copy-on-write versions make the swap safe — in-flight reads on the old version finish
//! cleanly. The VFS / lazy-block follower (fetch blocks on demand instead of a full local copy) is
//! the same interface with a harder backend; see DESIGN.md.

use crate::error::Result;
use crate::layout::Layout;
use crate::object_store::ObjectStore;
use crate::types::Cursor;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct FollowConfig {
    /// How often to poll the bucket for a newer version record.
    pub poll_interval: Duration,
    /// Local directory the follower materializes downloaded files into.
    pub local_dir: PathBuf,
}

pub struct Follower<S: ObjectStore> {
    store: S,
    layout: Layout,
    cfg: FollowConfig,
    /// The version the follower has fully downloaded and opened. `None` until first catch-up.
    applied: Option<Cursor>,
}

impl<S: ObjectStore> Follower<S> {
    pub fn new(store: S, layout: Layout, cfg: FollowConfig) -> Self {
        Self { store, layout, cfg, applied: None }
    }

    /// Seqno watermark the follower currently serves reads at, for lag measurement against the
    /// primary's latest version.
    pub fn applied_seqno(&self) -> Option<u64> {
        self.applied.map(|c| c.version_seqno)
    }

    /// One catch-up step: if a newer version record exists, download its missing files into
    /// `local_dir`, then swap the open read-only database to it.
    ///
    /// TODO: implement once restore download + the fjall read-only open/swap are wired. Reuse
    /// `restore::resolve_version(Latest)` to find the target; download only files not already
    /// local; verify checksums; open read-only; atomically swap.
    pub async fn poll_once(&mut self) -> Result<()> {
        todo!("download newest version's missing files, then swap the read-only Database")
    }

    /// Run the catch-up loop forever.
    pub async fn run(mut self) -> Result<()> {
        loop {
            self.poll_once().await?;
            tokio::time::sleep(self.cfg.poll_interval).await;
        }
    }
}
