//! fjallstream — async replication and point-in-time backup for [fjall](https://github.com/fjall-rs/fjall)
//! to object storage.
//!
//! See `DESIGN.md` for the model. In short: we replicate fjall's **version DAG** — the immutable
//! file set at each point in time — to a content-addressed object store, plus journal segments for
//! the unflushed tail. Restore picks a version and pulls its files; a hot follower keeps pulling the
//! newest version and swaps atomically. Litestream is the teacher, not the template: fjall's
//! immutable files and copy-on-write versions let us drop everything Litestream built to fight
//! SQLite's mutable pages.

pub mod capture;
mod compress;
pub mod error;
pub mod follower;
pub mod layout;
pub mod object_store;
pub mod replicator;
pub mod restore;
pub mod types;

pub use capture::{capture, Captured};
pub use error::{Error, Result};
pub use object_store::{LocalObjectStore, ObjectStore};
pub use replicator::{LocalVersion, ReplicateConfig, Replicator};
pub use types::{Cursor, FileId, Generation, PointerFile, RestoreTarget, VersionRecord};
