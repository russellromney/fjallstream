use crate::error::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};

/// The replication substrate. This is the only thing that changes per backend (S3, Tigris, local
/// filesystem). Keep it tiny: put, get, list, delete. Everything else is built on top.
///
/// Writes to `files/<file-id>` are write-once and immutable; an implementation may treat a `put`
/// to an already-present key as a no-op.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Bytes>;
    /// List keys under `prefix`. Returns full keys, not basenames.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: &str) -> Result<()>;

    /// Whether a key exists. Default implementation probes via `get`; backends should override
    /// with a cheaper HEAD where available, since the writer calls this once per file to skip
    /// re-uploading immutable files.
    async fn exists(&self, key: &str) -> Result<bool> {
        match self.get(key).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// A filesystem-backed [`ObjectStore`], for tests, local dev, and single-host backup to a
/// different disk. The "bucket" is a directory; keys map to nested paths under it.
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| Error::Io { path: parent.to_path_buf(), source })?;
        }
        // Write to a temp file then rename, so a reader never sees a half-written object.
        let tmp = path.with_extension("tmp-partial");
        tokio::fs::write(&tmp, &bytes)
            .await
            .map_err(|source| Error::Io { path: tmp.clone(), source })?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|source| Error::Io { path: path.clone(), source })?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.path_for(key);
        match tokio::fs::read(&path).await {
            Ok(v) => Ok(Bytes::from(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(key.to_string()))
            }
            Err(source) => Err(Error::Io { path, source }),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.path_for(prefix);
        let mut out = Vec::new();
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(source) => return Err(Error::Io { path: dir, source }),
        };
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|source| Error::Io { path: dir.clone(), source })?
        {
            if let Ok(rel) = entry.path().strip_prefix(&self.root) {
                if let Some(s) = rel.to_str() {
                    out.push(s.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(Error::Io { path, source }),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.path_for(key).exists())
    }
}

/// Helper so callers can hand a `&Path` root without importing PathBuf conversions.
impl From<&Path> for LocalObjectStore {
    fn from(p: &Path) -> Self {
        Self::new(p.to_path_buf())
    }
}
