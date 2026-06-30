use crate::error::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The replication substrate. This is the only thing that changes per backend (S3, Tigris, local
/// filesystem). Keep it tiny: put, get, list, delete. Everything else is built on top.
///
/// Contract:
/// - `put` is durable on return and atomic — a reader never sees a half-written object.
/// - `list(prefix)` returns **every** key under `prefix`, recursively, as full flat keys (matching
///   object-store prefix semantics, not a single directory level).
/// - Writes to `files/<relpath>` are write-once and immutable; a `put` to an already-present key may
///   be treated as a no-op.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Bytes>;
    /// Every key under `prefix`, recursively, as full keys (not basenames), sorted.
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

const TMP_SUFFIX: &str = ".fjallstream-tmp";

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
        // Write to a temp file (suffix APPENDED so it never collides with another key) then rename,
        // so a reader never sees a half-written object.
        let mut tmp = path.clone().into_os_string();
        tmp.push(TMP_SUFFIX);
        let tmp = PathBuf::from(tmp);
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
        let mut out = Vec::new();
        let mut stack = vec![self.path_for(prefix)];
        while let Some(dir) = stack.pop() {
            let mut rd = match tokio::fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => return Err(Error::Io { path: dir, source }),
            };
            while let Some(entry) = rd
                .next_entry()
                .await
                .map_err(|source| Error::Io { path: dir.clone(), source })?
            {
                let path = entry.path();
                let ft = entry
                    .file_type()
                    .await
                    .map_err(|source| Error::Io { path: path.clone(), source })?;
                if ft.is_dir() {
                    stack.push(path);
                } else if let Some(rel) = path.strip_prefix(&self.root).ok().and_then(|p| p.to_str()) {
                    let key = rel.replace('\\', "/");
                    if !key.ends_with(TMP_SUFFIX) {
                        out.push(key);
                    }
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

impl From<&Path> for LocalObjectStore {
    fn from(p: &Path) -> Self {
        Self::new(p.to_path_buf())
    }
}

/// An in-memory [`ObjectStore`] for fast tests. Keys are stored verbatim; `list` matches everything
/// under `prefix/`.
#[derive(Default)]
pub struct MemObjectStore {
    map: Mutex<BTreeMap<String, Bytes>>,
}

impl MemObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectStore for MemObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.map.lock().expect("poisoned").insert(key.to_string(), bytes);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        self.map
            .lock()
            .expect("poisoned")
            .get(key)
            .cloned()
            .ok_or_else(|| Error::NotFound(key.to_string()))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let needle = format!("{prefix}/");
        let map = self.map.lock().expect("poisoned");
        Ok(map
            .keys()
            .filter(|k| k.as_str() == prefix || k.starts_with(&needle))
            .cloned()
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.map.lock().expect("poisoned").remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.map.lock().expect("poisoned").contains_key(key))
    }
}
