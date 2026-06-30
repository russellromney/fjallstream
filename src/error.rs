use std::path::PathBuf;

/// Errors fjallstream can return.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store: {0}")]
    Store(String),

    #[error("object not found: {0}")]
    NotFound(String),

    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("serialize/deserialize: {0}")]
    Codec(#[from] serde_json::Error),

    #[error("fjall: {0}")]
    Fjall(String),

    /// The requested cursor has aged out of the retention window; the reader must
    /// re-bootstrap from a full snapshot.
    #[error("cursor {0} is outside the retention window; re-bootstrap required")]
    OutsideRetention(u64),

    #[error("checksum mismatch for file {0}")]
    ChecksumMismatch(String),
}

pub type Result<T> = std::result::Result<T, Error>;
