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

    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(String),
}

pub type Result<T> = std::result::Result<T, Error>;
