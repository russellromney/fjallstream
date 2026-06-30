//! Gzip helpers for shipping journals. A fjall journal is a ~64 MiB preallocated file that is mostly
//! zeros after a force-flush, so it compresses to a few KB — without this, every captured version
//! would ship the full 64 MiB.

use crate::error::{Error, Result};
use std::io::{Read, Write};
use std::path::PathBuf;

fn io_err(what: &str, source: std::io::Error) -> Error {
    Error::Io { path: PathBuf::from(what), source }
}

pub fn gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(bytes).map_err(|e| io_err("<gzip>", e))?;
    enc.finish().map_err(|e| io_err("<gzip>", e))
}

pub fn gunzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut dec = flate2::read::GzDecoder::new(bytes);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).map_err(|e| io_err("<gunzip>", e))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_shrinks_zeros() {
        let zeros = vec![0u8; 64 * 1024 * 1024];
        let z = gzip(&zeros).unwrap();
        // ~64 MiB of zeros compresses to a few hundred KB at fast level — a 100x+ reduction.
        assert!(z.len() < 1_000_000, "64 MiB of zeros should compress small, got {}", z.len());
        assert_eq!(gunzip(&z).unwrap(), zeros);

        let data = b"the quick brown fox".repeat(100);
        assert_eq!(gunzip(&gzip(&data).unwrap()).unwrap(), data);
    }
}
