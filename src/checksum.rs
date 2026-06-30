//! A stable content hash for integrity. We store one per uploaded file/journal in the version record
//! and verify it on restore, so bucket corruption is caught loudly *before* fjall ever opens the db
//! (fjall's own SFA/XXH3 checks are a second line — they only fire when a corrupt block is read).
//!
//! FNV-1a (64-bit): tiny, dependency-free, and — unlike `std`'s `DefaultHasher` — stable across Rust
//! versions, which matters because these hashes are written to durable storage and read back later.
//! This guards against accidental corruption, not a malicious adversary.

pub fn hash64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_and_sensitive() {
        assert_eq!(hash64(b""), 0xcbf2_9ce4_8422_2325, "FNV-1a empty must be the offset basis");
        assert_eq!(hash64(b"hello"), hash64(b"hello"), "deterministic");
        assert_ne!(hash64(b"hello"), hash64(b"hellp"), "single-bit change must differ");
        assert_ne!(hash64(b"ab"), hash64(b"ba"), "order-sensitive");
    }
}
