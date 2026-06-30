//! Shared test support: a fault-injecting object store (for crash tests) and a tiny deterministic
//! PRNG (for the property test, so runs are reproducible without a `rand` dependency).

#![allow(dead_code)]

use async_trait::async_trait;
use bytes::Bytes;
use fjallstream::{Error, ObjectStore, Result};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Wraps an object store and injects a failure on the Nth `put` or `get`, to simulate a crash mid
/// upload or mid restore. Counts are 0-based: `fail_put_after(3)` fails starting on the 4th put.
pub struct FaultyStore<S: ObjectStore> {
    inner: S,
    fail_put_after: Option<usize>,
    fail_get_after: Option<usize>,
    puts: AtomicUsize,
    gets: AtomicUsize,
}

impl<S: ObjectStore> FaultyStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            fail_put_after: None,
            fail_get_after: None,
            puts: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        }
    }
    pub fn fail_put_after(mut self, n: usize) -> Self {
        self.fail_put_after = Some(n);
        self
    }
    pub fn fail_get_after(mut self, n: usize) -> Self {
        self.fail_get_after = Some(n);
        self
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for FaultyStore<S> {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        let n = self.puts.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fail_put_after, Some(limit) if n >= limit) {
            return Err(Error::Store(format!("injected put failure (call {}, key {key})", n + 1)));
        }
        self.inner.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> Result<Bytes> {
        let n = self.gets.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fail_get_after, Some(limit) if n >= limit) {
            return Err(Error::Store(format!("injected get failure (call {}, key {key})", n + 1)));
        }
        self.inner.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }
    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }
}

/// Deterministic xorshift64* PRNG. Same seed ⇒ same sequence, so property-test failures reproduce.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    /// Uniform-ish in `[0, n)`.
    pub fn below(&mut self, n: u32) -> u32 {
        (self.next_u64() % u64::from(n)) as u32
    }
}
