# fjallstream — build plan

How we build it, and how we prove it. See `DESIGN.md` for the model.

Guiding rule: **this is a data-durability tool. A test that doesn't run against real fjall and real
object storage doesn't count.** Mocks are for fast inner-loop logic only. Every claim about
"replicated" or "restored" is verified by opening a real fjall database and reading the bytes back.

## Milestones

Each milestone ends with green tests at the layer it introduces. No milestone is "done" until its
tests pass against real infrastructure where the test matrix says so.

### M0 — Scaffold (done)
- Crate, `DESIGN.md`, dual license.
- `ObjectStore` trait + `LocalObjectStore`.
- `VersionRecord` / `Cursor` / `Layout` types.
- `Replicator::replicate_once` (file dedup + version record), `restore::{resolve_version,
  restore_to}`.
- Tests: layout ordering; replicate→resolve→restore round-trip; immutable-upload-once (tamper test).

### M1 — Object store: conformance + real backends
- `MemObjectStore` (in-memory, for fast tests).
- `S3ObjectStore` (feature `s3`) speaking S3/Tigris via the AWS SDK; reads `AWS_*` env.
- A **shared conformance suite** every backend runs (Local, Mem, S3): put/get/list/delete/exists,
  not-found semantics, overwrite, atomic put (no torn reads), prefix listing, large object.
- Tests: conformance suite green on Local + Mem always; on S3/Tigris when creds present (see Test
  matrix). Pattern borrowed from hadb's conformance approach.

### M2 — The fjall adapter (the seam)
- `FjallSource`: open/wrap a `fjall::Database`, and capture a `LocalVersion`:
  hold a `Snapshot` (pins GC), enumerate the on-disk immutable files, read the current seqno
  watermark, and read the journal tail past the last flush.
- Pin down the exact fjall 3.1.x API for: directory layout + file naming, current seqno / instant,
  `Snapshot` lifetime, journal location.
- Tests (real fjall):
  - capture reflects what's on disk (write keys → capture → file set + seqno match);
  - **snapshot pins GC**: write, capture (hold snapshot), force flush+compaction, assert the
    captured files still exist on disk until the snapshot drops;
  - journal tail captures writes made after the last flush.

### M3 — Writer loop + retention, real round-trip
- `Replicator::run`: capture every `interval`, `replicate_once`, `prune`.
- `prune`: compute the live file set across the retained version window, delete only files outside
  it. Never delete a file a retained record references.
- Force re-base (`snapshot_every`) so old files leave the bucket.
- Tests (real fjall + Local/Mem store):
  - **full round-trip**: write N keys → run writer → `restore_to(Latest)` into a fresh dir → open
    fjall → assert every key present and equal;
  - **point-in-time**: write batch A (seqno sA), write batch B (seqno sB) → restore at sA → assert A
    present, B absent;
  - **prune correctness**: after re-base + prune, restoring a still-retained version succeeds and a
    pruned version returns `OutsideRetention`;
  - prune never deletes a referenced file (assert by restoring every retained version).

### M4 — Cold restore, complete
- Verify each downloaded file's 128-bit XXH3 checksum (fjall SFA) before it lands; mismatch →
  `ChecksumMismatch`.
- Replay the journal segment up to the target seqno so the restored db opens at the exact point.
- Tests: restore opens at exact seqno including un-flushed tail; a corrupted bucket file is caught by
  checksum, not surfaced as silent data loss.

### M5 — Hot follower (local-copy)
- `Follower::poll_once`: find newest version (`resolve_version(Latest)`), download only missing
  files, verify checksums, open a read-only `fjall::Database` at the new file set, **atomically swap**
  the handle readers use (in-flight reads finish on the old version — CoW safe).
- `OutsideRetention` → re-bootstrap from the latest snapshot record.
- Tests (real fjall, two databases):
  - **convergence**: primary writes a stream; follower converges to the same key set within a lag
    bound;
  - **read consistency**: follower reads are always a clean snapshot — never a torn mid-swap state;
  - **re-bootstrap**: stall the follower past the retention window, then assert it detects
    `OutsideRetention`, re-bootstraps from a snapshot, and converges again.

### M6 — Crash, durability, and property tests
- **Crash injection**:
  - kill the writer mid-upload → assert the bucket has no version record referencing a missing file
    (record-written-last invariant holds; partial files are unreferenced and harmless);
  - kill the follower mid-download → assert it resumes from the last adopted version, never serves a
    partial one.
- **Property test** (`proptest`): generate randomized op sequences (puts, deletes, flushes,
  compactions) on a primary with continuous replication; at random points, restore to a fresh db and
  assert it equals the primary's state at that seqno. This is the comprehensive correctness oracle.
- **Real Tigris e2e** (gated): the M3 round-trip + M5 follower convergence, run against real Tigris
  using soup creds.

### M7 — Ship
- Docs (rustdoc, README usage), examples (`examples/replicate.rs`, `examples/restore.rs`).
- CI: fmt, clippy (`-D warnings`), test on Local+Mem; a separate job runs the real-Tigris gated
  tests via `soup run`.
- Publish `0.1` to crates.io (dual-licensed).

### Deferred (post-0.1)
- **VFS / lazy-block follower**: read-only open against a remote block source + cache instead of a
  full local copy (Litestream v0.5's VFS read replica). Needs fjall to open against a pluggable block
  source — upstream work, or adopt SlateDB, which is object-storage-native.
- **Version-change hook upstream in fjall/lsm-tree**, to replace the polling capture loop.
- Multiple concurrent followers; cross-region fan-out.

## Test matrix

| Layer | Backend | When it runs |
|---|---|---|
| Unit (layout, serde, retention math) | none | always, every `cargo test` |
| Object-store conformance | Local, Mem | always |
| Object-store conformance | S3 / Tigris | when `FJALLSTREAM_E2E=tigris` (soup creds) |
| Replicator integration (synthetic versions) | Local, Mem | always |
| fjall adapter | real fjall | always (fjall is embedded, no external infra) |
| Full round-trip + PITR | real fjall + Local | always |
| Full round-trip + PITR | real fjall + Tigris | when `FJALLSTREAM_E2E=tigris` |
| Hot follower convergence | real fjall + Local | always |
| Hot follower convergence | real fjall + Tigris | when `FJALLSTREAM_E2E=tigris` |
| Crash / durability | real fjall + Local | always |
| Property (proptest) | real fjall + Mem | always (bounded cases); extended in CI nightly |

### The skip policy (important)

Per project convention: integration tests that need an external platform **fail loudly, never
silently skip**. Concretely:

- The local/in-memory variant of every e2e **always runs** and is the logical oracle. There is no
  configuration under which the core round-trip goes untested.
- The real-Tigris variant is **selected** by `FJALLSTREAM_E2E=tigris`. When that gate is set (CI, or
  `soup run` locally) and creds are missing or the bucket is unreachable, the test **hard-fails** with
  the full error — it does not skip. Absence of the gate means "not selected for this run," not
  "silently passed."
- Each test run uses a unique bucket prefix (`fjallstream-test/<uuid>`) and cleans up after itself, so
  the shared Tigris bucket never collides across runs or with turbolite's tests.

## Running the real-platform tests

```sh
# creds come from the fjallstream soup project (development env)
soup run -- env FJALLSTREAM_E2E=tigris cargo test --features s3 -- --include-ignored
```

`soup run` injects `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL`
(Tigris endpoint), and `FJALLSTREAM_TEST_BUCKET`.
