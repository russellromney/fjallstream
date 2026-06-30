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
- **Spike done** (`examples/spike_layout.rs`, fjall 3.1.5): learned the real on-disk layout, confirmed
  `Snapshot` pins GC, and found `rotate_memtable_and_wait()` for force-flush. See DESIGN.md "fjall
  on-disk layout". This resolves what was the project's biggest unknown (consistent capture).

### M1 — Object store: conformance + real backends
- `MemObjectStore` (in-memory, for fast tests).
- `S3ObjectStore` (feature `s3`) speaking S3/Tigris via the AWS SDK; reads `AWS_*` env.
- A **shared conformance suite** every backend runs (Local, Mem, S3): put/get/list/delete/exists,
  not-found semantics, overwrite, atomic put (no torn reads), prefix listing, large object.
- Tests: conformance suite green on Local + Mem always; on S3/Tigris when creds present (see Test
  matrix). Pattern borrowed from hadb's conformance approach.

### M2 — The fjall adapter (the seam) — DONE, proven end-to-end
Built and green: `capture()` (flush → snapshot → walk → consistency-verified), `FileId` is now a
relative path, `VersionRecord` carries inline `pointers`, restore recreates the `lock` file, and the
decisive M3 round-trip (write → capture → replicate → restore → **open → read every key back**) passes
against real fjall 3.1.5 — no journal shipped, meta keyspace intact. Two findings folded in: capture
retries if compaction moves `current` mid-walk; restore must recreate the `lock` file (fjall opens it
without creating). Still TODO at this layer: a *stress* test that captures under continuous concurrent
writes/compaction (the consistency guard exists but isn't yet hammered).

Original plan (for reference):
- `capture(db, db_path, keyspaces) -> LocalVersion` (see DESIGN.md "Capture strategy"):
  1. `rotate_memtable_and_wait()` on each keyspace (force committed data into SSTs);
  2. `db.snapshot()` (pin GC), record `db.visible_seqno()`;
  3. walk the db dir: immutable files → `file_paths` (relative-path ids); `current` HEAD files →
     inline `pointers`; skip `*.jnl` and `lock`;
  4. drop the snapshot.
- Code changes this enables: `FileId` becomes a **relative path**; `VersionRecord` gains a
  `pointers: Vec<(String, Bytes)>` field; the writer stamps `ts_millis` at capture.
- Tests (real fjall, no external infra):
  - capture reflects on-disk state (write keys → capture → `file_paths` + seqno match);
  - **snapshot pins GC, stressed**: write, capture under a held snapshot, then force *real* flushes +
    compaction (the spike's churn didn't flush — this test must), assert captured files survive until
    the snapshot drops;
  - **whole-database**: a db with two keyspaces captures both keyspace subtrees + `version`.

### M3 — Writer loop + retention, real round-trip
- `Replicator::run`: capture every `interval`, `replicate_once`, `prune`.
- `prune`: compute the live file set across the retained version window, delete only files outside
  it. Never delete a file a retained record references.
- Force re-base (`snapshot_every`) so old files leave the bucket.
- Tests (real fjall + Local/Mem store):
  - **the decisive round-trip**: write N keys → run writer → `restore_to(Latest)` into a fresh dir →
    **open it as a `Database` and read every key back, asserting equal.** This is the single most
    important test; it also settles the open question of whether fjall opens a restored dir with no
    journal and with the meta keyspace intact. If it doesn't, the fix surfaces here.
  - **point-in-time by time** (the user-facing target): write batch A at time tA, batch B at tB →
    restore at tA → assert A present, B absent. (Seqno targets are the internal mechanism; users
    think in wall-clock, so the headline PITR test is time-based.)
  - **prune correctness**: after re-base + prune, restoring a still-retained version succeeds and a
    pruned version returns `OutsideRetention`;
  - prune never deletes a referenced file (assert by restoring every retained version).

### M4 — Cold restore, hardened
- Verify each downloaded file's 128-bit XXH3 checksum (fjall SFA) before it lands; mismatch →
  `ChecksumMismatch` (never silent data loss).
- Atomic restore: stage into a temp dir, fsync, then rename into place — a crashed restore leaves no
  half-built database.
- (No journal replay in 0.1 — force-flush at capture means every version is complete on its own;
  RPO = capture interval. Sub-flush RPO via journal streaming is deferred.)
- Tests: a corrupted bucket file is caught by checksum; an interrupted restore leaves the target
  either absent or complete, never partial.

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
- **Model-based test** (start small, `proptest`): randomized op sequences (puts, deletes, forced
  flushes) on a primary with continuous replication; at random points, restore to a fresh db and
  assert it equals the primary's state at that point. Note: fjall compaction can't be driven
  deterministically, so this is a bounded oracle, not exhaustive — the full property suite is
  post-0.1. Don't let it pretend to cover compaction it didn't trigger.
- **Real Tigris e2e** (gated): the M3 round-trip + M5 follower convergence, run against real Tigris
  using soup creds.

### M7 — Ship
- Docs (rustdoc, README usage), examples (`examples/replicate.rs`, `examples/restore.rs`).
- CI: fmt, clippy (`-D warnings`), test on Local+Mem; a separate job runs the real-Tigris gated
  tests via `soup run`.
- Publish `0.1` to crates.io (dual-licensed).

### Deferred (post-0.1)
- **Journal-tail streaming** for sub-flush RPO. Needs partial-journal replay-to-seqno, which fjall's
  public API doesn't expose. 0.1 ships flush-granularity RPO instead.
- **VFS / lazy-block follower**: read-only open against a remote block source + cache instead of a
  full local copy (Litestream v0.5's VFS read replica). Needs fjall to open against a pluggable block
  source — upstream work, or adopt SlateDB, which is object-storage-native.
- **Version-change hook upstream in fjall/lsm-tree**, to replace the polling capture loop.
- Multiple concurrent followers; cross-region fan-out.

## Open decisions (resolve before the milestone that needs them)

These came out of the design review; each changes scope, so decide deliberately:

- **Promotion / failover (before M5).** Can a follower be promoted to writer (new generation, fenced
  against the old one), or is recovery always restore-from-bucket? Decides whether "hot follower"
  means failover or read-only standby.
- **CLI (before M7).** Litestream's DR ergonomics come from a CLI (`litestream restore`). Ship a thin
  `fjallstream restore` / `generations` / `status` binary, or library-only? Leaning yes — DR happens
  under stress, not by writing Rust.
- **Observability (build into M3).** A durability tool must answer "how far behind is my backup right
  now?" Expose a `ReplicationStatus` (last successful version, lag seconds, pending bytes). Without it
  a stalled backup is invisible until you need it.
- **Split-brain fencing (before any promotion).** A fence/epoch per generation so a zombie old writer
  can't interleave the version log. Out of scope only if promotion is.
- **Encryption at rest** — Litestream supports age. Likely post-0.1; stated here so it's a choice, not
  an omission.

## User-journey e2e (the acceptance spine)

The milestone tests above are mostly *mechanism* checks. These are the **user-perspective** tests —
written as workflows a real operator runs, asserting the thing the user actually cares about. 0.1 is
not done until A, B, C, and E pass on Local and on Tigris.

- **A — Disaster recovery with measured RPO.** App writes continuously; `kill -9` the whole process
  mid-write; restore from the bucket into a clean dir; assert recovered state equals committed state
  minus at most one capture interval. (The number, not just "no dangling record.")
- **B — Point-in-time to a wall-clock time.** Write over time; restore to "T"; assert the state as of
  T. Exercises the real user-facing target (time), not just internal seqno.
- **C — Read replica serving an app.** A follower answers real reads, converges within a lag bound,
  and never returns a torn read across a version swap.
- **D — Object store goes down.** Tigris returns errors / times out for ~60s while the app keeps
  writing; assert the primary keeps serving (no block, no unbounded memory), replication backs off,
  then catches up with zero data loss. (Depends on the M3 backpressure policy.)
- **E — Restore on a clean machine.** Nothing local; the bucket is the only shared state. Proves the
  bucket is genuinely self-contained.
- **F — Failover/promotion.** Only if promotion is in scope (see Open decisions). Primary dies,
  follower promotes to a new generation, old data intact, app continues writing.

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
