# fjallstream — design

Async replication and point-in-time backup for [fjall](https://github.com/fjall-rs/fjall),
to object storage (S3, Tigris, local filesystem).

Litestream is the teacher, not the template. We take its principles and build natively for
fjall's structure.

## The bet

Object storage is the whole replication substrate. No second server to babysit, no consensus,
no quorum. The bucket is the channel. One writer, async, bounded lag. We deliberately give up
RPO=0; that scope cut is what makes the whole thing tractable.

## What we learned from Litestream

These principles survive into any object-storage replicator and we keep all of them:

1. Object storage is the substrate. Cheap, durable, operable.
2. Single writer, async, bounded lag. No consensus smuggled back in.
3. Ride the engine's existing ordering; don't invent change capture.
4. One base plus an ongoing tail. Re-base periodically to bound restore cost.
5. Restore and replicate are the same machinery, run forward. A hot follower is "restore that
   never stops."
6. Generations for lineage, so a restore-and-diverge can't corrupt the log.
7. Retention window, not infinite slots. A lagging reader gets cut off and re-bootstraps; it
   never wedges the writer.

## What we drop (SQLite accidents, not fjall problems)

Litestream's hardest machinery — shadow WAL, LTX page packaging, fighting checkpoint truncation,
page-level delta capture — exists for one reason: **SQLite mutates pages in place.** Most of
Litestream's complexity is fighting mutability to reconstruct a consistent state.

fjall already solved that internally:

- **Files are immutable.** Compaction writes new files, never overwrites. So we ship whole files,
  content-addressed, once. No page deltas, no torn reads, no packaging format.
- **The Version system is the consistency primitive, and it's materialized on disk.** A fjall
  `Version` is a point-in-time set of immutable tables, named by the `current` HEAD + `v<N>` manifest
  files. Retained copy-on-write until no snapshot references it. We don't reconstruct a snapshot — a
  Version *is* one, and it's already readable files we can ship.
- **A held snapshot pins GC.** The checkpoint-truncation race — Litestream's nastiest problem —
  becomes "hold a snapshot while uploading." Native primitive, no fight.

So we keep all seven principles and throw away the mechanics. **The unit of replication is the
Version, not the page or the WAL frame.**

## The model: replicate the Version DAG

Mirror fjall's version history to a content-addressed object store. The bucket holds:

- a **write-once file store** — the immutable files (SST tables, blob files, per-version manifest
  files), keyed by their path inside the database, deduped for free.
- an ordered **version log** — each record names the exact file set at that point, plus the bytes of
  the few *mutable* pointer files (see "fjall on-disk layout") captured inline.

Every operation is then the same thing from a different angle:

- **Writer:** before capturing, force a flush so all committed data lives in immutable files, not the
  journal. Hold a snapshot (pins GC), upload the not-yet-present files, write the version record with
  the pointer-file bytes inline.
- **Restore (cold):** pick a version record, pull its files, write the inline pointer files, open the
  database.
- **Follower (hot):** keep pulling the newest version record + new files, swap atomically (fjall's
  copy-on-write versions make in-flight reads safe). Same machinery, never stops.

### Why "base vs incremental" mostly dissolves

Every version is already an incremental delta: copy-on-write versions share files, so successive
version records overlap heavily. We force a fresh full snapshot only to prune dependency chains and
let old files GC out of the bucket — not because we need a consistency base.

## Cursor / position and RPO

The replication position is the **version sequence number** — a log position, **never** the per-key
seqno read out of a file (fjall 3 rewrites per-key seqnos to 0 during bottom-level compaction, so
file-embedded seqnos are not a stable cursor). The writer stamps wall-clock time on each version
record for human-facing point-in-time targets; time is never used for ordering.

We ship the journal per-version (gzip-compressed), but we do **not** continuously *stream* it. The
writer forces a flush at capture so committed data is in SSTs, then ships the journal — it carries
the **sequence-number watermark** that recovery needs. (We learned the hard way that skipping it
leaves the restored db at `visible_seqno = 0`: point `get()`s work but iterators, range scans, and
snapshots see nothing, and new writes collide on low seqnos. See "Two things the implementation
learned".) Every captured version is complete on its own, so **RPO = the capture interval**. A user
who needs tighter RPO shortens the interval. Continuous journal-tail streaming (sub-flush RPO) is
deferred — it needs a way to replay a partial journal up to a seqno that fjall's public API doesn't
expose.

## Correctness invariants

- **Download-then-swap.** A follower must have all of a version's files durably present and
  checksum-verified (fjall's SFA uses 128-bit XXH3) before adopting that version. Crash mid-download
  resumes from the last adopted version.
- **Hold a snapshot across upload.** Guarantees the files of the version being uploaded are not
  GC'd mid-flight. Verified in fjall 3.1.5: files captured under a held `Snapshot` stay on disk
  across subsequent flush + compaction.
- **Force a flush before capture.** `Keyspace::rotate_memtable_and_wait()` on each keyspace pushes
  committed data out of the journal into immutable SSTs, minimizing the journal's live content. We
  still ship the journal (gzip-compressed, ~64 MiB of mostly zeros → a few hundred KB) because it
  carries the seqno watermark recovery needs.
- **Mutable files ride inside the version record.** Only immutable files go in the content-addressed
  store — dedup assumes a path maps to one byte sequence forever. The per-keyspace `current` HEAD
  pointer is rewritten in place, so its bytes are captured inline in the version record, not deduped.
- **Retention is best-effort with a bounded window.** Keep files referenced by the last N version
  records / T minutes. A follower that falls outside the window re-bootstraps from a full snapshot.
  A follower must never pin the writer's GC indefinitely.
- **Generations.** A restore-and-diverge starts a new generation id so histories never cross.

## Consistency guarantees (0.1)

What a restore actually promises, and the limits we accept for now:

- **Per-keyspace crash-consistency, not a cross-keyspace transactional cut.** Capture flushes each
  keyspace then takes one snapshot; a transaction spanning keyspaces can land on different sides of
  the cut. Single-keyspace restores are crash-consistent. A true cross-keyspace cut is roadmap (C4).
- **The version seqno is an honest upper bound.** It's `db.seqno()` (the next sequence number) read
  *after* the journal is captured, so it exceeds every write the version contains. "Restore at or
  before S" therefore never returns content past S — it's conservative (it may decline a version
  whose content is ≤ S but whose recorded bound is > S), never wrong. Verified under concurrent
  writes by `tests/consistency.rs` (the restored cut is always a contiguous prefix).
- **Restore is atomic and won't clobber.** It stages into a sibling dir, fsyncs, and renames into
  place; it refuses a non-empty target. A crash leaves the target absent or complete, never torn.
- **RPO = capture interval; don't capture sub-second.** Each capture force-flushes a memtable
  (`rotate_memtable_and_wait`), creating an L0 SST. Capturing too often hammers the source LSM with
  compaction. Skipping the flush when nothing changed is roadmap (P3).

## fjall on-disk layout (3.x, verified)

Inspected against fjall 3.1.5 (`examples/spike_layout.rs`). A database is a **directory tree**, not a
flat set of files:

```
<db>/
  version                       # 4-byte format marker (immutable)
  lock                          # exclusive process lock (never replicated)
  0.jnl                         # journal: ONE preallocated ~64 MiB file (mutable; shipped per-version, gzipped)
  keyspaces/
    <id>/                       # one dir per keyspace; id 1 is the always-present meta keyspace
      current                   # ~25 B HEAD pointer to the active version manifest (MUTABLE)
      v0, v1, v2, ...           # version-manifest files — fjall's Version DAG (immutable, append-only)
      tables/<table-id>         # SST table files, named by id (immutable, id never reused)
```

This is the consistency boundary: a fjallstream "version" is the whole `keyspaces/` tree + the
top-level `version` file at one point in time. **All keyspaces replicate together** (a restore is
consistent across every keyspace in the database).

### File taxonomy (what goes where)

| File | Mutable? | How we replicate it |
|---|---|---|
| `keyspaces/<id>/tables/<table-id>` (SSTs, blobs) | no | content-addressed file store, dedup |
| `keyspaces/<id>/v<N>` (version manifests) | no | content-addressed file store, dedup |
| `version` (format marker) | no | content-addressed file store |
| `keyspaces/<id>/current` (HEAD pointer) | **yes** | bytes captured inline in the version record |
| `0.jnl` (journal) | yes | shipped per-version, gzip-compressed (carries the seqno watermark) |
| `lock` | — | never replicated |

## Object (bucket) layout

```
bucket/<db>/generations/<gen>/
    files/<relpath>            # immutable files keyed by their path in the db, uploaded once
    journals/<seqno>/<name>    # per-version journal files (gzip), e.g. journals/<seqno>/0.jnl
    versions/<seqno>.json      # the version record (below)
```

A version record is `{ seqno, parent, file_ids[], file_checksums[], dirs[], pointers[], journals[],
journal_checksums[], ts_millis }`. `pointers[]` carries `(relpath, bytes)` for the mutable HEAD files;
`journals[]` lists the per-version journal names (keyed per-version, not content-addressed, because
they're mutable). `file_checksums[]`/`journal_checksums[]` are FNV-1a hashes verified on restore.
Every version record is self-contained (a full file set), so there is no separate "snapshot" / re-base
concept — `prune` just keeps the newest N records and deletes files no retained record references.

## Capture strategy (M2), verified feasible

Buildable today against fjall's public API (no upstream changes):

1. **Force flush** — `rotate_memtable_and_wait()` on each keyspace so committed data is in SSTs.
2. **Pin** — `db.snapshot()`; record `db.visible_seqno()` as the version seqno.
3. **Walk** — collect every file under the db dir; immutable ones become `file_ids` (relpath),
   `current` files become inline `pointers`, `*.jnl` become per-version `journals`, skip `lock`.
4. **Drop snapshot**, then `replicate_once`: upload not-yet-present files + journals, write the record.

Four things the implementation learned by running it (fjall 3.1.5), all now handled:

- **Capture must be consistent against background compaction.** A held snapshot pins SST *data*, but
  compaction keeps rewriting the on-disk `v<N>` manifests and the `current` HEAD while we walk. We
  capture, then re-read every `current`; if none moved, the manifest was stable the whole walk, so
  each `current` points at a fully-written `v<N>` we captured. If one moved, we retry. (lsm-tree
  writes a new `v<N>` fully before flipping `current`, which is what makes the check sufficient.)
- **The `lock` file must exist to recover.** fjall's recovery opens `lock` without creating it, so a
  restored dir needs one. We don't replicate it (it's a 0-byte runtime artifact); restore recreates
  an empty one.
- **The journal carries the seqno watermark — you can't skip it.** We originally dropped the journal
  (force-flush put all data in SSTs). A property-test oracle caught the bug: recovery only restores
  the sequence counter during journal replay, so a journal-less restore comes up at `visible_seqno =
  0`. Point `get()`s (which read at `SeqNo::MAX`) work, but iterators/range/snapshots (which read at
  `visible_seqno`) see nothing, and new writes collide on low seqnos. Fix: ship the journal per-version,
  gzip-compressed.
- **The journal must be captured consistently, or restore fails with EINVAL.** The same oracle, run
  repeatedly, flaked ~37%: background journal maintenance truncates/rotates a journal between when the
  walk records its path and when we read its bytes, shipping a torn journal that recovery rejects. The
  snapshot pins SSTs, not journals. Fix: read journal bytes *inside* the consistency window, bracketed
  by length checks, and confirm the `*.jnl` set is unchanged; retry if anything moved. Ship those exact
  bytes (no late read).

**Restore** lays the `files/` tree back down, writes the inline pointer files, the per-version
journals, recreates an empty `lock`, and opens a `Database`. **Proven**: a real db is captured,
replicated, restored into a clean dir, opened, and every key reads back equal — via both point gets
*and* iteration (`len()`), with the meta keyspace (id 0) intact.

### Deferred (needs upstream fjall work or a bigger lift)

- Journal-tail streaming for sub-flush RPO (needs partial-journal replay-to-seqno; not in public API).
- A version-change hook to replace the polling capture loop.
- VFS / lazy-block follower: open read-only against a remote block source + cache instead of a full
  local copy (Litestream v0.5's VFS read replica). Or adopt SlateDB, which is built for this.

## Module map

- `object_store` — the `ObjectStore` trait + a `LocalObjectStore` for tests/dev. Only thing that
  changes per backend.
- `types` — `Generation`, `VersionRecord` (carries `file_paths` + inline `pointers`), `FileId` (a
  relative path inside the db), `Cursor`.
- `layout` — bucket key construction.
- `capture` — the fjall seam: flush + snapshot + walk → `LocalVersion`. Verified against fjall 3.1.5.
- `replicator` — the writer loop.
- `restore` — cold reader.
- `follower` — hot reader.
