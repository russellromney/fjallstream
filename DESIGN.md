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
- **The Version system is the consistency primitive.** A fjall `Version` is a point-in-time set
  of immutable tables + blob files, retained copy-on-write until no snapshot references it. We
  don't reconstruct a snapshot — a Version *is* one. We ship its file set.
- **A held snapshot pins GC.** The checkpoint-truncation race — Litestream's nastiest problem —
  becomes "hold a snapshot while uploading." Native primitive, no fight.

So we keep all seven principles and throw away the mechanics. **The unit of replication is the
Version, not the page or the WAL frame.**

## The model: replicate the Version DAG

Mirror fjall's version history to a content-addressed object store. The bucket holds two things:

- a **write-once file store** — immutable SSTs + blob files, keyed by file id, dedup for free.
- an ordered **version log** — each record is `{ seqno_watermark, file_ids[], parent }`, the exact
  file set at that point — plus **journal segments** for the unflushed tail.

Every operation is then the same thing from a different angle:

- **Writer:** mirror local version history forward. On each new version: upload its not-yet-present
  files, append the version record, ship the journal tail. Hold a snapshot across the upload so GC
  can't race the delete.
- **Restore (cold):** pick a version record, pull its `file_ids`, replay journal to the target seqno.
- **Follower (hot):** keep pulling the newest version record + new files, swap atomically (fjall's
  copy-on-write versions make in-flight reads safe). Same machinery, never stops.

### Why "base vs incremental" mostly dissolves

Every version is already an incremental delta: copy-on-write versions share files, so successive
version records overlap heavily in `file_ids`. We force a fresh full snapshot only to prune
dependency chains and let old files GC out of the bucket — not because we need a consistency base.
The journal is the only true tail replay, and only for the gap between the last flush and now.

## Cursor / position

The replication position is `(version_seq, journal_offset)` — a log position. **Never** the
per-key seqno read out of a file: fjall 3 rewrites per-key seqnos to 0 during bottom-level
compaction, so file-embedded seqnos are not a stable cursor.

## Correctness invariants

- **Download-then-swap.** A follower must have all of a version's files durably present and
  checksum-verified (fjall's SFA uses 128-bit XXH3) before adopting that version. Crash mid-download
  resumes from the last adopted version.
- **Hold a snapshot across upload.** Guarantees the files of the version being uploaded are not
  GC'd mid-flight.
- **Retention is best-effort with a bounded window.** Keep files referenced by the last N version
  records / T minutes. A follower that falls outside the window re-bootstraps from a full snapshot.
  A follower must never pin the writer's GC indefinitely.
- **Generations.** A restore-and-diverge starts a new generation id so histories never cross.

## Object layout

```
bucket/<db>/generations/<gen>/
    files/<file-id>            # immutable SSTs + blobs, uploaded once, dedup across versions
    versions/<seqno>.json      # { seqno, parent, file_ids[], journal_ref, ts }
    snapshots/<seqno>.json     # a version record flagged as a full re-base point
    journal/<from>-<to>.seg    # journal tail per version
```

## Pragmatic v1 vs upstream-later

Buildable today against fjall's public API (no upstream changes):

- Writer loop: hold `Snapshot`, list the database directory (immutable files are safe to read),
  upload new files, write a version record, ship the journal.
- Cold restore.
- Local-copy hot follower: download a new version's files, open a read-only `Database` at the new
  file set, atomically swap the handle readers use.

Needs fjall upstream work (deferred):

- Enumerate a version's exact file set via API (v1 lists the directory instead).
- A version-change hook to replace polling.
- VFS / lazy-block follower: open read-only against a remote block source + cache instead of a full
  local copy (the Litestream v0.5 VFS-read-replica idea). Or adopt SlateDB, which is built for this.

## Module map

- `object_store` — the `ObjectStore` trait + a `LocalObjectStore` for tests/dev. Only thing that
  changes per backend.
- `types` — `Generation`, `VersionRecord`, `FileId`, `Cursor`.
- `layout` — bucket key construction.
- `replicator` — the writer loop.
- `restore` — cold reader.
- `follower` — hot reader.
