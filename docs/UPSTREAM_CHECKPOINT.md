# Proposal: a checkpoint / file-set API in fjall

## Why

[fjallstream](https://github.com/russellromney/fjallstream) is a Litestream-style backup + async
replication layer for fjall: it ships a fjall database to object storage (S3/Tigris/local) for
point-in-time restore and read-only followers.

To replicate a database it needs **a consistent, complete file set for a point in time** — the
immutable LSM files plus the mutable bits (each keyspace's `current` HEAD, the journal that carries
the seqno watermark, the directory structure). Today it derives this from *outside* fjall: flush each
keyspace, hold a `db.snapshot()`, walk the directory, and guard against background compaction and
journal maintenance by re-checking `current` stability, the keyspace-set count, and the set of
journal files (to catch rotation) before reading them.

This works and is test-backed, but it sits outside fjall's synchronization boundary. The journal is
the fragile point: it's preallocated to a fixed 64 MiB, so a length check tells you nothing, and it's
truncated/rotated by background maintenance. We can catch rotation (the journal-file set changes) but
not an in-place truncation racing a single read — and a "did the bytes change between two reads" check
is unusable, because under any write load the journal is constantly appended to, so it always differs
and capture never succeeds. The only clean answer is a lock. The upstream checkpoint design calls for
**locking the journal** for exactly this reason. fjall v3 already has the groundwork to take a
consistent snapshot across keyspaces under writes + compaction, so the consistency source exists
internally; consumers just can't ask for it as a file set.

## What would help

Any one of these, lowest-effort first:

1. **`Database::checkpoint_to(path)`**. Lock the journal, fsync + copy it,
   hard-link/copy the immutable tables + blobs + manifests, copy meta + version files, prevent meta
   mutation during the copy. fjallstream would checkpoint to a scratch dir, then upload that file set
   and clean up. Simple, correct, O(changed files) if it hard-links.

2. **`Database::with_checkpoint(|cp| { … })`** — a scoped handle exposing the consistent file set
   without copying: the relative paths of the immutable files, the journal bytes (or a locked reader),
   and the `current` pointer bytes, for the cut. Lets a consumer upload incrementally (skip files it
   already has) instead of copying the whole db each time. This is the ideal for an
   incremental-to-object-storage replicator.

3. **`Snapshot::file_set()` / a manifest accessor** — given a held cross-keyspace snapshot, return the
   exact immutable files it references + the seqno watermark. Smallest surface; fjallstream already
   holds the snapshot and would stop walking the directory.

## What fjallstream would do with it

Replace the directory walk in `capture.rs` with a call to the API: ask for the checkpoint file set +
journal + pointers, upload exactly those (content-addressing the immutable files so each uploads once),
and write its version record. Everything downstream — replicator, restore, follower — is unchanged;
the capture seam is already isolated behind an internal `LocalVersion` struct.

Happy to prototype the consumer side against a draft API.
