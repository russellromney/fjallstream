# fjallstream — roadmap

Post-0.1-core work, deferred deliberately. The 0.1 core (capture → replicate → restore, local
backend) and its correctness fixes land first (see `PLAN.md`). These are the items we chose *not* to
solve in the local path, with the reason. Each tag (C#/P#) maps to a bugaboo from the design review.

## #1 — Move the capture seam into fjall (upstream checkpoint primitive)

The single most important next step, and an architectural one. Today `capture` infers a consistent
file set from *outside* fjall (flush → snapshot → directory walk → `current`/keyspace/journal
stability retries). That works and is test-backed, but it lives outside fjall's synchronization
boundary, so the journal guard can only be best-effort — never a real lock.

fjall v3 already takes cross-keyspace-consistent snapshots internally even under writes/compaction.
The right shape is an upstream **checkpoint / file-set API** — briefly lock the journal, fsync/copy
it, expose the immutable LSM files + meta + pointers for the consistent cut — which fjallstream
*consumes* instead of inferring from disk.

- Contribute or track upstream: `Database::checkpoint_manifest()` / `Database::with_checkpoint(|fs| …)`.
- Then `capture` becomes "ask fjall for the checkpoint file set, upload exactly that, write the version
  record." It's a contained swap: the seam is already isolated behind `LocalVersion` (replicator,
  restore, follower never touch fjall), so only `capture.rs` changes.
- Keep the filesystem-walk capture as a prototype / compatibility path, not the foundation.
- **Subsumes C4 (cross-keyspace consistent cut).** 0.1 is per-keyspace crash-consistent only;
  sequential flush + external file capture can tear a transaction spanning keyspaces. A true single
  cut is exactly what fjall's internal snapshot provides — so this is *resolved only when capture
  consumes the checkpoint API*, not as a separate piece of work.

Until this lands, treat fjallstream as a strong prototype / local-0.1 core, not production-grade
durable software. See DESIGN.md "The capture boundary".

## Done since the review

- **C5 — `ObjectStore::list` contract.** Resolved: `list` is now recursive (flat keys) on both Local
  and Mem, pinned by `tests/conformance.rs`.
- **C3 — seqno semantics.** Resolved enough: the recorded seqno is now an honest upper bound
  (`db.seqno()` read after the journal), so restore-at-or-before is conservative-but-correct. Verified
  by `tests/consistency.rs`. (A *tighter* seqno still wants a fjall flushed-high-water API — minor.)
- **`prune` + retention** — implemented (`tests/prune.rs`).
- **Local-copy hot follower** — implemented (`tests/follower_e2e.rs`).
- **Checksums on restore** — implemented (FNV-1a per file/journal, `ChecksumMismatch`).
- **MemObjectStore + conformance suite** — implemented.

## Land with S3 integration

These only matter once a high-latency, network backend exists. Build them in the S3 milestone.

- **`S3ObjectStore`** (feature `s3`) speaking S3/Tigris, + conformance run against real Tigris via the
  `FJALLSTREAM_E2E=tigris` gate.
- **P1 — Parallel uploads.** `replicate_once` uploads files one `await` at a time — fine on local
  disk, brutal on S3 (one RTT per file). `buffer_unordered(16–32)` the file puts; still write the
  version record last, after the join.
- **P2 — `exists` via HEAD, or drop the pre-put probe.** The default `exists` does a full GET; an S3
  impl must override with HEAD or every backup re-downloads everything. Better: skip the existence
  probe entirely and let `put` overwrite — immutable key ⇒ identical bytes ⇒ a re-PUT is harmless and
  saves a round trip.
- **P4 — Streaming / multipart put.** `replicate_once` slurps each whole file into RAM before
  uploading. Stream file → put, multipart for large SSTs on S3. (Also removes the OOM risk on small
  hosts for multi-GB tables.)

## Investigation / larger features

Not S3-specific, but bigger than the 0.1 core or blocked on a fjall API we don't have.

- **P3 — Skip flush when clean.** Capture force-flushes every interval, creating L0 SSTs even when
  nothing changed. Skip the `rotate_memtable_and_wait` when the active memtable is empty — needs a
  public dirty/size check. (0.1 makes flush configurable and documents interval guidance.)
- **P5 — Fast PITR lookup.** `resolve_version` for `Seqno`/`Time` targets downloads records
  newest-first until a match — O(n) GETs. Add an index or binary-search by the seqno embedded in the
  key for long histories. (Fine for `Latest`, which is just the last key.)

## Bigger bets (already noted in PLAN.md "Open decisions" / "Deferred")

- Hot follower hardening: VFS / lazy-block reads (Litestream v0.5 style); a refcount/grace story so a
  reader holding `database()` across two polls can't have its directory removed.
- Promotion / failover with generation fencing (split-brain protection).
- A `fjallstream` CLI for disaster recovery (`restore`, `generations`, `status`).
- `run()` resilience: it currently returns on the first error (caller restarts); add backoff/retry +
  an observability surface so transient store failures don't end the loop.
- Journal-tail streaming for sub-flush RPO (needs partial-journal replay-to-seqno).
- Journal shipping optimization: per-version journals are gzip-compressed (~64 MiB of zeros → a few
  hundred KB), but still shipped every version. Could content-hash to dedup unchanged journals, or
  trim to the used length before compressing.
- Version-change hook upstream in fjall/lsm-tree to replace the polling capture loop.
- Observability: replication lag / RPO status surface.
- Encryption at rest.
