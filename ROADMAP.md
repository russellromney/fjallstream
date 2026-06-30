# fjallstream — roadmap

Post-0.1-core work, deferred deliberately. The 0.1 core (capture → replicate → restore, local
backend) and its correctness fixes land first (see `PLAN.md`). These are the items we chose *not* to
solve in the local path, with the reason. Each tag (C#/P#) maps to a bugaboo from the design review.

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

- **C4 — Cross-keyspace consistent cut.** 0.1 guarantees per-keyspace crash-consistency only;
  sequential per-keyspace flush + file capture can tear a cross-keyspace transaction. A true single
  cut needs either atomic multi-keyspace flush or capturing through the snapshot's view (no public
  file-set-for-snapshot API). Revisit when tx-spanning backups matter.
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
