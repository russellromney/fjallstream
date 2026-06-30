# fjallstream — roadmap

Post-0.1-core work, deferred deliberately. The 0.1 core (capture → replicate → restore, local
backend) and its correctness fixes land first (see `PLAN.md`). These are the items we chose *not* to
solve in the local path, with the reason. Each tag (C#/P#) maps to a bugaboo from the design review.

## Land with S3 integration

These only matter once a high-latency, network backend exists, or are best solved as part of making
two backends agree. Build them in the S3 milestone, not before.

- **C5 — `ObjectStore::list` contract.** `LocalObjectStore::list` is currently shallow (one
  `read_dir`); S3 prefix-list is recursive. Pin the trait contract (recursive, flat keys), fix Local
  to match, and lock it with the shared conformance suite. Required before `prune` can enumerate
  `files/` correctly on both backends.
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

- **C3 — Precise durable seqno.** Today the recorded version seqno is an upper bound: writes in the
  flush→snapshot window aren't in the shipped SSTs. Record the exact flushed high-water instead.
  Likely needs a fjall API to read per-keyspace flushed seqno (internal today). Until then, 0.1
  documents the seqno as an upper bound and restore lands at-or-before it.
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

- Hot follower hardening: VFS / lazy-block reads (Litestream v0.5 style) vs. local-copy.
- Promotion / failover with generation fencing (split-brain protection).
- `prune` + retention-window GC of superseded files.
- A `fjallstream` CLI for disaster recovery (`restore`, `generations`, `status`).
- Journal-tail streaming for sub-flush RPO (needs partial-journal replay-to-seqno).
- Version-change hook upstream in fjall/lsm-tree to replace the polling capture loop.
- Observability: replication lag / RPO status surface.
- Encryption at rest.
