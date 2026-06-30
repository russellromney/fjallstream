# fjallstream

Backup and replication for [fjall](https://github.com/fjall-rs/fjall) databases, using object
storage (S3, Tigris, or a local folder).

## What it does

fjall is a key-value database that stores data in files on disk. fjallstream copies those files to
object storage so you can:

- **Back up** a live database without stopping it.
- **Restore** it later — to the latest state, or to an earlier point in time.
- **Run a read-only copy** (a follower) on another machine that keeps itself up to date.

It is async: writes don't wait for the upload. So a backup can be a few seconds behind. You trade a
little freshness for a lot of simplicity — no extra servers, no coordination, just a bucket.

## How it works

fjall's data files never change once written. When fjall flushes data, it writes new files and
leaves the old ones alone. fjallstream uses that:

1. Every so often, it tells fjall to flush, then takes a snapshot so the files can't be deleted yet.
2. It uploads any files the bucket doesn't already have. Because the files never change, each one is
   uploaded once.
3. It writes a small record listing exactly which files make up the database at that moment.

To **restore**, it reads one of those records, downloads the files it lists, and opens the database.
A **follower** does the same thing on a loop: pull the newest record, download what's new, swap to it.

That's the whole idea. See [DESIGN.md](./DESIGN.md) for the details and [PLAN.md](./PLAN.md) for the
build and test plan.

## Status

Early, and not yet usable. The core (uploading files, writing records, restoring) works and is
tested; the part that talks to a live fjall database is being built next. Not published yet.

## License

Dual licensed under either of [Apache 2.0](./LICENSE-APACHE) or [MIT](./LICENSE-MIT), at your option.
