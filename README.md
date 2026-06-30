# fjallstream

Async replication and point-in-time backup for [fjall](https://github.com/fjall-rs/fjall) — an
embeddable LSM key-value store in Rust — to object storage (S3, Tigris, local filesystem).

Litestream is the teacher, not the template. fjall's files are immutable and it has a copy-on-write
`Version` system, so we drop everything Litestream built to fight SQLite's mutable pages. **The unit
of replication is the Version, not the page or the WAL frame.** We mirror fjall's version history to
a content-addressed object store; restore picks a version and pulls its files; a hot follower is
"restore that never stops."

See [`DESIGN.md`](./DESIGN.md) for the full model and [`PLAN.md`](./PLAN.md) for the build and test
plan.

## Status

Early. The backend-agnostic core (object store, version records, replicate/restore round-trip) is
built and tested. The fjall adapter and hot follower are in progress. Not yet published.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the
work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
