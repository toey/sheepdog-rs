# Ask Mode Rules (sheepdog-rs)

## Documentation Context

- `sheepdog-proto` — read first for protocol types, errors, wire format. All other crates depend on it.
- `sheepdog-core` — read for async networking, consistent hashing, FEC implementation.
- `sheep` — storage daemon source is the primary reference for understanding the daemon's behavior.
- `dog` — CLI source is the reference for admin commands (no separate docs).
- Original Sheepdog C code is in `.llm/knowledge/sheepdog/` — useful for understanding protocol semantics.
- E2E test scripts (`scripts/test-io.sh`, `scripts/test-recovery.sh`, `scripts/test-ec.sh`) document real-world usage patterns.

## Counterintuitive Organization

- `crates/sheep/src/` has both daemon logic AND submodules for NBD, NFS, HTTP/S3 — not separated into distinct crates.
- `crates/sheep/src/store/` contains multiple storage backend implementations (plain, tree, md).
- The `sheepdog-core` crate has no async itself — it delegates to tokio via `tokio::net` and `async-trait`.
