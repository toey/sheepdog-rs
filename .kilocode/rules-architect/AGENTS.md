# Architect Mode Rules (sheepdog-rs)

## Architecture Overview

- 7 crates in workspace with strict dependency hierarchy: `sheepdog-proto` → `sheepdog-core` → `sheep`/`dog`/`shepherd`
- `sheepdog-proto` (no async) defines all wire protocol types, errors, ObjectId manipulation
- `sheepdog-core` (tokio async) implements consistent hashing, FEC (erasure coding), TCP transport
- `sheep` (main daemon) orchestrates: cluster membership, storage backends, NBD/NFS/HTTP/S3 servers
- `dog` (CLI) connects to sheep daemons via TCP for admin operations

## Dependency Constraints

- `sheepdog-proto` must remain async-free — no tokio imports allowed
- `sheepdog-core` depends on `sheepdog-proto` only — cannot import `sheep` or other application crates
- Feature flags in `sheep`: `http` (default), `nfs`, `dpdk` — conditional compilation with `#[cfg(feature = "...")]`
- `sheepfs` (FUSE) and `sheepdog-dpdk` (DPDK) are excluded from default-members — optional builds

## Storage Architecture

- Objects are 4MB chunks distributed via consistent hashing with vnodes
- Storage backends: `plain` (flat files), `tree` (directory tree), `md` (metadata database)
- Recovery and migration run as background workers in the sheep daemon
- Cluster membership supports: `sdcluster` (custom), `local` (single-node), `shepherd` (external monitor)
