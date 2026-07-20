# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Build & Test

Rust is not installed locally — all Rust commands run via Docker wrapper (`./docker`).

- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo build` — builds default-members (proto, core, sheep, dog, shepherd)
- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo build --release` — release build for binaries
- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo build -p sheepdog-dpdk` — requires DPDK system libraries (optional, Linux-only)
- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo build -p sheepfs` — requires libfuse (excluded from default-members)
- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo test` — runs all unit tests across all workspace crates
- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo test -p sheep` — run tests in a single crate
- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo test -p sheep --lib -- object_cache::tests::test_cache_basic_ops` — run a single test (note: test modules use `tests` suffix in path, e.g. `object_cache::tests`)
- `./docker run --rm -v "$(pwd)":/workspace -w /workspace rust cargo clippy` — lint check (no custom clippy.toml; uses defaults)

E2E test scripts (require built binaries, qemu-io):
- `scripts/test-io.sh` — I/O correctness (63 tests, NBD path, no object cache)
- `scripts/test-recovery.sh` — recovery after node failures
- `scripts/test-ec.sh` — erasure-coded VDI tests
- `scripts/cluster.sh start --format` — start 3-node local cluster

## Architecture

7 crates in workspace (see [`Cargo.toml`](Cargo.toml)):
- `sheepdog-proto` — protocol types, wire format, ObjectId, errors (no async)
- `sheepdog-core` — consistent hashing, FEC, TCP transport (async via tokio)
- `sheep` — storage daemon (main binary): cluster, store, NBD, NFS, HTTP/S3
- `dog` — CLI admin tool (clap-based)
- `shepherd` — cluster monitor
- `sheepfs` — FUSE mount (optional, requires libfuse)
- `sheepdog-dpdk` — DPDK kernel-bypass I/O (optional, Linux-only)

## Code Style

- Edition 2021, no rustfmt.toml/clippy.toml — rely on cargo defaults
- Module-level docs: `//!` at top of `mod.rs` and source files
- Public items: doc comments with `///`
- Error handling: `thiserror` for library errors, `SdError`/`SdResult<T>` from proto crate
- Tests: inline `#[cfg(test)]` modules at bottom of source files (never in separate `tests/` dirs)
- Test module naming: `mod tests { ... }` — so test paths are `crate::module::tests::test_name`
- `#[inline]` on hot-path const getters (e.g. ObjectId methods)
- Use `tracing` for logging (not `println!`)
