# Debug Mode Rules (sheepdog-rs)

## Debugging Distributed Storage

- E2E test scripts (test-io.sh, test-recovery.sh, test-ec.sh) use real sheep daemons and qemu-io — run from repo root after `cargo build`
- `scripts/cluster.sh start --nbd --format` — start 3-node cluster for debugging
- Logs go to `$DATA_ROOT/logs/node{N}.log` where DATA_ROOT defaults to `/tmp/sheepdog-*` (see scripts/defaults.sh)
- All async tests use `#[tokio::test(flavor = "current_thread")]` — for multi-threaded, add `flavor = "multi_thread"`
- No separate test directories — tests are inline at bottom of source files in `mod tests`

## Common Gotchas

- `sheepdog-proto` has NO async — if you see `tokio` imports in proto crate, that's wrong
- Tests that depend on the store layer need a real data directory (use `tempfile::tempdir()`)
- The `sheep` binary has feature-gated modules (http, nfs, dpdk) — missing features cause compile errors
