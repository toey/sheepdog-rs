# Project Coding Rules (Non-Obvious Only)

## Workspace Dependencies

- All crates share workspace-level dependencies defined in root [`Cargo.toml`](Cargo.toml). Use workspace inheritance (`workspace = true`) for shared deps: `tokio`, `serde`, `bincode`, `thiserror`, `tracing`, `tracing-subscriber`, `async-trait`, `clap`, `bytes`, `dashmap`, `sha1`, `crossbeam-channel`.
- `sheepdog-proto` is the lowest-level crate with no async — it defines `SdError`/`SdResult<T>`, `ObjectId`, `SdRequest`/`SdResponse`, and all protocol types. All other crates depend on it.
- `sheepdog-core` depends on `sheepdog-proto` and provides async networking (tokio), consistent hashing, FEC, and TCP transport.

## Error Handling

- Use `thiserror` for library error enums. The [`SdError`](crates/sheepdog-proto/src/error.rs:9) enum in `sheepdog-proto` is the canonical error type — derive `Serialize`/`Deserialize` for wire protocol compatibility.
- Return `SdResult<T>` (alias for `Result<T, SdError>`) from library functions.

## Imports & Module Organization

- Import order: `std` first, then external crates, then `crate`/workspace deps.
- Use `use` statements for types, not `self.` prefixes in paths.
- Conditional compilation: `#[cfg(feature = "http")]`, `#[cfg(feature = "dpdk")]`, `#[cfg(feature = "nfs")]`.

## Test Patterns

- Tests are always inline at the bottom of source files: `#[cfg(test)] mod tests { ... }`.
- Test module name: `tests` (not `test`). So test paths are `crate::module::tests::test_name`.
- Use plain `assert!` macros for tests; snapshot testing crates like `insta` are not currently used.
- For async tests, use `#[tokio::test]` with `single_threaded` for simple tests or `#[tokio::test(flavor = "current_thread")]`.
