//! Storage backend abstraction layer.
//!
//! The `StoreDriver` trait defines the interface that all storage backends
//! must implement. Each backend stores objects as files on disk, but
//! differs in directory layout and organization strategy.
//!
//! Available backends:
//! - **plain**: flat directory layout `{base}/obj/{oid_hex}`
//! - **tree**: hierarchical layout `{base}/obj/{vid_hex}/{oid_hex}`
//!
//! The `md` module manages multiple disks, distributing objects across
//! them based on available free space.

use async_trait::async_trait;
use sheepdog_proto::error::SdResult;
use sheepdog_proto::oid::ObjectId;

/// Trait that all storage backends must implement.
///
/// Every method that performs filesystem I/O should use `tokio::task::spawn_blocking`
/// internally, since disk I/O can block the async runtime.
#[async_trait]
pub trait StoreDriver: Send + Sync {
    /// Return the human-readable name of this backend (e.g. "plain", "tree").
    fn name(&self) -> &str;

    /// Initialize the store at `path`.
    ///
    /// If `first_time` is true, the store directory structure is created
    /// from scratch. Otherwise, the existing layout is validated.
    async fn init(&self, path: &std::path::Path, first_time: bool) -> SdResult<()>;

    /// Check whether an object exists in this store.
    async fn exist(&self, oid: ObjectId, ec_index: u8) -> bool;

    /// Create a new object and write its initial data.
    ///
    /// Returns `SdError::OidExist` if the object already exists.
    async fn create_and_write(&self, oid: ObjectId, ec_index: u8, data: &[u8]) -> SdResult<()>;

    /// Write `data` at `offset` into an existing object.
    async fn write(&self, oid: ObjectId, ec_index: u8, offset: u64, data: &[u8]) -> SdResult<()>;

    /// Read `length` bytes starting at `offset` from an object.
    async fn read(
        &self,
        oid: ObjectId,
        ec_index: u8,
        offset: u64,
        length: usize,
    ) -> SdResult<Vec<u8>>;

    /// Remove an object from the store.
    async fn remove(&self, oid: ObjectId, ec_index: u8) -> SdResult<()>;

    /// Return a list of all object IDs stored in this backend.
    async fn get_obj_list(&self) -> SdResult<Vec<ObjectId>>;

    /// Flush any pending writes to stable storage.
    async fn flush(&self) -> SdResult<()>;
}

/// Get a store driver by name.
pub fn get_driver(name: &str) -> Option<Box<dyn StoreDriver>> {
    match name {
        "plain" => Some(Box::new(plain::PlainStore::new())),
        "tree" => Some(Box::new(tree::TreeStore::new())),
        _ => None,
    }
}

/// List all available store driver names.
pub fn available_drivers() -> Vec<&'static str> {
    vec!["plain", "tree"]
}

pub mod common;
pub mod md;
pub mod plain;
pub mod tree;
