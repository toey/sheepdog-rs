//! Plain (flat) storage backend.
//!
//! Stores all objects in a single directory:
//!   `{base}/obj/{oid:016x}`
//!
//! This is the simplest backend and works well for small to medium deployments.
//! For nodes storing millions of objects, the `tree` backend is preferred
//! since it avoids large single-directory listings.
//!
//! All filesystem I/O is performed inside `tokio::task::spawn_blocking` to
//! avoid blocking the async runtime.

use std::path::{Path, PathBuf};
use std::sync::RwLock;

use async_trait::async_trait;
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use tracing::{debug, info, warn};

use super::common;
use super::StoreDriver;

/// Flat-directory storage backend.
///
/// Objects are stored as individual files named by their hex OID in a
/// single `obj/` directory under the base path.
pub struct PlainStore {
    /// Base directory for this store instance.
    /// Set during `init()`.
    base: RwLock<Option<PathBuf>>,
}

impl PlainStore {
    /// Create a new uninitialized PlainStore.
    pub fn new() -> Self {
        Self {
            base: RwLock::new(None),
        }
    }

    /// Get the base path, returning an error if not initialized.
    fn base_path(&self) -> SdResult<PathBuf> {
        self.base
            .read()
            .unwrap()
            .clone()
            .ok_or(SdError::NoStore)
    }

    /// Get the obj/ directory path.
    fn obj_dir(&self) -> SdResult<PathBuf> {
        Ok(self.base_path()?.join("obj"))
    }

    /// Compute the full path for an object.
    fn obj_path(&self, oid: ObjectId, ec_index: u8) -> SdResult<PathBuf> {
        Ok(common::plain_obj_path(&self.base_path()?, oid, ec_index))
    }
}

impl Default for PlainStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StoreDriver for PlainStore {
    fn name(&self) -> &str {
        "plain"
    }

    async fn init(&self, path: &Path, first_time: bool) -> SdResult<()> {
        let path = path.to_path_buf();
        let obj_dir = path.join("obj");

        if first_time {
            let obj_dir_clone = obj_dir.clone();
            tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&obj_dir_clone).map_err(|e| {
                    warn!("plain: failed to create obj dir {}: {}", obj_dir_clone.display(), e);
                    SdError::Eio
                })
            })
            .await
            .map_err(|_| SdError::SystemError)??;

            info!("plain: initialized store at {}", path.display());
        } else {
            // Validate existing layout
            let obj_dir_clone = obj_dir.clone();
            let exists = tokio::task::spawn_blocking(move || obj_dir_clone.is_dir())
                .await
                .map_err(|_| SdError::SystemError)?;

            if !exists {
                warn!("plain: obj dir missing at {}", obj_dir.display());
                return Err(SdError::NoStore);
            }

            debug!("plain: validated store at {}", path.display());
        }

        // Store the base path
        *self.base.write().unwrap() = Some(path);
        Ok(())
    }

    async fn exist(&self, oid: ObjectId, ec_index: u8) -> bool {
        let path = match self.obj_path(oid, ec_index) {
            Ok(p) => p,
            Err(_) => return false,
        };

        tokio::task::spawn_blocking(move || path.exists())
            .await
            .unwrap_or(false)
    }

    async fn create_and_write(&self, oid: ObjectId, ec_index: u8, data: &[u8]) -> SdResult<()> {
        let path = self.obj_path(oid, ec_index)?;
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            if path.exists() {
                return Err(SdError::OidExist);
            }
            common::atomic_write(&path, &data)?;
            debug!("plain: created object {}", oid);
            Ok(())
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn write(&self, oid: ObjectId, ec_index: u8, offset: u64, data: &[u8]) -> SdResult<()> {
        let path = self.obj_path(oid, ec_index)?;
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Err(SdError::NoObj);
            }
            common::write_at(&path, offset, &data)?;
            debug!("plain: wrote {} bytes at offset {} to {}", data.len(), offset, oid);
            Ok(())
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn read(
        &self,
        oid: ObjectId,
        ec_index: u8,
        offset: u64,
        length: usize,
    ) -> SdResult<Vec<u8>> {
        let path = self.obj_path(oid, ec_index)?;

        tokio::task::spawn_blocking(move || {
            let data = common::read_at(&path, offset, length)?;
            debug!("plain: read {} bytes at offset {} from {}", data.len(), offset, oid);
            Ok(data)
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn remove(&self, oid: ObjectId, ec_index: u8) -> SdResult<()> {
        let path = self.obj_path(oid, ec_index)?;

        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Err(SdError::NoObj);
            }
            std::fs::remove_file(&path).map_err(|e| {
                warn!("plain: failed to remove {}: {}", path.display(), e);
                SdError::Eio
            })?;
            debug!("plain: removed object {}", oid);
            Ok(())
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn get_obj_list(&self) -> SdResult<Vec<ObjectId>> {
        let obj_dir = self.obj_dir()?;

        tokio::task::spawn_blocking(move || {
            let oids = common::scan_obj_dir(&obj_dir)?;
            debug!("plain: found {} objects", oids.len());
            Ok(oids)
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn flush(&self) -> SdResult<()> {
        // The plain store syncs on every write, so flush is a no-op.
        // We could optionally call syncfs() here for extra safety.
        let base = self.base_path()?;

        tokio::task::spawn_blocking(move || {
            // Open the obj directory and fsync it to ensure directory entries are persisted
            if let Ok(dir) = std::fs::File::open(base.join("obj")) {
                let _ = dir.sync_all();
            }
            Ok(())
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_plain_store_init() {
        let tmp = std::env::temp_dir().join("sheep_test_plain_init");
        let _ = std::fs::remove_dir_all(&tmp);

        let store = PlainStore::new();
        assert_eq!(store.name(), "plain");

        store.init(&tmp, true).await.unwrap();
        assert!(tmp.join("obj").is_dir());

        // Re-init with first_time=false should succeed
        store.init(&tmp, false).await.unwrap();

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_plain_store_crud() {
        let tmp = std::env::temp_dir().join("sheep_test_plain_crud");
        let _ = std::fs::remove_dir_all(&tmp);

        let store = PlainStore::new();
        store.init(&tmp, true).await.unwrap();

        let oid = ObjectId::new(0x0000_002a_0000_0001);
        let data = b"hello sheepdog";

        // Create
        assert!(!store.exist(oid, 0).await);
        store.create_and_write(oid, 0, data).await.unwrap();
        assert!(store.exist(oid, 0).await);

        // Read
        let read_back = store.read(oid, 0, 0, data.len()).await.unwrap();
        assert_eq!(read_back, data);

        // Partial read
        let partial = store.read(oid, 0, 6, 8).await.unwrap();
        assert_eq!(partial, b"sheepdog");

        // Write at offset
        store.write(oid, 0, 6, b"SHEEPDOG").await.unwrap();
        let updated = store.read(oid, 0, 0, data.len()).await.unwrap();
        assert_eq!(updated, b"hello SHEEPDOG");

        // List
        let list = store.get_obj_list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], oid);

        // Remove
        store.remove(oid, 0).await.unwrap();
        assert!(!store.exist(oid, 0).await);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_plain_store_duplicate_create() {
        let tmp = std::env::temp_dir().join("sheep_test_plain_dup");
        let _ = std::fs::remove_dir_all(&tmp);

        let store = PlainStore::new();
        store.init(&tmp, true).await.unwrap();

        let oid = ObjectId::new(0x0000_002a_0000_0002);
        store.create_and_write(oid, 0, b"first").await.unwrap();

        let err = store.create_and_write(oid, 0, b"second").await.unwrap_err();
        assert_eq!(err, SdError::OidExist);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_plain_store_ec_index() {
        let tmp = std::env::temp_dir().join("sheep_test_plain_ec");
        let _ = std::fs::remove_dir_all(&tmp);

        let store = PlainStore::new();
        store.init(&tmp, true).await.unwrap();

        let oid = ObjectId::new(0x0000_002a_0000_0003);

        // Write with different ec_index values
        store.create_and_write(oid, 0, b"shard0").await.unwrap();
        store.create_and_write(oid, 1, b"shard1").await.unwrap();
        store.create_and_write(oid, 2, b"shard2").await.unwrap();

        assert_eq!(store.read(oid, 0, 0, 6).await.unwrap(), b"shard0");
        assert_eq!(store.read(oid, 1, 0, 6).await.unwrap(), b"shard1");
        assert_eq!(store.read(oid, 2, 0, 6).await.unwrap(), b"shard2");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
