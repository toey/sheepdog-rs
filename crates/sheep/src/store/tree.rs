//! Hierarchical (tree) storage backend.
//!
//! Stores objects in a 2-level directory hierarchy:
//!   `{base}/obj/{vid:06x}/{oid:016x}`
//!
//! Objects are grouped by their VDI ID into subdirectories, which provides
//! better filesystem locality when accessing objects belonging to the same
//! VDI. This layout scales better than `plain` for nodes storing millions
//! of objects because each directory contains fewer entries.
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

/// Hierarchical directory storage backend.
///
/// Objects are stored as files under a VDI-based subdirectory tree:
/// `{base}/obj/{vid:06x}/{oid:016x}`.
pub struct TreeStore {
    /// Base directory for this store instance.
    /// Set during `init()`.
    base: RwLock<Option<PathBuf>>,
}

impl TreeStore {
    /// Create a new uninitialized TreeStore.
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

    /// Compute the full path for an object (with VDI subdirectory).
    fn obj_path(&self, oid: ObjectId, ec_index: u8) -> SdResult<PathBuf> {
        Ok(common::tree_obj_path(&self.base_path()?, oid, ec_index))
    }

    /// Get the VDI subdirectory path for an object.
    fn vid_dir(&self, oid: ObjectId) -> SdResult<PathBuf> {
        let vid = oid.to_vid();
        Ok(self.base_path()?.join("obj").join(format!("{:06x}", vid)))
    }

    /// Ensure the VDI subdirectory exists.
    fn ensure_vid_dir(path: &Path) -> SdResult<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                warn!("tree: failed to create vid dir {}: {}", parent.display(), e);
                SdError::Eio
            })?;
        }
        Ok(())
    }
}

impl Default for TreeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StoreDriver for TreeStore {
    fn name(&self) -> &str {
        "tree"
    }

    async fn init(&self, path: &Path, first_time: bool) -> SdResult<()> {
        let path = path.to_path_buf();
        let obj_dir = path.join("obj");

        if first_time {
            let obj_dir_clone = obj_dir.clone();
            tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&obj_dir_clone).map_err(|e| {
                    warn!("tree: failed to create obj dir {}: {}", obj_dir_clone.display(), e);
                    SdError::Eio
                })
            })
            .await
            .map_err(|_| SdError::SystemError)??;

            info!("tree: initialized store at {}", path.display());
        } else {
            // Validate existing layout
            let obj_dir_clone = obj_dir.clone();
            let exists = tokio::task::spawn_blocking(move || obj_dir_clone.is_dir())
                .await
                .map_err(|_| SdError::SystemError)?;

            if !exists {
                warn!("tree: obj dir missing at {}", obj_dir.display());
                return Err(SdError::NoStore);
            }

            debug!("tree: validated store at {}", path.display());
        }

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
            // Ensure VDI subdirectory exists
            TreeStore::ensure_vid_dir(&path)?;
            common::atomic_write(&path, &data)?;
            debug!("tree: created object {}", oid);
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
            debug!("tree: wrote {} bytes at offset {} to {}", data.len(), offset, oid);
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
            debug!("tree: read {} bytes at offset {} from {}", data.len(), offset, oid);
            Ok(data)
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn remove(&self, oid: ObjectId, ec_index: u8) -> SdResult<()> {
        let path = self.obj_path(oid, ec_index)?;
        let vid_dir = self.vid_dir(oid)?;

        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Err(SdError::NoObj);
            }
            std::fs::remove_file(&path).map_err(|e| {
                warn!("tree: failed to remove {}: {}", path.display(), e);
                SdError::Eio
            })?;
            debug!("tree: removed object {}", oid);

            // Try to remove the VDI directory if it's now empty.
            // Ignore errors — the directory may not be empty.
            if vid_dir.is_dir() {
                let _ = std::fs::remove_dir(&vid_dir);
            }

            Ok(())
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn get_obj_list(&self) -> SdResult<Vec<ObjectId>> {
        let obj_dir = self.obj_dir()?;

        tokio::task::spawn_blocking(move || {
            let oids = common::scan_tree_obj_dir(&obj_dir)?;
            debug!("tree: found {} objects", oids.len());
            Ok(oids)
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    async fn flush(&self) -> SdResult<()> {
        let base = self.base_path()?;

        tokio::task::spawn_blocking(move || {
            // Sync the top-level obj directory
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
    async fn test_tree_store_init() {
        let tmp = std::env::temp_dir().join("sheep_test_tree_init");
        let _ = std::fs::remove_dir_all(&tmp);

        let store = TreeStore::new();
        assert_eq!(store.name(), "tree");

        store.init(&tmp, true).await.unwrap();
        assert!(tmp.join("obj").is_dir());

        // Re-init should succeed
        store.init(&tmp, false).await.unwrap();

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_tree_store_crud() {
        let tmp = std::env::temp_dir().join("sheep_test_tree_crud");
        let _ = std::fs::remove_dir_all(&tmp);

        let store = TreeStore::new();
        store.init(&tmp, true).await.unwrap();

        // Use a VDI data object so to_vid() returns a non-zero VDI id
        let oid = ObjectId::from_vid_data(42, 1);
        let data = b"hello tree store";

        // Create
        assert!(!store.exist(oid, 0).await);
        store.create_and_write(oid, 0, data).await.unwrap();
        assert!(store.exist(oid, 0).await);

        // Verify subdirectory structure: obj/{vid:06x}/{oid:016x}
        let vid_dir = tmp.join("obj").join(format!("{:06x}", oid.to_vid()));
        assert!(vid_dir.is_dir());

        // Read
        let read_back = store.read(oid, 0, 0, data.len()).await.unwrap();
        assert_eq!(read_back, data);

        // Partial read
        let partial = store.read(oid, 0, 6, 10).await.unwrap();
        assert_eq!(partial, b"tree store");

        // Write at offset
        store.write(oid, 0, 6, b"TREE STORE").await.unwrap();
        let updated = store.read(oid, 0, 0, data.len()).await.unwrap();
        assert_eq!(updated, b"hello TREE STORE");

        // List
        let list = store.get_obj_list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], oid);

        // Remove (should also try to clean up empty VDI dir)
        store.remove(oid, 0).await.unwrap();
        assert!(!store.exist(oid, 0).await);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_tree_store_multiple_vdis() {
        let tmp = std::env::temp_dir().join("sheep_test_tree_multi_vdi");
        let _ = std::fs::remove_dir_all(&tmp);

        let store = TreeStore::new();
        store.init(&tmp, true).await.unwrap();

        // Create objects for different VDIs
        let oid1 = ObjectId::from_vid_data(10, 1);
        let oid2 = ObjectId::from_vid_data(20, 2);
        let oid3 = ObjectId::from_vid_data(10, 3);

        store.create_and_write(oid1, 0, b"vdi10-obj1").await.unwrap();
        store.create_and_write(oid2, 0, b"vdi20-obj2").await.unwrap();
        store.create_and_write(oid3, 0, b"vdi10-obj3").await.unwrap();

        // Should have two VDI directories
        let obj_dir = tmp.join("obj");
        let vid10 = obj_dir.join(format!("{:06x}", 10));
        let vid20 = obj_dir.join(format!("{:06x}", 20));
        assert!(vid10.is_dir());
        assert!(vid20.is_dir());

        let list = store.get_obj_list().await.unwrap();
        assert_eq!(list.len(), 3);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
