//! Multi-disk (md) management.
//!
//! Manages multiple physical disks attached to a single sheep node.
//! Objects are distributed across disks based on available free space
//! using weighted random selection.
//!
//! Features:
//! - Weighted disk selection by free space
//! - Hot-plug and unplug of disks at runtime
//! - Object lookup across all attached disks
//! - Disk space tracking and reporting

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::common;
use super::StoreDriver;

/// Information about a single physical disk.
#[derive(Debug, Clone)]
pub struct DiskEntry {
    /// Unique identifier for this disk.
    pub disk_id: u64,
    /// Mount point / base path of this disk.
    pub path: PathBuf,
    /// Total disk space in bytes.
    pub space_total: u64,
    /// Free disk space in bytes.
    pub space_free: u64,
    /// Whether this disk is currently active.
    pub active: bool,
}

impl DiskEntry {
    /// Create a new DiskEntry by probing the filesystem at `path`.
    pub fn new(disk_id: u64, path: PathBuf) -> SdResult<Self> {
        let space = common::get_disk_space(&path)?;
        Ok(Self {
            disk_id,
            path,
            space_total: space.total,
            space_free: space.free,
            active: true,
        })
    }

    /// Refresh the space information for this disk.
    pub fn refresh_space(&mut self) -> SdResult<()> {
        let space = common::get_disk_space(&self.path)?;
        self.space_total = space.total;
        self.space_free = space.free;
        Ok(())
    }

    /// Get the obj/ directory for this disk.
    pub fn obj_dir(&self) -> PathBuf {
        self.path.join("obj")
    }
}

/// Multi-disk manager.
///
/// Distributes objects across multiple physical disks, selecting
/// target disks based on available free space (weighted random).
pub struct MdManager {
    /// Registered disks, indexed by disk_id.
    disks: RwLock<HashMap<u64, DiskEntry>>,
    /// Next disk_id to assign.
    next_id: RwLock<u64>,
    /// Name of the store driver to use on each disk (e.g. "plain" or "tree").
    store_driver_name: String,
}

impl MdManager {
    /// Create a new MdManager.
    pub fn new(store_driver_name: &str) -> Self {
        Self {
            disks: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
            store_driver_name: store_driver_name.to_string(),
        }
    }

    /// Add a disk to the manager (hot-plug).
    ///
    /// Initializes the disk's directory structure and registers it.
    pub async fn add_disk(&self, path: PathBuf) -> SdResult<u64> {
        let obj_dir = path.join("obj");

        // Create obj directory if it doesn't exist
        let obj_dir_clone = obj_dir.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&obj_dir_clone).map_err(|e| {
                error!("md: failed to create obj dir {}: {}", obj_dir_clone.display(), e);
                SdError::Eio
            })
        })
        .await
        .map_err(|_| SdError::SystemError)??;

        let mut next_id = self.next_id.write().await;
        let disk_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let entry = {
            let p = path.clone();
            tokio::task::spawn_blocking(move || DiskEntry::new(disk_id, p))
                .await
                .map_err(|_| SdError::SystemError)?
        }?;

        info!(
            "md: added disk {} at {} (total={}, free={})",
            disk_id,
            path.display(),
            entry.space_total,
            entry.space_free,
        );

        self.disks.write().await.insert(disk_id, entry);
        Ok(disk_id)
    }

    /// Remove a disk from the manager (unplug).
    ///
    /// The disk is marked inactive but its objects remain accessible
    /// until they are migrated away.
    pub async fn remove_disk(&self, disk_id: u64) -> SdResult<()> {
        let mut disks = self.disks.write().await;
        if let Some(entry) = disks.get_mut(&disk_id) {
            info!("md: removing disk {} at {}", disk_id, entry.path.display());
            entry.active = false;
            Ok(())
        } else {
            warn!("md: disk {} not found", disk_id);
            Err(SdError::NotFound)
        }
    }

    /// Purge a disk entirely from the manager.
    ///
    /// Only works on inactive disks.
    pub async fn purge_disk(&self, disk_id: u64) -> SdResult<()> {
        let mut disks = self.disks.write().await;
        if let Some(entry) = disks.get(&disk_id) {
            if entry.active {
                warn!("md: cannot purge active disk {}", disk_id);
                return Err(SdError::InvalidParms);
            }
            info!("md: purged disk {} at {}", disk_id, entry.path.display());
            disks.remove(&disk_id);
            Ok(())
        } else {
            Err(SdError::NotFound)
        }
    }

    /// Select a disk for writing a new object, using weighted random
    /// selection based on free space.
    ///
    /// Disks with more free space are more likely to be selected.
    pub async fn select_disk(&self) -> SdResult<DiskEntry> {
        let disks = self.disks.read().await;
        let active: Vec<&DiskEntry> = disks.values().filter(|d| d.active).collect();

        if active.is_empty() {
            return Err(SdError::NoSpace);
        }

        // Calculate total free space across all active disks
        let total_free: u64 = active.iter().map(|d| d.space_free).sum();
        if total_free == 0 {
            return Err(SdError::NoSpace);
        }

        // Weighted random selection
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let threshold = rng.gen_range(0..total_free);

        let mut cumulative = 0u64;
        for disk in &active {
            cumulative += disk.space_free;
            if cumulative > threshold {
                return Ok((*disk).clone());
            }
        }

        // Fallback to first active disk
        Ok(active[0].clone())
    }

    /// Find which disk contains a given object.
    ///
    /// Searches all disks (including inactive) for the object file.
    pub async fn find_obj_disk(
        &self,
        oid: ObjectId,
        ec_index: u8,
        use_tree: bool,
    ) -> SdResult<DiskEntry> {
        let disks = self.disks.read().await;

        for entry in disks.values() {
            let path = if use_tree {
                common::tree_obj_path(&entry.path, oid, ec_index)
            } else {
                common::plain_obj_path(&entry.path, oid, ec_index)
            };

            let found = {
                let p = path.clone();
                tokio::task::spawn_blocking(move || p.exists())
                    .await
                    .unwrap_or(false)
            };

            if found {
                return Ok(entry.clone());
            }
        }

        Err(SdError::NoObj)
    }

    /// Get the path to an object file, searching all disks.
    pub async fn get_obj_path(
        &self,
        oid: ObjectId,
        ec_index: u8,
        use_tree: bool,
    ) -> SdResult<PathBuf> {
        let disk = self.find_obj_disk(oid, ec_index, use_tree).await?;
        if use_tree {
            Ok(common::tree_obj_path(&disk.path, oid, ec_index))
        } else {
            Ok(common::plain_obj_path(&disk.path, oid, ec_index))
        }
    }

    /// Collect object IDs from all disks.
    pub async fn get_all_obj_list(&self, use_tree: bool) -> SdResult<Vec<ObjectId>> {
        let disks = self.disks.read().await;
        let mut all_oids = Vec::new();

        for entry in disks.values() {
            let obj_dir = entry.obj_dir();
            let oids = {
                let tree = use_tree;
                tokio::task::spawn_blocking(move || {
                    if tree {
                        common::scan_tree_obj_dir(&obj_dir)
                    } else {
                        common::scan_obj_dir(&obj_dir)
                    }
                })
                .await
                .map_err(|_| SdError::SystemError)?
            }?;
            all_oids.extend(oids);
        }

        debug!("md: found {} objects across {} disks", all_oids.len(), disks.len());
        Ok(all_oids)
    }

    /// Refresh disk space information for all disks.
    pub async fn refresh_all(&self) -> SdResult<()> {
        let mut disks = self.disks.write().await;
        for entry in disks.values_mut() {
            if entry.active {
                if let Err(e) = entry.refresh_space() {
                    warn!("md: failed to refresh space for disk {}: {}", entry.disk_id, e);
                }
            }
        }
        Ok(())
    }

    /// Get information about all registered disks.
    pub async fn get_disk_info(&self) -> Vec<DiskEntry> {
        let disks = self.disks.read().await;
        disks.values().cloned().collect()
    }

    /// Get the number of active disks.
    pub async fn nr_active_disks(&self) -> usize {
        let disks = self.disks.read().await;
        disks.values().filter(|d| d.active).count()
    }

    /// Get the total and free space across all active disks.
    pub async fn total_space(&self) -> (u64, u64) {
        let disks = self.disks.read().await;
        let total: u64 = disks.values().filter(|d| d.active).map(|d| d.space_total).sum();
        let free: u64 = disks.values().filter(|d| d.active).map(|d| d.space_free).sum();
        (total, free)
    }

    /// Check if an object exists on any disk.
    pub async fn obj_exists(
        &self,
        oid: ObjectId,
        ec_index: u8,
        use_tree: bool,
    ) -> bool {
        self.find_obj_disk(oid, ec_index, use_tree).await.is_ok()
    }

    /// Write an object to the best available disk.
    pub async fn write_obj(
        &self,
        oid: ObjectId,
        ec_index: u8,
        data: &[u8],
        use_tree: bool,
    ) -> SdResult<()> {
        let disk = self.select_disk().await?;
        let path = if use_tree {
            common::tree_obj_path(&disk.path, oid, ec_index)
        } else {
            common::plain_obj_path(&disk.path, oid, ec_index)
        };

        let data = data.to_vec();
        tokio::task::spawn_blocking(move || {
            if use_tree {
                // Ensure VDI subdirectory exists for tree layout
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        warn!("md: failed to create dir {}: {}", parent.display(), e);
                        SdError::Eio
                    })?;
                }
            }
            common::atomic_write(&path, &data)
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    /// Remove an object from whichever disk it resides on.
    pub async fn remove_obj(
        &self,
        oid: ObjectId,
        ec_index: u8,
        use_tree: bool,
    ) -> SdResult<()> {
        let path = self.get_obj_path(oid, ec_index, use_tree).await?;

        tokio::task::spawn_blocking(move || {
            std::fs::remove_file(&path).map_err(|e| {
                warn!("md: failed to remove {}: {}", path.display(), e);
                SdError::Eio
            })
        })
        .await
        .map_err(|_| SdError::SystemError)?
    }

    /// Read an object from whichever disk it resides on.
    pub async fn read_obj(
        &self,
        oid: ObjectId,
        ec_index: u8,
        offset: u64,
        length: usize,
        use_tree: bool,
    ) -> SdResult<Vec<u8>> {
        let path = self.get_obj_path(oid, ec_index, use_tree).await?;

        tokio::task::spawn_blocking(move || common::read_at(&path, offset, length))
            .await
            .map_err(|_| SdError::SystemError)?
    }
}

/// Convenience wrapper: multi-disk-aware StoreDriver.
///
/// Delegates all operations to the MdManager, which distributes
/// objects across multiple disks.
pub struct MdStore {
    manager: Arc<MdManager>,
    use_tree: bool,
}

impl MdStore {
    /// Create a new MdStore wrapping the given MdManager.
    pub fn new(manager: Arc<MdManager>, use_tree: bool) -> Self {
        Self { manager, use_tree }
    }

    /// Get a reference to the underlying MdManager.
    pub fn manager(&self) -> &Arc<MdManager> {
        &self.manager
    }
}

#[async_trait::async_trait]
impl StoreDriver for MdStore {
    fn name(&self) -> &str {
        if self.use_tree {
            "md-tree"
        } else {
            "md-plain"
        }
    }

    async fn init(&self, _path: &Path, _first_time: bool) -> SdResult<()> {
        // MdStore is initialized by adding disks to the MdManager.
        Ok(())
    }

    async fn exist(&self, oid: ObjectId, ec_index: u8) -> bool {
        self.manager.obj_exists(oid, ec_index, self.use_tree).await
    }

    async fn create_and_write(&self, oid: ObjectId, ec_index: u8, data: &[u8]) -> SdResult<()> {
        if self.manager.obj_exists(oid, ec_index, self.use_tree).await {
            return Err(SdError::OidExist);
        }
        self.manager.write_obj(oid, ec_index, data, self.use_tree).await
    }

    async fn write(&self, oid: ObjectId, ec_index: u8, offset: u64, data: &[u8]) -> SdResult<()> {
        let path = self
            .manager
            .get_obj_path(oid, ec_index, self.use_tree)
            .await?;
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || common::write_at(&path, offset, &data))
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
        self.manager
            .read_obj(oid, ec_index, offset, length, self.use_tree)
            .await
    }

    async fn remove(&self, oid: ObjectId, ec_index: u8) -> SdResult<()> {
        self.manager.remove_obj(oid, ec_index, self.use_tree).await
    }

    async fn get_obj_list(&self) -> SdResult<Vec<ObjectId>> {
        self.manager.get_all_obj_list(self.use_tree).await
    }

    async fn flush(&self) -> SdResult<()> {
        // Flush by syncing each disk's obj directory
        let disks = self.manager.get_disk_info().await;
        for disk in disks {
            if disk.active {
                let obj_dir = disk.obj_dir();
                tokio::task::spawn_blocking(move || {
                    if let Ok(dir) = std::fs::File::open(&obj_dir) {
                        let _ = dir.sync_all();
                    }
                })
                .await
                .map_err(|_| SdError::SystemError)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_md_manager_add_disk() {
        let tmp = std::env::temp_dir().join("sheep_test_md_add");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mgr = MdManager::new("plain");
        let disk_id = mgr.add_disk(tmp.clone()).await.unwrap();
        assert_eq!(disk_id, 1);

        let info = mgr.get_disk_info().await;
        assert_eq!(info.len(), 1);
        assert!(info[0].active);
        assert!(info[0].space_total > 0);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_md_manager_select_disk() {
        let tmp1 = std::env::temp_dir().join("sheep_test_md_sel1");
        let tmp2 = std::env::temp_dir().join("sheep_test_md_sel2");
        let _ = std::fs::remove_dir_all(&tmp1);
        let _ = std::fs::remove_dir_all(&tmp2);
        std::fs::create_dir_all(&tmp1).unwrap();
        std::fs::create_dir_all(&tmp2).unwrap();

        let mgr = MdManager::new("plain");
        mgr.add_disk(tmp1.clone()).await.unwrap();
        mgr.add_disk(tmp2.clone()).await.unwrap();

        // Should select one of the disks
        let disk = mgr.select_disk().await.unwrap();
        assert!(disk.active);

        let _ = std::fs::remove_dir_all(&tmp1);
        let _ = std::fs::remove_dir_all(&tmp2);
    }

    #[tokio::test]
    async fn test_md_manager_remove_disk() {
        let tmp = std::env::temp_dir().join("sheep_test_md_remove");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mgr = MdManager::new("plain");
        let disk_id = mgr.add_disk(tmp.clone()).await.unwrap();

        mgr.remove_disk(disk_id).await.unwrap();
        assert_eq!(mgr.nr_active_disks().await, 0);

        // Purge after deactivation
        mgr.purge_disk(disk_id).await.unwrap();
        assert_eq!(mgr.get_disk_info().await.len(), 0);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_md_store_write_read() {
        let tmp = std::env::temp_dir().join("sheep_test_md_store");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mgr = Arc::new(MdManager::new("plain"));
        mgr.add_disk(tmp.clone()).await.unwrap();

        let store = MdStore::new(mgr, false);
        let oid = ObjectId::new(0x0000_002a_0000_00ff);

        store.create_and_write(oid, 0, b"md-data").await.unwrap();
        assert!(store.exist(oid, 0).await);

        let data = store.read(oid, 0, 0, 7).await.unwrap();
        assert_eq!(data, b"md-data");

        store.remove(oid, 0).await.unwrap();
        assert!(!store.exist(oid, 0).await);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
