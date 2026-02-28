//! System-wide daemon state.
//!
//! In the C version this was a single global `struct system_info *sys`.
//! In Rust, we wrap it in `Arc<RwLock<>>` and pass explicitly.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bitvec::prelude::*;
use tokio::sync::{Notify, RwLock};

use sheepdog_proto::defaults::{
    DEFAULT_CACHE_SIZE_MB, DEFAULT_RECOVERY_MAX_EXEC_COUNT,
    DEFAULT_RECOVERY_QUEUE_WORK_INTERVAL_MS,
};
use sheepdog_proto::node::{ClusterInfo, ClusterStatus, NodeStatus, SdNode};
use sheepdog_proto::vdi::VdiState;
use sheepdog_core::transport::PeerTransport;

/// Shared daemon state, replaces the C global `sys`.
pub type SharedSys = Arc<RwLock<SystemInfo>>;

/// System configuration and runtime state.
pub struct SystemInfo {
    /// Cluster-wide metadata (epoch, copies, status, node list).
    pub cinfo: ClusterInfo,

    /// This node's identity.
    pub this_node: SdNode,

    /// Local node initialization status.
    pub node_status: NodeStatus,

    /// VDI usage bitmap — 1 bit per VDI id (up to SD_NR_VDIS = 16M).
    pub vdi_inuse: BitVec<u8, Msb0>,

    /// Per-VDI state (lock state, copies, snapshot flag).
    pub vdi_state: BTreeMap<u32, VdiState>,

    /// Base directory for persistent data.
    pub dir: PathBuf,

    /// Listen address for client connections.
    pub listen_addr: SocketAddr,

    /// Whether recovery is globally disabled.
    pub disable_recovery: bool,

    /// Whether the node is in gateway-only mode (no local storage).
    pub gateway_mode: bool,

    /// Current recovery epoch (0 if idle).
    pub recovery_epoch: u32,

    /// Maximum number of transport connections.
    pub nr_sobjs: u32,

    /// Journal directory (optional).
    pub journal_dir: Option<PathBuf>,

    /// Journal size in bytes.
    pub journal_size: u64,

    /// Enable object cache.
    pub object_cache_enabled: bool,

    /// Object cache size in MB.
    pub object_cache_size: u64,

    /// Enable direct I/O.
    pub use_directio: bool,

    /// Notify channel for epoch changes.
    pub epoch_notify: Arc<Notify>,

    /// Notify channel for shutdown.
    pub shutdown_notify: Arc<Notify>,

    /// Object cache instance (None if caching disabled).
    pub object_cache: Option<Arc<crate::object_cache::ObjectCache>>,

    /// Peer transport (TCP or DPDK) for data-plane I/O.
    pub peer_transport: Arc<dyn PeerTransport>,

    /// Multi-disk paths.
    pub md_disks: Vec<PathBuf>,

    /// Deleted VDI bitmap.
    pub vdi_deleted: BitVec<u8, Msb0>,

    /// Whether tracing/profiling is enabled.
    pub tracing_enabled: bool,

    /// Recovery tuning: max concurrent recovery operations.
    pub recovery_max_exec_count: u32,

    /// Recovery tuning: interval between queued work items (ms).
    pub recovery_queue_work_interval: u64,

    /// Recovery tuning: whether throttling is active.
    pub recovery_throttling: bool,
}

impl SystemInfo {
    pub fn new(
        listen_addr: SocketAddr,
        dir: PathBuf,
        this_node: SdNode,
        peer_transport: Arc<dyn PeerTransport>,
    ) -> Self {
        Self {
            cinfo: ClusterInfo::default(),
            this_node,
            node_status: NodeStatus::Initialization,
            vdi_inuse: bitvec![u8, Msb0; 0; sheepdog_proto::constants::SD_NR_VDIS as usize],
            vdi_state: BTreeMap::new(),
            dir,
            listen_addr,
            disable_recovery: false,
            gateway_mode: false,
            recovery_epoch: 0,
            nr_sobjs: 0,
            journal_dir: None,
            journal_size: 0,
            object_cache_enabled: false,
            object_cache_size: DEFAULT_CACHE_SIZE_MB,
            use_directio: false,
            epoch_notify: Arc::new(Notify::new()),
            shutdown_notify: Arc::new(Notify::new()),
            object_cache: None,
            peer_transport,
            md_disks: Vec::new(),
            vdi_deleted: bitvec![u8, Msb0; 0; sheepdog_proto::constants::SD_NR_VDIS as usize],
            tracing_enabled: false,
            recovery_max_exec_count: DEFAULT_RECOVERY_MAX_EXEC_COUNT,
            recovery_queue_work_interval: DEFAULT_RECOVERY_QUEUE_WORK_INTERVAL_MS,
            recovery_throttling: false,
        }
    }

    /// Check if the cluster is operational.
    pub fn is_cluster_ok(&self) -> bool {
        self.cinfo.status == ClusterStatus::Ok
    }

    /// Get current cluster epoch.
    pub fn epoch(&self) -> u32 {
        self.cinfo.epoch
    }

    /// Bump epoch and notify waiters.
    pub fn bump_epoch(&mut self) {
        self.cinfo.epoch += 1;
        self.epoch_notify.notify_waiters();
    }

    /// Set VDI as in-use.
    pub fn set_vdi_inuse(&mut self, vid: u32) {
        if (vid as usize) < self.vdi_inuse.len() {
            self.vdi_inuse.set(vid as usize, true);
        }
    }

    /// Clear VDI from in-use.
    pub fn clear_vdi_inuse(&mut self, vid: u32) {
        if (vid as usize) < self.vdi_inuse.len() {
            self.vdi_inuse.set(vid as usize, false);
        }
    }

    /// Check if VDI is in use.
    pub fn is_vdi_inuse(&self, vid: u32) -> bool {
        if (vid as usize) < self.vdi_inuse.len() {
            self.vdi_inuse[vid as usize]
        } else {
            false
        }
    }

    /// Get the storage object path.
    pub fn obj_path(&self) -> PathBuf {
        self.dir.join("obj")
    }

    /// Get the epoch log path.
    pub fn epoch_path(&self) -> PathBuf {
        self.dir.join("epoch")
    }

    /// Get the config path.
    pub fn config_path(&self) -> PathBuf {
        self.dir.join("config")
    }
}
