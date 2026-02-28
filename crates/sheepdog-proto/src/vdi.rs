/// VDI (Virtual Disk Image) state and locking types.

use serde::{Deserialize, Serialize};

use crate::node::NodeId;

/// VDI lock state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockState {
    Unlocked,
    /// Exclusively locked by one owner
    Locked,
    /// Shared lock (for iSCSI multipath)
    Shared,
}

impl Default for LockState {
    fn default() -> Self {
        Self::Unlocked
    }
}

/// Per-node shared lock state (for iSCSI multipath).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SharedLockState {
    Modified,
    Shared,
    Invalidated,
}

/// Runtime state of a VDI, tracked per-node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VdiState {
    /// VDI id
    pub vid: u32,
    /// Virtual disk size in bytes
    pub vdi_size: u64,
    /// VDI name
    pub name: String,
    /// Number of replicas
    pub nr_copies: u8,
    /// Whether this is a snapshot
    pub snapshot: bool,
    /// Copy policy (0 = replicate, >0 = erasure coding)
    pub copy_policy: u8,
    /// Current lock state
    pub lock_state: LockState,
    /// Lock owner (for normal locking)
    pub lock_owner: Option<NodeId>,
    /// Shared lock participants and their states (for iSCSI multipath)
    pub participants: Vec<(NodeId, SharedLockState)>,
}

impl VdiState {
    pub fn new(vid: u32, nr_copies: u8, copy_policy: u8) -> Self {
        Self {
            vid,
            vdi_size: 0,
            name: String::new(),
            nr_copies,
            snapshot: false,
            copy_policy,
            lock_state: LockState::default(),
            lock_owner: None,
            participants: Vec::new(),
        }
    }
}
