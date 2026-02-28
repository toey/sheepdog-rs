/// VDI inode and BTree index structures.

use serde::{Deserialize, Serialize};

use crate::constants::*;

/// Generation reference for copy-on-write tracking.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationReference {
    pub generation: i32,
    pub count: i32,
}

/// VDI inode — the main metadata structure for a virtual disk.
///
/// In the C version this was a massive fixed-size struct (~4.5MB).
/// In Rust we use a more memory-efficient representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdInode {
    /// VDI name (up to 256 bytes)
    pub name: String,
    /// Snapshot tag
    pub tag: String,
    /// Creation timestamp (epoch seconds)
    pub create_time: u64,
    /// Snapshot creation time
    pub snap_ctime: u64,
    /// VM clock in nanoseconds
    pub vm_clock_nsec: u64,
    /// Virtual disk size in bytes
    pub vdi_size: u64,
    /// VM state size
    pub vm_state_size: u64,
    /// Copy policy (0 = replicate, >0 = erasure coding scheme)
    pub copy_policy: u8,
    /// Store policy
    pub store_policy: u8,
    /// Number of replicas
    pub nr_copies: u8,
    /// Block size shift (log2 of block size)
    pub block_size_shift: u8,
    /// Snapshot ID
    pub snap_id: u32,
    /// VDI ID
    pub vdi_id: u32,
    /// Parent VDI ID (for snapshots/clones)
    pub parent_vdi_id: u32,
    /// BTree counter for indirect indexing
    pub btree_counter: u32,
    /// Data VDI ID index (maps data object index → VDI id).
    /// Sparse — only populated entries are stored.
    pub data_vdi_id: Vec<u32>,
    /// Generation references for copy-on-write tracking.
    pub gref: Vec<GenerationReference>,
}

impl SdInode {
    /// Create a new empty inode.
    pub fn new() -> Self {
        Self {
            name: String::new(),
            tag: String::new(),
            create_time: 0,
            snap_ctime: 0,
            vm_clock_nsec: 0,
            vdi_size: 0,
            vm_state_size: 0,
            copy_policy: 0,
            store_policy: 0,
            nr_copies: SD_DEFAULT_COPIES,
            block_size_shift: 22, // 4MB default
            snap_id: 0,
            vdi_id: 0,
            parent_vdi_id: 0,
            btree_counter: 0,
            data_vdi_id: vec![0; SD_INODE_DATA_INDEX],
            gref: vec![GenerationReference::default(); SD_INODE_DATA_INDEX],
        }
    }

    /// Check if this inode represents a snapshot.
    pub fn is_snapshot(&self) -> bool {
        self.snap_ctime != 0
    }

    /// Count the number of data objects needed for this VDI.
    pub fn count_data_objs(&self) -> u64 {
        if self.vdi_size == 0 {
            return 0;
        }
        (self.vdi_size + SD_DATA_OBJ_SIZE - 1) / SD_DATA_OBJ_SIZE
    }

    /// Get the VDI id for a data object at the given index.
    pub fn get_vid(&self, idx: u32) -> u32 {
        let idx = idx as usize;
        if idx < self.data_vdi_id.len() {
            self.data_vdi_id[idx]
        } else {
            0
        }
    }

    /// Set the VDI id for a data object at the given index.
    pub fn set_vid(&mut self, idx: u32, vid: u32) {
        let idx = idx as usize;
        if idx >= self.data_vdi_id.len() {
            self.data_vdi_id.resize(idx + 1, 0);
        }
        self.data_vdi_id[idx] = vid;
    }
}

impl Default for SdInode {
    fn default() -> Self {
        Self::new()
    }
}

/// BTree index entry — maps a data object index to a VDI id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdIndex {
    /// Index of data object
    pub idx: u32,
    /// VDI id
    pub vdi_id: u32,
}

/// BTree indirect index entry — points to a child node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdIndirectIdx {
    /// Max index within this indirect node
    pub idx: u32,
    /// Object ID of the child node
    pub oid: u64,
}

/// BTree index header (stored at the start of each index node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdIndexHeader {
    /// Magic number (INODE_BTREE_MAGIC = 0x6274)
    pub magic: u16,
    /// Depth: 2 = root node, 1 = indirect node
    pub depth: u16,
    /// Number of entries in this node
    pub entries: u32,
}

/// BTree node types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BTreeNodeType {
    /// Root/leaf node with direct entries
    Head = 1,
    /// Index node with direct entries
    Index = 2,
    /// Indirect index node
    IndirectIdx = 4,
}

/// VDI attribute stored as a separate object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VdiAttr {
    /// VDI name
    pub name: String,
    /// Snapshot tag
    pub tag: String,
    /// Creation time
    pub ctime: u64,
    /// Snapshot id
    pub snap_id: u32,
    /// Attribute key
    pub key: String,
    /// Attribute value
    pub value: Vec<u8>,
}
