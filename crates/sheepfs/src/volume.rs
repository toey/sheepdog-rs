//! VDI volume representation for FUSE.
//!
//! Each VDI appears as a file under the mountpoint whose size matches
//! the VDI's virtual disk size.

use std::sync::Arc;

/// Cached VDI volume information.
#[derive(Debug, Clone)]
pub struct VdiVolume {
    /// VDI ID.
    pub vid: u32,
    /// VDI name.
    pub name: String,
    /// Virtual disk size in bytes.
    pub size: u64,
    /// Number of data copies.
    pub copies: u8,
    /// Whether this is a snapshot.
    pub snapshot: bool,
    /// Snapshot ID (0 for current).
    pub snap_id: u32,
}

impl VdiVolume {
    /// Compute the data object index for a given file offset.
    pub fn offset_to_obj_index(&self, offset: u64) -> u32 {
        (offset / sheepdog_proto::constants::SD_DATA_OBJ_SIZE) as u32
    }

    /// Compute the offset within a data object.
    pub fn offset_within_obj(&self, offset: u64) -> u64 {
        offset % sheepdog_proto::constants::SD_DATA_OBJ_SIZE
    }
}
