//! Object space allocator for HTTP key-value operations.
//!
//! Tracks free space within VDI data objects for efficient key-value storage.
//! Each VDI has a fixed number of data slots; the allocator manages which
//! slots are free/used.

use std::collections::BTreeSet;

use sheepdog_proto::error::{SdError, SdResult};
use tracing::debug;

/// Object allocator for a single bucket/VDI.
pub struct ObjectAllocator {
    /// VDI ID this allocator manages.
    vid: u32,
    /// Set of free data object indices.
    free_indices: BTreeSet<u32>,
    /// Total number of available data slots.
    total_slots: u32,
    /// Next index to try for allocation.
    next_idx: u32,
}

impl ObjectAllocator {
    /// Create a new allocator for a VDI with the given number of data slots.
    pub fn new(vid: u32, total_slots: u32) -> Self {
        let free_indices: BTreeSet<u32> = (0..total_slots).collect();
        Self {
            vid,
            free_indices,
            total_slots,
            next_idx: 0,
        }
    }

    /// Allocate a free data object index.
    pub fn alloc(&mut self) -> SdResult<u32> {
        if let Some(&idx) = self.free_indices.iter().next() {
            self.free_indices.remove(&idx);
            debug!("oalloc: allocated idx {} for vid {:#x}", idx, self.vid);
            Ok(idx)
        } else {
            Err(SdError::NoSpace)
        }
    }

    /// Free a previously allocated index.
    pub fn free(&mut self, idx: u32) {
        if idx < self.total_slots {
            self.free_indices.insert(idx);
            debug!("oalloc: freed idx {} for vid {:#x}", idx, self.vid);
        }
    }

    /// Get the number of free slots.
    pub fn nr_free(&self) -> u32 {
        self.free_indices.len() as u32
    }

    /// Get the number of used slots.
    pub fn nr_used(&self) -> u32 {
        self.total_slots - self.nr_free()
    }

    /// Mark a specific index as used (e.g., during recovery scan).
    pub fn mark_used(&mut self, idx: u32) {
        self.free_indices.remove(&idx);
    }
}
