//! BTree inode operations for VDI data indexing.
//!
//! Sheepdog uses a B-tree structure to map data object indices to VDI IDs.
//! For small VDIs (up to 1M data objects), the index is stored inline in
//! the inode. For larger VDIs, it uses an indirect B-tree with separate
//! index objects.

use sheepdog_proto::inode::{SdInode, SdIndex};

/// Walk the inline data index of an inode, calling the callback for each
/// non-zero entry.
pub fn inode_index_walk(inode: &SdInode, mut callback: impl FnMut(SdIndex)) {
    for (idx, &vid) in inode.data_vdi_id.iter().enumerate() {
        if vid != 0 {
            callback(SdIndex {
                idx: idx as u32,
                vdi_id: vid,
            });
        }
    }
}

/// Count the number of allocated data objects in the inode.
pub fn inode_count_allocated(inode: &SdInode) -> usize {
    inode.data_vdi_id.iter().filter(|&&v| v != 0).count()
}

/// Collect statistics about the inode's data allocation.
pub fn inode_stat(inode: &SdInode) -> (u64, u64) {
    let mut my_objs: u64 = 0;
    let mut cow_objs: u64 = 0;
    let nr_objs = inode.count_data_objs();

    for i in 0..nr_objs.min(inode.data_vdi_id.len() as u64) {
        let vid = inode.data_vdi_id[i as usize];
        if vid == 0 {
            continue;
        }
        if vid == inode.vdi_id {
            my_objs += 1;
        } else {
            cow_objs += 1;
        }
    }

    (my_objs, cow_objs)
}
