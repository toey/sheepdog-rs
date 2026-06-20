//! Logical device mapping — maps SCSI LBA to Sheepdog OIDs.

use sheepdog_proto::oid::ObjectId;

/// Sheepdog object size: 4MB
pub const SHEEPDOG_OBJECT_SIZE: u64 = 4 * 1024 * 1024;

/// Default SCSI block size: 512 bytes
pub const SCSI_BLOCK_SIZE: u64 = 512;

/// Number of blocks per object (with 512-byte blocks)
pub const BLOCKS_PER_OBJECT: u64 = SHEEPDOG_OBJECT_SIZE / SCSI_BLOCK_SIZE; // 8192

/// Convert a SCSI LBA range to Sheepdog object ranges.
///
/// Returns a list of `(object_index, offset_in_object, bytes_to_transfer)` tuples.
/// Handles multi-object transfers transparently.
///
/// # Arguments
/// * `start_lba` — Starting logical block address
/// * `num_blocks` — Number of blocks to transfer
/// * `block_size` — Block size in bytes (typically 512)
///
/// # Returns
/// Vector of (object_index, offset_in_object, bytes_to_transfer)
pub fn lba_to_object_range(
    start_lba: u64,
    num_blocks: u64,
    block_size: u64,
) -> Vec<(u64, u64, u64)> {
    if num_blocks == 0 {
        return Vec::new();
    }

    let start_byte = start_lba.saturating_mul(block_size);
    let end_byte = start_byte.saturating_add(num_blocks.saturating_mul(block_size));

    let mut result = Vec::new();
    let mut current_byte = start_byte;

    while current_byte < end_byte {
        let object_index = current_byte / SHEEPDOG_OBJECT_SIZE;
        let offset = current_byte % SHEEPDOG_OBJECT_SIZE;
        let bytes_remaining = end_byte - current_byte;
        let object_space = SHEEPDOG_OBJECT_SIZE - offset;
        let bytes_to_transfer = bytes_remaining.min(object_space);

        result.push((object_index, offset, bytes_to_transfer));
        current_byte = current_byte.saturating_add(bytes_to_transfer);
    }

    result
}

/// Construct a Sheepdog ObjectId from VDI ID and object index.
///
/// Object ID layout (64 bits):
/// - Bits 0-31: data object index
/// - Bits 32-55: VDI ID (24 bits)
/// - Bits 56-59: reserved VDI object space
/// - Bits 60-63: object type identifier (0 for data objects)
pub fn vid_to_oid(vid: u32, object_index: u64) -> ObjectId {
    // Data objects have type identifier 0 (bits 60-63 = 0)
    // VDI space starts at bit 32
    let raw = (vid as u64) << 32 | object_index;
    ObjectId::new(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_range() {
        let ranges = lba_to_object_range(0, 0, 512);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_single_block() {
        // LBA 0, 1 block (512 bytes) → object 0, offset 0, 512 bytes
        let ranges = lba_to_object_range(0, 1, 512);
        assert_eq!(ranges, vec![(0, 0, 512)]);
    }

    #[test]
    fn test_full_object() {
        // LBA 0, 8192 blocks (4MB) → single object
        let ranges = lba_to_object_range(0, 8192, 512);
        assert_eq!(ranges, vec![(0, 0, 4 * 1024 * 1024)]);
    }

    #[test]
    #[test]
    fn test_cross_object_boundary() {
        // LBA 8190, 4 blocks → spans objects 0 and 1
        // Object 0: bytes 4193280-4194303 (2 blocks, 1024 bytes)
        // Object 1: bytes 0-1023 (2 blocks, 1024 bytes)
        let ranges = lba_to_object_range(8190, 4, 512);
        assert_eq!(ranges, vec![
            (0, 4193280, 1024),
            (1, 0, 1024),
        ]);
    }
    #[test]
    fn test_multi_object() {
        // LBA 0, 16384 blocks (8MB) → 2 objects
        let ranges = lba_to_object_range(0, 16384, 512);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], (0, 0, 4 * 1024 * 1024));
        assert_eq!(ranges[1], (1, 0, 4 * 1024 * 1024));
    }

    #[test]
    fn test_mid_object() {
        // LBA 4000, 100 blocks → within object 0
        let ranges = lba_to_object_range(4000, 100, 512);
        assert_eq!(ranges, vec![(0, 4000 * 512, 100 * 512)]);
    }

    #[test]
    fn test_oid_construction() {
        let oid = vid_to_oid(0x1234, 5);
        assert_eq!(oid.raw(), (0x1234u64 << 32) | 5);
        assert!(oid.is_data_obj());
    }

    #[test]
    fn test_large_lba() {
        // Test overflow protection with large values
        let ranges = lba_to_object_range(u64::MAX / 512 + 10, 1, 512);
        // Should handle saturating arithmetic gracefully
        assert!(ranges.len() <= 2);
    }
}
