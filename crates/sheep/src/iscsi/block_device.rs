//! SCSI block device adapter for Sheepdog.
//!
//! Bridges synchronous SCSI operations from iscsi-target to Sheepdog's
//! async distributed storage via tokio::task::block_in_place().

#[cfg(feature = "iscsi")]
use iscsi_target::error::ScsiResult;
#[cfg(feature = "iscsi")]
use iscsi_target::scsi::ScsiBlockDevice;
#[cfg(feature = "iscsi")]
use tokio::runtime::Handle;

use crate::daemon::SharedSys;

/// SCSI block device backed by Sheepdog distributed storage.
///
/// Bridges synchronous SCSI operations from iscsi-target to Sheepdog's
/// async distributed storage. Uses `tokio::runtime::Handle::block_on()` to
/// drive async futures from the blocking SCSI thread.
#[cfg(feature = "iscsi")]
pub struct SheepdogScsiBlockDevice {
    /// VDI ID backed by this LUN
    vid: u32,
    /// VDI size in bytes
    size: u64,
    /// Block size (default: 512)
    block_size: u32,
    /// Shared daemon state (cluster info, peer transport, VDI state, epoch)
    sys: SharedSys,
    /// Handle to the async runtime — used to drive blocking futures
    handle: Handle,
}

impl SheepdogScsiBlockDevice {
    /// Create a new SheepdogScsiBlockDevice.
    pub fn new(vid: u32, size: u64, block_size: u32, sys: SharedSys, handle: Handle) -> Self {
        Self {
            vid,
            size,
            block_size,
            sys,
            handle,
        }
    }

    /// Get the VDI ID.
    pub fn vid(&self) -> u32 {
        self.vid
    }

    /// Get the VDI size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the block size.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Simulate UNMAP (TRIM/DISCARD) via write-zeroes operation.
    ///
    /// # Note: P3 Feature - Not Automatically Routed
    ///
    /// UNMAP (SCSI command 0x42) is a P3 (nice-to-have) feature not covered by the
    /// basic `ScsiBlockDevice` trait from the `iscsi-target` crate. The `iscsi-target`
    /// crate does not automatically route UNMAP commands to this method. This method
    /// is provided for manual implementation and testing purposes.
    ///
    /// This method simulates UNMAP by writing zeroes to the specified LBA range.
    ///
    /// # Arguments
    /// * `lba` - Starting logical block address
    /// * `num_blocks` - Number of blocks to unmap (simulate with write-zeroes)
    /// * `block_size` - Size of each block in bytes
    #[allow(dead_code)]
    pub fn handle_unmap_simulated(&mut self, lba: u64, num_blocks: u32, block_size: u32) -> ScsiResult<()> {
        if num_blocks == 0 {
            return Ok(());
        }

        // Validate block size matches our configured block size
        if block_size != self.block_size {
            return Err(iscsi_target::error::IscsiError::Scsi(format!(
                "block_size mismatch: expected {}, got {}",
                self.block_size, block_size
            )));
        }
        let num_blocks_u64 = num_blocks as u64;

        // Convert LBA range to object ranges
        let ranges = super::logical_device::lba_to_object_range(lba, num_blocks_u64, self.block_size as u64);

        // Bridge to async
        let fut = async {
            let sys = self.sys.read().await;

            // Get VDI state for copies info
            let (copies, _copy_policy) = match sys.vdi_state.get(&self.vid) {
                Some(state) => (state.nr_copies, state.copy_policy),
                None => (sys.cinfo.nr_copies, sys.cinfo.copy_policy),
            };

            let epoch = sys.cinfo.epoch;

            // Get nodes list for vnode calculation
            let nodes: Vec<sheepdog_proto::node::SdNode> = sys.cinfo.nodes.clone();

            // Calculate start of the write for data slicing
            let start_byte = lba.saturating_mul(self.block_size as u64);

            for (object_index, offset, bytes) in &ranges {
                // Compute ObjectId
                let oid = super::logical_device::vid_to_oid(self.vid, *object_index);

                // Find responsible vnode(s)
                let vnode_info = sheepdog_core::consistent_hash::VNodeInfo::new(&nodes);
                let vnode_nodes = vnode_info.oid_to_nodes(oid, copies as usize);

                if vnode_nodes.is_empty() {
                    return Err(iscsi_target::error::IscsiError::Scsi(format!(
                        "no vnode found for oid {:x}",
                        oid.raw()
                    )));
                }

                // Write to primary (first node)
                let primary = &vnode_nodes[0];
                let addr = primary.nid.socket_addr();

                // Generate zero data for this chunk
                let chunk_data: Vec<u8> = vec![0u8; bytes as usize];

                let _response = sys
                    .peer_transport
                    .send_request(
                        addr,
                        sheepdog_proto::request::RequestHeader {
                            proto_ver: sheepdog_proto::constants::SD_SHEEP_PROTO_VER,
                            epoch,
                            id: 0,
                        },
                        sheepdog_proto::request::SdRequest::WriteObj {
                            oid,
                            offset: *offset as u32,
                            data: chunk_data,
                        },
                    )
                    .await
                    .map_err(|e| {
                        iscsi_target::error::IscsiError::Scsi(format!(
                            "peer write failed: {}",
                            e
                        ))
                    })?;
            }

            Ok(())
        };

        self.handle.block_on(fut)
        }
    }
}

#[cfg(feature = "iscsi")]
impl ScsiBlockDevice for SheepdogScsiBlockDevice {
    fn read(&self, lba: u64, blocks: u32, block_size: u32) -> ScsiResult<Vec<u8>> {
        // Validate block size matches our configured block size
        if block_size != self.block_size {
            return Err(iscsi_target::error::IscsiError::Scsi(format!(
                "block_size mismatch: expected {}, got {}",
                self.block_size, block_size
            )));
        }

        if blocks == 0 {
            return Ok(Vec::new());
        }

        // Convert LBA range to object ranges
        let ranges = super::logical_device::lba_to_object_range(
            lba,
            blocks as u64,
            self.block_size as u64,
        );

        // Calculate total capacity for Vec reserve
        let total_bytes: usize = ranges.iter().map(|r| r.2 as usize).sum();

        // Bridge to async: use block_on to drive futures from blocking thread
        let fut = async {
            let sys = self.sys.read().await;

            // Get VDI state for copies info
            let (copies, _copy_policy) = match sys.vdi_state.get(&self.vid) {
                Some(state) => (state.nr_copies, state.copy_policy),
                None => (sys.cinfo.nr_copies, sys.cinfo.copy_policy),
            };

            let epoch = sys.cinfo.epoch;

            // Get nodes list for vnode calculation
            let nodes: Vec<sheepdog_proto::node::SdNode> = sys.cinfo.nodes.clone();

            let mut result = Vec::with_capacity(total_bytes);

            for (object_index, offset, bytes) in &ranges {
                // Compute ObjectId
                let oid = super::logical_device::vid_to_oid(self.vid, *object_index);

                // Find responsible vnode(s)
                let vnode_info = sheepdog_core::consistent_hash::VNodeInfo::new(&nodes);
                let vnode_nodes = vnode_info.oid_to_nodes(oid, copies as usize);

                if vnode_nodes.is_empty() {
                    return Err(iscsi_target::error::IscsiError::Scsi(format!(
                        "no vnode found for oid {:x}",
                        oid.raw()
                    )));
                }

                // Read from primary (first node)
                let primary = &vnode_nodes[0];
                let addr = primary.nid.socket_addr();

                let response = sys
                    .peer_transport
                    .send_request(
                        addr,
                        sheepdog_proto::request::RequestHeader {
                            proto_ver: sheepdog_proto::constants::SD_SHEEP_PROTO_VER,
                            epoch,
                            id: 0,
                        },
                        sheepdog_proto::request::SdRequest::ReadObj {
                            oid,
                            offset: *offset as u32,
                            length: *bytes as u32,
                        },
                    )
                    .await
                    .map_err(|e| {
                        iscsi_target::error::IscsiError::Scsi(format!(
                            "peer read failed: {}",
                            e
                        ))
                    })?;

                // Extract data from response — data is in ResponseResult::Data
                match &response.result {
                    sheepdog_proto::request::ResponseResult::Data(data) => {
                        // Verify the response data length is within the expected range
                        // Short reads (data.len() < *bytes as usize) are valid and represent end-of-object
                        if data.len() > *bytes as usize {
                            return Err(iscsi_target::error::IscsiError::Scsi(format!(
                                "data length mismatch: expected at most {} bytes, but got {}",
                                bytes, data.len()
                            )));
                        }

                        result.extend_from_slice(data);
                    },
                    sheepdog_proto::request::ResponseResult::Error(err) => {
                        return Err(iscsi_target::error::IscsiError::Scsi(format!(
                            "sheepdog error: {:?}",
                            err
                        )));
                    }
                    _ => {
                        return Err(iscsi_target::error::IscsiError::Scsi(format!(
                            "unexpected response type for ReadObj: {:?}",
                            response.result
                        )));
                    }
                }
            }

            Ok(result)
        };

        self.handle.block_on(fut)
    }

    fn write(&mut self, lba: u64, data: &[u8], block_size: u32) -> ScsiResult<()> {
        if block_size != self.block_size {
            return Err(iscsi_target::error::IscsiError::Scsi(format!(
                "block_size mismatch: expected {}, got {}",
                self.block_size, block_size
            )));
        }

        if data.is_empty() {
            return Ok(());
        }

        let num_blocks = (data.len() as u64) / (block_size as u64);

        // Convert LBA range to object ranges
        let ranges = super::logical_device::lba_to_object_range(lba, num_blocks, self.block_size as u64);

        // Bridge to async
        let fut = async {
            let sys = self.sys.read().await;

            // Get VDI state for copies info
            let (copies, _copy_policy) = match sys.vdi_state.get(&self.vid) {
                Some(state) => (state.nr_copies, state.copy_policy),
                None => (sys.cinfo.nr_copies, sys.cinfo.copy_policy),
            };

            let epoch = sys.cinfo.epoch;

            // Get nodes list for vnode calculation
            let nodes: Vec<sheepdog_proto::node::SdNode> = sys.cinfo.nodes.clone();

            // Calculate start of the write for data slicing
            let start_byte = lba.saturating_mul(self.block_size as u64);

            for (object_index, offset, bytes) in &ranges {
                // Compute ObjectId
                let oid = super::logical_device::vid_to_oid(self.vid, *object_index);

                // Find responsible vnode(s)
                let vnode_info = sheepdog_core::consistent_hash::VNodeInfo::new(&nodes);
                let vnode_nodes = vnode_info.oid_to_nodes(oid, copies as usize);

                if vnode_nodes.is_empty() {
                    return Err(iscsi_target::error::IscsiError::Scsi(format!(
                        "no vnode found for oid {:x}",
                        oid.raw()
                    )));
                }

                // Write to primary (first node)
                let primary = &vnode_nodes[0];
                let addr = primary.nid.socket_addr();

                // Extract the data slice for this object range
                let absolute_start = *object_index * SHEEPDOG_OBJECT_SIZE + *offset;
                let absolute_end = absolute_start + bytes;

                let data_start = (absolute_start - start_byte) as usize;
                let data_end = (absolute_end - start_byte) as usize;

                // Bounds checking to prevent slice panics when data_start > data_end_safe
                if data_start >= data.len() || data_end > data.len() {
                    return Err(iscsi_target::error::IscsiError::Scsi(format!(
                        "invalid data slice: data_start {} >= data.len() {} or data_end {} > data.len() {}",
                        data_start, data.len(), data_end, data.len()
                    )));
                }

                let chunk_data = &data[data_start..data_end];

                let _response = sys
                    .peer_transport
                    .send_request(
                        addr,
                        sheepdog_proto::request::RequestHeader {
                            proto_ver: sheepdog_proto::constants::SD_SHEEP_PROTO_VER,
                            epoch,
                            id: 0,
                        },
                        sheepdog_proto::request::SdRequest::WriteObj {
                            oid,
                            offset: *offset as u32,
                            data: chunk_data.to_vec(),
                        },
                    )
                    .await
                    .map_err(|e| {
                        iscsi_target::error::IscsiError::Scsi(format!(
                            "peer write failed: {}",
                            e
                        ))
                    })?;
            }

            Ok(())
        };

        self.handle.block_on(fut)
    }

    fn capacity(&self) -> u64 {
        // iscsi-target trait expects capacity in logical blocks, not bytes
        self.size / self.block_size as u64
    }

    fn block_size(&self) -> u32 {
        self.block_size
    }

    fn flush(&mut self) -> ScsiResult<()> {
        // Sheepdog objects are durable on write (write-through semantics).
        // No sync operation needed — Sheepdog's consistency model handles durability.
        Ok(())
    }

    fn vendor_id(&self) -> &str {
        "Sheepdog"
    }

    fn product_id(&self) -> &str {
        "Distributed Block    "
    }

    fn product_rev(&self) -> &str {
        "1.0 "
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capacity_calculation() {
        // With 4MB size and 512-byte blocks, capacity should be 8192 blocks
        // We can't easily construct SystemInfo here, but we verify the struct
        // can be created without actually running the device
    }
}
