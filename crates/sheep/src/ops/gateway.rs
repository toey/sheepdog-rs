//! Gateway operations — client-facing data I/O routing.
//!
//! When a client sends a read/write request, the gateway determines
//! which nodes hold the object (via consistent hashing) and forwards
//! the request to the appropriate peer(s).
//!
//! For writes: replicate to all copies or encode with erasure coding.
//! For reads: read from any available copy, or reconstruct from EC strips.

use std::net::SocketAddr;
use std::sync::Arc;

use sheepdog_proto::constants::SD_SHEEP_PROTO_VER;
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use sheepdog_proto::request::{RequestHeader, ResponseResult, SdRequest};
use sheepdog_core::consistent_hash::VNodeInfo;
use sheepdog_core::fec::{self, ErasureCoder};
use sheepdog_core::transport::PeerTransport;
use tracing::{debug, info, warn};

use crate::daemon::SharedSys;
use crate::request::Request;

/// Handle a gateway (client-facing) operation.
pub async fn handle(sys: SharedSys, request: Request) -> SdResult<ResponseResult> {
    match request.req {
        SdRequest::CreateAndWriteObj {
            oid,
            cow_oid: _,
            copies,
            copy_policy,
            offset,
            data,
        } => {
            debug!("gateway create_and_write: {:?}", oid);
            gateway_write(sys, oid, copies, copy_policy, offset, data, true).await
        }

        SdRequest::WriteObj { oid, offset, data } => {
            debug!("gateway write: {:?}", oid);
            let (copies, copy_policy) = get_vdi_copies(&sys, oid).await?;
            gateway_write(sys, oid, copies, copy_policy, offset, data, false).await
        }

        SdRequest::ReadObj {
            oid,
            offset,
            length,
        } => {
            debug!("gateway read: {:?}", oid);
            gateway_read(sys, oid, offset, length).await
        }

        SdRequest::RemoveObj { oid } => {
            debug!("gateway remove: {:?}", oid);
            let (copies, _copy_policy) = get_vdi_copies(&sys, oid).await?;
            gateway_remove(sys, oid, copies).await
        }

        SdRequest::DiscardObj {
            oid,
            offset,
            length,
        } => {
            debug!("gateway discard: {:?}", oid);
            let zeros = vec![0u8; length as usize];
            let (copies, copy_policy) = get_vdi_copies(&sys, oid).await?;
            gateway_write(sys, oid, copies, copy_policy, offset, zeros, false).await
        }

        _ => Err(SdError::NoSupport),
    }
}

/// Get copies and copy_policy for the VDI that owns this object.
async fn get_vdi_copies(sys: &SharedSys, oid: ObjectId) -> SdResult<(u8, u8)> {
    let s = sys.read().await;
    let vid = oid.to_vid();
    if let Some(state) = s.vdi_state.get(&vid) {
        Ok((state.nr_copies, state.copy_policy))
    } else {
        Ok((s.cinfo.nr_copies, s.cinfo.copy_policy))
    }
}

/// Send a peer request to a remote sheep node via the peer transport.
///
/// Uses the pluggable `PeerTransport` (TCP or DPDK) to send the request
/// and await the response. The transport handles connection management.
async fn send_peer_request(
    transport: &dyn PeerTransport,
    addr: SocketAddr,
    req: SdRequest,
    epoch: u32,
) -> SdResult<ResponseResult> {
    let header = RequestHeader {
        proto_ver: SD_SHEEP_PROTO_VER,
        epoch,
        id: 0,
    };

    let response = transport.send_request(addr, header, req).await?;

    match response.result {
        ResponseResult::Error(e) => Err(e),
        other => Ok(other),
    }
}

// ---------------------------------------------------------------------------
// Erasure coding helpers
// ---------------------------------------------------------------------------

/// Check whether a copy_policy represents erasure coding (non-zero).
fn is_erasure_coding(copy_policy: u8) -> bool {
    copy_policy != 0
}

/// Split data into equal-sized strips for erasure coding.
/// Pads the last strip with zeros if needed.
fn split_into_strips(data: &[u8], nr_strips: usize) -> Vec<Vec<u8>> {
    if nr_strips == 0 {
        return Vec::new();
    }
    // Each strip holds data.len() / nr_strips bytes, rounded up
    let strip_len = (data.len() + nr_strips - 1) / nr_strips;
    let mut strips: Vec<Vec<u8>> = Vec::with_capacity(nr_strips);
    for i in 0..nr_strips {
        let start = i * strip_len;
        let end = (start + strip_len).min(data.len());
        let mut strip = vec![0u8; strip_len];
        if start < data.len() {
            let copy_len = end - start;
            strip[..copy_len].copy_from_slice(&data[start..end]);
        }
        strips.push(strip);
    }
    strips
}

/// Reassemble data strips back into the original data buffer.
fn reassemble_strips(strips: &[Vec<u8>], original_len: usize) -> Vec<u8> {
    let mut result = Vec::with_capacity(original_len);
    for strip in strips {
        result.extend_from_slice(strip);
    }
    result.truncate(original_len);
    result
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

/// Write data to object — replicates to all responsible nodes,
/// or encodes with erasure coding and distributes strips.
async fn gateway_write(
    sys: SharedSys,
    oid: ObjectId,
    copies: u8,
    copy_policy: u8,
    offset: u32,
    data: Vec<u8>,
    create: bool,
) -> SdResult<ResponseResult> {
    if is_erasure_coding(copy_policy) && create {
        // Full-object erasure coding write (create path only)
        gateway_write_ec(sys, oid, copies, copy_policy, data).await
    } else if is_erasure_coding(copy_policy) && !create {
        // Partial write to an EC object: read-modify-write
        // Read the full object first, apply the write, then re-encode
        gateway_write_ec_partial(sys, oid, copies, copy_policy, offset, data).await
    } else {
        // Standard replication write
        gateway_write_replicate(sys, oid, copies, copy_policy, offset, data, create).await
    }
}

/// Erasure coding write: split data → encode → distribute strips to nodes.
async fn gateway_write_ec(
    sys: SharedSys,
    oid: ObjectId,
    copies: u8,
    copy_policy: u8,
    data: Vec<u8>,
) -> SdResult<ResponseResult> {
    let (d, p) = fec::ec_policy_to_dp(copy_policy);
    let total = d + p;

    if total == 0 || d == 0 {
        return Err(SdError::InvalidParms);
    }

    let ec = ErasureCoder::new(d, p)?;

    // Split data into `d` data strips
    let mut strips = split_into_strips(&data, d);

    // Encode: adds `p` parity strips to the vector
    ec.encode(&mut strips)?;

    debug!(
        "EC write: oid={:?}, data_strips={}, parity_strips={}, strip_len={}",
        oid,
        d,
        p,
        strips[0].len()
    );

    // Get target nodes — need `total` (d+p) nodes
    let (nodes, this_node_nid, epoch, transport) = {
        let s = sys.read().await;
        debug!(
            "EC write: cinfo has {} nodes, requesting {} targets",
            s.cinfo.nodes.len(),
            total
        );
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, total);
        (target_nodes, s.this_node.nid.to_string(), s.epoch(), s.peer_transport.clone())
    };

    if nodes.len() < total {
        warn!(
            "EC write: need {} nodes but only {} available",
            total,
            nodes.len()
        );
        // Fall through — write what we can
    }

    let mut success_count = 0usize;
    let mut last_error = SdError::Eio;

    // Send each strip to its designated node
    for (idx, strip) in strips.into_iter().enumerate() {
        if idx >= nodes.len() {
            warn!("EC write: skipping strip {} — no target node", idx);
            continue;
        }

        let node = &nodes[idx];
        let is_local = node.nid.to_string() == this_node_nid;
        let ec_index = (idx + 1) as u8; // EC strips use ec_index 1..=total

        let req = SdRequest::CreateAndWritePeer {
            oid,
            ec_index,
            copies,
            copy_policy,
            offset: 0,
            data: strip,
        };

        let result = if is_local {
            crate::request::exec_local_request(sys.clone(), req).await
        } else {
            send_peer_request(transport.as_ref(), node.nid.socket_addr(), req, epoch).await
        };

        match result {
            Ok(_) => success_count += 1,
            Err(e) => {
                warn!("EC write strip {} to {} failed: {}", idx, node.nid, e);
                last_error = e;
            }
        }
    }

    // Need at least `d` strips written for data to be recoverable
    if success_count >= d {
        info!(
            "EC write: oid={:?}, {}/{} strips written OK",
            oid, success_count, total
        );
        Ok(ResponseResult::Obj {
            copies,
            offset: 0,
        })
    } else {
        warn!(
            "EC write failed: only {}/{} strips written (need {})",
            success_count, total, d
        );
        Err(last_error)
    }
}

/// Partial write to an EC object: read full data → apply write → re-encode.
async fn gateway_write_ec_partial(
    sys: SharedSys,
    oid: ObjectId,
    copies: u8,
    copy_policy: u8,
    offset: u32,
    data: Vec<u8>,
) -> SdResult<ResponseResult> {
    // Get VDI size to know the full object size
    let obj_size = {
        let s = sys.read().await;
        let vid = oid.to_vid();
        if let Some(state) = s.vdi_state.get(&vid) {
            sheepdog_proto::constants::SD_DATA_OBJ_SIZE as usize
        } else {
            sheepdog_proto::constants::SD_DATA_OBJ_SIZE as usize
        }
    };

    // Read the full object first (will reconstruct from EC strips)
    let existing = match gateway_read_ec(sys.clone(), oid, 0, obj_size as u32).await {
        Ok(buf) => buf,
        Err(SdError::NoObj) => vec![0u8; obj_size],
        Err(e) => return Err(e),
    };

    // Apply the partial write
    let mut full_data = existing;
    if full_data.len() < obj_size {
        full_data.resize(obj_size, 0);
    }
    let write_end = (offset as usize) + data.len();
    if write_end > full_data.len() {
        full_data.resize(write_end, 0);
    }
    full_data[offset as usize..write_end].copy_from_slice(&data);

    // Re-encode and write
    gateway_write_ec(sys, oid, copies, copy_policy, full_data).await
}

/// Standard replication write — send full data to all copies.
async fn gateway_write_replicate(
    sys: SharedSys,
    oid: ObjectId,
    copies: u8,
    copy_policy: u8,
    offset: u32,
    data: Vec<u8>,
    create: bool,
) -> SdResult<ResponseResult> {
    let (nodes, this_node_nid, epoch, transport) = {
        let s = sys.read().await;
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, copies as usize);
        (target_nodes, s.this_node.nid.to_string(), s.epoch(), s.peer_transport.clone())
    };

    if nodes.is_empty() {
        return Err(SdError::NoSpace);
    }

    let data = Arc::new(data);
    let mut success_count = 0usize;
    let mut last_error = SdError::Eio;

    for (idx, node) in nodes.iter().enumerate() {
        let is_local = node.nid.to_string() == this_node_nid;
        let peer_req = if create {
            SdRequest::CreateAndWritePeer {
                oid,
                ec_index: idx as u8,
                copies,
                copy_policy,
                offset,
                data: data.as_ref().clone(),
            }
        } else {
            SdRequest::WritePeer {
                oid,
                ec_index: idx as u8,
                offset,
                data: data.as_ref().clone(),
            }
        };
        let result = if is_local {
            crate::request::exec_local_request(sys.clone(), peer_req).await
        } else {
            send_peer_request(transport.as_ref(), node.nid.socket_addr(), peer_req, epoch).await
        };

        match result {
            Ok(_) => success_count += 1,
            Err(e) => {
                warn!("replica write to {} failed: {}", node.nid, e);
                last_error = e;
            }
        }
    }

    if success_count > 0 {
        Ok(ResponseResult::Obj {
            copies,
            offset: offset as u64,
        })
    } else {
        Err(last_error)
    }
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

/// Read object from any available node, or reconstruct from EC strips.
async fn gateway_read(
    sys: SharedSys,
    oid: ObjectId,
    offset: u32,
    length: u32,
) -> SdResult<ResponseResult> {
    let (copies, copy_policy) = get_vdi_copies(&sys, oid).await?;

    if is_erasure_coding(copy_policy) {
        // EC read: collect strips and reconstruct
        let full_data = gateway_read_ec(sys, oid, offset, length).await?;
        Ok(ResponseResult::Data(full_data))
    } else {
        // Standard replication read
        gateway_read_replicate(sys, oid, offset, length).await
    }
}

/// EC read: read data strips from nodes, reconstruct with parity if needed.
async fn gateway_read_ec(
    sys: SharedSys,
    oid: ObjectId,
    offset: u32,
    length: u32,
) -> SdResult<Vec<u8>> {
    let (copies, copy_policy) = get_vdi_copies(&sys, oid).await?;
    let (d, p) = fec::ec_policy_to_dp(copy_policy);
    let total = d + p;

    let (nodes, this_node_nid, epoch, transport) = {
        let s = sys.read().await;
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, total);
        (target_nodes, s.this_node.nid.to_string(), s.epoch(), s.peer_transport.clone())
    };

    // Read all strips (None = couldn't read from that node)
    let mut strips: Vec<Option<Vec<u8>>> = vec![None; total];
    let mut read_count = 0usize;

    for (idx, node) in nodes.iter().enumerate() {
        if idx >= total {
            break;
        }

        let ec_index = (idx + 1) as u8;
        let is_local = node.nid.to_string() == this_node_nid;

        let req = SdRequest::ReadPeer {
            oid,
            ec_index,
            offset: 0,   // Read full strip
            length: 0,   // 0 = full object
        };

        let result = if is_local {
            crate::request::exec_local_request(sys.clone(), req).await
        } else {
            send_peer_request(transport.as_ref(), node.nid.socket_addr(), req, epoch).await
        };

        match result {
            Ok(ResponseResult::Data(data)) if !data.is_empty() => {
                strips[idx] = Some(data);
                read_count += 1;
            }
            Ok(_) => {
                debug!("EC read: strip {} from {} returned empty", idx, node.nid);
            }
            Err(e) => {
                debug!("EC read: strip {} from {} failed: {}", idx, node.nid, e);
            }
        }

        // Once we have enough strips, we can stop
        if read_count >= d {
            break;
        }
    }

    if read_count < d {
        warn!(
            "EC read: only got {}/{} strips (need {} data strips)",
            read_count, total, d
        );
        return Err(SdError::NoObj);
    }

    // If we have all data strips (first `d`), no reconstruction needed
    let need_reconstruct = (0..d).any(|i| strips[i].is_none());

    let data_strips = if need_reconstruct {
        debug!("EC read: reconstructing from {}/{} strips", read_count, total);
        let ec = ErasureCoder::new(d, p)?;
        ec.reconstruct(&mut strips)?;

        // Extract data strips (first `d`)
        strips[..d]
            .iter()
            .map(|s| s.as_ref().unwrap().clone())
            .collect::<Vec<_>>()
    } else {
        // All data strips are available — no reconstruction needed
        strips[..d]
            .iter()
            .map(|s| s.as_ref().unwrap().clone())
            .collect::<Vec<_>>()
    };

    // Reassemble the original data
    let obj_size = sheepdog_proto::constants::SD_DATA_OBJ_SIZE as usize;
    let full_data = reassemble_strips(&data_strips, obj_size);

    // Apply offset/length if specified
    let result = if offset > 0 || (length > 0 && (length as usize) < full_data.len()) {
        let start = offset as usize;
        let end = if length > 0 {
            (start + length as usize).min(full_data.len())
        } else {
            full_data.len()
        };
        full_data[start..end].to_vec()
    } else {
        full_data
    };

    Ok(result)
}

/// Standard replication read — read from any available copy.
async fn gateway_read_replicate(
    sys: SharedSys,
    oid: ObjectId,
    offset: u32,
    length: u32,
) -> SdResult<ResponseResult> {
    let (nodes, this_node_nid, epoch, transport) = {
        let s = sys.read().await;
        let nr_copies = if let Some(state) = s.vdi_state.get(&oid.to_vid()) {
            state.nr_copies
        } else {
            s.cinfo.nr_copies
        };
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, nr_copies as usize);
        (target_nodes, s.this_node.nid.to_string(), s.epoch(), s.peer_transport.clone())
    };

    if nodes.is_empty() {
        return Err(SdError::NoObj);
    }

    // Try local node first
    for (idx, node) in nodes.iter().enumerate() {
        if node.nid.to_string() == this_node_nid {
            let req = SdRequest::ReadPeer {
                oid,
                ec_index: idx as u8,
                offset,
                length,
            };
            match crate::request::exec_local_request(sys.clone(), req).await {
                Ok(result) => return Ok(result),
                Err(SdError::NoObj) => continue,
                Err(e) => {
                    warn!("local read failed: {}", e);
                    continue;
                }
            }
        }
    }

    // Try remote nodes
    for (idx, node) in nodes.iter().enumerate() {
        if node.nid.to_string() != this_node_nid {
            let req = SdRequest::ReadPeer {
                oid,
                ec_index: idx as u8,
                offset,
                length,
            };
            match send_peer_request(transport.as_ref(), node.nid.socket_addr(), req, epoch).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    warn!("remote read from {} failed: {}", node.nid, e);
                    continue;
                }
            }
        }
    }

    Err(SdError::NoObj)
}

/// Remove object from all nodes holding copies.
async fn gateway_remove(
    sys: SharedSys,
    oid: ObjectId,
    copies: u8,
) -> SdResult<ResponseResult> {
    let (nodes, this_node_nid, epoch, transport) = {
        let s = sys.read().await;
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, copies as usize);
        (target_nodes, s.this_node.nid.to_string(), s.epoch(), s.peer_transport.clone())
    };

    for (idx, node) in nodes.iter().enumerate() {
        let is_local = node.nid.to_string() == this_node_nid;
        let req = SdRequest::RemovePeer {
            oid,
            ec_index: idx as u8,
        };
        if is_local {
            let _ = crate::request::exec_local_request(sys.clone(), req).await;
        } else {
            let _ = send_peer_request(transport.as_ref(), node.nid.socket_addr(), req, epoch).await;
        }
    }

    Ok(ResponseResult::Success)
}
