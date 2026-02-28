//! Gateway operations — client-facing data I/O routing.
//!
//! When a client sends a read/write request, the gateway determines
//! which nodes hold the object (via consistent hashing) and forwards
//! the request to the appropriate peer(s).
//!
//! For writes: replicate to all copies or encode with erasure coding.
//! For reads: read from any available copy.

use std::net::SocketAddr;
use std::sync::Arc;

use sheepdog_proto::constants::SD_SHEEP_PROTO_VER;
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use sheepdog_proto::request::{RequestHeader, ResponseResult, SdRequest, SdResponse};
use sheepdog_core::consistent_hash::VNodeInfo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

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

/// Send a peer request to a remote sheep node and return the result.
async fn send_peer_request(
    addr: SocketAddr,
    req: SdRequest,
    epoch: u32,
) -> SdResult<ResponseResult> {
    let mut stream = sheepdog_core::net::connect_to_addr(addr).await?;

    let header = RequestHeader {
        proto_ver: SD_SHEEP_PROTO_VER,
        epoch,
        id: 0,
    };

    let req_data =
        bincode::serialize(&(header, req)).map_err(|_| SdError::SystemError)?;

    stream
        .write_u32(req_data.len() as u32)
        .await
        .map_err(|_| SdError::NetworkError)?;
    stream
        .write_all(&req_data)
        .await
        .map_err(|_| SdError::NetworkError)?;

    let resp_len = stream
        .read_u32()
        .await
        .map_err(|_| SdError::NetworkError)? as usize;

    if resp_len > 64 * 1024 * 1024 {
        return Err(SdError::InvalidParms);
    }

    let mut resp_buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp_buf)
        .await
        .map_err(|_| SdError::NetworkError)?;

    let response: SdResponse =
        bincode::deserialize(&resp_buf).map_err(|_| SdError::SystemError)?;

    match response.result {
        ResponseResult::Error(e) => Err(e),
        other => Ok(other),
    }
}

/// Write data to object — replicates to all responsible nodes.
async fn gateway_write(
    sys: SharedSys,
    oid: ObjectId,
    copies: u8,
    copy_policy: u8,
    offset: u32,
    data: Vec<u8>,
    create: bool,
) -> SdResult<ResponseResult> {
    let (nodes, this_node_nid, epoch) = {
        let s = sys.read().await;
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, copies as usize);
        (target_nodes, s.this_node.nid.to_string(), s.epoch())
    };

    if nodes.is_empty() {
        return Err(SdError::NoSpace);
    }

    let data = Arc::new(data);
    let mut success_count = 0usize;
    let mut last_error = SdError::Eio;

    for (idx, node) in nodes.iter().enumerate() {
        let is_local = node.nid.to_string() == this_node_nid;
        let result = if is_local {
            let peer_req = if create {
                SdRequest::CreateAndWritePeer {
                    oid,
                    ec_index: idx as u8,
                    copies,
                    copy_policy,
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
            crate::request::exec_local_request(sys.clone(), peer_req).await
        } else {
            let peer_req = if create {
                SdRequest::CreateAndWritePeer {
                    oid,
                    ec_index: idx as u8,
                    copies,
                    copy_policy,
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
            send_peer_request(node.nid.socket_addr(), peer_req, epoch).await
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

/// Read object from any available node.
async fn gateway_read(
    sys: SharedSys,
    oid: ObjectId,
    offset: u32,
    length: u32,
) -> SdResult<ResponseResult> {
    let (nodes, this_node_nid, epoch) = {
        let s = sys.read().await;
        let nr_copies = if let Some(state) = s.vdi_state.get(&oid.to_vid()) {
            state.nr_copies
        } else {
            s.cinfo.nr_copies
        };
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, nr_copies as usize);
        (target_nodes, s.this_node.nid.to_string(), s.epoch())
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
            match send_peer_request(node.nid.socket_addr(), req, epoch).await {
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
    let (nodes, this_node_nid, epoch) = {
        let s = sys.read().await;
        let vnode_info = VNodeInfo::new(&s.cinfo.nodes);
        let target_nodes = vnode_info.oid_to_nodes(oid, copies as usize);
        (target_nodes, s.this_node.nid.to_string(), s.epoch())
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
            let _ = send_peer_request(node.nid.socket_addr(), req, epoch).await;
        }
    }

    Ok(ResponseResult::Success)
}
