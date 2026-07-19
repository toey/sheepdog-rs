//! Peer operations — I/O requests from other sheep nodes.
//!
//! These handle reading/writing data objects on the local store
//! on behalf of requests forwarded by gateway operations.

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use sheepdog_proto::request::{ResponseResult, SdRequest};
use tracing::{debug, info, warn};

use crate::daemon::SharedSys;
use crate::request::Request;

/// Handle a peer I/O operation.
pub async fn handle(sys: SharedSys, request: Request) -> SdResult<ResponseResult> {
    match request.req {
        SdRequest::CreateAndWritePeer {
            oid,
            ec_index,
            copies,
            copy_policy,
            offset,
            data,
        } => create_and_write(sys, oid, ec_index, copies, copy_policy, offset, data).await,

        SdRequest::ReadPeer {
            oid,
            ec_index,
            offset,
            length,
        } => read(sys, oid, ec_index, offset, length).await,

        SdRequest::WritePeer {
            oid,
            ec_index,
            offset,
            data,
        } => write(sys, oid, ec_index, offset, data).await,

        SdRequest::RemovePeer { oid, ec_index } => remove(sys, oid, ec_index).await,

        SdRequest::FlushPeer => flush(sys).await,

        SdRequest::GetObjList { tgt_epoch } => get_obj_list(sys, tgt_epoch).await,

        SdRequest::GetEpoch { tgt_epoch } => get_epoch(sys, tgt_epoch).await,

        SdRequest::Exist { oid, ec_index } => exist(sys, oid, ec_index).await,

        SdRequest::OidsExist { oids } => oids_exist(sys, oids).await,

        SdRequest::GetHash { oid, tgt_epoch } => get_hash(sys, oid, tgt_epoch).await,

        SdRequest::RepairReplica { oid } => repair_replica(sys, oid).await,

        SdRequest::DecrefPeer {
            oid,
            generation,
            count,
        } => {
            debug!("decref peer: {:?} gen={} count={}", oid, generation, count);
            // TODO: implement reference counting
            Ok(ResponseResult::Success)
        }

        _ => Err(SdError::NoSupport),
    }
}

/// Create a new object and write initial data.
///
/// Creates a full-size (SD_DATA_OBJ_SIZE = 4 MB) zero-filled object on disk,
/// then writes the provided data at the given offset. This ensures that sparse
/// writes (e.g., writing 4K at offset 1M in a new object) produce a correctly
/// sized object with zeros in unwritten regions.
async fn create_and_write(
    sys: SharedSys,
    oid: ObjectId,
    ec_index: u8,
    _copies: u8,
    _copy_policy: u8,
    offset: u32,
    data: Vec<u8>,
) -> SdResult<ResponseResult> {
    use sheepdog_proto::constants::SD_DATA_OBJ_SIZE;

    debug!(
        "create_and_write: oid={:?}, ec_index={}, offset={}, len={}",
        oid,
        ec_index,
        offset,
        data.len()
    );

    // Determine the full object size.
    // For EC strips or inode objects the data may be smaller than 4MB,
    // so only pad to SD_DATA_OBJ_SIZE for regular data objects.
    let obj_size = if ec_index > 0 || !oid.is_data_obj() {
        // EC strip or non-data object: use actual data size
        (offset as usize) + data.len()
    } else {
        SD_DATA_OBJ_SIZE as usize
    };

    // Prepare data with correct padding for store's create_and_write.
    // The store interface writes data at offset 0, so we need to pad/shift
    // the data to match the expected layout.
    let prepared_data = if offset == 0 && data.len() >= obj_size {
        // Fast path: data already covers the entire object
        data
    } else {
        // Create a zero-filled buffer of obj_size, then write data at offset
        let mut buf = vec![0u8; obj_size];
        if offset as usize + data.len() <= buf.len() {
            buf[offset as usize..offset as usize + data.len()].copy_from_slice(&data);
        } else {
            return Err(SdError::InvalidParms);
        }
        buf
    };

    // Use store interface for create_and_write
    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    store.create_and_write(oid, ec_index, &prepared_data).await?;

    Ok(ResponseResult::Success)
}

/// Read data from an existing object.
async fn read(
    sys: SharedSys,
    oid: ObjectId,
    ec_index: u8,
    offset: u32,
    length: u32,
) -> SdResult<ResponseResult> {
    debug!(
        "read: oid={:?}, ec_index={}, offset={}, len={}",
        oid, ec_index, offset, length
    );

    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    let data = store.read(oid, ec_index, offset as u64, length as usize).await?;

    Ok(ResponseResult::Data(data))
}

/// Write data to an existing object at an offset.
async fn write(
    sys: SharedSys,
    oid: ObjectId,
    ec_index: u8,
    offset: u32,
    data: Vec<u8>,
) -> SdResult<ResponseResult> {
    debug!(
        "write: oid={:?}, ec_index={}, offset={}, len={}",
        oid,
        ec_index,
        offset,
        data.len()
    );

    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    store.write(oid, ec_index, offset as u64, &data).await?;

    Ok(ResponseResult::Success)
}

/// Remove (delete) an object.
async fn remove(sys: SharedSys, oid: ObjectId, ec_index: u8) -> SdResult<ResponseResult> {
    debug!("remove: oid={:?}, ec_index={}", oid, ec_index);

    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    store.remove(oid, ec_index).await?;

    Ok(ResponseResult::Success)
}

/// Flush all pending writes to disk.
async fn flush(sys: SharedSys) -> SdResult<ResponseResult> {
    debug!("flush peer");
    let store = {
        let s = sys.read().await;
        s.store.clone()
    };
    store.flush().await?;
    Ok(ResponseResult::Success)
}

/// Get the list of object IDs stored locally for a given epoch.
async fn get_obj_list(sys: SharedSys, tgt_epoch: u32) -> SdResult<ResponseResult> {
    debug!("get obj list for epoch {}", tgt_epoch);

    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    let object_ids = store.get_obj_list().await?;

    // Convert Vec<ObjectId> to Vec<u64> for serialization
    let oids: Vec<u64> = object_ids.iter().map(|oid| oid.raw()).collect();

    let data = bincode::serialize(&oids).unwrap_or_default();
    Ok(ResponseResult::Data(data))
}

/// Get epoch log data for a specific epoch.
async fn get_epoch(sys: SharedSys, tgt_epoch: u32) -> SdResult<ResponseResult> {
    debug!("get epoch {}", tgt_epoch);
    let dir = {
        let s = sys.read().await;
        s.dir.clone()
    };
    let log = crate::config::load_epoch_log(&dir, tgt_epoch).await?;
    let data = bincode::serialize(&log).unwrap_or_default();
    Ok(ResponseResult::Data(data))
}

/// Check if an object exists locally.
async fn exist(sys: SharedSys, oid: ObjectId, ec_index: u8) -> SdResult<ResponseResult> {
    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    if store.exist(oid, ec_index).await {
        Ok(ResponseResult::Success)
    } else {
        Err(SdError::NoObj)
    }
}

/// Batch check if multiple objects exist locally.
async fn oids_exist(sys: SharedSys, oids: Vec<ObjectId>) -> SdResult<ResponseResult> {
    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    // OidsExist has no ec_index field, use ec_index=0
    let mut existing: Vec<ObjectId> = Vec::new();
    for oid in oids {
        if store.exist(oid, 0).await {
            existing.push(oid);
        }
    }

    let data = bincode::serialize(&existing).unwrap_or_default();
    Ok(ResponseResult::Data(data))
}

/// Get the SHA1 hash of an object for consistency checking.
async fn get_hash(sys: SharedSys, oid: ObjectId, _tgt_epoch: u32) -> SdResult<ResponseResult> {
    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    // Read object data through store interface
    let data = store.read(oid, 0, 0, usize::MAX).await?;

    use sha1::{Digest, Sha1};
    let hash = Sha1::digest(&data);
    let mut result = [0u8; 20];
    result.copy_from_slice(&hash);

    Ok(ResponseResult::Hash { digest: result })
}

/// Repair a replica by fetching the object from another node that has it
/// and rewriting it locally. Uses the peer transport for network I/O.
async fn repair_replica(sys: SharedSys, oid: ObjectId) -> SdResult<ResponseResult> {
    use sheepdog_proto::constants::SD_SHEEP_PROTO_VER;
    use sheepdog_proto::request::RequestHeader;
    use sheepdog_core::consistent_hash::VNodeInfo;

    debug!("repair_replica: {:?}", oid);

    let (nodes, this_nid, epoch, transport) = {
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

    // Get the store for writing the repaired data
    let store = {
        let s = sys.read().await;
        s.store.clone()
    };

    // Try to read the object from each peer (skip self)
    for (idx, node) in nodes.iter().enumerate() {
        if node.nid.to_string() == this_nid {
            continue;
        }

        let addr = node.nid.socket_addr();
        let header = RequestHeader {
            proto_ver: SD_SHEEP_PROTO_VER,
            epoch,
            id: 0,
        };
        let req = SdRequest::ReadPeer {
            oid,
            ec_index: idx as u8,
            offset: 0,
            length: 0, // 0 = read entire object
        };

        let result = match transport.send_request(addr, header, req).await {
            Ok(resp) => match resp.result {
                ResponseResult::Data(d) => Ok(d),
                ResponseResult::Error(e) => Err(e),
                _ => Err(SdError::InvalidParms),
            },
            Err(e) => Err(e),
        };

        match result {
            Ok(data) if !data.is_empty() => {
                // Write the fetched data locally using store interface
                store.create_and_write(oid, 0, &data).await?;

                info!("repair_replica: repaired {:?} from {} ({} bytes)", oid, addr, data.len());
                return Ok(ResponseResult::Success);
            }
            Ok(_) => continue,
            Err(e) => {
                warn!("repair_replica: read from {} failed: {}", addr, e);
                continue;
            }
        }
    }

    warn!("repair_replica: failed to repair {:?} - no peer had the object", oid);
    Err(SdError::NoObj)
}

/// Compute the filesystem path for an object.
fn get_obj_path(
    sys: &crate::daemon::SystemInfo,
    oid: ObjectId,
    ec_index: u8,
) -> std::path::PathBuf {
    use crate::store::common::oid_to_filename;
    sys.obj_path().join(oid_to_filename(oid, ec_index))
}
