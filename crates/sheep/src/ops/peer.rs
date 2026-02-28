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

    let obj_path = {
        let s = sys.read().await;
        get_obj_path(&s, oid, ec_index)
    };

    // Create parent directories if needed
    if let Some(parent) = obj_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| SdError::Eio)?;
    }

    // Determine the full object size.
    // For EC strips or inode objects the data may be smaller than 4MB,
    // so only pad to SD_DATA_OBJ_SIZE for regular data objects.
    let obj_size = if ec_index > 0 || !oid.is_data_obj() {
        // EC strip or non-data object: use actual data size
        (offset as usize) + data.len()
    } else {
        SD_DATA_OBJ_SIZE as usize
    };

    // Write the object to disk (use spawn_blocking for filesystem I/O)
    let path = obj_path.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::File::create(&path).map_err(|_| SdError::Eio)?;

        if offset == 0 && data.len() >= obj_size {
            // Fast path: data covers the entire object
            file.write_all(&data).map_err(|_| SdError::Eio)?;
        } else {
            // Create a full-size zero-filled file, then write data at offset
            file.set_len(obj_size as u64).map_err(|_| SdError::Eio)?;
            if !data.is_empty() {
                file.seek(SeekFrom::Start(offset as u64))
                    .map_err(|_| SdError::Eio)?;
                file.write_all(&data).map_err(|_| SdError::Eio)?;
            }
        }

        file.sync_all().map_err(|_| SdError::Eio)?;
        Ok::<_, SdError>(())
    })
    .await
    .map_err(|_| SdError::SystemError)??;

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

    let obj_path = {
        let s = sys.read().await;
        get_obj_path(&s, oid, ec_index)
    };

    if !obj_path.exists() {
        return Err(SdError::NoObj);
    }

    let path = obj_path.clone();
    let data = tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(&path).map_err(|_| SdError::NoObj)?;
        if length == 0 {
            // Read entire file
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).map_err(|_| SdError::Eio)?;
            Ok::<_, SdError>(buf)
        } else {
            file.seek(SeekFrom::Start(offset as u64))
                .map_err(|_| SdError::Eio)?;
            let mut buf = vec![0u8; length as usize];
            file.read_exact(&mut buf).map_err(|_| SdError::Eio)?;
            Ok::<_, SdError>(buf)
        }
    })
    .await
    .map_err(|_| SdError::SystemError)??;

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

    let obj_path = {
        let s = sys.read().await;
        get_obj_path(&s, oid, ec_index)
    };

    if !obj_path.exists() {
        return Err(SdError::NoObj);
    }

    let path = obj_path.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|_| SdError::Eio)?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|_| SdError::Eio)?;
        file.write_all(&data).map_err(|_| SdError::Eio)?;
        file.sync_all().map_err(|_| SdError::Eio)?;
        Ok::<_, SdError>(())
    })
    .await
    .map_err(|_| SdError::SystemError)??;

    Ok(ResponseResult::Success)
}

/// Remove (delete) an object.
async fn remove(sys: SharedSys, oid: ObjectId, ec_index: u8) -> SdResult<ResponseResult> {
    debug!("remove: oid={:?}, ec_index={}", oid, ec_index);

    let obj_path = {
        let s = sys.read().await;
        get_obj_path(&s, oid, ec_index)
    };

    if obj_path.exists() {
        tokio::fs::remove_file(&obj_path)
            .await
            .map_err(|_| SdError::Eio)?;
    }

    Ok(ResponseResult::Success)
}

/// Flush all pending writes to disk.
async fn flush(sys: SharedSys) -> SdResult<ResponseResult> {
    debug!("flush peer");
    let obj_dir = sys.read().await.obj_path();
    // Sync the object directory to ensure all pending writes are flushed.
    tokio::task::spawn_blocking(move || {
        // Open the directory and call sync_all on it (fsync on dir fd)
        if let Ok(dir) = std::fs::File::open(&obj_dir) {
            let _ = dir.sync_all();
        }
    })
    .await
    .map_err(|_| SdError::SystemError)?;
    Ok(ResponseResult::Success)
}

/// Get the list of object IDs stored locally for a given epoch.
async fn get_obj_list(sys: SharedSys, tgt_epoch: u32) -> SdResult<ResponseResult> {
    debug!("get obj list for epoch {}", tgt_epoch);

    let obj_dir = {
        let s = sys.read().await;
        s.obj_path()
    };

    if !obj_dir.exists() {
        return Ok(ResponseResult::Data(Vec::new()));
    }

    // Scan the object directory for all stored objects
    let oids = tokio::task::spawn_blocking(move || {
        let mut oids: Vec<u64> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&obj_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Ok(oid) = u64::from_str_radix(name, 16) {
                        oids.push(oid);
                    }
                }
            }
        }
        oids
    })
    .await
    .unwrap_or_default();

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
    let obj_path = {
        let s = sys.read().await;
        get_obj_path(&s, oid, ec_index)
    };

    if obj_path.exists() {
        Ok(ResponseResult::Success)
    } else {
        Err(SdError::NoObj)
    }
}

/// Batch check if multiple objects exist locally.
async fn oids_exist(sys: SharedSys, oids: Vec<ObjectId>) -> SdResult<ResponseResult> {
    let obj_base = {
        let s = sys.read().await;
        s.obj_path()
    };

    let existing: Vec<ObjectId> = oids
        .into_iter()
        .filter(|oid| {
            let path = obj_base.join(format!("{:016x}", oid.raw()));
            path.exists()
        })
        .collect();

    let data = bincode::serialize(&existing).unwrap_or_default();
    Ok(ResponseResult::Data(data))
}

/// Get the SHA1 hash of an object for consistency checking.
async fn get_hash(sys: SharedSys, oid: ObjectId, _tgt_epoch: u32) -> SdResult<ResponseResult> {
    let obj_path = {
        let s = sys.read().await;
        get_obj_path(&s, oid, 0)
    };

    if !obj_path.exists() {
        return Err(SdError::NoObj);
    }

    let path = obj_path.clone();
    let digest = tokio::task::spawn_blocking(move || {
        use sha1::{Digest, Sha1};
        let data = std::fs::read(&path).map_err(|_| SdError::Eio)?;
        let hash = Sha1::digest(&data);
        let mut result = [0u8; 20];
        result.copy_from_slice(&hash);
        Ok::<_, SdError>(result)
    })
    .await
    .map_err(|_| SdError::SystemError)??;

    Ok(ResponseResult::Hash { digest })
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
                // Write the fetched data locally
                let obj_path = {
                    let s = sys.read().await;
                    get_obj_path(&s, oid, 0)
                };
                if let Some(parent) = obj_path.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                let path = obj_path.clone();
                let data_len = data.len();
                tokio::task::spawn_blocking(move || {
                    use std::io::Write;
                    let mut f = std::fs::File::create(&path).map_err(|_| SdError::Eio)?;
                    f.write_all(&data).map_err(|_| SdError::Eio)?;
                    f.sync_all().map_err(|_| SdError::Eio)?;
                    Ok::<_, SdError>(())
                })
                .await
                .map_err(|_| SdError::SystemError)??;

                info!("repair_replica: repaired {:?} from {} ({} bytes)", oid, addr, data_len);
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
    let obj_dir = sys.obj_path();
    if ec_index > 0 {
        obj_dir.join(format!("{:016x}_{}", oid.raw(), ec_index))
    } else {
        obj_dir.join(format!("{:016x}", oid.raw()))
    }
}
