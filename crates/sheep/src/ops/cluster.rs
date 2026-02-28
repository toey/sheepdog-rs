//! Cluster-wide operations — executed on one node, result broadcast to all.
//!
//! These operations require distributed coordination. In the C version,
//! they used `cdrv->block()` and `cdrv->unblock()` for atomic execution.
//! Here we use the cluster driver trait's block/notify mechanism.

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::node::ClusterStatus;
use sheepdog_proto::request::{ResponseResult, SdRequest};
use tracing::{debug, info};

use crate::daemon::SharedSys;
use crate::request::Request;

/// Handle a cluster operation.
pub async fn handle(sys: SharedSys, request: Request) -> SdResult<ResponseResult> {
    match request.req {
        SdRequest::NewVdi {
            name,
            vdi_size,
            base_vid,
            copies,
            copy_policy,
            store_policy,
            snap_id,
            vdi_type,
        } => {
            info!("creating VDI: name={}, size={}", name, vdi_size);
            new_vdi(
                sys,
                &name,
                vdi_size,
                base_vid,
                copies,
                copy_policy,
                store_policy,
                snap_id,
                vdi_type,
            )
            .await
        }

        SdRequest::DelVdi { name, snap_id } => {
            info!("deleting VDI: name={}, snap_id={}", name, snap_id);
            del_vdi(sys, &name, snap_id).await
        }

        SdRequest::LockVdi { name, lock_type } => {
            debug!("locking VDI: name={}, type={}", name, lock_type);
            lock_vdi(sys, &name, lock_type).await
        }

        SdRequest::ReleaseVdi { name } => {
            debug!("releasing VDI: name={}", name);
            release_vdi(sys, &name).await
        }

        SdRequest::GetVdiInfo {
            name,
            tag,
            snap_id,
        } => {
            debug!("get VDI info: name={}", name);
            get_vdi_info(sys, &name, &tag, snap_id).await
        }

        SdRequest::Snapshot { name, tag } => {
            info!("snapshot VDI: name={}, tag={}", name, tag);
            snapshot_vdi(sys, &name, &tag).await
        }

        SdRequest::MakeFs {
            copies,
            copy_policy,
            flags,
            store,
        } => {
            info!("formatting cluster: copies={}, store={}", copies, store);
            make_fs(sys, copies, copy_policy, flags, &store).await
        }

        SdRequest::Shutdown => {
            info!("shutdown requested");
            shutdown(sys).await
        }

        SdRequest::AlterClusterCopy {
            copies,
            copy_policy,
        } => {
            info!("altering cluster copies: {}", copies);
            alter_cluster_copy(sys, copies, copy_policy).await
        }

        SdRequest::AlterVdiCopy {
            vid,
            copies,
            copy_policy,
        } => {
            info!("altering VDI {} copies: {}", vid, copies);
            alter_vdi_copy(sys, vid, copies, copy_policy).await
        }

        SdRequest::NotifyVdiAdd {
            old_vid,
            new_vid,
            copies,
            copy_policy,
            set_bitmap,
        } => {
            debug!("notify VDI add: {} -> {}", old_vid, new_vid);
            notify_vdi_add(sys, old_vid, new_vid, copies, copy_policy, set_bitmap).await
        }

        SdRequest::NotifyVdiDel { vid } => {
            debug!("notify VDI del: {}", vid);
            notify_vdi_del(sys, vid).await
        }

        SdRequest::InodeCoherence { vid, validate } => {
            debug!("inode coherence: vid={}", vid);
            inode_coherence(sys, vid, validate).await
        }

        SdRequest::VdiStateSnapshotCtl { get, tgt_epoch } => {
            debug!("VDI state snapshot ctl: get={}", get);
            vdi_state_snapshot(sys, get, tgt_epoch).await
        }

        SdRequest::NfsCreate { name } => {
            info!("NFS create: {}", name);
            nfs_create(sys, &name).await
        }

        SdRequest::NfsDelete { name } => {
            info!("NFS delete: {}", name);
            nfs_delete(sys, &name).await
        }

        _ => Err(SdError::NoSupport),
    }
}

// --- VDI Operations ---

async fn new_vdi(
    sys: SharedSys,
    name: &str,
    vdi_size: u64,
    base_vid: u32,
    copies: u8,
    copy_policy: u8,
    _store_policy: u8,
    snap_id: u32,
    _vdi_type: u32,
) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;

    // Find a free VDI ID using hash of name
    let vid = crate::vdi::find_free_vdi_id(&s, name)?;

    // Mark VDI as in-use
    s.set_vdi_inuse(vid);

    // Create VDI state entry
    let vdi_state = sheepdog_proto::vdi::VdiState {
        vid,
        nr_copies: copies,
        snapshot: false,
        copy_policy,
        lock_state: sheepdog_proto::vdi::LockState::Unlocked,
        lock_owner: None,
        participants: Vec::new(),
    };
    s.vdi_state.insert(vid, vdi_state);

    info!(
        "created VDI: name={}, vid={:#x}, size={}, copies={}",
        name, vid, vdi_size, copies
    );

    Ok(ResponseResult::Vdi {
        vdi_id: vid,
        attr_id: 0,
        copies,
    })
}

async fn del_vdi(sys: SharedSys, name: &str, _snap_id: u32) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;

    // Find VDI by name — simplified lookup
    let vid = crate::vdi::lookup_vdi_by_name(&s, name)?;

    s.clear_vdi_inuse(vid);
    s.vdi_state.remove(&vid);

    info!("deleted VDI: name={}, vid={:#x}", name, vid);
    Ok(ResponseResult::Success)
}

async fn lock_vdi(sys: SharedSys, name: &str, lock_type: u32) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;
    let vid = crate::vdi::lookup_vdi_by_name(&s, name)?;

    if let Some(state) = s.vdi_state.get_mut(&vid) {
        match state.lock_state {
            sheepdog_proto::vdi::LockState::Unlocked => {
                if lock_type == 0 {
                    // Normal exclusive lock
                    state.lock_state = sheepdog_proto::vdi::LockState::Locked;
                } else {
                    // Shared lock
                    state.lock_state = sheepdog_proto::vdi::LockState::Shared;
                }
                Ok(ResponseResult::Vdi {
                    vdi_id: vid,
                    attr_id: 0,
                    copies: state.nr_copies,
                })
            }
            sheepdog_proto::vdi::LockState::Shared if lock_type != 0 => {
                // Allow additional shared lock
                Ok(ResponseResult::Vdi {
                    vdi_id: vid,
                    attr_id: 0,
                    copies: state.nr_copies,
                })
            }
            _ => Err(SdError::VdiLocked),
        }
    } else {
        Err(SdError::NoVdi)
    }
}

async fn release_vdi(sys: SharedSys, name: &str) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;
    let vid = crate::vdi::lookup_vdi_by_name(&s, name)?;

    if let Some(state) = s.vdi_state.get_mut(&vid) {
        state.lock_state = sheepdog_proto::vdi::LockState::Unlocked;
        state.lock_owner = None;
        state.participants.clear();
        Ok(ResponseResult::Success)
    } else {
        Err(SdError::NoVdi)
    }
}

async fn get_vdi_info(
    sys: SharedSys,
    name: &str,
    _tag: &str,
    _snap_id: u32,
) -> SdResult<ResponseResult> {
    let s = sys.read().await;
    let vid = crate::vdi::lookup_vdi_by_name(&s, name)?;

    if let Some(state) = s.vdi_state.get(&vid) {
        Ok(ResponseResult::Vdi {
            vdi_id: vid,
            attr_id: 0,
            copies: state.nr_copies,
        })
    } else {
        Err(SdError::NoVdi)
    }
}

async fn snapshot_vdi(sys: SharedSys, name: &str, _tag: &str) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;
    let vid = crate::vdi::lookup_vdi_by_name(&s, name)?;

    if let Some(state) = s.vdi_state.get_mut(&vid) {
        state.snapshot = true;
        state.lock_state = sheepdog_proto::vdi::LockState::Unlocked;
        Ok(ResponseResult::Vdi {
            vdi_id: vid,
            attr_id: 0,
            copies: state.nr_copies,
        })
    } else {
        Err(SdError::NoVdi)
    }
}

// --- Cluster Management ---

async fn make_fs(
    sys: SharedSys,
    copies: u8,
    copy_policy: u8,
    flags: u16,
    store: &str,
) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;

    if s.cinfo.status != ClusterStatus::WaitForFormat {
        return Err(SdError::InvalidParms);
    }

    s.cinfo.nr_copies = copies;
    s.cinfo.copy_policy = copy_policy;
    s.cinfo.flags = flags;
    s.cinfo.default_store = store.to_string();
    s.cinfo.ctime = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    s.cinfo.status = ClusterStatus::Ok;

    // Bump epoch for the initial format
    s.bump_epoch();

    info!(
        "cluster formatted: copies={}, store={}, epoch={}",
        copies,
        store,
        s.epoch()
    );

    Ok(ResponseResult::Success)
}

async fn shutdown(sys: SharedSys) -> SdResult<ResponseResult> {
    let s = sys.read().await;
    s.shutdown_notify.notify_waiters();
    Ok(ResponseResult::Success)
}

async fn alter_cluster_copy(
    sys: SharedSys,
    copies: u8,
    copy_policy: u8,
) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;
    s.cinfo.nr_copies = copies;
    s.cinfo.copy_policy = copy_policy;
    info!("cluster copies changed to {}", copies);
    Ok(ResponseResult::Success)
}

async fn alter_vdi_copy(
    sys: SharedSys,
    vid: u32,
    copies: u8,
    copy_policy: u8,
) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;
    if let Some(state) = s.vdi_state.get_mut(&vid) {
        state.nr_copies = copies;
        state.copy_policy = copy_policy;
        info!("VDI {:#x} copies changed to {}", vid, copies);
        Ok(ResponseResult::Success)
    } else {
        Err(SdError::NoVdi)
    }
}

// --- VDI Notifications ---

async fn notify_vdi_add(
    sys: SharedSys,
    old_vid: u32,
    new_vid: u32,
    copies: u8,
    copy_policy: u8,
    set_bitmap: bool,
) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;

    if set_bitmap {
        s.set_vdi_inuse(new_vid);
        if old_vid > 0 && old_vid != new_vid {
            // If snapshot, old VDI stays in-use
        }
    }

    let vdi_state = sheepdog_proto::vdi::VdiState {
        vid: new_vid,
        nr_copies: copies,
        snapshot: false,
        copy_policy,
        lock_state: sheepdog_proto::vdi::LockState::Unlocked,
        lock_owner: None,
        participants: Vec::new(),
    };
    s.vdi_state.insert(new_vid, vdi_state);

    Ok(ResponseResult::Success)
}

async fn notify_vdi_del(sys: SharedSys, vid: u32) -> SdResult<ResponseResult> {
    let mut s = sys.write().await;
    s.clear_vdi_inuse(vid);
    s.vdi_state.remove(&vid);
    Ok(ResponseResult::Success)
}

async fn inode_coherence(
    _sys: SharedSys,
    _vid: u32,
    _validate: bool,
) -> SdResult<ResponseResult> {
    // TODO: implement inode coherence protocol
    Ok(ResponseResult::Success)
}

async fn vdi_state_snapshot(
    _sys: SharedSys,
    _get: bool,
    _tgt_epoch: u32,
) -> SdResult<ResponseResult> {
    // TODO: implement VDI state snapshot for recovery
    Ok(ResponseResult::Success)
}

async fn nfs_create(_sys: SharedSys, _name: &str) -> SdResult<ResponseResult> {
    // TODO: implement when NFS feature is enabled
    Err(SdError::NoSupport)
}

async fn nfs_delete(_sys: SharedSys, _name: &str) -> SdResult<ResponseResult> {
    // TODO: implement when NFS feature is enabled
    Err(SdError::NoSupport)
}
