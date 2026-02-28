//! Local operations — processed entirely on this node.
//!
//! These include queries (stat, node list) and control operations
//! (enable/disable recovery, logging).

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::request::{ResponseResult, SdRequest};
use tracing::{debug, info};

use crate::daemon::SharedSys;
use crate::request::Request;

/// Handle a local operation.
pub async fn handle(sys: SharedSys, request: Request) -> SdResult<ResponseResult> {
    match request.req {
        SdRequest::GetNodeList => get_node_list(sys).await,
        SdRequest::StatSheep => stat_sheep(sys).await,
        SdRequest::StatCluster => stat_cluster(sys).await,
        SdRequest::GetStoreList => get_store_list().await,
        SdRequest::ClusterInfo => stat_cluster(sys).await,
        SdRequest::ReadVdis => read_vdis(sys).await,
        SdRequest::FlushVdi { vid } => flush_vdi(sys, vid).await,
        SdRequest::GetVdiCopies { vid } => get_vdi_copies(sys, vid).await,
        SdRequest::ReadDelVdis => read_del_vdis(sys).await,

        SdRequest::EnableRecover => {
            info!("enabling recovery");
            sys.write().await.disable_recovery = false;
            Ok(ResponseResult::Success)
        }
        SdRequest::DisableRecover => {
            info!("disabling recovery");
            sys.write().await.disable_recovery = true;
            Ok(ResponseResult::Success)
        }
        SdRequest::CompleteRecovery { epoch } => {
            info!("recovery complete for epoch {}", epoch);
            let mut s = sys.write().await;
            if s.recovery_epoch == epoch {
                s.recovery_epoch = 0;
            }
            Ok(ResponseResult::Success)
        }
        SdRequest::GetRecovery => {
            let s = sys.read().await;
            let data = bincode::serialize(&s.recovery_epoch).unwrap_or_default();
            Ok(ResponseResult::Data(data))
        }
        SdRequest::SetRecovery {
            max_exec_count,
            queue_work_interval,
            throttling,
        } => {
            let mut s = sys.write().await;
            s.recovery_max_exec_count = max_exec_count;
            s.recovery_queue_work_interval = queue_work_interval;
            s.recovery_throttling = throttling;
            Ok(ResponseResult::Success)
        }

        SdRequest::KillNode => {
            info!("kill node requested");
            let mut s = sys.write().await;
            s.cinfo.status = sheepdog_proto::node::ClusterStatus::Killed;
            s.shutdown_notify.notify_waiters();
            Ok(ResponseResult::Success)
        }
        SdRequest::Cleanup => {
            info!("cleanup requested");
            let dir = sys.read().await.obj_path();
            cleanup_stale_objects(&dir).await;
            Ok(ResponseResult::Success)
        }

        // --- Cache operations ---
        SdRequest::FlushDelCache { vid } => {
            debug!("flush/del cache for vid {}", vid);
            let s = sys.read().await;
            if let Some(cache) = &s.object_cache {
                let flushed = cache.flush_vid(vid);
                debug!("flushed {} entries for vid {:#x}", flushed.len(), vid);
            }
            Ok(ResponseResult::Success)
        }
        SdRequest::DeleteCache { vid } => {
            debug!("delete cache for vid {}", vid);
            let s = sys.read().await;
            if let Some(cache) = &s.object_cache {
                let _evicted = cache.purge_all();
            }
            Ok(ResponseResult::Success)
        }
        SdRequest::GetCacheInfo => {
            let s = sys.read().await;
            if let Some(cache) = &s.object_cache {
                let stats = cache.stats();
                let data = bincode::serialize(&(
                    stats.entries as u64,
                    stats.capacity as u64,
                    stats.hits,
                    stats.misses,
                    stats.dirty as u64,
                    stats.bytes_cached,
                    stats.evictions,
                    stats.flushes,
                ))
                .unwrap_or_default();
                Ok(ResponseResult::Data(data))
            } else {
                Ok(ResponseResult::Data(Vec::new()))
            }
        }
        SdRequest::CachePurge => {
            info!("cache purge requested");
            let s = sys.read().await;
            if let Some(cache) = &s.object_cache {
                let dirty = cache.purge_all();
                info!("purged cache, {} dirty entries discarded", dirty.len());
            }
            Ok(ResponseResult::Success)
        }

        // --- Multi-disk operations ---
        SdRequest::MdInfo => {
            let s = sys.read().await;
            let disk_info: Vec<(String, u64)> = s
                .md_disks
                .iter()
                .map(|p| (p.display().to_string(), get_path_free_space(p)))
                .collect();
            let data = bincode::serialize(&disk_info).unwrap_or_default();
            Ok(ResponseResult::Data(data))
        }
        SdRequest::MdPlug { path } => {
            info!("MD plug: {}", path);
            let p = std::path::PathBuf::from(&path);
            if !p.is_dir() {
                return Err(SdError::InvalidParms);
            }
            let mut s = sys.write().await;
            if !s.md_disks.iter().any(|d| d == &p) {
                s.md_disks.push(p);
            }
            Ok(ResponseResult::Success)
        }
        SdRequest::MdUnplug { path } => {
            info!("MD unplug: {}", path);
            let p = std::path::PathBuf::from(&path);
            let mut s = sys.write().await;
            s.md_disks.retain(|d| d != &p);
            Ok(ResponseResult::Success)
        }
        SdRequest::Reweight => {
            info!("reweight requested");
            Ok(ResponseResult::Success)
        }

        // --- Logging/tracing ---
        SdRequest::GetLogLevel => Ok(ResponseResult::Data(vec![0])),
        SdRequest::SetLogLevel { level } => {
            info!("set log level: {}", level);
            Ok(ResponseResult::Success)
        }
        SdRequest::Stat => stat_sheep(sys).await,
        SdRequest::TraceEnable => {
            sys.write().await.tracing_enabled = true;
            Ok(ResponseResult::Success)
        }
        SdRequest::TraceDisable => {
            sys.write().await.tracing_enabled = false;
            Ok(ResponseResult::Success)
        }
        SdRequest::TraceStatus => {
            let data = bincode::serialize(&sys.read().await.tracing_enabled).unwrap_or_default();
            Ok(ResponseResult::Data(data))
        }
        SdRequest::TraceReadBuf => Ok(ResponseResult::Data(Vec::new())),

        // --- VDI helpers ---
        SdRequest::DecrefObj { oid, generation, count } => {
            debug!("decref obj: {:?} gen={} count={}", oid, generation, count);
            Ok(ResponseResult::Success)
        }
        SdRequest::PreventInodeUpdate { oid } => {
            debug!("prevent inode update: {:?}", oid);
            Ok(ResponseResult::Success)
        }
        SdRequest::AllowInodeUpdate { oid } => {
            debug!("allow inode update: {:?}", oid);
            Ok(ResponseResult::Success)
        }
        SdRequest::ForwardObj { oid, addr, port } => {
            debug!("forward obj {:?} to {}:{}", oid, addr, port);
            Ok(ResponseResult::Success)
        }

        _ => Err(SdError::NoSupport),
    }
}

async fn get_node_list(sys: SharedSys) -> SdResult<ResponseResult> {
    Ok(ResponseResult::NodeList(sys.read().await.cinfo.nodes.clone()))
}

async fn stat_sheep(sys: SharedSys) -> SdResult<ResponseResult> {
    let s = sys.read().await;
    let nr_nodes = s.cinfo.nodes.len() as u32;
    let store_size = s.this_node.space;
    let store_free = get_path_free_space(&s.dir);
    Ok(ResponseResult::NodeInfo { nr_nodes, store_size, store_free })
}

async fn stat_cluster(sys: SharedSys) -> SdResult<ResponseResult> {
    let data = bincode::serialize(&sys.read().await.cinfo).unwrap_or_default();
    Ok(ResponseResult::Data(data))
}

async fn get_store_list() -> SdResult<ResponseResult> {
    let data = bincode::serialize(&vec!["plain", "tree"]).unwrap_or_default();
    Ok(ResponseResult::Data(data))
}

async fn read_vdis(sys: SharedSys) -> SdResult<ResponseResult> {
    Ok(ResponseResult::Data(sys.read().await.vdi_inuse.as_raw_slice().to_vec()))
}

async fn flush_vdi(sys: SharedSys, vid: u32) -> SdResult<ResponseResult> {
    debug!("flush VDI {:#x}", vid);
    let s = sys.read().await;
    if let Some(cache) = &s.object_cache {
        cache.flush_vid(vid);
    }
    Ok(ResponseResult::Success)
}

async fn get_vdi_copies(sys: SharedSys, vid: u32) -> SdResult<ResponseResult> {
    let s = sys.read().await;
    if let Some(state) = s.vdi_state.get(&vid) {
        Ok(ResponseResult::Vdi { vdi_id: vid, attr_id: 0, copies: state.nr_copies })
    } else {
        Err(SdError::NoVdi)
    }
}

async fn read_del_vdis(sys: SharedSys) -> SdResult<ResponseResult> {
    Ok(ResponseResult::Data(sys.read().await.vdi_deleted.as_raw_slice().to_vec()))
}

fn get_path_free_space(path: &std::path::Path) -> u64 {
    use nix::sys::statvfs::statvfs;
    if let Ok(stat) = statvfs(path) {
        return stat.blocks_available() as u64 * stat.fragment_size() as u64;
    }
    0
}

async fn cleanup_stale_objects(obj_dir: &std::path::Path) {
    if !obj_dir.exists() {
        return;
    }
    let dir = obj_dir.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with(".tmp_") {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
    })
    .await;
}
