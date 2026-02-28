//! Cluster configuration persistence.
//!
//! Saves/loads cluster info and epoch logs to disk so that a restarting
//! node can rejoin with the correct epoch.

use std::path::Path;

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::node::{ClusterInfo, EpochLog, SdNode};
use tokio::fs;
use tracing::{debug, error, warn};

/// Save cluster info to the config file.
pub async fn save_config(dir: &Path, cinfo: &ClusterInfo) -> SdResult<()> {
    let path = dir.join("config");
    let data = bincode::serialize(cinfo).map_err(|_| SdError::SystemError)?;
    fs::write(&path, &data).await.map_err(|e| {
        error!("failed to write config to {}: {}", path.display(), e);
        SdError::Eio
    })?;
    debug!("saved config: epoch={}", cinfo.epoch);
    Ok(())
}

/// Load cluster info from the config file.
pub async fn load_config(dir: &Path) -> SdResult<ClusterInfo> {
    let path = dir.join("config");
    if !path.exists() {
        return Err(SdError::NotFormatted);
    }
    let data = fs::read(&path).await.map_err(|e| {
        error!("failed to read config from {}: {}", path.display(), e);
        SdError::Eio
    })?;
    bincode::deserialize(&data).map_err(|_| {
        error!("corrupt config file: {}", path.display());
        SdError::SystemError
    })
}

/// Save an epoch log entry.
pub async fn save_epoch_log(dir: &Path, log: &EpochLog) -> SdResult<()> {
    let epoch_dir = dir.join("epoch");
    fs::create_dir_all(&epoch_dir).await.map_err(|_| SdError::Eio)?;

    let path = epoch_dir.join(format!("{:08}", log.epoch));
    let data = bincode::serialize(log).map_err(|_| SdError::SystemError)?;
    fs::write(&path, &data).await.map_err(|e| {
        error!("failed to write epoch log {}: {}", log.epoch, e);
        SdError::Eio
    })?;
    debug!("saved epoch log: epoch={}, nodes={}", log.epoch, log.nodes.len());
    Ok(())
}

/// Load an epoch log for a specific epoch.
pub async fn load_epoch_log(dir: &Path, epoch: u32) -> SdResult<EpochLog> {
    let path = dir.join("epoch").join(format!("{:08}", epoch));
    if !path.exists() {
        return Err(SdError::InvalidEpoch);
    }
    let data = fs::read(&path).await.map_err(|_| SdError::Eio)?;
    bincode::deserialize(&data).map_err(|_| {
        error!("corrupt epoch log: {}", path.display());
        SdError::SystemError
    })
}

/// Get the latest epoch number from disk.
pub async fn get_latest_epoch(dir: &Path) -> SdResult<u32> {
    let epoch_dir = dir.join("epoch");
    if !epoch_dir.exists() {
        return Ok(0);
    }

    let mut latest = 0u32;
    let mut entries = fs::read_dir(&epoch_dir).await.map_err(|_| SdError::Eio)?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(epoch) = name.parse::<u32>() {
                latest = latest.max(epoch);
            }
        }
    }
    Ok(latest)
}

/// Get node list for a specific epoch.
pub async fn get_epoch_nodes(dir: &Path, epoch: u32) -> SdResult<Vec<SdNode>> {
    let log = load_epoch_log(dir, epoch).await?;
    Ok(log.nodes)
}

/// Build an EpochLog from the current cluster info.
pub fn build_epoch_log(cinfo: &ClusterInfo) -> EpochLog {
    EpochLog {
        ctime: cinfo.ctime,
        time: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        epoch: cinfo.epoch,
        disable_recovery: cinfo.disable_recovery,
        nr_copies: cinfo.nr_copies,
        copy_policy: cinfo.copy_policy,
        flags: cinfo.flags,
        drv_name: cinfo.default_store.clone(),
        nodes: cinfo.nodes.clone(),
    }
}

/// Remove epoch logs for epochs strictly greater than the given epoch.
pub async fn remove_epoch_logs_after(dir: &Path, epoch: u32) -> SdResult<()> {
    let epoch_dir = dir.join("epoch");
    if !epoch_dir.exists() {
        return Ok(());
    }

    let mut entries = fs::read_dir(&epoch_dir).await.map_err(|_| SdError::Eio)?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(e) = name.parse::<u32>() {
                if e > epoch {
                    warn!("removing stale epoch log: {}", e);
                    let _ = fs::remove_file(entry.path()).await;
                }
            }
        }
    }
    Ok(())
}
