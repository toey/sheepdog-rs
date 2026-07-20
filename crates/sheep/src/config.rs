//! Cluster configuration persistence and iSCSI daemon configuration.
//!
//! Saves/loads cluster info and epoch logs to disk so that a restarting
//! node can rejoin with the correct epoch.
use std::path::Path;

use serde::Deserialize;

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
#[allow(dead_code)]
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
#[allow(dead_code)]
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

// ================================================================
// iSCSI Daemon Configuration
// ================================================================

/// Local deserializable auth config for TOML parsing.
#[derive(Deserialize, Default, Clone, Debug)]
#[cfg_attr(not(feature = "iscsi"), allow(dead_code))]
pub struct IscsiAuthConfig {
    #[serde(default)]
    pub auth_type: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub target_username: Option<String>,
    #[serde(default)]
    pub initiator_secret: Option<String>,
}

#[cfg(feature = "iscsi")]
impl IscsiAuthConfig {
    /// Convert to iscsi_target::AuthConfig.
    pub fn to_auth_config(&self) -> iscsi_target::AuthConfig {
        match self.auth_type.as_deref() {
            Some("chap") => {
                let username = self.username.as_deref().unwrap_or("default");
                let secret = self.secret.as_deref().unwrap_or_default();
                if secret.is_empty() {
                    tracing::warn!("CHAP secret is empty for target, using empty string");
                }
                iscsi_target::AuthConfig::Chap {
                    credentials: iscsi_target::ChapCredentials::new(username, secret),
                }
            },
            Some("mutual_chap") => {
                let target_username = self.target_username.as_deref().unwrap_or("default");
                let target_secret = self.secret.as_deref().unwrap_or_default();
                let initiator_username = self.username.as_deref().unwrap_or("initiator");
                let initiator_secret = self.initiator_secret.as_deref().unwrap_or_default();

                if target_secret.is_empty() || initiator_secret.is_empty() {
                    tracing::warn!("Mutual CHAP: one or more secrets are empty");
                }

                iscsi_target::AuthConfig::MutualChap {
                    target_credentials: iscsi_target::ChapCredentials::new(target_username, target_secret),
                    initiator_credentials: iscsi_target::ChapCredentials::new(initiator_username, initiator_secret),
                }
            },
            _ => iscsi_target::AuthConfig::None,
        }
    }
}

/// Full TOML config file format.
#[derive(Deserialize, Default, Clone, Debug)]
#[cfg_attr(not(feature = "iscsi"), allow(dead_code))]
pub struct TomlConfig {
    #[serde(default)]
    pub iscsi: IscsiConfig,
}

/// iSCSI daemon configuration.
#[derive(Deserialize, Default, Clone, Debug)]
#[cfg_attr(not(feature = "iscsi"), allow(dead_code))]
pub struct IscsiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    #[serde(default)]
    pub luns: Vec<LunConfig>,
}

fn default_listen_address() -> String {
    "0.0.0.0:3260".to_string()
}

/// Per-LUN configuration (one LUN = one iSCSI target).
#[derive(Deserialize, Default, Clone, Debug)]
#[cfg_attr(not(feature = "iscsi"), allow(dead_code))]
pub struct LunConfig {
    #[serde(default)]
    pub target_name: String,
    #[serde(default)]
    pub target_alias: Option<String>,
    #[serde(default)]
    pub lun: u16,
    #[serde(default)]
    pub vid: u32,
    #[serde(default)]
    pub size: u64,
    #[serde(default = "default_block_size")]
    pub block_size: u32,
    #[serde(default)]
    pub auth: IscsiAuthConfig,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_max_sessions")]
    pub max_sessions: u32,
    #[serde(default)]
    pub allowed_initiators: Option<Vec<String>>,
}

fn default_block_size() -> u32 {
    512
}

fn default_max_connections() -> u32 {
    16
}

fn default_max_sessions() -> u32 {
    256
}

#[cfg(feature = "iscsi")]
impl LunConfig {
    /// Helper to create a CHAP-authenticated LUN config.
    pub fn with_chap(mut self, username: impl Into<String>, secret: impl Into<String>) -> Self {
        self.auth = IscsiAuthConfig {
            auth_type: Some("chap".to_string()),
            username: Some(username.into()),
            secret: Some(secret.into()),
            ..Default::default()
        };
        self
    }
}
