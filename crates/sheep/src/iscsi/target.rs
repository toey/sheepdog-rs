//! iSCSI target configuration.
//!
//! Represents an iSCSI target with its IQN, LUNs, and authentication settings.
//! This is loaded from the TOML config file.

use iscsi_target::AuthConfig;

/// iSCSI target configuration.
pub struct IscsiTargetConfig {
    /// Target name (IQN format: iqn.yyyy-mm.<reversed domain>:label)
    pub target_name: String,
    /// Target alias (optional human-readable name)
    pub target_alias: Option<String>,
    /// SCSI LUN number (typically 0 for single-LUN targets)
    pub lun: u16,
    /// VDI ID backed by this LUN
    pub vid: u32,
    /// VDI size in bytes
    pub size: u64,
    /// Block size (default: 512)
    pub block_size: u32,
    /// CHAP authentication configuration
    pub auth: AuthConfig,
    /// Maximum concurrent connections for this target (default: 16)
    pub max_connections: u32,
    /// Maximum concurrent sessions for this target (default: 256)
    pub max_sessions: u32,
    /// Allowed initiator IQNs (None = allow all)
    pub allowed_initiators: Option<Vec<String>>,
}
