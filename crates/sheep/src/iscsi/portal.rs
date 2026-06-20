//! iSCSI network portal configuration.
//!
//! Defines the listen address and related network settings for the iSCSI target.

/// iSCSI portal configuration.
pub struct IscsiPortalConfig {
    /// Listen address (default: "0.0.0.0:3260")
    pub listen_address: String,
}

impl Default for IscsiPortalConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0:3260".to_string(),
        }
    }
}
