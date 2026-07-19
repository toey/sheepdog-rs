//! iSCSI target configuration and lifecycle management.
//!
//! Provides managed iSCSI target handles backed by `SheepdogScsiBlockDevice`,
//! with lifecycle control (spawn, shutdown, join) via background OS threads.

#[cfg(feature = "iscsi")]
use std::sync::Arc;

#[cfg(feature = "iscsi")]
use iscsi_target::IscsiTarget as SdIscsiTarget;
#[cfg(feature = "iscsi")]
use tracing::{error, info, warn};

#[cfg(feature = "iscsi")]
use crate::iscsi::block_device::SheepdogScsiBlockDevice;

/// Per-LUN managed target with lifecycle control.
///
/// The handle stores an `Arc<SdIscsiTarget<D>>` shared with the background thread.
/// The background thread calls `.run()` on the Arc guard; the handle calls `.stop()` on it.
///
/// Fields:
///   - `target: Arc<SdIscsiTarget<SheepdogScsiBlockDevice>>` — shared reference for .stop()
///   - `thread: Option<std::thread::JoinHandle<()>>` — for join-on-shutdown
///   - `target_name: String` — metadata for logging
///   - `target_alias: Option<String>` — metadata for logging
///   - `vid: u32` — metadata for logging
///   - `size: u64` — VDI size in bytes
///   - `block_size: u32` — block size
///   - `chap_enabled: bool` — whether CHAP authentication is enabled
#[cfg(feature = "iscsi")]
pub struct IscsiTargetHandle {
    /// Shared reference to the underlying iSCSI target. Both the handle and the
    /// background thread hold clones of this Arc. The handle uses it to call .stop().
    target: Arc<SdIscsiTarget<SheepdogScsiBlockDevice>>,
    /// Background thread handle for join-on-shutdown.
    thread: Option<std::thread::JoinHandle<()>>,
    /// The target IQN name (for logging).
    target_name: String,
    /// The target alias (for logging/listing).
    target_alias: Option<String>,
    /// The VID this serves.
    vid: u32,
    /// The VDI size in bytes.
    size: u64,
    /// The block size.
    block_size: u32,
    /// Whether CHAP authentication is enabled.
    chap_enabled: bool,
}

#[cfg(feature = "iscsi")]
impl IscsiTargetHandle {
    /// Get the VDI ID this target serves.
    pub fn vid(&self) -> u32 {
        self.vid
    }

    /// Get the target name.
    pub fn target_name(&self) -> &str {
        &self.target_name
    }

    /// Get the target alias.
    pub fn target_alias(&self) -> &Option<String> {
        &self.target_alias
    }

    /// Get the VDI size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the block size.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Check if CHAP authentication is enabled.
    pub fn chap_enabled(&self) -> bool {
        self.chap_enabled
    }

    /// Signal the underlying target to shut down gracefully.
    ///
    /// Calls .stop() on the Arc-shared target. Since .stop() takes &self and the
    /// target's internals use Arc<AtomicBool>, this works even if the thread is
    /// currently inside .run(). No explicit shutdown flag is needed — .stop()
    /// sets the iscsi-crate's internal AtomicBool directly.
    pub fn shutdown(&self) {
        self.target.stop();
    }

    /// Join the background thread. Takes ownership of the handle.
    pub fn join(&mut self) {
        if let Some(h) = self.thread.take() {
            match h.join() {
                Ok(()) => info!("iSCSI target thread joined cleanly for VID {:x}", self.vid),
                Err(_) => error!("iSCSI target thread for VID {:x} panicked", self.vid),
            }
        }
    }
}

/// Manages the lifecycle of one or more iSCSI targets.
/// This is the top-level struct returned by `start_iscsi_server()`
/// and passed to main.rs for shutdown coordination.
#[cfg(feature = "iscsi")]
pub struct IscsiServer {
    /// Per-LUN entries with their managed targets and thread handles.
    pub targets: Vec<IscsiTargetHandle>,
}

#[cfg(feature = "iscsi")]
impl IscsiServer {
    /// Initiate graceful shutdown of all targets and join threads.
    ///
    /// Takes `&mut self` because `join(&mut self)` on each entry needs
    /// mutable access to take ownership of the JoinHandle.
    pub fn shutdown_all(&mut self) {
        for entry in &mut self.targets {
            info!("Shutting down iSCSI target for VID {:x}", entry.vid);
            entry.shutdown();
            entry.join();
            info!("iSCSI target stopped for VID {:x}", entry.vid);
        }
        self.targets.clear();
    }
}

#[cfg(feature = "iscsi")]
impl Drop for IscsiServer {
    fn drop(&mut self) {
        if !self.targets.is_empty() {
            warn!("IscsiServer dropped without explicit shutdown — calling shutdown_all()");
            self.shutdown_all();
        }
    }
}

/// Builder for managed iSCSI targets.
///
/// Provides a fluent API to configure bind address, target name, authentication,
/// and connection/session limits before spawning the target on a background thread.
#[cfg(feature = "iscsi")]
pub struct IscsiTargetBuilder {
    bind_addr: String,
    target_name: String,
    auth_config: iscsi_target::AuthConfig,
    max_connections: u32,
    max_sessions: u32,
}

#[cfg(feature = "iscsi")]
impl IscsiTargetBuilder {
    /// Create a new builder with the given bind address and target name.
    pub fn new(bind_addr: String, target_name: String) -> Self {
        Self {
            bind_addr,
            target_name,
            auth_config: iscsi_target::AuthConfig::None,
            max_connections: 16,
            max_sessions: 256,
        }
    }

    /// Set the authentication configuration.
    pub fn with_auth(mut self, auth: iscsi_target::AuthConfig) -> Self {
        self.auth_config = auth;
        self
    }

    /// Set the maximum number of concurrent connections.
    pub fn with_max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    /// Set the maximum number of concurrent sessions.
    pub fn with_max_sessions(mut self, n: u32) -> Self {
        self.max_sessions = n;
        self
    }

    /// Build the managed target and spawn it on a background thread.
    ///
    /// Returns `Ok(IscsiTargetHandle)` on success, or an error if the underlying
    /// iscsi-crate target fails to build.
    ///
    /// **Ownership model:** `SdIscsiTarget` is wrapped in `Arc` and shared between
    /// the handle (for .stop()) and the background thread (for .run()). Both .run()
    /// and .stop() take &self, so no mutex is needed for the target itself.
    pub fn build(
        self,
        device: SheepdogScsiBlockDevice,
    ) -> Result<IscsiTargetHandle, iscsi_target::error::IscsiError> {
        let vid = device.vid();
        let size = device.size();
        let block_size = device.block_size();
        let target_name = self.target_name.clone();

        // Check if CHAP is enabled based on auth_config
        let chap_enabled = matches!(self.auth_config, iscsi_target::AuthConfig::Chap { .. });

        // Build the underlying iscsi-crate target.
        // iscsi-crate's `build()` takes `self` (consuming the builder) and
        // returns `ScsiResult<IscsiTarget<D>>` — the `?` handles potential build failures.
        let sd_target: SdIscsiTarget<SheepdogScsiBlockDevice> =
            SdIscsiTarget::builder()
                .bind_addr(&self.bind_addr)
                .target_name(&self.target_name)
                .with_auth(self.auth_config)
                .max_connections(self.max_connections)
                .max_sessions(self.max_sessions)
                .build(device)?;

        // Wrap in Arc for shared ownership between handle and thread
        let shared_target = Arc::new(sd_target);

        // Clone for the thread closure
        let target_for_thread = Arc::clone(&shared_target);

        let thread = std::thread::Builder::new()
            .name(format!("iscsi-target-{:x}", vid))
            .spawn(move || {
                info!("iSCSI target thread started for VID {vid:x}");

                // .run() takes &self — call it directly on the Arc guard.
                // No .take() — the Arc keeps the target alive for both
                // the thread (run) and the handle (stop).
                let result = target_for_thread.run();

                match result {
                    Ok(()) => info!("iSCSI target stopped cleanly for VID {vid:x}"),
                    Err(e) => error!("iSCSI target error for VID {vid:x}: {e}"),
                }
            })
            .expect("failed to spawn iSCSI target thread");

        Ok(IscsiTargetHandle {
            target: shared_target,
            thread: Some(thread),
            target_name,
            target_alias: None, // Default to no alias
            vid,
            size,
            block_size,
            chap_enabled,
        })
    }
}

#[cfg(feature = "iscsi")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{IscsiAuthConfig, LunConfig};

    /// Test 1: IscsiTargetBuilder::new() creates builder with correct defaults.
    #[test]
    fn test_builder_defaults() {
        let builder = IscsiTargetBuilder::new(
            "127.0.0.1:3260".to_string(),
            "iqn.2025-01.io.github:sheepdog.test".to_string(),
        );
        // Verify builder was created successfully with correct defaults
        assert_eq!(builder.max_connections, 16);
        assert_eq!(builder.max_sessions, 256);
        assert!(matches!(builder.auth_config, iscsi_target::AuthConfig::None));
    }

    /// Test 2: with_auth() sets auth config correctly (builder pattern returns self).
    #[test]
    fn test_builder_with_auth() {
        let auth = iscsi_target::AuthConfig::Chap {
            credentials: iscsi_target::ChapCredentials::new("user", "secret"),
        };
        let builder = IscsiTargetBuilder::new("127.0.0.1:3260".to_string(), "iqn.test".to_string())
            .with_auth(auth);
        // Verify builder pattern returns self with CHAP auth configured
        assert!(matches!(builder.auth_config, iscsi_target::AuthConfig::Chap { .. }));
    }

    /// Test 3: with_max_connections() sets max connections.
    #[test]
    fn test_builder_with_max_connections() {
        let builder = IscsiTargetBuilder::new("127.0.0.1:3260".to_string(), "iqn.test".to_string())
            .with_max_connections(32);
        assert_eq!(builder.max_connections, 32);
    }

    /// Test 4: with_max_sessions() sets max sessions.
    #[test]
    fn test_builder_with_max_sessions() {
        let builder = IscsiTargetBuilder::new("127.0.0.1:3260".to_string(), "iqn.test".to_string())
            .with_max_sessions(128);
        assert_eq!(builder.max_sessions, 128);
    }

    /// Test 5: IscsiAuthConfig::to_auth_config() — None auth returns AuthConfig::None.
    #[test]
    fn test_auth_config_none() {
        let auth_config = IscsiAuthConfig::default();
        let result = auth_config.to_auth_config();
        assert!(matches!(result, iscsi_target::AuthConfig::None));
    }

    /// Test 6: IscsiAuthConfig::to_auth_config() — Chap auth returns correct AuthConfig.
    #[test]
    fn test_auth_config_chap() {
        let auth_config = IscsiAuthConfig {
            auth_type: Some("chap".to_string()),
            username: Some("myuser".to_string()),
            secret: Some("mysecret".to_string()),
            ..Default::default()
        };
        let result = auth_config.to_auth_config();
        assert!(matches!(
            result,
            iscsi_target::AuthConfig::Chap { .. }
        ));
    }

    /// Test 7: IscsiAuthConfig::to_auth_config() — MutualChap auth returns correct AuthConfig.
    #[test]
    fn test_auth_config_mutual_chap() {
        let auth_config = IscsiAuthConfig {
            auth_type: Some("mutual_chap".to_string()),
            username: Some("initiator".to_string()),
            secret: Some("target_secret".to_string()),
            target_username: Some("target".to_string()),
            initiator_secret: Some("initiator_secret".to_string()),
        };
        let result = auth_config.to_auth_config();
        assert!(matches!(
            result,
            iscsi_target::AuthConfig::MutualChap { .. }
        ));
    }

    /// Test 8: IscsiAuthConfig::to_auth_config() — Chap with empty secret uses defaults.
    #[test]
    fn test_auth_config_chap_empty_secret() {
        let auth_config = IscsiAuthConfig {
            auth_type: Some("chap".to_string()),
            username: None, // Will use "default"
            secret: None,   // Will use empty string
            ..Default::default()
        };
        let result = auth_config.to_auth_config();
        assert!(matches!(
            result,
            iscsi_target::AuthConfig::Chap { .. }
        ));
    }

    /// Test 9: LunConfig::with_chap() builds a CHAP-authenticated config.
    #[test]
    fn test_lun_config_with_chap() {
        let lun = LunConfig::default().with_chap("testuser", "testsecret");
        assert_eq!(lun.auth.auth_type, Some("chap".to_string()));
        assert_eq!(lun.auth.username, Some("testuser".to_string()));
        assert_eq!(lun.auth.secret, Some("testsecret".to_string()));
    }
}
