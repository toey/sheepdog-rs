//! iSCSI target configuration and lifecycle management.
//!
//! Provides managed iSCSI target handles backed by `SheepdogScsiBlockDevice`,
//! with lifecycle control (spawn, shutdown, join) via background OS threads.

use std::sync::Arc;

use iscsi_target::IscsiTarget as SdIscsiTarget;
use tracing::{error, info, warn};

use crate::iscsi::block_device::SheepdogScsiBlockDevice;

/// iSCSI target configuration.
///
/// **Deprecated:** Use [`IscsiConfig`]/[`LunConfig`] (config.rs) for TOML parsing,
/// or [`IscsiTargetHandle`] for runtime. This struct has no consumer.
#[deprecated(note = "Use IscsiConfig/LunConfig (config.rs) for TOML parsing, or IscsiTargetHandle for runtime. This struct has no consumer.")]
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
    pub auth: iscsi_target::AuthConfig,
    /// Maximum concurrent connections for this target (default: 16)
    pub max_connections: u32,
    /// Maximum concurrent sessions for this target (default: 256)
    pub max_sessions: u32,
    /// Allowed initiator IQNs (None = allow all)
    pub allowed_initiators: Option<Vec<String>>,
}

/// Per-LUN managed target with lifecycle control.
///
/// The handle stores an `Arc<SdIscsiTarget<D>>` shared with the background thread.
/// The background thread calls `.run()` on the Arc guard; the handle calls `.stop()` on it.
///
/// Fields:
///   - `target: Arc<SdIscsiTarget<SheepdogScsiBlockDevice>>` — shared reference for .stop()
///   - `thread: Option<std::thread::JoinHandle<()>>` — for join-on-shutdown
///   - `target_name: String` — metadata for logging
///   - `vid: u32` — metadata for logging
pub struct IscsiTargetHandle {
    /// Shared reference to the underlying iSCSI target. Both the handle and the
    /// background thread hold clones of this Arc. The handle uses it to call .stop().
    target: Arc<SdIscsiTarget<SheepdogScsiBlockDevice>>,
    /// Background thread handle for join-on-shutdown.
    thread: Option<std::thread::JoinHandle<()>>,
    /// The target IQN name (for logging).
    target_name: String,
    /// The VID this serves.
    vid: u32,
}

impl IscsiTargetHandle {
    /// Get the VDI ID this target serves.
    pub fn vid(&self) -> u32 {
        self.vid
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
pub struct IscsiServer {
    /// Per-LUN entries with their managed targets and thread handles.
    pub targets: Vec<IscsiTargetHandle>,
}

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
pub struct IscsiTargetBuilder {
    bind_addr: String,
    target_name: String,
    auth_config: iscsi_target::AuthConfig,
    max_connections: u32,
    max_sessions: u32,
}

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
        let target_name = self.target_name.clone();

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
            vid,
        })
    }
}
