//! iSCSI target implementation.
//!
//! Provides an iSCSI target driver using the `iscsi-target` library.
#![allow(dead_code)]
//! Bridges SCSI commands (INQUIRY, READ/WRITE, etc.) to Sheepdog's
//! distributed storage via the `SheepdogScsiBlockDevice` adapter.

mod block_device;
mod command;
mod logical_device;
mod portal;
mod target;

pub use block_device::SheepdogScsiBlockDevice;
pub use target::{IscsiServer, IscsiTargetBuilder, IscsiTargetHandle};

use tracing::{info, warn};

use crate::config::IscsiConfig;

/// Start all iSCSI targets configured in `config`.
/// Returns `Ok(IscsiServer)` on success, or propagates build errors from the underlying iscsi-crate (IscsiError).
///
/// **Error handling:** If a LUN fails to build, this function shuts down any previously-succeeded
/// targets before returning the error.
pub fn start_iscsi_server(
    sys: crate::daemon::SharedSys,
    config: IscsiConfig,
    handle: tokio::runtime::Handle,
) -> Result<IscsiServer, iscsi_target::error::IscsiError> {
    if !config.enabled {
        info!("iSCSI target disabled in config");
        return Ok(IscsiServer {
            targets: Vec::new(),
        });
    }

    if config.luns.is_empty() {
        warn!("iSCSI target enabled but no LUNs configured");
        return Ok(IscsiServer {
            targets: Vec::new(),
        });
    }

    let mut entries: Vec<IscsiTargetHandle> = Vec::new();

    for lun_cfg in &config.luns {
        let vid = lun_cfg.vid;

        // Create the block device for this LUN
        let device = SheepdogScsiBlockDevice::new(
            lun_cfg.vid,
            lun_cfg.size,
            lun_cfg.block_size,
            sys.clone(),
            handle.clone(),
        );

        // Convert config auth to iscsi_target::AuthConfig
        let auth_config = lun_cfg.auth.to_auth_config();

        // Build and spawn the target
        let entry = match IscsiTargetBuilder::new(
            config.listen_address.clone(),
            lun_cfg.target_name.clone(),
        )
        .with_auth(auth_config)
        .with_max_connections(lun_cfg.max_connections)
        .with_max_sessions(lun_cfg.max_sessions)
        .build(device) {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to build iSCSI target for VID {:x}: {}", vid, e);
                // Shut down already-built targets
                for prev in entries {
                    warn!("Shutting down previously-built target for VID {:x}", prev.vid());
                    prev.shutdown();
                }
                return Err(e);
            }
        };

        entries.push(entry);
    }

    info!("Started {} iSCSI target(s)", entries.len());
    Ok(IscsiServer { targets: entries })
}
