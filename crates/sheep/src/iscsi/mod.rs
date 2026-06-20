//! iSCSI target implementation.
//!
//! Provides an iSCSI target driver using the `iscsi-target` library.
//! Bridges SCSI commands (INQUIRY, READ/WRITE, etc.) to Sheepdog's
//! distributed storage via the `SheepdogScsiBlockDevice` adapter.

mod block_device;
mod command;
mod logical_device;
mod portal;
mod target;

pub use block_device::SheepdogScsiBlockDevice;
pub use portal::IscsiPortalConfig;
pub use target::IscsiTargetConfig;

// Re-export iscsi_target types used in config
pub use iscsi_target::AuthConfig;
pub use iscsi_target::ChapCredentials;
