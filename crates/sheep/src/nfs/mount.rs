//! NFS MOUNT protocol handler.
//!
//! The MOUNT protocol is used by NFS clients to discover exports
//! and obtain initial file handles for mounting.

use sheepdog_proto::error::{SdError, SdResult};
use tracing::debug;

/// Mount protocol procedure numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum MountProc {
    Null = 0,
    Mnt = 1,
    Dump = 2,
    Umnt = 3,
    UmntAll = 4,
    Export = 5,
}

/// Export entry — describes a VDI available for NFS mounting.
#[derive(Debug, Clone)]
pub struct ExportEntry {
    pub path: String,
    pub vid: u32,
}

/// Handle a MOUNT/MNT request — returns a file handle for the VDI root.
pub fn handle_mnt(path: &str, exports: &[ExportEntry]) -> SdResult<Vec<u8>> {
    debug!("MOUNT MNT: {}", path);
    for export in exports {
        if export.path == path {
            // Return root file handle for this VDI
            let handle = super::handler::NfsFileHandle::root(export.vid);
            return Ok(handle.to_bytes());
        }
    }
    Err(SdError::NoVdi)
}

/// Handle an EXPORT request — list available exports.
pub fn handle_export(exports: &[ExportEntry]) -> Vec<String> {
    exports.iter().map(|e| e.path.clone()).collect()
}
