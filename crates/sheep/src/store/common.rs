//! Shared helper functions for store operations.
//!
//! Provides utilities for:
//! - Computing object file paths from OIDs
//! - Atomic file writes (write-to-tmp then rename)
//! - Scanning directories for stored objects
//! - Checking available disk space

use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use tracing::{debug, warn};

/// Format an object ID as a hex filename.
///
/// If `ec_index` is non-zero, appends `_XX` suffix for erasure coding shards.
pub fn oid_to_filename(oid: ObjectId, ec_index: u8) -> String {
    if ec_index == 0 {
        format!("{:016x}", oid.raw())
    } else {
        format!("{:016x}_{:02x}", oid.raw(), ec_index)
    }
}

/// Parse an object ID (and optional ec_index) from a filename.
///
/// Returns `None` if the filename is not a valid object name.
pub fn filename_to_oid(name: &str) -> Option<(ObjectId, u8)> {
    if let Some(pos) = name.find('_') {
        // Has ec_index suffix
        let oid_hex = &name[..pos];
        let ec_hex = &name[pos + 1..];
        let raw = u64::from_str_radix(oid_hex, 16).ok()?;
        let ec = u8::from_str_radix(ec_hex, 16).ok()?;
        Some((ObjectId::new(raw), ec))
    } else {
        let raw = u64::from_str_radix(name, 16).ok()?;
        Some((ObjectId::new(raw), 0))
    }
}

/// Build the object file path for the "plain" layout.
///
/// Layout: `{base}/obj/{oid_hex}`
pub fn plain_obj_path(base: &Path, oid: ObjectId, ec_index: u8) -> PathBuf {
    base.join("obj").join(oid_to_filename(oid, ec_index))
}

/// Build the object file path for the "tree" layout.
///
/// Layout: `{base}/obj/{vid:06x}/{oid_hex}`
pub fn tree_obj_path(base: &Path, oid: ObjectId, ec_index: u8) -> PathBuf {
    let vid = oid.to_vid();
    base.join("obj")
        .join(format!("{:06x}", vid))
        .join(oid_to_filename(oid, ec_index))
}

/// Atomic write: write data to a temporary file, then rename into place.
///
/// This ensures that a crash during write doesn't leave a partially written
/// object file. The temp file is created in the same directory as `target`
/// to guarantee same-filesystem rename.
pub fn atomic_write(target: &Path, data: &[u8]) -> SdResult<()> {
    let dir = target.parent().ok_or(SdError::InvalidParms)?;
    fs::create_dir_all(dir).map_err(|_| SdError::Eio)?;

    let tmp_path = dir.join(format!(".tmp_{}", std::process::id()));

    // Write to temp file
    let mut file = fs::File::create(&tmp_path).map_err(|e| {
        warn!("failed to create tmp file {}: {}", tmp_path.display(), e);
        SdError::Eio
    })?;
    file.write_all(data).map_err(|e| {
        warn!("failed to write tmp file {}: {}", tmp_path.display(), e);
        let _ = fs::remove_file(&tmp_path);
        SdError::Eio
    })?;
    file.sync_all().map_err(|e| {
        warn!("failed to sync tmp file {}: {}", tmp_path.display(), e);
        let _ = fs::remove_file(&tmp_path);
        SdError::Eio
    })?;
    drop(file);

    // Atomic rename
    fs::rename(&tmp_path, target).map_err(|e| {
        warn!(
            "failed to rename {} -> {}: {}",
            tmp_path.display(),
            target.display(),
            e
        );
        let _ = fs::remove_file(&tmp_path);
        SdError::Eio
    })?;

    Ok(())
}

/// Write data at a specific offset within an existing file.
///
/// The file must already exist. Missing bytes between the current file
/// size and `offset` are zero-filled implicitly by the OS on seek.
pub fn write_at(path: &Path, offset: u64, data: &[u8]) -> SdResult<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| {
            warn!("failed to open {} for write: {}", path.display(), e);
            SdError::NoObj
        })?;

    file.seek(SeekFrom::Start(offset)).map_err(|e| {
        warn!("failed to seek in {}: {}", path.display(), e);
        SdError::Eio
    })?;

    file.write_all(data).map_err(|e| {
        warn!("failed to write to {}: {}", path.display(), e);
        SdError::Eio
    })?;

    file.sync_all().map_err(|_| SdError::Eio)?;

    Ok(())
}

/// Read data at a specific offset from an existing file.
pub fn read_at(path: &Path, offset: u64, length: usize) -> SdResult<Vec<u8>> {
    let mut file = fs::File::open(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            SdError::NoObj
        } else {
            warn!("failed to open {} for read: {}", path.display(), e);
            SdError::Eio
        }
    })?;

    file.seek(SeekFrom::Start(offset)).map_err(|e| {
        warn!("failed to seek in {}: {}", path.display(), e);
        SdError::Eio
    })?;

    let mut buf = vec![0u8; length];
    let n = file.read(&mut buf).map_err(|e| {
        warn!("failed to read from {}: {}", path.display(), e);
        SdError::Eio
    })?;
    buf.truncate(n);

    Ok(buf)
}

/// Scan a directory for object files and return their OIDs.
///
/// Parses hex filenames into ObjectId values. Non-object files are skipped
/// with a debug-level warning.
pub fn scan_obj_dir(dir: &Path) -> SdResult<Vec<ObjectId>> {
    let mut oids = Vec::new();

    if !dir.exists() {
        return Ok(oids);
    }

    let entries = fs::read_dir(dir).map_err(|e| {
        warn!("failed to read dir {}: {}", dir.display(), e);
        SdError::Eio
    })?;

    for entry in entries {
        let entry = entry.map_err(|_| SdError::Eio)?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Skip hidden/temporary files
        if name_str.starts_with('.') {
            continue;
        }

        if let Some((oid, _ec)) = filename_to_oid(&name_str) {
            oids.push(oid);
        } else {
            debug!("skipping non-object file: {}", name_str);
        }
    }

    Ok(oids)
}

/// Scan the tree layout: iterate over VDI subdirectories and collect all OIDs.
pub fn scan_tree_obj_dir(base_obj_dir: &Path) -> SdResult<Vec<ObjectId>> {
    let mut oids = Vec::new();

    if !base_obj_dir.exists() {
        return Ok(oids);
    }

    let entries = fs::read_dir(base_obj_dir).map_err(|e| {
        warn!("failed to read dir {}: {}", base_obj_dir.display(), e);
        SdError::Eio
    })?;

    for entry in entries {
        let entry = entry.map_err(|_| SdError::Eio)?;
        let file_type = entry.file_type().map_err(|_| SdError::Eio)?;

        if !file_type.is_dir() {
            continue;
        }

        // Each subdirectory is a VDI id in hex
        let sub_dir = entry.path();
        let sub_oids = scan_obj_dir(&sub_dir)?;
        oids.extend(sub_oids);
    }

    Ok(oids)
}

/// Information about disk space usage.
#[derive(Debug, Clone)]
pub struct DiskSpace {
    /// Total disk space in bytes.
    pub total: u64,
    /// Free disk space in bytes.
    pub free: u64,
    /// Available disk space for non-root users in bytes.
    pub available: u64,
}

/// Get disk space information for the filesystem containing `path`.
///
/// Uses `nix::sys::statvfs` on Unix platforms.
pub fn get_disk_space(path: &Path) -> SdResult<DiskSpace> {
    use nix::sys::statvfs::statvfs;

    let stat = statvfs(path).map_err(|e| {
        warn!("statvfs failed for {}: {}", path.display(), e);
        SdError::Eio
    })?;

    let block_size = stat.block_size() as u64;
    Ok(DiskSpace {
        total: stat.blocks() as u64 * block_size,
        free: stat.blocks_free() as u64 * block_size,
        available: stat.blocks_available() as u64 * block_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_filename_roundtrip() {
        let oid = ObjectId::new(0x0000_002a_0000_0064);
        let name = oid_to_filename(oid, 0);
        assert_eq!(name, "0000002a00000064");
        let (parsed_oid, ec) = filename_to_oid(&name).unwrap();
        assert_eq!(parsed_oid, oid);
        assert_eq!(ec, 0);
    }

    #[test]
    fn test_oid_filename_with_ec_index() {
        let oid = ObjectId::new(0x0000_002a_0000_0064);
        let name = oid_to_filename(oid, 3);
        assert_eq!(name, "0000002a00000064_03");
        let (parsed_oid, ec) = filename_to_oid(&name).unwrap();
        assert_eq!(parsed_oid, oid);
        assert_eq!(ec, 3);
    }

    #[test]
    fn test_filename_to_oid_invalid() {
        assert!(filename_to_oid("not_hex_at_all").is_none());
        assert!(filename_to_oid("").is_none());
    }
}
