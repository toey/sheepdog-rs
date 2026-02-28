//! NFS filesystem abstraction layer.
//!
//! Maps NFS operations to sheepdog VDI data operations.

use std::collections::BTreeMap;

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;

/// File type in the NFS filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Regular,
    Directory,
    Symlink,
}

/// File attributes (like stat).
#[derive(Debug, Clone)]
pub struct FileAttr {
    pub ino: u64,
    pub size: u64,
    pub file_type: FileType,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
}

impl FileAttr {
    pub fn root_dir() -> Self {
        Self {
            ino: 1,
            size: 4096,
            file_type: FileType::Directory,
            mode: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
        }
    }
}

/// Directory entry.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub ino: u64,
    pub file_type: FileType,
}

/// NFS filesystem state for a single VDI export.
pub struct NfsFilesystem {
    /// VDI ID backing this filesystem.
    pub vid: u32,
    /// Inode allocation counter.
    next_ino: u64,
    /// Inode number → file attributes.
    attrs: BTreeMap<u64, FileAttr>,
    /// Parent inode → child entries.
    dir_entries: BTreeMap<u64, Vec<DirEntry>>,
    /// Inode → sheepdog object ID mapping.
    oid_map: BTreeMap<u64, ObjectId>,
}

impl NfsFilesystem {
    pub fn new(vid: u32) -> Self {
        let mut attrs = BTreeMap::new();
        attrs.insert(1, FileAttr::root_dir());

        Self {
            vid,
            next_ino: 2,
            attrs,
            dir_entries: BTreeMap::new(),
            oid_map: BTreeMap::new(),
        }
    }

    /// Get file attributes by inode number.
    pub fn getattr(&self, ino: u64) -> SdResult<&FileAttr> {
        self.attrs.get(&ino).ok_or(SdError::NoObj)
    }

    /// Lookup a name in a directory.
    pub fn lookup(&self, parent_ino: u64, name: &str) -> SdResult<u64> {
        if let Some(entries) = self.dir_entries.get(&parent_ino) {
            for entry in entries {
                if entry.name == name {
                    return Ok(entry.ino);
                }
            }
        }
        Err(SdError::NoObj)
    }

    /// Read directory entries.
    pub fn readdir(&self, ino: u64) -> SdResult<Vec<DirEntry>> {
        Ok(self.dir_entries.get(&ino).cloned().unwrap_or_default())
    }

    /// Create a file in a directory.
    pub fn create(&mut self, parent_ino: u64, name: &str, mode: u32) -> SdResult<u64> {
        let ino = self.next_ino;
        self.next_ino += 1;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        self.attrs.insert(
            ino,
            FileAttr {
                ino,
                size: 0,
                file_type: FileType::Regular,
                mode,
                nlink: 1,
                uid: 0,
                gid: 0,
                atime: now,
                mtime: now,
                ctime: now,
            },
        );

        let entries = self.dir_entries.entry(parent_ino).or_default();
        entries.push(DirEntry {
            name: name.to_string(),
            ino,
            file_type: FileType::Regular,
        });

        Ok(ino)
    }

    /// Remove a file from a directory.
    pub fn remove(&mut self, parent_ino: u64, name: &str) -> SdResult<u64> {
        let entries = self.dir_entries.get_mut(&parent_ino).ok_or(SdError::NoObj)?;
        let pos = entries
            .iter()
            .position(|e| e.name == name)
            .ok_or(SdError::NoObj)?;
        let entry = entries.remove(pos);
        let ino = entry.ino;
        self.attrs.remove(&ino);
        self.oid_map.remove(&ino);
        Ok(ino)
    }

    /// Map an inode to a sheepdog object ID.
    pub fn set_oid(&mut self, ino: u64, oid: ObjectId) {
        self.oid_map.insert(ino, oid);
    }

    /// Get the sheepdog object ID for an inode.
    pub fn get_oid(&self, ino: u64) -> Option<ObjectId> {
        self.oid_map.get(&ino).copied()
    }
}
