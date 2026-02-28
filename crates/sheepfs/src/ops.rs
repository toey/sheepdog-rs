//! FUSE operation implementations.
//!
//! Implements the fuser::Filesystem trait to serve sheepdog data
//! through a FUSE mount. Connects to the local sheep daemon to
//! read VDI data and metadata.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::time::{Duration, Instant, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use tracing::{debug, warn};

use crate::config::SheepfsConfig;
use crate::volume::VdiVolume;

/// FUSE inode numbers.
const ROOT_INO: u64 = 1;
const VDI_DIR_INO: u64 = 2;
const NODE_DIR_INO: u64 = 3;

/// TTL for cached attributes.
const TTL: Duration = Duration::from_secs(10);

/// The sheepfs FUSE filesystem.
pub struct SheepFs {
    config: SheepfsConfig,
    /// Cached VDI list (refreshed periodically).
    vdis: BTreeMap<u32, VdiVolume>,
    /// VDI name → inode mapping (inodes start at 100).
    vdi_inodes: BTreeMap<String, u64>,
    /// Inode → VDI VID mapping.
    ino_to_vid: BTreeMap<u64, u32>,
    /// Next available inode.
    next_ino: u64,
    /// Last time VDI list was refreshed.
    last_refresh: Option<Instant>,
    /// Tokio runtime for async sheep daemon communication.
    rt: tokio::runtime::Runtime,
}

impl SheepFs {
    pub fn new(config: SheepfsConfig) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for sheepfs");

        Self {
            config,
            vdis: BTreeMap::new(),
            vdi_inodes: BTreeMap::new(),
            ino_to_vid: BTreeMap::new(),
            next_ino: 100,
            last_refresh: None,
            rt,
        }
    }

    fn root_attr(&self) -> FileAttr {
        FileAttr {
            ino: ROOT_INO,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 3,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn dir_attr(&self, ino: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn vdi_file_attr(&self, ino: u64, size: u64) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o444,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Refresh VDI list from the sheep daemon if cache has expired.
    fn maybe_refresh_vdis(&mut self) {
        let should_refresh = match self.last_refresh {
            Some(t) => t.elapsed() > self.config.cache_timeout,
            None => true,
        };

        if !should_refresh {
            return;
        }

        debug!("refreshing VDI list from sheep daemon");

        match self.rt.block_on(Self::fetch_vdi_list(self.config.sheep_addr)) {
            Ok(vdi_list) => self.apply_vdi_list(vdi_list),
            Err(e) => warn!("failed to refresh VDI list: {}", e),
        }

        self.last_refresh = Some(Instant::now());
    }

    fn apply_vdi_list(&mut self, vdi_list: Vec<VdiVolume>) {
        for vdi in vdi_list {
            if !self.vdi_inodes.contains_key(&vdi.name) {
                let ino = self.next_ino;
                self.next_ino += 1;
                self.vdi_inodes.insert(vdi.name.clone(), ino);
                self.ino_to_vid.insert(ino, vdi.vid);
            }
            self.vdis.insert(vdi.vid, vdi);
        }
    }

    /// Fetch VDI list from the sheep daemon.
    async fn fetch_vdi_list(
        addr: std::net::SocketAddr,
    ) -> Result<Vec<VdiVolume>, Box<dyn std::error::Error>> {
        use sheepdog_proto::constants::SD_PROTO_VER;
        use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse, ResponseResult};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = sheepdog_core::net::connect_to_addr(addr).await?;

        let header = RequestHeader {
            proto_ver: SD_PROTO_VER,
            epoch: 0,
            id: 1,
        };
        let req = SdRequest::ReadVdis;
        let req_data = bincode::serialize(&(header, req))?;

        stream.write_u32(req_data.len() as u32).await?;
        stream.write_all(&req_data).await?;

        let resp_len = stream.read_u32().await? as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;

        let response: SdResponse = bincode::deserialize(&resp_buf)?;

        match response.result {
            ResponseResult::Data(bitmap) => {
                let mut vdis = Vec::new();
                for (byte_idx, byte) in bitmap.iter().enumerate() {
                    for bit in 0..8u32 {
                        if (byte >> (7 - bit)) & 1 == 1 {
                            let vid = (byte_idx * 8 + bit as usize) as u32;
                            vdis.push(VdiVolume {
                                vid,
                                name: format!("vdi_{:#x}", vid),
                                size: sheepdog_proto::constants::SD_DATA_OBJ_SIZE * 1024,
                                copies: 3,
                                snapshot: false,
                                snap_id: 0,
                            });
                        }
                    }
                }
                Ok(vdis)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Read VDI data from the sheep daemon.
    fn read_vdi_data(&self, vid: u32, offset: u64, size: u32) -> Vec<u8> {
        match self.rt.block_on(Self::read_vdi_data_async(
            self.config.sheep_addr,
            vid,
            offset,
            size,
        )) {
            Ok(data) => data,
            Err(e) => {
                warn!("failed to read VDI {:#x} at offset {}: {}", vid, offset, e);
                vec![0u8; size as usize]
            }
        }
    }

    /// Read VDI data from the sheep daemon (async).
    async fn read_vdi_data_async(
        addr: std::net::SocketAddr,
        vid: u32,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        use sheepdog_proto::constants::{SD_DATA_OBJ_SIZE, SD_PROTO_VER};
        use sheepdog_proto::oid::ObjectId;
        use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse, ResponseResult};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let obj_index = (offset / SD_DATA_OBJ_SIZE) as u32;
        let obj_offset = (offset % SD_DATA_OBJ_SIZE) as u32;
        let oid = ObjectId::from_vid_data(vid, obj_index as u64);

        let mut stream = sheepdog_core::net::connect_to_addr(addr).await?;

        let header = RequestHeader {
            proto_ver: SD_PROTO_VER,
            epoch: 0,
            id: 3,
        };
        let req = SdRequest::ReadObj {
            oid,
            offset: obj_offset,
            length: size,
        };
        let req_data = bincode::serialize(&(header, req))?;

        stream.write_u32(req_data.len() as u32).await?;
        stream.write_all(&req_data).await?;

        let resp_len = stream.read_u32().await? as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;

        let response: SdResponse = bincode::deserialize(&resp_buf)?;

        match response.result {
            ResponseResult::Data(data) => Ok(data),
            _ => Ok(vec![0u8; size as usize]),
        }
    }
}

impl Filesystem for SheepFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();
        debug!("lookup: parent={}, name={}", parent, name_str);

        match parent {
            ROOT_INO => match name_str.as_ref() {
                "vdi" => reply.entry(&TTL, &self.dir_attr(VDI_DIR_INO), 0),
                "node" => reply.entry(&TTL, &self.dir_attr(NODE_DIR_INO), 0),
                _ => reply.error(libc::ENOENT),
            },
            VDI_DIR_INO => {
                self.maybe_refresh_vdis();
                if let Some(&ino) = self.vdi_inodes.get(name_str.as_ref()) {
                    if let Some(&vid) = self.ino_to_vid.get(&ino) {
                        if let Some(vdi) = self.vdis.get(&vid) {
                            reply.entry(&TTL, &self.vdi_file_attr(ino, vdi.size), 0);
                            return;
                        }
                    }
                }
                reply.error(libc::ENOENT);
            }
            _ => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        debug!("getattr: ino={}", ino);
        match ino {
            ROOT_INO => reply.attr(&TTL, &self.root_attr()),
            VDI_DIR_INO | NODE_DIR_INO => reply.attr(&TTL, &self.dir_attr(ino)),
            _ => {
                if let Some(&vid) = self.ino_to_vid.get(&ino) {
                    if let Some(vdi) = self.vdis.get(&vid) {
                        reply.attr(&TTL, &self.vdi_file_attr(ino, vdi.size));
                        return;
                    }
                }
                reply.error(libc::ENOENT);
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        debug!("read: ino={}, offset={}, size={}", ino, offset, size);

        if let Some(&vid) = self.ino_to_vid.get(&ino) {
            if let Some(vdi) = self.vdis.get(&vid) {
                let offset = offset as u64;
                if offset >= vdi.size {
                    reply.data(&[]);
                    return;
                }
                let read_size = std::cmp::min(size as u64, vdi.size - offset) as u32;
                let data = self.read_vdi_data(vid, offset, read_size);
                reply.data(&data);
                return;
            }
        }
        reply.error(libc::ENOENT);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir: ino={}, offset={}", ino, offset);

        match ino {
            ROOT_INO => {
                let entries: Vec<(u64, FileType, &str)> = vec![
                    (ROOT_INO, FileType::Directory, "."),
                    (ROOT_INO, FileType::Directory, ".."),
                    (VDI_DIR_INO, FileType::Directory, "vdi"),
                    (NODE_DIR_INO, FileType::Directory, "node"),
                ];
                for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(ino, (i + 1) as i64, kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            VDI_DIR_INO => {
                self.maybe_refresh_vdis();
                let mut entries: Vec<(u64, FileType, String)> = vec![
                    (VDI_DIR_INO, FileType::Directory, ".".to_string()),
                    (ROOT_INO, FileType::Directory, "..".to_string()),
                ];
                for (name, &ino) in &self.vdi_inodes {
                    entries.push((ino, FileType::RegularFile, name.clone()));
                }
                for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(ino, (i + 1) as i64, kind, &name) {
                        break;
                    }
                }
                reply.ok();
            }
            NODE_DIR_INO => {
                let entries: Vec<(u64, FileType, &str)> = vec![
                    (NODE_DIR_INO, FileType::Directory, "."),
                    (ROOT_INO, FileType::Directory, ".."),
                ];
                for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(ino, (i + 1) as i64, kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            _ => {
                reply.error(libc::ENOENT);
            }
        }
    }
}
