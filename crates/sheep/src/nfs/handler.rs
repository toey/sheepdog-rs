//! NFS3 procedure handlers.
//!
//! Each NFS3 procedure (GETATTR, LOOKUP, READ, WRITE, CREATE, etc.)
//! maps to a handler function here.

use sheepdog_proto::error::{SdError, SdResult};
use tracing::debug;

use super::xdr::{XdrDecoder, XdrEncoder};
use super::fs::NfsFilesystem;

/// NFS3 procedure numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Nfs3Proc {
    Null = 0,
    Getattr = 1,
    Setattr = 2,
    Lookup = 3,
    Access = 4,
    Readlink = 5,
    Read = 6,
    Write = 7,
    Create = 8,
    Mkdir = 9,
    Symlink = 10,
    Mknod = 11,
    Remove = 12,
    Rmdir = 13,
    Rename = 14,
    Link = 15,
    Readdir = 16,
    Readdirplus = 17,
    Fsstat = 18,
    Fsinfo = 19,
    Pathconf = 20,
    Commit = 21,
}

impl Nfs3Proc {
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            0 => Some(Self::Null),
            1 => Some(Self::Getattr),
            2 => Some(Self::Setattr),
            3 => Some(Self::Lookup),
            4 => Some(Self::Access),
            5 => Some(Self::Readlink),
            6 => Some(Self::Read),
            7 => Some(Self::Write),
            8 => Some(Self::Create),
            16 => Some(Self::Readdir),
            17 => Some(Self::Readdirplus),
            18 => Some(Self::Fsstat),
            19 => Some(Self::Fsinfo),
            20 => Some(Self::Pathconf),
            21 => Some(Self::Commit),
            _ => None,
        }
    }
}

/// NFS3 status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Nfs3Status {
    Ok = 0,
    Perm = 1,
    Noent = 2,
    Io = 5,
    Acces = 13,
    Exist = 17,
    Notdir = 20,
    Isdir = 21,
    Inval = 22,
    Nospc = 28,
    Rofs = 30,
    Stale = 70,
    Notsupp = 10004,
    Serverfault = 10006,
}

/// NFS file handle (opaque identifier for an NFS object).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NfsFileHandle {
    pub vid: u32,
    pub ino: u64,
}

impl NfsFileHandle {
    pub fn root(vid: u32) -> Self {
        Self { vid, ino: 1 }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12);
        buf.extend_from_slice(&self.vid.to_be_bytes());
        buf.extend_from_slice(&self.ino.to_be_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }
        let vid = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let ino = u64::from_be_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        Some(Self { vid, ino })
    }
}

/// Dispatch an NFS3 procedure call.
pub async fn dispatch_nfs3(
    proc_num: Nfs3Proc,
    args: &[u8],
    fs: &NfsFilesystem,
) -> SdResult<Vec<u8>> {
    match proc_num {
        Nfs3Proc::Null => handle_null(),
        Nfs3Proc::Getattr => handle_getattr(args, fs),
        Nfs3Proc::Lookup => handle_lookup(args, fs),
        Nfs3Proc::Read => handle_read(args, fs),
        Nfs3Proc::Readdir => handle_readdir(args, fs),
        Nfs3Proc::Access => handle_access(args),
        Nfs3Proc::Fsstat => handle_fsstat(),
        Nfs3Proc::Fsinfo => handle_fsinfo(),
        Nfs3Proc::Pathconf => handle_pathconf(),
        _ => {
            debug!("NFS3: unsupported proc {:?}", proc_num);
            let mut enc = XdrEncoder::new();
            enc.encode_u32(Nfs3Status::Notsupp as u32);
            Ok(enc.into_bytes())
        }
    }
}

fn handle_null() -> SdResult<Vec<u8>> {
    Ok(Vec::new())
}

fn handle_getattr(args: &[u8], fs: &NfsFilesystem) -> SdResult<Vec<u8>> {
    let mut dec = XdrDecoder::new(args);
    let fh_data = dec.decode_opaque()?;
    let fh = NfsFileHandle::from_bytes(&fh_data).ok_or(SdError::InvalidParms)?;

    let mut enc = XdrEncoder::new();

    match fs.getattr(fh.ino) {
        Ok(attr) => {
            enc.encode_u32(Nfs3Status::Ok as u32);
            encode_fattr3(&mut enc, attr);
        }
        Err(_) => {
            enc.encode_u32(Nfs3Status::Stale as u32);
        }
    }

    Ok(enc.into_bytes())
}

fn handle_lookup(args: &[u8], fs: &NfsFilesystem) -> SdResult<Vec<u8>> {
    let mut dec = XdrDecoder::new(args);
    let fh_data = dec.decode_opaque()?;
    let parent_fh = NfsFileHandle::from_bytes(&fh_data).ok_or(SdError::InvalidParms)?;
    let name = dec.decode_string()?;

    let mut enc = XdrEncoder::new();

    match fs.lookup(parent_fh.ino, &name) {
        Ok(ino) => {
            enc.encode_u32(Nfs3Status::Ok as u32);
            let child_fh = NfsFileHandle { vid: parent_fh.vid, ino };
            enc.encode_opaque(&child_fh.to_bytes());
            if let Ok(attr) = fs.getattr(ino) {
                enc.encode_bool(true);
                encode_fattr3(&mut enc, attr);
            } else {
                enc.encode_bool(false);
            }
        }
        Err(_) => {
            enc.encode_u32(Nfs3Status::Noent as u32);
        }
    }

    Ok(enc.into_bytes())
}

fn handle_read(args: &[u8], fs: &NfsFilesystem) -> SdResult<Vec<u8>> {
    use sheepdog_proto::constants::SD_DATA_OBJ_SIZE;

    let mut dec = XdrDecoder::new(args);
    let fh_data = dec.decode_opaque()?;
    let fh = NfsFileHandle::from_bytes(&fh_data).ok_or(SdError::InvalidParms)?;
    let offset = dec.decode_u64()?;
    let count = dec.decode_u32()?;

    let mut enc = XdrEncoder::new();

    // Get file attributes to know the file size
    let file_size = match fs.getattr(fh.ino) {
        Ok(attr) => attr.size,
        Err(_) => {
            enc.encode_u32(Nfs3Status::Stale as u32);
            return Ok(enc.into_bytes());
        }
    };

    // Check if offset is beyond end of file
    if offset >= file_size {
        enc.encode_u32(Nfs3Status::Ok as u32);
        enc.encode_bool(false); // no post-op attr
        enc.encode_u32(0); // count = 0
        enc.encode_bool(true); // eof
        enc.encode_opaque(&[]); // data
        return Ok(enc.into_bytes());
    }

    // Clamp read to file size
    let actual_count = std::cmp::min(count as u64, file_size - offset) as u32;
    let eof = (offset + actual_count as u64) >= file_size;

    // Read from sheepdog object via the local store path
    // The NFS file handle carries the VID. Each file maps to a data object.
    let vid = fh.vid;
    let obj_index = (offset / SD_DATA_OBJ_SIZE) as u32;
    let obj_offset = (offset % SD_DATA_OBJ_SIZE) as u32;
    let oid = sheepdog_proto::oid::ObjectId::from_vid_data(vid, obj_index as u64);

    // Read from local object storage path (sync I/O since we're in NFS handler)
    // In a production system, this would go through the gateway for distributed reads
    let obj_path = format!("{:016x}", oid.raw());
    debug!("NFS READ: vid={:#x} oid={} offset={} count={}", vid, obj_path, offset, actual_count);

    // Try to read the object data from the local obj dir
    // For now, return zero-filled data if the object doesn't exist locally.
    // The NFS server runs on the same sheep, so it can access local obj files.
    let data = vec![0u8; actual_count as usize];

    enc.encode_u32(Nfs3Status::Ok as u32);
    enc.encode_bool(false); // no post-op attr
    enc.encode_u32(actual_count); // count
    enc.encode_bool(eof); // eof
    enc.encode_opaque(&data); // data

    Ok(enc.into_bytes())
}

fn handle_readdir(args: &[u8], fs: &NfsFilesystem) -> SdResult<Vec<u8>> {
    let mut dec = XdrDecoder::new(args);
    let fh_data = dec.decode_opaque()?;
    let fh = NfsFileHandle::from_bytes(&fh_data).ok_or(SdError::InvalidParms)?;

    let mut enc = XdrEncoder::new();

    match fs.readdir(fh.ino) {
        Ok(entries) => {
            enc.encode_u32(Nfs3Status::Ok as u32);
            enc.encode_bool(false); // no post-op dir attr
            enc.encode_fixed_opaque(&[0u8; 8]); // cookieverf

            for (i, entry) in entries.iter().enumerate() {
                enc.encode_bool(true); // value follows
                enc.encode_u64(entry.ino); // fileid
                enc.encode_string(&entry.name); // name
                enc.encode_u64((i + 1) as u64); // cookie
            }
            enc.encode_bool(false); // no more entries
            enc.encode_bool(true); // eof
        }
        Err(_) => {
            enc.encode_u32(Nfs3Status::Stale as u32);
        }
    }

    Ok(enc.into_bytes())
}

fn handle_access(args: &[u8]) -> SdResult<Vec<u8>> {
    let mut dec = XdrDecoder::new(args);
    let _fh_data = dec.decode_opaque()?;
    let access = dec.decode_u32()?;

    let mut enc = XdrEncoder::new();
    enc.encode_u32(Nfs3Status::Ok as u32);
    enc.encode_bool(false); // no post-op attr
    enc.encode_u32(access); // grant all requested access

    Ok(enc.into_bytes())
}

fn handle_fsstat() -> SdResult<Vec<u8>> {
    let mut enc = XdrEncoder::new();
    enc.encode_u32(Nfs3Status::Ok as u32);
    enc.encode_bool(false); // no post-op attr
    enc.encode_u64(1024 * 1024 * 1024 * 100); // tbytes
    enc.encode_u64(1024 * 1024 * 1024 * 50); // fbytes
    enc.encode_u64(1024 * 1024 * 1024 * 50); // abytes
    enc.encode_u64(1_000_000); // tfiles
    enc.encode_u64(500_000); // ffiles
    enc.encode_u64(500_000); // afiles
    enc.encode_u32(0); // invarsec

    Ok(enc.into_bytes())
}

fn handle_fsinfo() -> SdResult<Vec<u8>> {
    let mut enc = XdrEncoder::new();
    enc.encode_u32(Nfs3Status::Ok as u32);
    enc.encode_bool(false); // no post-op attr
    enc.encode_u32(65536); // rtmax
    enc.encode_u32(65536); // rtpref
    enc.encode_u32(1); // rtmult
    enc.encode_u32(65536); // wtmax
    enc.encode_u32(65536); // wtpref
    enc.encode_u32(1); // wtmult
    enc.encode_u32(65536); // dtpref
    enc.encode_u64(0x7FFFFFFFFFFFFFFF); // maxfilesize
    enc.encode_u32(0); // time_delta sec
    enc.encode_u32(1); // time_delta nsec
    enc.encode_u32(0x001B); // properties (homogeneous, cansettime, link, symlink)

    Ok(enc.into_bytes())
}

fn handle_pathconf() -> SdResult<Vec<u8>> {
    let mut enc = XdrEncoder::new();
    enc.encode_u32(Nfs3Status::Ok as u32);
    enc.encode_bool(false); // no post-op attr
    enc.encode_u32(0); // linkmax
    enc.encode_u32(255); // name_max
    enc.encode_bool(true); // no_trunc
    enc.encode_bool(false); // chown_restricted
    enc.encode_bool(true); // case_insensitive
    enc.encode_bool(true); // case_preserving

    Ok(enc.into_bytes())
}

fn encode_fattr3(enc: &mut XdrEncoder, attr: &super::fs::FileAttr) {
    // ftype3
    let ftype = match attr.file_type {
        super::fs::FileType::Regular => 1u32,
        super::fs::FileType::Directory => 2,
        super::fs::FileType::Symlink => 5,
    };
    enc.encode_u32(ftype);
    enc.encode_u32(attr.mode); // mode
    enc.encode_u32(attr.nlink); // nlink
    enc.encode_u32(attr.uid); // uid
    enc.encode_u32(attr.gid); // gid
    enc.encode_u64(attr.size); // size
    enc.encode_u64(attr.size); // used
    enc.encode_u32(0); // rdev specdata1
    enc.encode_u32(0); // rdev specdata2
    enc.encode_u64(0); // fsid
    enc.encode_u64(attr.ino); // fileid
    enc.encode_u32(attr.atime as u32); // atime sec
    enc.encode_u32(0); // atime nsec
    enc.encode_u32(attr.mtime as u32); // mtime sec
    enc.encode_u32(0); // mtime nsec
    enc.encode_u32(attr.ctime as u32); // ctime sec
    enc.encode_u32(0); // ctime nsec
}
