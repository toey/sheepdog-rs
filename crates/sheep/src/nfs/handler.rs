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
    let obj_hex = format!("{:016x}", oid.raw());
    debug!("NFS READ: vid={:#x} oid={} offset={} count={}", vid, obj_hex, offset, actual_count);

    // Try to read the object data from the local obj dir
    // Try plain layout: obj/{oid_hex}
    let plain_obj_path = format!("obj/{}", obj_hex);
    let file_data = std::fs::read(&plain_obj_path).or_else(|_| {
        // Try tree layout: obj/{vid:06x}/{oid_hex}
        let vid_hex = format!("{:06x}", vid);
        let tree_obj_path = format!("obj/{}/{}", vid_hex, obj_hex);
        std::fs::read(&tree_obj_path)
    });

    let data = match file_data {
        Ok(fd) => {
            // Truncate to actual_count if file_data is larger
            let copy_len = std::cmp::min(fd.len(), actual_count as usize);
            let mut data = vec![0u8; actual_count as usize];
            data[..copy_len].copy_from_slice(&fd[..copy_len]);
            data
        }
        Err(_) => {
            // Return I/O error to NFS client
            let mut enc_err = XdrEncoder::new();
            enc_err.encode_u32(Nfs3Status::Io as u32);
            return Ok(enc_err.into_bytes());
        }
    };

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

#[cfg(feature = "nfs")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::fs::{DirEntry, FileType, FileAttr};

    // Test 1: Nfs3Procedure enum — all variants, From<u32> conversion
    #[test]
    fn test_nfs3_proc_from_u32() {
        // Supported procedures (mapped in from_u32)
        assert_eq!(Nfs3Proc::from_u32(0), Some(Nfs3Proc::Null));
        assert_eq!(Nfs3Proc::from_u32(1), Some(Nfs3Proc::Getattr));
        assert_eq!(Nfs3Proc::from_u32(2), Some(Nfs3Proc::Setattr));
        assert_eq!(Nfs3Proc::from_u32(3), Some(Nfs3Proc::Lookup));
        assert_eq!(Nfs3Proc::from_u32(4), Some(Nfs3Proc::Access));
        assert_eq!(Nfs3Proc::from_u32(5), Some(Nfs3Proc::Readlink));
        assert_eq!(Nfs3Proc::from_u32(6), Some(Nfs3Proc::Read));
        assert_eq!(Nfs3Proc::from_u32(7), Some(Nfs3Proc::Write));
        assert_eq!(Nfs3Proc::from_u32(8), Some(Nfs3Proc::Create));
        assert_eq!(Nfs3Proc::from_u32(16), Some(Nfs3Proc::Readdir));
        assert_eq!(Nfs3Proc::from_u32(17), Some(Nfs3Proc::Readdirplus));
        assert_eq!(Nfs3Proc::from_u32(18), Some(Nfs3Proc::Fsstat));
        assert_eq!(Nfs3Proc::from_u32(19), Some(Nfs3Proc::Fsinfo));
        assert_eq!(Nfs3Proc::from_u32(20), Some(Nfs3Proc::Pathconf));
        assert_eq!(Nfs3Proc::from_u32(21), Some(Nfs3Proc::Commit));

        // Unsupported procedures return None (not mapped in from_u32)
        assert_eq!(Nfs3Proc::from_u32(9), None);   // Mkdir
        assert_eq!(Nfs3Proc::from_u32(10), None);  // Symlink
        assert_eq!(Nfs3Proc::from_u32(11), None);  // Mknod
        assert_eq!(Nfs3Proc::from_u32(12), None);  // Remove
        assert_eq!(Nfs3Proc::from_u32(13), None);  // Rmdir
        assert_eq!(Nfs3Proc::from_u32(14), None);  // Rename
        assert_eq!(Nfs3Proc::from_u32(15), None);  // Link
        assert_eq!(Nfs3Proc::from_u32(99), None);
        assert_eq!(Nfs3Proc::from_u32(100), None);
        assert_eq!(Nfs3Proc::from_u32(u32::MAX), None);
    }
    #[test]
    fn test_nfs3_proc_repr() {
        // Verify repr(u32) matches the discriminant
        assert_eq!(Nfs3Proc::Null as u32, 0);
        assert_eq!(Nfs3Proc::Getattr as u32, 1);
        assert_eq!(Nfs3Proc::Setattr as u32, 2);
        assert_eq!(Nfs3Proc::Lookup as u32, 3);
        assert_eq!(Nfs3Proc::Access as u32, 4);
        assert_eq!(Nfs3Proc::Readlink as u32, 5);
        assert_eq!(Nfs3Proc::Read as u32, 6);
        assert_eq!(Nfs3Proc::Write as u32, 7);
        assert_eq!(Nfs3Proc::Create as u32, 8);
        assert_eq!(Nfs3Proc::Mkdir as u32, 9);
        assert_eq!(Nfs3Proc::Symlink as u32, 10);
        assert_eq!(Nfs3Proc::Mknod as u32, 11);
        assert_eq!(Nfs3Proc::Remove as u32, 12);
        assert_eq!(Nfs3Proc::Rmdir as u32, 13);
        assert_eq!(Nfs3Proc::Rename as u32, 14);
        assert_eq!(Nfs3Proc::Link as u32, 15);
        assert_eq!(Nfs3Proc::Readdir as u32, 16);
        assert_eq!(Nfs3Proc::Readdirplus as u32, 17);
        assert_eq!(Nfs3Proc::Fsstat as u32, 18);
        assert_eq!(Nfs3Proc::Fsinfo as u32, 19);
        assert_eq!(Nfs3Proc::Pathconf as u32, 20);
        assert_eq!(Nfs3Proc::Commit as u32, 21);
    }

    // Test 2: NfsFileHandle encode/decode round-trip
    #[test]
    fn test_nfs_file_handle_roundtrip() {
        let fh = NfsFileHandle { vid: 0x12345678, ino: 0xDEADBEEFCAFEBABE };
        let bytes = fh.to_bytes();
        assert_eq!(bytes.len(), 12);
        let decoded = NfsFileHandle::from_bytes(&bytes).expect("should decode");
        assert_eq!(decoded, fh);
    }

    #[test]
    fn test_nfs_file_handle_root() {
        let fh = NfsFileHandle::root(0xABCD);
        assert_eq!(fh.vid, 0xABCD);
        assert_eq!(fh.ino, 1);
        let bytes = fh.to_bytes();
        assert_eq!(bytes.len(), 12);
        // vid = 0xABCD in big-endian (4 bytes)
        assert_eq!(bytes[0], 0x00);
        assert_eq!(bytes[1], 0x00);
        assert_eq!(bytes[2], 0xAB);
        assert_eq!(bytes[3], 0xCD);
        // ino = 1 in big-endian (8 bytes)
        assert_eq!(bytes[4], 0x00);
        assert_eq!(bytes[5], 0x00);
        assert_eq!(bytes[6], 0x00);
        assert_eq!(bytes[7], 0x00);
        assert_eq!(bytes[8], 0x00);
        assert_eq!(bytes[9], 0x00);
        assert_eq!(bytes[10], 0x00);
        assert_eq!(bytes[11], 0x01);
    }

    #[test]
    fn test_nfs_file_handle_decode_failures() {
        // Too short
        assert!(NfsFileHandle::from_bytes(&[0u8; 11]).is_none());
        assert!(NfsFileHandle::from_bytes(&[]).is_none());
        // Exact length should succeed
        assert!(NfsFileHandle::from_bytes(&[0u8; 12]).is_some());
    }

    #[test]
    fn test_nfs_file_handle_edge_values() {
        let fh = NfsFileHandle { vid: 0, ino: 0 };
        let bytes = fh.to_bytes();
        assert_eq!(NfsFileHandle::from_bytes(&bytes).unwrap(), fh);

        let fh = NfsFileHandle { vid: 0xFFFFFFFF, ino: 0xFFFFFFFFFFFFFFFF };
        let bytes = fh.to_bytes();
        assert_eq!(NfsFileHandle::from_bytes(&bytes).unwrap(), fh);
    }

    // Test 3: Nfs3Status — all variants convert to/from wire format
    #[test]
    fn test_nfs3_status_repr() {
        assert_eq!(Nfs3Status::Ok as u32, 0);
        assert_eq!(Nfs3Status::Perm as u32, 1);
        assert_eq!(Nfs3Status::Noent as u32, 2);
        assert_eq!(Nfs3Status::Io as u32, 5);
        assert_eq!(Nfs3Status::Acces as u32, 13);
        assert_eq!(Nfs3Status::Exist as u32, 17);
        assert_eq!(Nfs3Status::Notdir as u32, 20);
        assert_eq!(Nfs3Status::Isdir as u32, 21);
        assert_eq!(Nfs3Status::Inval as u32, 22);
        assert_eq!(Nfs3Status::Nospc as u32, 28);
        assert_eq!(Nfs3Status::Rofs as u32, 30);
        assert_eq!(Nfs3Status::Stale as u32, 70);
        assert_eq!(Nfs3Status::Notsupp as u32, 10004);
        assert_eq!(Nfs3Status::Serverfault as u32, 10006);
    }

    #[test]
    fn test_nfs3_status_roundtrip_via_xdr() {
        // Encode each status as XDR u32 and decode back
        let statuses = vec![
            Nfs3Status::Ok,
            Nfs3Status::Perm,
            Nfs3Status::Noent,
            Nfs3Status::Io,
            Nfs3Status::Acces,
            Nfs3Status::Exist,
            Nfs3Status::Notdir,
            Nfs3Status::Isdir,
            Nfs3Status::Inval,
            Nfs3Status::Nospc,
            Nfs3Status::Rofs,
            Nfs3Status::Stale,
            Nfs3Status::Notsupp,
            Nfs3Status::Serverfault,
        ];

        for status in statuses {
            let mut enc = XdrEncoder::new();
            enc.encode_u32(status as u32);
            let bytes = enc.into_bytes();

            let mut dec = XdrDecoder::new(&bytes);
            let decoded = dec.decode_u32().unwrap();
            assert_eq!(decoded, status as u32, "status mismatch for {:?}", status);
        }
    }

    // Test 4: FileAttr encode/decode round-trip via encode_fattr3
    #[test]
    fn test_fattr3_encode_roundtrip() {
        let attr = FileAttr {
            ino: 42,
            size: 1024,
            file_type: FileType::Regular,
            mode: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            atime: 1700000000,
            mtime: 1700000000,
            ctime: 1700000000,
        };

        let mut enc = XdrEncoder::new();
        encode_fattr3(&mut enc, &attr);
        let bytes = enc.into_bytes();

        // Verify the encoded data can be decoded
        let mut dec = XdrDecoder::new(&bytes);

        // ftype3
        assert_eq!(dec.decode_u32().unwrap(), 1); // Regular
        // mode
        assert_eq!(dec.decode_u32().unwrap(), 0o644);
        // nlink
        assert_eq!(dec.decode_u32().unwrap(), 1);
        // uid
        assert_eq!(dec.decode_u32().unwrap(), 1000);
        // gid
        assert_eq!(dec.decode_u32().unwrap(), 1000);
        // size
        assert_eq!(dec.decode_u64().unwrap(), 1024);
        // used
        assert_eq!(dec.decode_u64().unwrap(), 1024);
        // rdev specdata1
        assert_eq!(dec.decode_u32().unwrap(), 0);
        // rdev specdata2
        assert_eq!(dec.decode_u32().unwrap(), 0);
        // fsid
        assert_eq!(dec.decode_u64().unwrap(), 0);
        // fileid
        assert_eq!(dec.decode_u64().unwrap(), 42);
        // atime sec
        assert_eq!(dec.decode_u32().unwrap(), 1700000000);
        // atime nsec
        assert_eq!(dec.decode_u32().unwrap(), 0);
        // mtime sec
        assert_eq!(dec.decode_u32().unwrap(), 1700000000);
        // mtime nsec
        assert_eq!(dec.decode_u32().unwrap(), 0);
        // ctime sec
        assert_eq!(dec.decode_u32().unwrap(), 1700000000);
        // ctime nsec
        assert_eq!(dec.decode_u32().unwrap(), 0);
    }

    #[test]
    fn test_fattr3_directory_type() {
        let attr = FileAttr {
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
        };

        let mut enc = XdrEncoder::new();
        encode_fattr3(&mut enc, &attr);
        let bytes = enc.into_bytes();

        let mut dec = XdrDecoder::new(&bytes);
        assert_eq!(dec.decode_u32().unwrap(), 2); // Directory type
        assert_eq!(dec.decode_u32().unwrap(), 0o755); // mode
    }

    #[test]
    fn test_fattr3_symlink_type() {
        let attr = FileAttr {
            ino: 100,
            size: 64,
            file_type: FileType::Symlink,
            mode: 0o777,
            nlink: 1,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
        };

        let mut enc = XdrEncoder::new();
        encode_fattr3(&mut enc, &attr);
        let bytes = enc.into_bytes();

        let mut dec = XdrDecoder::new(&bytes);
        assert_eq!(dec.decode_u32().unwrap(), 5); // Symlink type
    }

    // Test 5: Directory entry encoding
    #[test]
    fn test_dir_entry_encoding() {
        let entries = vec![
            DirEntry {
                name: ".".to_string(),
                ino: 1,
                file_type: FileType::Directory,
            },
            DirEntry {
                name: "..".to_string(),
                ino: 1,
                file_type: FileType::Directory,
            },
            DirEntry {
                name: "file.txt".to_string(),
                ino: 5,
                file_type: FileType::Regular,
            },
        ];

        let mut enc = XdrEncoder::new();

        for (i, entry) in entries.iter().enumerate() {
            enc.encode_bool(true); // value follows
            enc.encode_u64(entry.ino); // fileid
            enc.encode_string(&entry.name); // name
            enc.encode_u64((i + 1) as u64); // cookie
        }
        enc.encode_bool(false); // no more entries
        enc.encode_bool(true); // eof

        let bytes = enc.into_bytes();

        // Decode and verify
        let mut dec = XdrDecoder::new(&bytes);

        for (i, expected) in entries.iter().enumerate() {
            assert!(dec.decode_bool().unwrap(), "value should follow");
            assert_eq!(dec.decode_u64().unwrap(), expected.ino);
            assert_eq!(dec.decode_string().unwrap(), expected.name);
            assert_eq!(dec.decode_u64().unwrap(), (i + 1) as u64);
        }

        assert!(!dec.decode_bool().unwrap(), "no more entries");
        assert!(dec.decode_bool().unwrap(), "eof");
    }

    // Test 6: dispatch_nfs3() — test implemented procedure dispatch
    #[tokio::test]
    async fn test_dispatch_null() {
        let fs = NfsFilesystem::new(0x12345678);
        let result = dispatch_nfs3(Nfs3Proc::Null, &[], &fs).await.unwrap();
        assert!(result.is_empty(), "NULL should return empty response");
    }

    #[tokio::test]
    async fn test_dispatch_getattr_root() {
        let fs = NfsFilesystem::new(0x12345678);

        // Encode a file handle for root inode
        let root_fh = NfsFileHandle::root(0x12345678);
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&root_fh.to_bytes());
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Getattr, &args, &fs).await.unwrap();

        // Decode: status + fattr3
        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        // ftype = Directory (2)
        assert_eq!(dec.decode_u32().unwrap(), 2);
        // mode = 0o755
        assert_eq!(dec.decode_u32().unwrap(), 0o755);
        // nlink = 2
        assert_eq!(dec.decode_u32().unwrap(), 2);
        // uid = 0
        assert_eq!(dec.decode_u32().unwrap(), 0);
        // gid = 0
        assert_eq!(dec.decode_u32().unwrap(), 0);
        // size = 4096
        assert_eq!(dec.decode_u64().unwrap(), 4096);
    }

    #[tokio::test]
    async fn test_dispatch_getattr_stale_handle() {
        let fs = NfsFilesystem::new(0x12345678);

        // File handle for non-existent inode
        let fh = NfsFileHandle { vid: 0x12345678, ino: 99999 };
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&fh.to_bytes());
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Getattr, &args, &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Stale as u32);
    }

    #[tokio::test]
    async fn test_dispatch_fsstat() {
        let fs = NfsFilesystem::new(0x12345678);
        let result = dispatch_nfs3(Nfs3Proc::Fsstat, &[], &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op attr
        assert_eq!(dec.decode_u64().unwrap(), 1024 * 1024 * 1024 * 100); // tbytes
        assert_eq!(dec.decode_u64().unwrap(), 1024 * 1024 * 1024 * 50); // fbytes
        assert_eq!(dec.decode_u64().unwrap(), 1024 * 1024 * 1024 * 50); // abytes
        assert_eq!(dec.decode_u64().unwrap(), 1_000_000); // tfiles
        assert_eq!(dec.decode_u64().unwrap(), 500_000); // ffiles
        assert_eq!(dec.decode_u64().unwrap(), 500_000); // afiles
        assert_eq!(dec.decode_u32().unwrap(), 0); // invarsec
    }

    #[tokio::test]
    async fn test_dispatch_fsinfo() {
        let fs = NfsFilesystem::new(0x12345678);
        let result = dispatch_nfs3(Nfs3Proc::Fsinfo, &[], &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op attr
        assert_eq!(dec.decode_u32().unwrap(), 65536); // rtmax
        assert_eq!(dec.decode_u32().unwrap(), 65536); // rtpref
        assert_eq!(dec.decode_u32().unwrap(), 1); // rtmult
        assert_eq!(dec.decode_u32().unwrap(), 65536); // wtmax
        assert_eq!(dec.decode_u32().unwrap(), 65536); // wtpref
        assert_eq!(dec.decode_u32().unwrap(), 1); // wtmult
        assert_eq!(dec.decode_u32().unwrap(), 65536); // dtpref
        assert_eq!(dec.decode_u64().unwrap(), 0x7FFFFFFFFFFFFFFF); // maxfilesize
        assert_eq!(dec.decode_u32().unwrap(), 0); // time_delta sec
        assert_eq!(dec.decode_u32().unwrap(), 1); // time_delta nsec
        assert_eq!(dec.decode_u32().unwrap(), 0x001B); // properties
    }

    #[tokio::test]
    async fn test_dispatch_pathconf() {
        let fs = NfsFilesystem::new(0x12345678);
        let result = dispatch_nfs3(Nfs3Proc::Pathconf, &[], &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op attr
        assert_eq!(dec.decode_u32().unwrap(), 0); // linkmax
        assert_eq!(dec.decode_u32().unwrap(), 255); // name_max
        assert!(dec.decode_bool().unwrap()); // no_trunc
        assert!(!dec.decode_bool().unwrap()); // chown_restricted
        assert!(dec.decode_bool().unwrap()); // case_insensitive
        assert!(dec.decode_bool().unwrap()); // case_preserving
    }

    #[tokio::test]
    async fn test_dispatch_readdir_empty() {
        let fs = NfsFilesystem::new(0x12345678);

        // Root directory has no children (empty dir_entries)
        let root_fh = NfsFileHandle::root(0x12345678);
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&root_fh.to_bytes());
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Readdir, &args, &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op dir attr
        // cookieverf (8 bytes fixed opaque)
        dec.decode_fixed_opaque(8).unwrap();
        // no entries, then eof
        assert!(!dec.decode_bool().unwrap()); // no more entries
        assert!(dec.decode_bool().unwrap()); // eof
    }

    #[tokio::test]
    async fn test_dispatch_readdir_with_entries() {
        let mut fs = NfsFilesystem::new(0x12345678);

        // Add a child entry to root
        fs.create(1, "testfile", 0o644).unwrap();

        let root_fh = NfsFileHandle::root(0x12345678);
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&root_fh.to_bytes());
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Readdir, &args, &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op dir attr
        dec.decode_fixed_opaque(8).unwrap(); // cookieverf
        // One entry
        assert!(dec.decode_bool().unwrap()); // value follows
        assert_eq!(dec.decode_u64().unwrap(), 2); // ino assigned by fs.create
        assert_eq!(dec.decode_string().unwrap(), "testfile");
        assert_eq!(dec.decode_u64().unwrap(), 1); // cookie
        assert!(!dec.decode_bool().unwrap()); // no more entries
        assert!(dec.decode_bool().unwrap()); // eof
    }

    #[tokio::test]
    async fn test_dispatch_read_empty_file() {
        let mut fs = NfsFilesystem::new(0x12345678);

        // Create a file with 0 size
        let ino = fs.create(1, "empty", 0o644).unwrap();

        let fh = NfsFileHandle { vid: 0x12345678, ino };
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&fh.to_bytes());
        enc.encode_u64(0); // offset
        enc.encode_u32(1024); // count
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Read, &args, &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op attr
        assert_eq!(dec.decode_u32().unwrap(), 0); // count = 0
        assert!(dec.decode_bool().unwrap()); // eof
    }

    #[tokio::test]
    async fn test_dispatch_read_beyond_eof() {
        let mut fs = NfsFilesystem::new(0x12345678);

        // Create a file with 100 bytes
        let attr = FileAttr {
            ino: 2,
            size: 100,
            file_type: FileType::Regular,
            mode: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
        };
        fs.set_attr(2, attr).unwrap();

        let fh = NfsFileHandle { vid: 0x12345678, ino: 2 };
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&fh.to_bytes());
        enc.encode_u64(200); // offset beyond EOF
        enc.encode_u32(100); // count
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Read, &args, &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op attr
        assert_eq!(dec.decode_u32().unwrap(), 0); // count = 0
        assert!(dec.decode_bool().unwrap()); // eof
    }

    #[tokio::test]
    async fn test_dispatch_access() {
        let fs = NfsFilesystem::new(0x12345678);

        let fh = NfsFileHandle::root(0x12345678);
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&fh.to_bytes());
        enc.encode_u32(7); // access: READ + WRITE + EXEC
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Access, &args, &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Ok as u32);
        assert!(!dec.decode_bool().unwrap()); // no post-op attr
        assert_eq!(dec.decode_u32().unwrap(), 7); // grant all requested access
    }

    // Test 7: dispatch_nfs3() — test unsupported procedure returns Notsupp
    #[tokio::test]
    async fn test_dispatch_unsupported_returns_notsupp() {
        let fs = NfsFilesystem::new(0x12345678);

        // Setattr (2) is not implemented
        let result = dispatch_nfs3(Nfs3Proc::Setattr, &[], &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Notsupp as u32);
    }

    #[tokio::test]
    async fn test_dispatch_write_returns_notsupp() {
        let fs = NfsFilesystem::new(0x12345678);

        let result = dispatch_nfs3(Nfs3Proc::Write, &[], &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Notsupp as u32);
    }

    #[tokio::test]
    async fn test_dispatch_create_returns_notsupp() {
        let fs = NfsFilesystem::new(0x12345678);

        let result = dispatch_nfs3(Nfs3Proc::Create, &[], &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Notsupp as u32);
    }

    // Test lookup with non-existent file
    #[tokio::test]
    async fn test_dispatch_lookup_not_found() {
        let fs = NfsFilesystem::new(0x12345678);

        let root_fh = NfsFileHandle::root(0x12345678);
        let mut enc = XdrEncoder::new();
        enc.encode_opaque(&root_fh.to_bytes());
        enc.encode_string("nonexistent");
        let args = enc.into_bytes();

        let result = dispatch_nfs3(Nfs3Proc::Lookup, &args, &fs).await.unwrap();

        let mut dec = XdrDecoder::new(&result);
        assert_eq!(dec.decode_u32().unwrap(), Nfs3Status::Noent as u32);
    }
}
