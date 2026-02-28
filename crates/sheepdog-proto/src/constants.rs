/// Sheepdog protocol and system constants.

/// Client protocol version
pub const SD_PROTO_VER: u8 = 0x02;
/// Internal sheep-to-sheep protocol version
pub const SD_SHEEP_PROTO_VER: u8 = 0x09;

/// Default listen port for client connections
pub const SD_LISTEN_PORT: u16 = 7000;

/// Default number of copies
pub const SD_DEFAULT_COPIES: u8 = 3;
/// Maximum copies (for erasure coding: 128 data + 127 parity)
pub const SD_MAX_COPIES: u16 = 255;

/// Default virtual nodes per physical node
pub const SD_DEFAULT_VNODES: u16 = 128;
/// Maximum nodes in a cluster
pub const SD_MAX_NODES: usize = 6144;

/// Data object size (4 MB)
pub const SD_DATA_OBJ_SIZE: u64 = 1 << 22;
/// Maximum data objects per VDI (2^32)
pub const MAX_DATA_OBJS: u64 = 1 << 32;
/// Old max data objects (for legacy inode format)
pub const OLD_MAX_DATA_OBJS: u64 = 1 << 20;

/// Maximum VDI name length
pub const SD_MAX_VDI_LEN: usize = 256;
/// Maximum VDI tag length
pub const SD_MAX_VDI_TAG_LEN: usize = 256;
/// Maximum VDI attribute key length
pub const SD_MAX_VDI_ATTR_KEY_LEN: usize = 256;
/// Maximum VDI attribute value length
pub const SD_MAX_VDI_ATTR_VALUE_LEN: usize = 65536;
/// Maximum snapshot tag length
pub const SD_MAX_SNAPSHOT_TAG_LEN: usize = 256;

/// Total number of VDIs (2^24 = 16M)
pub const SD_NR_VDIS: u32 = 1 << 24;

/// Maximum VDI size (4MB * 2^32 = 16 EB)
pub const SD_MAX_VDI_SIZE: u64 = SD_DATA_OBJ_SIZE * MAX_DATA_OBJS;

/// Inline data index entries in the inode (1M entries)
pub const SD_INODE_DATA_INDEX: usize = 1 << 20;

/// Ledger object size
pub const SD_LEDGER_OBJ_SIZE: u64 = 1 << 22;

/// Maximum children (legacy, space now reused for btree_counter)
pub const OLD_MAX_CHILDREN: usize = 1024;

/// Maximum number of disks per node
pub const DISK_MAX: usize = 32;

/// Store name length
pub const STORE_LEN: usize = 16;

/// Maximum disks for md_info
pub const MD_MAX_DISK: usize = 64;

/// Cache info maximum
pub const CACHE_MAX: usize = 1024;

/// BTree magic number
pub const INODE_BTREE_MAGIC: u16 = 0x6274;

/// Cluster flags
pub const SD_CLUSTER_FLAG_STRICT: u16 = 0x0001;
pub const SD_CLUSTER_FLAG_DISKMODE: u16 = 0x0002;
pub const SD_CLUSTER_FLAG_USE_LOCK: u16 = 0x0008;

/// Lock types for VDI operations
pub const LOCK_TYPE_NORMAL: u32 = 0;
pub const LOCK_TYPE_SHARED: u32 = 1;
