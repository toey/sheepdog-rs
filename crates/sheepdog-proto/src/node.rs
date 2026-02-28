/// Node and cluster types.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::constants::*;

/// Network address of a sheepdog node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    /// Primary address for cluster communication
    pub addr: IpAddr,
    /// Primary port
    pub port: u16,
    /// Optional separate IO address (for dedicated data NIC)
    pub io_addr: Option<IpAddr>,
    /// IO port (if separate IO address is used)
    pub io_port: u16,
}

impl NodeId {
    pub fn new(addr: IpAddr, port: u16) -> Self {
        Self {
            addr,
            port,
            io_addr: None,
            io_port: 0,
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr, self.port)
    }

    pub fn io_socket_addr(&self) -> SocketAddr {
        match self.io_addr {
            Some(addr) if self.io_port > 0 => SocketAddr::new(addr, self.io_port),
            _ => self.socket_addr(),
        }
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self {
            addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: SD_LISTEN_PORT,
            io_addr: None,
            io_port: 0,
        }
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.addr, self.port)
    }
}

impl PartialOrd for NodeId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.addr
            .to_string()
            .cmp(&other.addr.to_string())
            .then(self.port.cmp(&other.port))
    }
}

/// Disk information for multi-disk support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskInfo {
    pub disk_id: u64,
    pub disk_space: u64,
}

/// A sheepdog cluster member node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdNode {
    /// Network identifier
    pub nid: NodeId,
    /// Number of virtual nodes
    pub nr_vnodes: u16,
    /// Fault domain zone
    pub zone: u32,
    /// Available storage space in bytes
    pub space: u64,
    /// Per-disk information (for multi-disk setups)
    pub disks: Vec<DiskInfo>,
}

impl SdNode {
    pub fn new(nid: NodeId) -> Self {
        Self {
            nid,
            nr_vnodes: SD_DEFAULT_VNODES,
            zone: 0,
            space: 0,
            disks: Vec::new(),
        }
    }
}

impl PartialOrd for SdNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SdNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.nid.cmp(&other.nid)
    }
}

impl fmt::Display for SdNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (vnodes={}, zone={}, space={})",
            self.nid, self.nr_vnodes, self.zone, self.space
        )
    }
}

/// Cluster status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterStatus {
    Ok,
    WaitForFormat,
    WaitForJoin,
    Shutdown,
    Killed,
}

/// Node-local status during initialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Initialization,
    CollectingCinfo,
    Ok,
}

/// Cluster-wide configuration and state, broadcast to all members.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    /// Internal protocol version
    pub proto_ver: u8,
    /// Whether recovery is disabled
    pub disable_recovery: bool,
    /// Current epoch number
    pub epoch: u32,
    /// Cluster creation time
    pub ctime: u64,
    /// Cluster flags
    pub flags: u16,
    /// Default number of copies
    pub nr_copies: u8,
    /// Copy policy (0 = replicate, >0 = erasure coding)
    pub copy_policy: u8,
    /// Cluster status
    pub status: ClusterStatus,
    /// Default store backend name
    pub default_store: String,
    /// Current member list
    pub nodes: Vec<SdNode>,
}

impl Default for ClusterInfo {
    fn default() -> Self {
        Self {
            proto_ver: SD_SHEEP_PROTO_VER,
            disable_recovery: false,
            epoch: 0,
            ctime: 0,
            flags: 0,
            nr_copies: SD_DEFAULT_COPIES,
            copy_policy: 0,
            status: ClusterStatus::WaitForFormat,
            default_store: String::new(),
            nodes: Vec::new(),
        }
    }
}

/// Epoch log entry — records cluster state at a specific epoch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochLog {
    pub ctime: u64,
    pub time: u64,
    pub epoch: u32,
    pub disable_recovery: bool,
    pub nr_copies: u8,
    pub copy_policy: u8,
    pub flags: u16,
    pub drv_name: String,
    pub nodes: Vec<SdNode>,
}
