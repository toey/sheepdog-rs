/// Request and response types for the sheepdog protocol.
///
/// In the C version, requests and responses were 48-byte fixed-size structs
/// with C unions. In Rust, we use proper enums serialized with serde+bincode.

use serde::{Deserialize, Serialize};

use crate::error::SdError;
use crate::node::SdNode;
use crate::oid::ObjectId;

/// Common header for all requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestHeader {
    /// Protocol version
    pub proto_ver: u8,
    /// Cluster epoch
    pub epoch: u32,
    /// Request ID (for matching responses)
    pub id: u32,
}

/// Client and internal request types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SdRequest {
    // ---- Data object operations (client protocol) ----
    CreateAndWriteObj {
        oid: ObjectId,
        cow_oid: ObjectId,
        copies: u8,
        copy_policy: u8,
        offset: u32,
        data: Vec<u8>,
    },
    ReadObj {
        oid: ObjectId,
        offset: u32,
        length: u32,
    },
    WriteObj {
        oid: ObjectId,
        offset: u32,
        data: Vec<u8>,
    },
    RemoveObj {
        oid: ObjectId,
    },
    DiscardObj {
        oid: ObjectId,
        offset: u32,
        length: u32,
    },

    // ---- VDI operations ----
    NewVdi {
        name: String,
        vdi_size: u64,
        base_vid: u32,
        copies: u8,
        copy_policy: u8,
        store_policy: u8,
        snap_id: u32,
        vdi_type: u32,
    },
    LockVdi {
        name: String,
        lock_type: u32,
    },
    ReleaseVdi {
        name: String,
    },
    GetVdiInfo {
        name: String,
        tag: String,
        snap_id: u32,
    },
    ReadVdis,
    FlushVdi {
        vid: u32,
    },
    DelVdi {
        name: String,
        snap_id: u32,
    },

    // ---- Cluster management (internal protocol) ----
    GetNodeList,
    MakeFs {
        copies: u8,
        copy_policy: u8,
        flags: u16,
        store: String,
    },
    Shutdown,
    StatSheep,
    StatCluster,
    GetStoreList,
    Snapshot {
        name: String,
        tag: String,
    },
    Cleanup,
    KillNode,
    ClusterInfo,
    AlterClusterCopy {
        copies: u8,
        copy_policy: u8,
    },
    AlterVdiCopy {
        vid: u32,
        copies: u8,
        copy_policy: u8,
    },

    // ---- Peer operations (inter-sheep) ----
    CreateAndWritePeer {
        oid: ObjectId,
        ec_index: u8,
        copies: u8,
        copy_policy: u8,
        offset: u32,
        data: Vec<u8>,
    },
    ReadPeer {
        oid: ObjectId,
        ec_index: u8,
        offset: u32,
        length: u32,
    },
    WritePeer {
        oid: ObjectId,
        ec_index: u8,
        offset: u32,
        data: Vec<u8>,
    },
    RemovePeer {
        oid: ObjectId,
        ec_index: u8,
    },
    FlushPeer,
    GetObjList {
        tgt_epoch: u32,
    },
    GetEpoch {
        tgt_epoch: u32,
    },
    Exist {
        oid: ObjectId,
        ec_index: u8,
    },
    OidsExist {
        oids: Vec<ObjectId>,
    },
    GetHash {
        oid: ObjectId,
        tgt_epoch: u32,
    },
    RepairReplica {
        oid: ObjectId,
    },

    // ---- Recovery control ----
    EnableRecover,
    DisableRecover,
    CompleteRecovery {
        epoch: u32,
    },
    GetRecovery,
    SetRecovery {
        max_exec_count: u32,
        queue_work_interval: u64,
        throttling: bool,
    },

    // ---- Cache operations ----
    FlushDelCache {
        vid: u32,
    },
    DeleteCache {
        vid: u32,
    },
    GetCacheInfo,
    CachePurge,

    // ---- Multi-disk operations ----
    MdInfo,
    MdPlug {
        path: String,
    },
    MdUnplug {
        path: String,
    },
    Reweight,

    // ---- Logging/tracing ----
    GetLogLevel,
    SetLogLevel {
        level: u32,
    },
    Stat,
    TraceEnable,
    TraceDisable,
    TraceStatus,
    TraceReadBuf,

    // ---- VDI state ----
    NotifyVdiAdd {
        old_vid: u32,
        new_vid: u32,
        copies: u8,
        copy_policy: u8,
        set_bitmap: bool,
    },
    NotifyVdiDel {
        vid: u32,
    },
    GetVdiCopies {
        vid: u32,
    },
    VdiStateSnapshotCtl {
        get: bool,
        tgt_epoch: u32,
    },
    InodeCoherence {
        vid: u32,
        validate: bool,
    },
    ReadDelVdis,
    DecrefObj {
        oid: ObjectId,
        generation: u32,
        count: u32,
    },
    DecrefPeer {
        oid: ObjectId,
        generation: u32,
        count: u32,
    },
    PreventInodeUpdate {
        oid: ObjectId,
    },
    AllowInodeUpdate {
        oid: ObjectId,
    },

    // ---- NFS operations ----
    NfsCreate {
        name: String,
    },
    NfsDelete {
        name: String,
    },

    // ---- Forwarding ----
    ForwardObj {
        oid: ObjectId,
        addr: std::net::IpAddr,
        port: u16,
    },
}

/// Response from a sheepdog operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdResponse {
    /// Protocol version
    pub proto_ver: u8,
    /// Cluster epoch
    pub epoch: u32,
    /// Request ID this responds to
    pub id: u32,
    /// Response result
    pub result: ResponseResult,
}

/// Response payload variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseResult {
    /// Success with no additional data
    Success,
    /// Success with raw data payload
    Data(Vec<u8>),
    /// Error
    Error(SdError),
    /// Object operation result
    Obj {
        copies: u8,
        offset: u64,
    },
    /// VDI operation result
    Vdi {
        vdi_id: u32,
        attr_id: u32,
        copies: u8,
    },
    /// Node info result
    NodeInfo {
        nr_nodes: u32,
        store_size: u64,
        store_free: u64,
    },
    /// Hash result
    Hash {
        digest: [u8; 20],
    },
    /// Node list
    NodeList(Vec<SdNode>),
}

impl SdResponse {
    /// Create a success response.
    pub fn success(proto_ver: u8, epoch: u32, id: u32) -> Self {
        Self {
            proto_ver,
            epoch,
            id,
            result: ResponseResult::Success,
        }
    }

    /// Create an error response.
    pub fn error(proto_ver: u8, epoch: u32, id: u32, err: SdError) -> Self {
        Self {
            proto_ver,
            epoch,
            id,
            result: ResponseResult::Error(err),
        }
    }

    /// Check if this response indicates success.
    pub fn is_success(&self) -> bool {
        !matches!(self.result, ResponseResult::Error(_))
    }
}
