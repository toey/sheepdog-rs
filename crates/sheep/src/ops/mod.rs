//! Operation dispatch — replaces the C `sd_op_template[]` array.
//!
//! In the C version, each opcode mapped to a `sd_op_template` struct with
//! `process_work` and `process_main` function pointers. We replace that with
//! a `match` on the `SdRequest` enum — much more type-safe.

pub mod cluster;
pub mod gateway;
pub mod local;
pub mod peer;

use std::pin::Pin;
use std::future::Future;

use sheepdog_proto::error::SdResult;
use sheepdog_proto::request::{ResponseResult, SdRequest};
use tracing::debug;

use crate::daemon::SharedSys;
use crate::request::Request;

/// Operation classification — determines routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    /// Cluster-wide: executed on one node, result broadcast to all.
    Cluster,
    /// Local: processed entirely on this node.
    Local,
    /// Peer: I/O request from another sheep (data object read/write).
    Peer,
    /// Gateway: client-facing request routed to the responsible nodes.
    Gateway,
}

/// Classify a request into its operation type.
pub fn classify(req: &SdRequest) -> OpType {
    match req {
        // Gateway operations — client data I/O
        SdRequest::CreateAndWriteObj { .. }
        | SdRequest::ReadObj { .. }
        | SdRequest::WriteObj { .. }
        | SdRequest::RemoveObj { .. }
        | SdRequest::DiscardObj { .. } => OpType::Gateway,

        // Peer operations — inter-sheep data I/O
        SdRequest::CreateAndWritePeer { .. }
        | SdRequest::ReadPeer { .. }
        | SdRequest::WritePeer { .. }
        | SdRequest::RemovePeer { .. }
        | SdRequest::FlushPeer
        | SdRequest::GetObjList { .. }
        | SdRequest::GetEpoch { .. }
        | SdRequest::Exist { .. }
        | SdRequest::OidsExist { .. }
        | SdRequest::GetHash { .. }
        | SdRequest::RepairReplica { .. }
        | SdRequest::DecrefPeer { .. } => OpType::Peer,

        // Cluster operations — distributed coordination
        SdRequest::NewVdi { .. }
        | SdRequest::DelVdi { .. }
        | SdRequest::LockVdi { .. }
        | SdRequest::ReleaseVdi { .. }
        | SdRequest::GetVdiInfo { .. }
        | SdRequest::Snapshot { .. }
        | SdRequest::MakeFs { .. }
        | SdRequest::Shutdown
        | SdRequest::AlterClusterCopy { .. }
        | SdRequest::AlterVdiCopy { .. }
        | SdRequest::NotifyVdiAdd { .. }
        | SdRequest::NotifyVdiDel { .. }
        | SdRequest::InodeCoherence { .. }
        | SdRequest::VdiStateSnapshotCtl { .. }
        | SdRequest::NfsCreate { .. }
        | SdRequest::NfsDelete { .. } => OpType::Cluster,

        // Everything else is local
        _ => OpType::Local,
    }
}

/// Process a request — routes to the appropriate operation handler.
///
/// Returns a boxed future to break the recursive async cycle:
/// `gateway::handle → exec_local_request → dispatch → process → gateway::handle`
pub fn process(
    sys: SharedSys,
    request: Request,
) -> Pin<Box<dyn Future<Output = SdResult<ResponseResult>> + Send>> {
    Box::pin(async move {
        let op_type = classify(&request.req);
        debug!("processing {:?} request", op_type);

        match op_type {
            OpType::Gateway => gateway::handle(sys, request).await,
            OpType::Peer => peer::handle(sys, request).await,
            OpType::Cluster => cluster::handle(sys, request).await,
            OpType::Local => local::handle(sys, request).await,
        }
    })
}
