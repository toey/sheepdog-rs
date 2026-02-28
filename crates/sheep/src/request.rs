//! Request pipeline: receive, dispatch, process, respond.
//!
//! In the C version, requests flowed through:
//!   listen_handler → client_handler → rx_work → rx_main → queue_request
//!   → work queue → process_work → op_done → put_request → tx_work
//!
//! In Rust we use tokio tasks instead of epoll + work queues:
//!   accept_loop → spawn(handle_client) → read_request → dispatch → respond

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse, ResponseResult};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info};

use crate::daemon::SharedSys;
use crate::ops;

/// Opaque request context — tracks a request through the pipeline.
pub struct Request {
    /// The decoded request message.
    pub req: SdRequest,
    /// Request header.
    pub header: RequestHeader,
    /// Whether this is a local (loopback) request.
    pub local: bool,
}

/// Accept loop: listen for incoming client connections and spawn handlers.
pub async fn accept_loop(sys: SharedSys) -> SdResult<()> {
    let addr = {
        let s = sys.read().await;
        s.listen_addr
    };

    let listener = sheepdog_core::net::create_listen_socket(
        &addr.ip().to_string(),
        addr.port(),
    )
    .await?;

    info!("listening on {}", addr);

    // Extract shutdown notify before entering the select loop
    let shutdown_notify = {
        let s = sys.read().await;
        s.shutdown_notify.clone()
    };

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        debug!("accepted connection from {}", peer);
                        let sys_clone = sys.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(sys_clone, stream).await {
                                debug!("client {} disconnected: {}", peer, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("accept error: {}", e);
                    }
                }
            }
            _ = shutdown_notify.notified() => {
                info!("accept loop shutting down");
                break;
            }
        }
    }
    Ok(())
}

/// Handle a single client connection: read requests, process, send responses.
async fn handle_client(sys: SharedSys, mut stream: TcpStream) -> SdResult<()> {
    loop {
        // Read request frame: length-prefixed bincode
        let len = match stream.read_u32().await {
            Ok(n) => n as usize,
            Err(_) => return Ok(()), // Client disconnected cleanly
        };

        if len > 64 * 1024 * 1024 {
            error!("request too large: {} bytes", len);
            return Err(SdError::InvalidParms);
        }

        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.map_err(|_| SdError::NetworkError)?;

        // Deserialize request
        let (header, req): (RequestHeader, SdRequest) =
            bincode::deserialize(&buf).map_err(|_| {
                error!("failed to deserialize request");
                SdError::InvalidParms
            })?;

        // Check epoch
        let epoch_result = check_epoch(&sys, &header).await;

        let response = if let Err(e) = epoch_result {
            SdResponse::error(header.proto_ver, header.epoch, header.id, e)
        } else {
            // Dispatch and process
            let request = Request {
                req,
                header: header.clone(),
                local: false,
            };
            dispatch(sys.clone(), request).await
        };

        // Serialize and send response
        let resp_data = bincode::serialize(&response).map_err(|_| SdError::SystemError)?;
        stream
            .write_u32(resp_data.len() as u32)
            .await
            .map_err(|_| SdError::NetworkError)?;
        stream
            .write_all(&resp_data)
            .await
            .map_err(|_| SdError::NetworkError)?;
    }
}

/// Check request epoch against cluster epoch.
///
/// We allow requests from newer epochs (the sender may have processed
/// more cluster events than us). Only reject requests from significantly
/// older epochs, which indicates a stale sender.
async fn check_epoch(sys: &SharedSys, header: &RequestHeader) -> SdResult<()> {
    let s = sys.read().await;
    let cluster_epoch = s.epoch();

    // Epoch 0 = skip check (internal/local requests)
    if header.epoch == 0 {
        return Ok(());
    }

    // Reject requests from epochs that are too old
    if header.epoch > 0 && header.epoch + 100 < cluster_epoch {
        return Err(SdError::OldNodeVer);
    }

    // Allow requests from newer epochs — the sender knows about
    // cluster events we haven't processed yet. We'll catch up.
    Ok(())
}

/// Dispatch a request to the appropriate handler.
pub(crate) async fn dispatch(sys: SharedSys, request: Request) -> SdResponse {
    let header = &request.header;
    let proto_ver = header.proto_ver;
    let epoch = header.epoch;
    let id = header.id;

    // Check cluster status
    {
        let s = sys.read().await;
        match s.cinfo.status {
            sheepdog_proto::node::ClusterStatus::Killed => {
                return SdResponse::error(proto_ver, epoch, id, SdError::Killed);
            }
            sheepdog_proto::node::ClusterStatus::Shutdown => {
                return SdResponse::error(proto_ver, epoch, id, SdError::Shutdown);
            }
            sheepdog_proto::node::ClusterStatus::WaitForFormat => {
                if !is_force_op(&request.req) {
                    return SdResponse::error(proto_ver, epoch, id, SdError::WaitForFormat);
                }
            }
            sheepdog_proto::node::ClusterStatus::WaitForJoin => {
                if !is_force_op(&request.req) {
                    return SdResponse::error(proto_ver, epoch, id, SdError::WaitForJoin);
                }
            }
            sheepdog_proto::node::ClusterStatus::Ok => {}
        }
    }

    // Route to appropriate handler
    match ops::process(sys, request).await {
        Ok(result) => SdResponse {
            proto_ver,
            epoch,
            id,
            result,
        },
        Err(e) => SdResponse::error(proto_ver, epoch, id, e),
    }
}

/// Check if the operation should be processed regardless of cluster status.
fn is_force_op(req: &SdRequest) -> bool {
    matches!(
        req,
        SdRequest::MakeFs { .. }
            | SdRequest::Shutdown
            | SdRequest::GetNodeList
            | SdRequest::StatCluster
            | SdRequest::StatSheep
            | SdRequest::GetStoreList
            // Peer I/O always allowed — the sending node has already
            // verified cluster status; we just store/retrieve data.
            | SdRequest::CreateAndWritePeer { .. }
            | SdRequest::ReadPeer { .. }
            | SdRequest::WritePeer { .. }
            | SdRequest::RemovePeer { .. }
            | SdRequest::FlushPeer
            | SdRequest::Exist { .. }
            | SdRequest::GetObjList { .. }
    )
}

/// Send a local (loopback) request and get the response.
/// Used by internal code (e.g., VDI ops) that needs to talk to the local sheep.
pub async fn exec_local_request(sys: SharedSys, req: SdRequest) -> SdResult<ResponseResult> {
    let epoch = sys.read().await.epoch();
    let request = Request {
        req,
        header: RequestHeader {
            proto_ver: sheepdog_proto::constants::SD_SHEEP_PROTO_VER,
            epoch,
            id: 0,
        },
        local: true,
    };

    let response = dispatch(sys, request).await;
    match response.result {
        ResponseResult::Error(e) => Err(e),
        other => Ok(other),
    }
}
