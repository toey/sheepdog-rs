//! TCP-based peer transport — the default (kernel networking) implementation.
//!
//! Wire format: `u32 length prefix (big-endian) + bincode(RequestHeader, SdRequest)`
//! Response:    `u32 length prefix (big-endian) + bincode(SdResponse)`
//!
//! Uses [`SockfdCache`] for connection pooling to avoid TCP handshake overhead
//! on repeated peer requests to the same node.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error};

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse};

use crate::sockfd_cache::SockfdCache;
use crate::transport::{PeerListener, PeerRequest, PeerResponder, PeerTransport};

/// Maximum response size (64 MB) — protects against corrupt length prefixes.
const MAX_RESPONSE_SIZE: usize = 64 * 1024 * 1024;

// ─── PeerTransport implementation ─────────────────────────────────────────────

/// TCP-based peer transport.
///
/// Uses kernel TCP sockets with optional connection pooling.
/// This is the default transport and is always available.
pub struct TcpTransport {
    cache: SockfdCache,
    shutdown: Arc<Notify>,
}

impl TcpTransport {
    /// Create a new TCP transport with connection pooling.
    ///
    /// `max_conns_per_node` controls the pool size per peer.
    pub fn new(max_conns_per_node: usize) -> Self {
        Self {
            cache: SockfdCache::new(max_conns_per_node),
            shutdown: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl PeerTransport for TcpTransport {
    fn name(&self) -> &str {
        "tcp"
    }

    async fn send_request(
        &self,
        addr: SocketAddr,
        header: RequestHeader,
        req: SdRequest,
    ) -> SdResult<SdResponse> {
        // Serialize the request
        let req_data =
            bincode::serialize(&(header, req)).map_err(|_| SdError::SystemError)?;

        // Get or create a connection
        let nid = sheepdog_proto::node::NodeId {
            addr: addr.ip(),
            port: addr.port(),
            io_addr: None,
            io_port: 0,
        };

        let mut stream = self.cache.get_or_connect(&nid).await?;

        // Write: u32 length + payload
        let write_result = async {
            stream
                .write_u32(req_data.len() as u32)
                .await
                .map_err(|_| SdError::NetworkError)?;
            stream
                .write_all(&req_data)
                .await
                .map_err(|_| SdError::NetworkError)?;

            // Read: u32 length + response
            let resp_len = stream
                .read_u32()
                .await
                .map_err(|_| SdError::NetworkError)? as usize;

            if resp_len > MAX_RESPONSE_SIZE {
                return Err(SdError::InvalidParms);
            }

            let mut resp_buf = vec![0u8; resp_len];
            stream
                .read_exact(&mut resp_buf)
                .await
                .map_err(|_| SdError::NetworkError)?;

            let response: SdResponse =
                bincode::deserialize(&resp_buf).map_err(|_| SdError::SystemError)?;

            Ok::<(SdResponse, _), SdError>((response, stream))
        }
        .await;

        match write_result {
            Ok((response, stream)) => {
                // Return the connection to the pool for reuse
                self.cache.put(&nid, stream);
                Ok(response)
            }
            Err(e) => {
                // Connection failed — don't return to pool
                self.cache.clear_node(&nid);
                Err(e)
            }
        }
    }

    async fn start_listener(
        &self,
        bind_addr: SocketAddr,
    ) -> SdResult<Box<dyn PeerListener>> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|e| {
                error!("TCP peer listener failed to bind {}: {}", bind_addr, e);
                SdError::SystemError
            })?;

        debug!("TCP peer listener on {}", bind_addr);

        let (tx, rx) = mpsc::channel::<PeerRequest>(256);
        let shutdown = self.shutdown.clone();

        // Accept loop: runs in a spawned task, sends PeerRequests to the channel.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer_addr)) => {
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = handle_tcp_peer(stream, peer_addr, tx).await {
                                        debug!("TCP peer {} closed: {}", peer_addr, e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("TCP accept error: {}", e);
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        debug!("TCP peer listener shutting down");
                        break;
                    }
                }
            }
        });

        Ok(Box::new(TcpPeerListener { rx: tokio::sync::Mutex::new(rx) }))
    }

    async fn shutdown(&self) -> SdResult<()> {
        self.shutdown.notify_waiters();
        self.cache.clear_all();
        Ok(())
    }
}

// ─── TCP connection handler ───────────────────────────────────────────────────

/// Handle a single inbound TCP connection from a peer.
///
/// Reads length-prefixed bincode frames, creates `PeerRequest`s, and sends
/// them through the channel to the listener.
async fn handle_tcp_peer(
    mut stream: tokio::net::TcpStream,
    _peer_addr: SocketAddr,
    tx: mpsc::Sender<PeerRequest>,
) -> SdResult<()> {
    stream.set_nodelay(true).ok();

    loop {
        // Read frame length
        let frame_len = match stream.read_u32().await {
            Ok(len) => len as usize,
            Err(_) => return Ok(()), // Connection closed
        };

        if frame_len == 0 || frame_len > MAX_RESPONSE_SIZE {
            return Err(SdError::InvalidParms);
        }

        // Read frame
        let mut buf = vec![0u8; frame_len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|_| SdError::NetworkError)?;

        // Deserialize
        let (header, req): (RequestHeader, SdRequest) =
            bincode::deserialize(&buf).map_err(|_| SdError::SystemError)?;

        // Create a one-shot responder that writes back on this stream
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<SdResponse>();

        let request = PeerRequest {
            header,
            req,
            responder: Box::new(TcpResponder { tx: resp_tx }),
        };

        // Send to the listener channel
        if tx.send(request).await.is_err() {
            return Ok(()); // Listener dropped
        }

        // Wait for the response and send it back on the TCP stream
        let response = match resp_rx.await {
            Ok(resp) => resp,
            Err(_) => return Ok(()), // Responder dropped
        };

        let resp_data =
            bincode::serialize(&response).map_err(|_| SdError::SystemError)?;

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

// ─── PeerListener implementation ──────────────────────────────────────────────

struct TcpPeerListener {
    rx: tokio::sync::Mutex<mpsc::Receiver<PeerRequest>>,
}

#[async_trait]
impl PeerListener for TcpPeerListener {
    async fn accept(&self) -> SdResult<PeerRequest> {
        let mut rx = self.rx.lock().await;
        rx.recv()
            .await
            .ok_or(SdError::SystemError)
    }
}

// ─── PeerResponder implementation ─────────────────────────────────────────────

struct TcpResponder {
    tx: tokio::sync::oneshot::Sender<SdResponse>,
}

#[async_trait]
impl PeerResponder for TcpResponder {
    async fn respond(self: Box<Self>, response: SdResponse) -> SdResult<()> {
        self.tx
            .send(response)
            .map_err(|_| SdError::SystemError)
    }
}
