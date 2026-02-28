//! Peer transport abstraction — decouples data-plane I/O from the transport layer.
//!
//! Two implementations:
//! - [`TcpTransport`](crate::tcp_transport::TcpTransport) — standard kernel TCP (always available)
//! - `DpdkTransport` — DPDK user-space networking (feature-gated, Linux only)
//!
//! The gateway, recovery worker, and repair_replica all use `PeerTransport`
//! to send/receive peer requests, making the transport pluggable at startup.

use std::net::SocketAddr;

use async_trait::async_trait;
use sheepdog_proto::error::SdResult;
use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse};

/// Abstraction over the peer-to-peer data transport.
///
/// All peer I/O (gateway forwarding, recovery fetching, replica repair)
/// goes through this trait. The daemon selects TCP or DPDK at startup.
#[async_trait]
pub trait PeerTransport: Send + Sync + 'static {
    /// Human-readable name for logging ("tcp" or "dpdk").
    fn name(&self) -> &str;

    /// Send a request to a peer and await the response.
    ///
    /// This is the core RPC primitive used by the gateway to forward
    /// read/write operations to the nodes responsible for an object.
    async fn send_request(
        &self,
        addr: SocketAddr,
        header: RequestHeader,
        req: SdRequest,
    ) -> SdResult<SdResponse>;

    /// Start listening for inbound peer requests on `bind_addr`.
    ///
    /// Returns a `PeerListener` that yields incoming requests.
    /// Each request comes with a `PeerResponder` to send the reply.
    async fn start_listener(
        &self,
        bind_addr: SocketAddr,
    ) -> SdResult<Box<dyn PeerListener>>;

    /// Shut down the transport, releasing all resources.
    async fn shutdown(&self) -> SdResult<()>;
}

/// Listener for inbound peer requests.
#[async_trait]
pub trait PeerListener: Send + Sync {
    /// Accept the next inbound request. Blocks (async) until one arrives.
    async fn accept(&self) -> SdResult<PeerRequest>;
}

/// An inbound peer request with its response channel.
pub struct PeerRequest {
    /// Request header (epoch, proto version, id).
    pub header: RequestHeader,
    /// The actual request payload.
    pub req: SdRequest,
    /// Send the response back to the requesting peer.
    pub responder: Box<dyn PeerResponder>,
}

/// Handle for sending a response back to a peer.
#[async_trait]
pub trait PeerResponder: Send {
    /// Send the response. Consumes self — each request gets exactly one reply.
    async fn respond(self: Box<Self>, response: SdResponse) -> SdResult<()>;
}
