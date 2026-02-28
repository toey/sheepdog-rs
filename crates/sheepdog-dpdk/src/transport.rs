//! DPDK-based peer transport — high-performance user-space networking.
//!
//! Uses DPDK poll-mode drivers with UDP for peer-to-peer data I/O.
//! A dedicated poll thread handles RX/TX while communicating with the
//! tokio runtime via channels.
//!
//! Architecture:
//!
//! ```text
//!   tokio tasks                DPDK poll thread
//!   ──────────                ─────────────────
//!   send_request() ──tx_chan──► rte_eth_tx_burst()
//!                                   │
//!   oneshot::recv() ◄─pending─── rte_eth_rx_burst()
//!                                   │
//!   accept()       ◄──rx_chan──── dispatch (req/resp)
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tracing::{debug, error, info, warn};

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse};

use sheepdog_core::transport::{PeerListener, PeerRequest, PeerResponder, PeerTransport};

use crate::eal::{DpdkConfig, DpdkContext};
use crate::packet;

/// DPDK-based peer transport.
///
/// When DPDK libraries are available, this runs a dedicated poll thread
/// that handles RX/TX via the DPDK PMD (poll-mode driver).
///
/// When DPDK is not installed (stub mode), `new()` returns an error.
pub struct DpdkTransport {
    /// DPDK runtime context.
    _ctx: Arc<DpdkContext>,

    /// Next request ID (atomic counter for unique IDs).
    next_req_id: AtomicU64,

    /// Pending outbound requests awaiting responses.
    /// Key: request_id, Value: oneshot sender for the response.
    pending: Arc<DashMap<u64, oneshot::Sender<SdResponse>>>,

    /// Channel for sending TX requests to the DPDK poll thread.
    tx_send: crossbeam_channel::Sender<TxRequest>,

    /// Channel for receiving inbound peer requests (for the listener).
    rx_recv: Arc<Mutex<mpsc::Receiver<PeerRequest>>>,
    rx_send: mpsc::Sender<PeerRequest>,

    /// Shutdown signal.
    shutdown: Arc<Notify>,

    /// Local UDP port for the data plane.
    data_port: u16,
}

/// A TX request queued for the DPDK poll thread.
struct TxRequest {
    /// Destination address.
    dst: SocketAddr,
    /// Serialized message data (will be fragmented).
    data: Vec<u8>,
    /// Whether this is a response (sets FLAG_IS_RESPONSE).
    is_response: bool,
    /// Request ID for fragment headers.
    request_id: u64,
}

impl DpdkTransport {
    /// Create a new DPDK transport.
    ///
    /// Initializes DPDK EAL, configures ports, and starts the poll thread.
    pub fn new(config: DpdkConfig) -> Result<Self, String> {
        let data_port = config.data_port;
        let ctx = Arc::new(DpdkContext::init(config)?);

        let pending: Arc<DashMap<u64, oneshot::Sender<SdResponse>>> =
            Arc::new(DashMap::new());
        let (tx_send, _tx_recv) = crossbeam_channel::bounded::<TxRequest>(4096);
        let (rx_send, rx_recv) = mpsc::channel::<PeerRequest>(256);
        let shutdown = Arc::new(Notify::new());

        // TODO: Start the DPDK poll thread.
        //
        // The poll thread would:
        // 1. Call rte_eth_rx_burst() to receive packets
        // 2. Parse Eth/IP/UDP/DpdkPeerHeader
        // 3. Reassemble fragments
        // 4. For responses: look up pending map, send via oneshot
        // 5. For requests: send to rx_send channel
        // 6. Drain tx_recv channel, build packets, call rte_eth_tx_burst()
        //
        // This requires a dedicated core (configured via EAL -l args).
        // The thread is started via rte_eal_remote_launch() or std::thread::spawn.

        info!("DPDK transport initialized (data_port={})", data_port);

        Ok(Self {
            _ctx: ctx,
            next_req_id: AtomicU64::new(1),
            pending,
            tx_send,
            rx_recv: Arc::new(Mutex::new(rx_recv)),
            rx_send,
            shutdown,
            data_port,
        })
    }
}

#[async_trait]
impl PeerTransport for DpdkTransport {
    fn name(&self) -> &str {
        "dpdk"
    }

    async fn send_request(
        &self,
        addr: SocketAddr,
        header: RequestHeader,
        req: SdRequest,
    ) -> SdResult<SdResponse> {
        let request_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);

        // Serialize the request (same bincode format as TCP)
        let data = bincode::serialize(&(header, req))
            .map_err(|_| SdError::SystemError)?;

        // Register pending response
        let (resp_tx, resp_rx) = oneshot::channel();
        self.pending.insert(request_id, resp_tx);

        // Queue for TX
        let tx_req = TxRequest {
            dst: addr,
            data,
            is_response: false,
            request_id,
        };

        self.tx_send
            .send(tx_req)
            .map_err(|_| {
                self.pending.remove(&request_id);
                SdError::NetworkError
            })?;

        // Await response with timeout
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            resp_rx,
        )
        .await
        .map_err(|_| {
            self.pending.remove(&request_id);
            warn!("DPDK request {} timed out to {}", request_id, addr);
            SdError::NetworkError
        })?
        .map_err(|_| {
            self.pending.remove(&request_id);
            SdError::SystemError
        })?;

        Ok(response)
    }

    async fn start_listener(
        &self,
        bind_addr: SocketAddr,
    ) -> SdResult<Box<dyn PeerListener>> {
        debug!("DPDK peer listener on {} (data_port={})", bind_addr, self.data_port);

        // The rx_recv channel is already being fed by the DPDK poll thread.
        // Just wrap it in a PeerListener.
        Ok(Box::new(DpdkPeerListener {
            rx: self.rx_recv.clone(),
        }))
    }

    async fn shutdown(&self) -> SdResult<()> {
        info!("DPDK transport shutting down");
        self.shutdown.notify_waiters();
        self.pending.clear();
        Ok(())
    }
}

// ─── PeerListener ─────────────────────────────────────────────────────────────

struct DpdkPeerListener {
    rx: Arc<Mutex<mpsc::Receiver<PeerRequest>>>,
}

#[async_trait]
impl PeerListener for DpdkPeerListener {
    async fn accept(&self) -> SdResult<PeerRequest> {
        let mut rx = self.rx.lock().await;
        rx.recv()
            .await
            .ok_or(SdError::SystemError)
    }
}

// ─── PeerResponder ────────────────────────────────────────────────────────────

/// Responder that sends the reply back via the DPDK TX channel.
struct DpdkResponder {
    tx_send: crossbeam_channel::Sender<TxRequest>,
    dst: SocketAddr,
    request_id: u64,
}

#[async_trait]
impl PeerResponder for DpdkResponder {
    async fn respond(self: Box<Self>, response: SdResponse) -> SdResult<()> {
        let data = bincode::serialize(&response)
            .map_err(|_| SdError::SystemError)?;

        let tx_req = TxRequest {
            dst: self.dst,
            data,
            is_response: true,
            request_id: self.request_id,
        };

        self.tx_send
            .send(tx_req)
            .map_err(|_| SdError::NetworkError)?;

        Ok(())
    }
}
