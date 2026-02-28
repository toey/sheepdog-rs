//! P2P TCP mesh cluster driver (`sdcluster`).
//!
//! This is the Rust port of the original `sdcluster.c` cluster driver.
//! It implements a fully-connected TCP mesh between all sheepdog nodes.
//!
//! ## Design
//!
//! - Each node listens on a cluster port (listen_port + 1 by default).
//! - When a node starts, it connects to a set of seed addresses.
//! - Once connected, the peer list is exchanged and every node maintains
//!   connections to all other nodes.
//! - Leader election: the node with the smallest `NodeId` is the leader.
//!   The leader is responsible for ordering join/leave events.
//! - Heartbeats are sent every 5 seconds. If no heartbeat is received
//!   within 15 seconds a peer is considered dead.
//!
//! ## Wire Protocol
//!
//! All messages are length-prefixed: a 4-byte little-endian u32 giving
//! the size of the bincode-encoded `ClusterMessage`, followed by the
//! encoded bytes.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio::time;
use tracing::{debug, error, info, warn};

use sheepdog_proto::defaults::{
    DEFAULT_CLUSTER_EVENT_CHANNEL_SIZE, DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_SECS,
    DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT_SECS, DEFAULT_CLUSTER_MAX_MESSAGE_SIZE,
    DEFAULT_CLUSTER_PEER_WRITE_CHANNEL_SIZE, DEFAULT_CLUSTER_PORT_OFFSET,
};
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::node::SdNode;

use super::{ClusterDriver, ClusterEvent};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Interval between heartbeat messages.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_SECS);

/// If no message is received from a peer within this window it is declared dead.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT_SECS);

/// Maximum size of a single wire message.
const MAX_MESSAGE_SIZE: u32 = DEFAULT_CLUSTER_MAX_MESSAGE_SIZE;

/// Channel buffer size for the event queue.
const EVENT_CHANNEL_SIZE: usize = DEFAULT_CLUSTER_EVENT_CHANNEL_SIZE;

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

/// Message types exchanged over the TCP mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    /// Join request from a new node.
    Join {
        node: SdNode,
    },
    /// Response to a join request, carrying the current member list.
    JoinResponse {
        success: bool,
        members: Vec<SdNode>,
    },
    /// Graceful leave announcement.
    Leave {
        node: SdNode,
    },
    /// Periodic heartbeat.
    Heartbeat {
        node: SdNode,
    },
    /// Broadcast notification with opaque payload.
    Notify {
        data: Vec<u8>,
    },
    /// Cluster-wide block request (phase 1).
    Block,
    /// Cluster-wide unblock with result payload (phase 2).
    Unblock {
        data: Vec<u8>,
    },
    /// Leader election message.
    Election {
        candidate: SdNode,
    },
    /// Response to an election message.
    ElectionResponse {
        leader: SdNode,
    },
}

// ---------------------------------------------------------------------------
// Peer tracking
// ---------------------------------------------------------------------------

/// Runtime state for a single connected peer.
#[derive(Debug)]
struct PeerState {
    /// The node descriptor.
    node: SdNode,
    /// Address we use to reach this peer.
    addr: SocketAddr,
    /// Instant of the last message received from this peer.
    last_seen: tokio::time::Instant,
    /// Sender half of the write channel for this peer connection.
    /// Messages placed here are serialized and written to the TCP stream.
    write_tx: mpsc::Sender<ClusterMessage>,
}

// ---------------------------------------------------------------------------
// Shared inner state
// ---------------------------------------------------------------------------

struct Inner {
    /// This node's identity.
    this_node: Option<SdNode>,
    /// Address the cluster listener is bound to.
    listen_addr: Option<SocketAddr>,
    /// Active peers keyed by their `NodeId` string representation.
    peers: HashMap<String, PeerState>,
    /// Whether this node has joined the cluster.
    joined: bool,
    /// Seed addresses used for initial connection.
    seeds: Vec<SocketAddr>,
}

impl Inner {
    fn new() -> Self {
        Self {
            this_node: None,
            listen_addr: None,
            peers: HashMap::new(),
            joined: false,
            seeds: Vec::new(),
        }
    }

    /// Determine the current leader: the node with the smallest NodeId
    /// among this node and all connected peers.
    fn current_leader(&self) -> Option<SdNode> {
        let mut candidates: Vec<&SdNode> = self.peers.values().map(|p| &p.node).collect();
        if let Some(ref this) = self.this_node {
            candidates.push(this);
        }
        candidates.into_iter().min_by_key(|n| &n.nid).cloned()
    }

    /// Check if this node is the current leader.
    fn is_leader(&self) -> bool {
        match (&self.this_node, self.current_leader()) {
            (Some(this), Some(leader)) => this.nid == leader.nid,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// SdClusterDriver
// ---------------------------------------------------------------------------

/// P2P TCP mesh cluster driver.
pub struct SdClusterDriver {
    state: Arc<RwLock<Inner>>,
    /// Outbound event channel: events are placed here by the network tasks
    /// and consumed by `recv_event`.
    event_tx: mpsc::Sender<ClusterEvent>,
    event_rx: Arc<Mutex<mpsc::Receiver<ClusterEvent>>>,
    /// Notified when background tasks should shut down.
    shutdown: Arc<Notify>,
    /// The cluster port offset from the sheep listen port.
    port_offset: u16,
}

impl SdClusterDriver {
    /// Create a new TCP mesh cluster driver.
    ///
    /// `seeds` is a list of addresses of existing cluster members to
    /// contact during join. For the very first node this may be empty.
    ///
    /// `port_offset` is added to the sheep listen port to derive the
    /// cluster communication port (default: 1).
    pub fn new(seeds: Vec<SocketAddr>, port_offset: u16) -> Self {
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_SIZE);
        Self {
            state: Arc::new(RwLock::new(Inner::new())),
            event_tx: tx,
            event_rx: Arc::new(Mutex::new(rx)),
            shutdown: Arc::new(Notify::new()),
            port_offset,
        }
    }

    /// Convenience constructor with the default port offset of 1.
    pub fn with_seeds(seeds: Vec<SocketAddr>) -> Self {
        Self::new(seeds, 1)
    }

    // -------------------------------------------------------------------
    // Wire helpers
    // -------------------------------------------------------------------

    /// Write a length-prefixed, bincode-encoded message to a stream.
    async fn write_message(
        stream: &mut (impl AsyncWriteExt + Unpin),
        msg: &ClusterMessage,
    ) -> SdResult<()> {
        let encoded =
            bincode::serialize(msg).map_err(|_| SdError::ClusterError)?;
        let len = encoded.len() as u32;
        if len > MAX_MESSAGE_SIZE {
            return Err(SdError::ClusterError);
        }
        stream
            .write_all(&len.to_le_bytes())
            .await
            .map_err(|_| SdError::NetworkError)?;
        stream
            .write_all(&encoded)
            .await
            .map_err(|_| SdError::NetworkError)?;
        stream.flush().await.map_err(|_| SdError::NetworkError)?;
        Ok(())
    }

    /// Read a length-prefixed, bincode-encoded message from a stream.
    async fn read_message(
        stream: &mut (impl AsyncReadExt + Unpin),
    ) -> SdResult<ClusterMessage> {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|_| SdError::NetworkError)?;
        let len = u32::from_le_bytes(len_buf);
        if len > MAX_MESSAGE_SIZE {
            return Err(SdError::ClusterError);
        }
        let mut buf = vec![0u8; len as usize];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|_| SdError::NetworkError)?;
        bincode::deserialize(&buf).map_err(|_| SdError::ClusterError)
    }

    // -------------------------------------------------------------------
    // Background tasks
    // -------------------------------------------------------------------

    /// Start the TCP listener that accepts inbound peer connections.
    fn spawn_listener(
        state: Arc<RwLock<Inner>>,
        event_tx: mpsc::Sender<ClusterEvent>,
        shutdown: Arc<Notify>,
        listen_addr: SocketAddr,
    ) {
        tokio::spawn(async move {
            let listener = match TcpListener::bind(listen_addr).await {
                Ok(l) => {
                    info!("sdcluster: listening on {}", listen_addr);
                    l
                }
                Err(e) => {
                    error!("sdcluster: failed to bind {}: {}", listen_addr, e);
                    return;
                }
            };

            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        info!("sdcluster: listener shutting down");
                        break;
                    }
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, peer_addr)) => {
                                debug!("sdcluster: accepted connection from {}", peer_addr);
                                let state_c = state.clone();
                                let event_tx_c = event_tx.clone();
                                let shutdown_c = shutdown.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_connection(
                                        state_c,
                                        event_tx_c,
                                        shutdown_c,
                                        stream,
                                        peer_addr,
                                    )
                                    .await
                                    {
                                        debug!("sdcluster: connection from {} ended: {}", peer_addr, e);
                                    }
                                });
                            }
                            Err(e) => {
                                warn!("sdcluster: accept error: {}", e);
                            }
                        }
                    }
                }
            }
        });
    }

    /// Handle a single inbound TCP connection.
    ///
    /// Reads messages in a loop and dispatches them. When a `Join` is
    /// received, registers the peer and sends a `JoinResponse`.
    async fn handle_connection(
        state: Arc<RwLock<Inner>>,
        event_tx: mpsc::Sender<ClusterEvent>,
        shutdown: Arc<Notify>,
        stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> SdResult<()> {
        let (read_half, write_half) = stream.into_split();
        let mut reader = tokio::io::BufReader::new(read_half);
        let mut writer = tokio::io::BufWriter::new(write_half);

        // The first message from an inbound connection should be a Join or
        // Heartbeat identifying the peer.
        let first_msg = Self::read_message(&mut reader).await?;

        let peer_key = match &first_msg {
            ClusterMessage::Join { node } => {
                let key = node.nid.to_string();
                debug!("sdcluster: peer {} joining via {}", key, peer_addr);

                // Register the peer.
                let (write_tx, mut write_rx) = mpsc::channel::<ClusterMessage>(DEFAULT_CLUSTER_PEER_WRITE_CHANNEL_SIZE);

                {
                    let mut s = state.write().await;
                    let members: Vec<SdNode> = {
                        let mut m: Vec<SdNode> =
                            s.peers.values().map(|p| p.node.clone()).collect();
                        if let Some(ref this) = s.this_node {
                            m.push(this.clone());
                        }
                        m
                    };

                    // Send join response with member list.
                    let resp = ClusterMessage::JoinResponse {
                        success: true,
                        members,
                    };
                    Self::write_message(&mut writer, &resp).await?;

                    s.peers.insert(
                        key.clone(),
                        PeerState {
                            node: node.clone(),
                            addr: peer_addr,
                            last_seen: tokio::time::Instant::now(),
                            write_tx,
                        },
                    );
                }

                // Spawn a writer task that drains the write channel.
                let shutdown_w = shutdown.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = shutdown_w.notified() => break,
                            msg = write_rx.recv() => {
                                match msg {
                                    Some(m) => {
                                        if Self::write_message(&mut writer, &m).await.is_err() {
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                });

                // Deliver join event to the daemon.
                let _ = event_tx.send(ClusterEvent::Join(node.clone())).await;

                key
            }
            ClusterMessage::Heartbeat { node } => {
                let key = node.nid.to_string();
                // Update last_seen for an already-registered peer.
                let mut s = state.write().await;
                if let Some(peer) = s.peers.get_mut(&key) {
                    peer.last_seen = tokio::time::Instant::now();
                }
                key
            }
            _ => {
                warn!("sdcluster: unexpected first message from {}", peer_addr);
                return Err(SdError::ClusterError);
            }
        };

        // Read loop for subsequent messages.
        loop {
            let msg = tokio::select! {
                _ = shutdown.notified() => break,
                result = Self::read_message(&mut reader) => {
                    match result {
                        Ok(m) => m,
                        Err(_) => break,
                    }
                }
            };

            Self::dispatch_message(&state, &event_tx, &peer_key, msg).await?;
        }

        // Peer disconnected: remove and deliver leave event.
        Self::remove_peer(&state, &event_tx, &peer_key).await;

        Ok(())
    }

    /// Dispatch a received message to the appropriate handler.
    async fn dispatch_message(
        state: &Arc<RwLock<Inner>>,
        event_tx: &mpsc::Sender<ClusterEvent>,
        peer_key: &str,
        msg: ClusterMessage,
    ) -> SdResult<()> {
        match msg {
            ClusterMessage::Heartbeat { node } => {
                let mut s = state.write().await;
                if let Some(peer) = s.peers.get_mut(peer_key) {
                    peer.last_seen = tokio::time::Instant::now();
                    peer.node = node;
                }
            }
            ClusterMessage::Leave { node } => {
                info!("sdcluster: peer {} announced leave", node.nid);
                Self::remove_peer(state, event_tx, peer_key).await;
            }
            ClusterMessage::Notify { data } => {
                let _ = event_tx.send(ClusterEvent::Notify(data)).await;
            }
            ClusterMessage::Block => {
                let _ = event_tx.send(ClusterEvent::Block).await;
            }
            ClusterMessage::Unblock { data } => {
                let _ = event_tx.send(ClusterEvent::Unblock(data)).await;
            }
            ClusterMessage::Join { node } => {
                // Duplicate join on an existing connection: update.
                let mut s = state.write().await;
                if let Some(peer) = s.peers.get_mut(peer_key) {
                    peer.node = node.clone();
                    peer.last_seen = tokio::time::Instant::now();
                }
                drop(s);
                let _ = event_tx.send(ClusterEvent::Join(node)).await;
            }
            ClusterMessage::JoinResponse { .. } => {
                // Only expected on outbound connections; ignore on inbound.
                debug!("sdcluster: ignoring JoinResponse on inbound connection");
            }
            ClusterMessage::Election { candidate } => {
                debug!("sdcluster: received election from {}", candidate.nid);
                let s = state.read().await;
                if let Some(leader) = s.current_leader() {
                    if let Some(peer) = s.peers.get(peer_key) {
                        let resp = ClusterMessage::ElectionResponse {
                            leader: leader.clone(),
                        };
                        let _ = peer.write_tx.send(resp).await;
                    }
                }
            }
            ClusterMessage::ElectionResponse { leader } => {
                debug!("sdcluster: election result: leader={}", leader.nid);
            }
        }
        Ok(())
    }

    /// Remove a peer from the peer map and emit a Leave event.
    async fn remove_peer(
        state: &Arc<RwLock<Inner>>,
        event_tx: &mpsc::Sender<ClusterEvent>,
        peer_key: &str,
    ) {
        let mut s = state.write().await;
        if let Some(peer) = s.peers.remove(peer_key) {
            info!("sdcluster: removed peer {}", peer_key);
            drop(s);
            let _ = event_tx.send(ClusterEvent::Leave(peer.node)).await;
        }
    }

    /// Spawn the heartbeat sender that periodically pings all peers.
    fn spawn_heartbeat(
        state: Arc<RwLock<Inner>>,
        shutdown: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            let mut interval = time::interval(HEARTBEAT_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = interval.tick() => {
                        let s = state.read().await;
                        let node = match &s.this_node {
                            Some(n) => n.clone(),
                            None => continue,
                        };
                        if !s.joined {
                            continue;
                        }
                        let msg = ClusterMessage::Heartbeat { node };
                        for peer in s.peers.values() {
                            let _ = peer.write_tx.send(msg.clone()).await;
                        }
                    }
                }
            }
        });
    }

    /// Spawn the dead-peer reaper that removes peers whose heartbeat
    /// has timed out.
    fn spawn_reaper(
        state: Arc<RwLock<Inner>>,
        event_tx: mpsc::Sender<ClusterEvent>,
        shutdown: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            let mut interval = time::interval(HEARTBEAT_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = interval.tick() => {
                        let now = tokio::time::Instant::now();
                        let dead_keys: Vec<String> = {
                            let s = state.read().await;
                            s.peers
                                .iter()
                                .filter(|(_, p)| now.duration_since(p.last_seen) > HEARTBEAT_TIMEOUT)
                                .map(|(k, _)| k.clone())
                                .collect()
                        };

                        for key in dead_keys {
                            warn!("sdcluster: peer {} heartbeat timeout, removing", key);
                            Self::remove_peer(&state, &event_tx, &key).await;
                        }
                    }
                }
            }
        });
    }

    /// Connect to a seed peer, send a Join, and process the JoinResponse
    /// to discover other members.
    async fn connect_to_seed(
        state: Arc<RwLock<Inner>>,
        event_tx: mpsc::Sender<ClusterEvent>,
        shutdown: Arc<Notify>,
        seed_addr: SocketAddr,
        port_offset: u16,
    ) -> SdResult<Vec<SdNode>> {
        let stream = TcpStream::connect(seed_addr)
            .await
            .map_err(|_| SdError::NetworkError)?;

        let (read_half, write_half) = stream.into_split();
        let mut reader = tokio::io::BufReader::new(read_half);
        let mut writer = tokio::io::BufWriter::new(write_half);

        // Send our join request.
        let this_node = {
            let s = state.read().await;
            s.this_node.clone().ok_or(SdError::ClusterError)?
        };

        let join_msg = ClusterMessage::Join {
            node: this_node.clone(),
        };
        Self::write_message(&mut writer, &join_msg).await?;

        // Read the join response.
        let resp = Self::read_message(&mut reader).await?;

        let members = match resp {
            ClusterMessage::JoinResponse { success, members } => {
                if !success {
                    return Err(SdError::JoinFailed);
                }
                members
            }
            _ => return Err(SdError::ClusterError),
        };

        // Register this seed as a peer.
        let peer_key = {
            // Find the seed's SdNode in the members list by matching both IP
            // and port (node.port + port_offset == seed cluster port).
            // IP-only matching is wrong when multiple nodes share the same IP
            // (e.g. localhost testing with 127.0.0.1).
            let key_node = members.iter().find(|n| {
                n.nid.addr == seed_addr.ip()
                    && n.nid.port + port_offset == seed_addr.port()
            });
            match key_node {
                Some(n) => n.nid.to_string(),
                None => seed_addr.to_string(),
            }
        };

        let (write_tx, mut write_rx) = mpsc::channel::<ClusterMessage>(DEFAULT_CLUSTER_PEER_WRITE_CHANNEL_SIZE);

        {
            let mut s = state.write().await;
            // Register the seed peer for the inbound reader half.
            // Match by both IP and port (with offset) to avoid misidentifying
            // peers when multiple nodes share the same IP address.
            let seed_node = members
                .iter()
                .find(|n| {
                    n.nid.addr == seed_addr.ip()
                        && n.nid.port + port_offset == seed_addr.port()
                })
                .cloned()
                .unwrap_or_else(|| this_node.clone());

            s.peers.insert(
                peer_key.clone(),
                PeerState {
                    node: seed_node,
                    addr: seed_addr,
                    last_seen: tokio::time::Instant::now(),
                    write_tx,
                },
            );
        }

        // Spawn writer task.
        let shutdown_w = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_w.notified() => break,
                    msg = write_rx.recv() => {
                        match msg {
                            Some(m) => {
                                if Self::write_message(&mut writer, &m).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        // Spawn reader task.
        let state_r = state.clone();
        let event_tx_r = event_tx.clone();
        let shutdown_r = shutdown.clone();
        let pk = peer_key.clone();
        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = shutdown_r.notified() => break,
                    result = Self::read_message(&mut reader) => {
                        match result {
                            Ok(m) => m,
                            Err(_) => break,
                        }
                    }
                };
                if Self::dispatch_message(&state_r, &event_tx_r, &pk, msg)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Self::remove_peer(&state_r, &event_tx_r, &pk).await;
        });

        Ok(members)
    }

    /// Connect to a discovered peer (non-seed). Same logic as seed but
    /// we already know the node identity.
    async fn connect_to_peer(
        state: Arc<RwLock<Inner>>,
        event_tx: mpsc::Sender<ClusterEvent>,
        shutdown: Arc<Notify>,
        peer_node: SdNode,
        peer_cluster_addr: SocketAddr,
    ) -> SdResult<()> {
        let peer_key = peer_node.nid.to_string();

        // Skip if already connected.
        {
            let s = state.read().await;
            if s.peers.contains_key(&peer_key) {
                return Ok(());
            }
            // Don't connect to ourselves.
            if let Some(ref this) = s.this_node {
                if this.nid == peer_node.nid {
                    return Ok(());
                }
            }
        }

        let stream = TcpStream::connect(peer_cluster_addr)
            .await
            .map_err(|_| SdError::NetworkError)?;

        let (read_half, write_half) = stream.into_split();
        let mut reader = tokio::io::BufReader::new(read_half);
        let mut writer = tokio::io::BufWriter::new(write_half);

        // Announce ourselves.
        let this_node = {
            let s = state.read().await;
            s.this_node.clone().ok_or(SdError::ClusterError)?
        };
        let join_msg = ClusterMessage::Join { node: this_node };
        Self::write_message(&mut writer, &join_msg).await?;

        // Read join response (may be ignored for secondary connections).
        let _resp = Self::read_message(&mut reader).await?;

        let (write_tx, mut write_rx) = mpsc::channel::<ClusterMessage>(DEFAULT_CLUSTER_PEER_WRITE_CHANNEL_SIZE);

        {
            let mut s = state.write().await;
            s.peers.insert(
                peer_key.clone(),
                PeerState {
                    node: peer_node,
                    addr: peer_cluster_addr,
                    last_seen: tokio::time::Instant::now(),
                    write_tx,
                },
            );
        }

        // Writer task.
        let shutdown_w = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_w.notified() => break,
                    msg = write_rx.recv() => {
                        match msg {
                            Some(m) => {
                                if Self::write_message(&mut writer, &m).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        // Reader task.
        let state_r = state.clone();
        let event_tx_r = event_tx.clone();
        let shutdown_r = shutdown.clone();
        let pk = peer_key.clone();
        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = shutdown_r.notified() => break,
                    result = Self::read_message(&mut reader) => {
                        match result {
                            Ok(m) => m,
                            Err(_) => break,
                        }
                    }
                };
                if Self::dispatch_message(&state_r, &event_tx_r, &pk, msg)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Self::remove_peer(&state_r, &event_tx_r, &pk).await;
        });

        Ok(())
    }

    /// Broadcast a message to all connected peers.
    async fn broadcast(state: &RwLock<Inner>, msg: ClusterMessage) {
        let s = state.read().await;
        for peer in s.peers.values() {
            if peer.write_tx.send(msg.clone()).await.is_err() {
                warn!("sdcluster: failed to send to peer {}", peer.node.nid);
            }
        }
    }
}

#[async_trait]
impl ClusterDriver for SdClusterDriver {
    fn name(&self) -> &str {
        "sdcluster"
    }

    async fn init(&self, this_node: &SdNode) -> SdResult<()> {
        let cluster_port = this_node.nid.port + self.port_offset;
        let listen_addr = SocketAddr::new(this_node.nid.addr, cluster_port);

        {
            let mut s = self.state.write().await;
            s.this_node = Some(this_node.clone());
            s.listen_addr = Some(listen_addr);
        }

        info!("sdcluster: init node {} (cluster port {})", this_node.nid, cluster_port);

        // Start the listener, heartbeat, and reaper.
        Self::spawn_listener(
            self.state.clone(),
            self.event_tx.clone(),
            self.shutdown.clone(),
            listen_addr,
        );
        Self::spawn_heartbeat(self.state.clone(), self.shutdown.clone());
        Self::spawn_reaper(
            self.state.clone(),
            self.event_tx.clone(),
            self.shutdown.clone(),
        );

        Ok(())
    }

    async fn join(&self, node: &SdNode) -> SdResult<()> {
        let seeds = {
            let s = self.state.read().await;
            s.seeds.clone()
        };

        if seeds.is_empty() {
            // First node in the cluster: just emit our own join event.
            info!("sdcluster: first node, self-joining");
            {
                let mut s = self.state.write().await;
                s.joined = true;
            }
            self.event_tx
                .send(ClusterEvent::Join(node.clone()))
                .await
                .map_err(|_| SdError::ClusterError)?;
            return Ok(());
        }

        // Try each seed until one succeeds.
        let mut last_err = SdError::NetworkError;
        let mut discovered_members = Vec::new();

        for seed in &seeds {
            match Self::connect_to_seed(
                self.state.clone(),
                self.event_tx.clone(),
                self.shutdown.clone(),
                *seed,
                self.port_offset,
            )
            .await
            {
                Ok(members) => {
                    discovered_members = members;
                    break;
                }
                Err(e) => {
                    warn!("sdcluster: failed to connect to seed {}: {}", seed, e);
                    last_err = e;
                }
            }
        }

        if discovered_members.is_empty() {
            let s = self.state.read().await;
            if s.peers.is_empty() {
                return Err(last_err);
            }
        }

        // Connect to all discovered members we are not yet connected to.
        for member in &discovered_members {
            let cluster_addr = SocketAddr::new(
                member.nid.addr,
                member.nid.port + self.port_offset,
            );
            if let Err(e) = Self::connect_to_peer(
                self.state.clone(),
                self.event_tx.clone(),
                self.shutdown.clone(),
                member.clone(),
                cluster_addr,
            )
            .await
            {
                debug!(
                    "sdcluster: could not connect to discovered peer {}: {}",
                    member.nid, e
                );
            }
        }

        {
            let mut s = self.state.write().await;
            s.joined = true;
        }

        // Emit our own join event.
        self.event_tx
            .send(ClusterEvent::Join(node.clone()))
            .await
            .map_err(|_| SdError::ClusterError)?;

        info!(
            "sdcluster: joined cluster with {} peers",
            self.state.read().await.peers.len()
        );

        Ok(())
    }

    async fn leave(&self) -> SdResult<()> {
        let node = {
            let s = self.state.read().await;
            s.this_node.clone().ok_or(SdError::ClusterError)?
        };

        info!("sdcluster: leaving cluster");

        // Announce departure to all peers.
        let msg = ClusterMessage::Leave { node: node.clone() };
        Self::broadcast(&self.state, msg).await;

        // Mark as not-joined and clear peers.
        {
            let mut s = self.state.write().await;
            s.joined = false;
            s.peers.clear();
        }

        // Emit leave event locally.
        self.event_tx
            .send(ClusterEvent::Leave(node))
            .await
            .map_err(|_| SdError::ClusterError)?;

        // Signal background tasks to stop.
        self.shutdown.notify_waiters();

        Ok(())
    }

    async fn notify(&self, data: &[u8]) -> SdResult<()> {
        let s = self.state.read().await;
        if !s.joined {
            return Err(SdError::ClusterError);
        }
        drop(s);

        let msg = ClusterMessage::Notify {
            data: data.to_vec(),
        };
        Self::broadcast(&self.state, msg).await;

        // Also deliver to ourselves.
        self.event_tx
            .send(ClusterEvent::Notify(data.to_vec()))
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn block(&self) -> SdResult<()> {
        let s = self.state.read().await;
        if !s.joined {
            return Err(SdError::ClusterError);
        }
        drop(s);

        Self::broadcast(&self.state, ClusterMessage::Block).await;

        self.event_tx
            .send(ClusterEvent::Block)
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn unblock(&self, data: &[u8]) -> SdResult<()> {
        let s = self.state.read().await;
        if !s.joined {
            return Err(SdError::ClusterError);
        }
        drop(s);

        let msg = ClusterMessage::Unblock {
            data: data.to_vec(),
        };
        Self::broadcast(&self.state, msg).await;

        self.event_tx
            .send(ClusterEvent::Unblock(data.to_vec()))
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn get_local_addr(&self) -> SdResult<SocketAddr> {
        let s = self.state.read().await;
        s.listen_addr.ok_or(SdError::ClusterError)
    }

    async fn recv_event(&self) -> SdResult<ClusterEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await.ok_or(SdError::ClusterError)
    }
}

impl Drop for SdClusterDriver {
    fn drop(&mut self) {
        // Signal all background tasks to stop.
        self.shutdown.notify_waiters();
    }
}

/// Builder for configuring and constructing an `SdClusterDriver`.
pub struct SdClusterDriverBuilder {
    seeds: Vec<SocketAddr>,
    port_offset: u16,
}

impl SdClusterDriverBuilder {
    pub fn new() -> Self {
        Self {
            seeds: Vec::new(),
            port_offset: DEFAULT_CLUSTER_PORT_OFFSET,
        }
    }

    /// Add a seed address.
    pub fn seed(mut self, addr: SocketAddr) -> Self {
        self.seeds.push(addr);
        self
    }

    /// Add multiple seed addresses.
    pub fn seeds(mut self, addrs: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.seeds.extend(addrs);
        self
    }

    /// Set the cluster port offset (default: 1).
    pub fn port_offset(mut self, offset: u16) -> Self {
        self.port_offset = offset;
        self
    }

    /// Build the driver. The returned driver is not yet initialized;
    /// call `init` followed by `join`.
    ///
    /// This is an async method because it needs to store the seed list
    /// in the driver's internal state.
    pub async fn build(self) -> SdClusterDriver {
        let driver = SdClusterDriver::new(self.seeds.clone(), self.port_offset);
        // Store seeds inside the driver state for use during join.
        {
            let mut s = driver.state.write().await;
            s.seeds = self.seeds;
        }
        driver
    }
}

impl Default for SdClusterDriverBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sheepdog_proto::node::NodeId;
    use std::net::{IpAddr, Ipv4Addr};

    fn make_node(port: u16) -> SdNode {
        let nid = NodeId::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        SdNode::new(nid)
    }

    #[tokio::test]
    async fn test_sdcluster_driver_name() {
        let driver = SdClusterDriver::new(vec![], 1);
        assert_eq!(driver.name(), "sdcluster");
    }

    #[tokio::test]
    async fn test_sdcluster_single_node_join() {
        // A single node with no seeds should self-join.
        let driver = SdClusterDriver::new(vec![], 1);
        let node = make_node(7000);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();

        // Should receive our own Join event.
        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Join(n) => assert_eq!(n.nid, node.nid),
            other => panic!("expected Join, got {:?}", other),
        }

        // Clean up.
        driver.shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn test_sdcluster_notify_self() {
        let driver = SdClusterDriver::new(vec![], 1);
        let node = make_node(7010);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();
        let _ = driver.recv_event().await.unwrap(); // consume Join

        let payload = b"test-notify";
        driver.notify(payload).await.unwrap();

        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Notify(data) => assert_eq!(data, payload),
            other => panic!("expected Notify, got {:?}", other),
        }

        driver.shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn test_sdcluster_block_unblock_self() {
        let driver = SdClusterDriver::new(vec![], 1);
        let node = make_node(7020);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();
        let _ = driver.recv_event().await.unwrap(); // consume Join

        driver.block().await.unwrap();
        let event = driver.recv_event().await.unwrap();
        assert!(matches!(event, ClusterEvent::Block));

        driver.unblock(b"done").await.unwrap();
        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Unblock(data) => assert_eq!(data, b"done"),
            other => panic!("expected Unblock, got {:?}", other),
        }

        driver.shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn test_sdcluster_leave() {
        let driver = SdClusterDriver::new(vec![], 1);
        let node = make_node(7030);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();
        let _ = driver.recv_event().await.unwrap(); // consume Join

        driver.leave().await.unwrap();

        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Leave(n) => assert_eq!(n.nid, node.nid),
            other => panic!("expected Leave, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_sdcluster_get_local_addr() {
        let driver = SdClusterDriver::new(vec![], 1);
        let node = make_node(7040);

        driver.init(&node).await.unwrap();

        let addr = driver.get_local_addr().await.unwrap();
        assert_eq!(addr.port(), 7041); // 7040 + 1 (port offset)

        driver.shutdown.notify_waiters();
    }

    #[tokio::test]
    async fn test_message_serialization_roundtrip() {
        let node = make_node(8000);

        let messages = vec![
            ClusterMessage::Join { node: node.clone() },
            ClusterMessage::JoinResponse {
                success: true,
                members: vec![node.clone()],
            },
            ClusterMessage::Leave { node: node.clone() },
            ClusterMessage::Heartbeat { node: node.clone() },
            ClusterMessage::Notify {
                data: vec![1, 2, 3],
            },
            ClusterMessage::Block,
            ClusterMessage::Unblock {
                data: vec![4, 5, 6],
            },
            ClusterMessage::Election {
                candidate: node.clone(),
            },
            ClusterMessage::ElectionResponse { leader: node },
        ];

        for msg in &messages {
            let encoded = bincode::serialize(msg).unwrap();
            let decoded: ClusterMessage = bincode::deserialize(&encoded).unwrap();
            // Just verify it round-trips without panic.
            let _ = format!("{:?}", decoded);
        }
    }

    #[test]
    fn test_inner_leader_election() {
        let mut inner = Inner::new();
        let node_a = make_node(7000);
        let node_b = make_node(7001);
        let node_c = make_node(6999);

        inner.this_node = Some(node_a.clone());

        // With only this node, it should be the leader.
        assert!(inner.is_leader());

        // Add a peer with a higher port: this node still leads.
        let (tx, _rx) = mpsc::channel(1);
        inner.peers.insert(
            node_b.nid.to_string(),
            PeerState {
                node: node_b,
                addr: "127.0.0.1:7002".parse().unwrap(),
                last_seen: tokio::time::Instant::now(),
                write_tx: tx,
            },
        );
        assert!(inner.is_leader());

        // Add a peer with a lower port: it becomes the leader.
        let (tx2, _rx2) = mpsc::channel(1);
        inner.peers.insert(
            node_c.nid.to_string(),
            PeerState {
                node: node_c,
                addr: "127.0.0.1:7000".parse().unwrap(),
                last_seen: tokio::time::Instant::now(),
                write_tx: tx2,
            },
        );
        assert!(!inner.is_leader());
    }
}
