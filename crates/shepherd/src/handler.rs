//! Shepherd message handlers and coordination logic.
//!
//! Monitors sheep daemon health via heartbeats and coordinates
//! cluster-wide operations like rolling restarts.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::node::SdNode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Shepherd configuration.
pub struct ShepherdConfig {
    pub listen_addr: SocketAddr,
    pub sheep_port: u16,
    pub heartbeat_interval: Duration,
    pub failure_timeout: Duration,
}

/// Messages from sheep to shepherd.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShepherdMsg {
    Register { node: SdNode },
    Heartbeat { epoch: u32 },
    Unregister,
    StatusRequest,
}

/// Messages from shepherd to sheep.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShepherdResponse {
    Ok,
    Status { nodes: Vec<NodeInfo> },
    Error(String),
}

/// Published node info for status queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub addr: String,
    pub alive: bool,
    pub last_heartbeat_secs_ago: u64,
    pub failed_heartbeats: u32,
}

/// State of a monitored sheep node.
#[derive(Debug, Clone)]
struct NodeState {
    node: SdNode,
    last_heartbeat: Instant,
    alive: bool,
    failed_heartbeats: u32,
}

/// Shepherd daemon — monitors and coordinates sheep nodes.
pub struct Shepherd {
    config: ShepherdConfig,
    nodes: Arc<RwLock<BTreeMap<String, NodeState>>>,
}

impl Shepherd {
    pub fn new(config: ShepherdConfig) -> Self {
        Self {
            config,
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Main run loop — listens for registrations and monitors heartbeats.
    pub async fn run(&self) -> SdResult<()> {
        let listener = sheepdog_core::net::create_listen_socket(
            &self.config.listen_addr.ip().to_string(),
            self.config.listen_addr.port(),
        )
        .await?;

        info!("shepherd accepting connections on {}", self.config.listen_addr);

        // Spawn background heartbeat monitor
        let nodes_clone = self.nodes.clone();
        let failure_timeout = self.config.failure_timeout;
        let heartbeat_interval = self.config.heartbeat_interval;
        tokio::spawn(async move {
            monitor_loop(nodes_clone, heartbeat_interval, failure_timeout).await;
        });

        // Main accept loop
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    debug!("connection from {}", peer);
                    let nodes = self.nodes.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(nodes, stream).await {
                            debug!("connection {} error: {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    error!("accept error: {}", e);
                }
            }
        }
    }
}

/// Handle a single connection from a sheep node.
async fn handle_connection(
    nodes: Arc<RwLock<BTreeMap<String, NodeState>>>,
    mut stream: TcpStream,
) -> SdResult<()> {
    loop {
        let len = match stream.read_u32().await {
            Ok(n) => n as usize,
            Err(_) => return Ok(()),
        };

        if len > 1024 * 1024 {
            return Err(SdError::InvalidParms);
        }

        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|_| SdError::NetworkError)?;

        let msg: ShepherdMsg = match bincode::deserialize(&buf) {
            Ok(m) => m,
            Err(_) => {
                let resp = ShepherdResponse::Error("bad message".into());
                send_response(&mut stream, &resp).await?;
                continue;
            }
        };

        let resp = match msg {
            ShepherdMsg::Register { node } => {
                let nid = node.nid.to_string();
                info!("registering node: {}", nid);
                let mut n = nodes.write().await;
                n.insert(
                    nid,
                    NodeState {
                        node,
                        last_heartbeat: Instant::now(),
                        alive: true,
                        failed_heartbeats: 0,
                    },
                );
                ShepherdResponse::Ok
            }
            ShepherdMsg::Heartbeat { epoch } => {
                if let Some(peer) = stream.peer_addr().ok() {
                    let key = peer.ip().to_string();
                    let mut n = nodes.write().await;
                    // Find node by matching IP
                    for (_, state) in n.iter_mut() {
                        if state.node.nid.addr.to_string() == key {
                            state.last_heartbeat = Instant::now();
                            state.alive = true;
                            state.failed_heartbeats = 0;
                            debug!("heartbeat from {} epoch={}", key, epoch);
                            break;
                        }
                    }
                }
                ShepherdResponse::Ok
            }
            ShepherdMsg::Unregister => {
                if let Some(peer) = stream.peer_addr().ok() {
                    let key = peer.ip().to_string();
                    let mut n = nodes.write().await;
                    let remove_key: Option<String> = n.iter()
                        .find(|(_, s)| s.node.nid.addr.to_string() == key)
                        .map(|(k, _)| k.clone());
                    if let Some(k) = remove_key {
                        info!("unregistering node: {}", k);
                        n.remove(&k);
                    }
                }
                ShepherdResponse::Ok
            }
            ShepherdMsg::StatusRequest => {
                let n = nodes.read().await;
                let now = Instant::now();
                let infos: Vec<NodeInfo> = n
                    .values()
                    .map(|s| NodeInfo {
                        addr: s.node.nid.to_string(),
                        alive: s.alive,
                        last_heartbeat_secs_ago: now
                            .duration_since(s.last_heartbeat)
                            .as_secs(),
                        failed_heartbeats: s.failed_heartbeats,
                    })
                    .collect();
                ShepherdResponse::Status { nodes: infos }
            }
        };

        send_response(&mut stream, &resp).await?;
    }
}

async fn send_response(stream: &mut TcpStream, resp: &ShepherdResponse) -> SdResult<()> {
    let data = bincode::serialize(resp).map_err(|_| SdError::SystemError)?;
    stream
        .write_u32(data.len() as u32)
        .await
        .map_err(|_| SdError::NetworkError)?;
    stream
        .write_all(&data)
        .await
        .map_err(|_| SdError::NetworkError)?;
    Ok(())
}

/// Background loop that checks heartbeat timeouts and probes nodes.
async fn monitor_loop(
    nodes: Arc<RwLock<BTreeMap<String, NodeState>>>,
    interval: Duration,
    timeout: Duration,
) {
    loop {
        tokio::time::sleep(interval).await;

        let mut n = nodes.write().await;
        let now = Instant::now();

        for (nid, state) in n.iter_mut() {
            if state.alive && now.duration_since(state.last_heartbeat) > timeout {
                warn!("node {} heartbeat timeout ({:?}), marking failed", nid, timeout);
                state.alive = false;
                state.failed_heartbeats += 1;
            }
        }

        let total = n.len();
        let alive = n.values().filter(|s| s.alive).count();
        if total > 0 {
            debug!("health check: {}/{} nodes alive", alive, total);
        }
    }
}
