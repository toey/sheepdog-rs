//! Local (single-node) cluster driver.
//!
//! This driver is designed for development and testing. It does not perform
//! any real networking. Instead, cluster events are passed through an
//! in-process tokio mpsc channel, and the node immediately "joins" itself.
//!
//! Because there is only one node, leader election is trivial (this node is
//! always the leader), and the heartbeat mechanism is a no-op.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, info};

use sheepdog_proto::defaults::DEFAULT_LOCAL_EVENT_CHANNEL_SIZE;
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::node::SdNode;

use super::{ClusterDriver, ClusterEvent};

/// Internal state of the local cluster driver.
struct LocalState {
    /// The identity of this node, set during `init`.
    this_node: Option<SdNode>,
    /// Whether the node has joined.
    joined: bool,
    /// The local address reported by `get_local_addr`.
    local_addr: SocketAddr,
    /// Whether the cluster is in a blocked state.
    blocked: bool,
}

/// A single-node cluster driver that delivers events via an in-process channel.
///
/// All operations complete immediately. There is no network I/O.
///
/// # Example
///
/// ```ignore
/// let driver = LocalDriver::new("127.0.0.1:7000".parse().unwrap());
/// driver.init(&my_node).await?;
/// driver.join(&my_node).await?;
/// // The join event is immediately available:
/// let event = driver.recv_event().await?;
/// ```
pub struct LocalDriver {
    state: RwLock<LocalState>,
    /// Sender half — used by driver methods to enqueue events.
    event_tx: mpsc::Sender<ClusterEvent>,
    /// Receiver half — consumed by `recv_event`.
    /// Wrapped in a Mutex because `mpsc::Receiver::recv` takes `&mut self`.
    event_rx: Arc<Mutex<mpsc::Receiver<ClusterEvent>>>,
}

impl LocalDriver {
    /// Create a new local cluster driver.
    ///
    /// `local_addr` is the address that `get_local_addr` will return.
    /// No listener is actually created.
    pub fn new(local_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel(DEFAULT_LOCAL_EVENT_CHANNEL_SIZE);
        Self {
            state: RwLock::new(LocalState {
                this_node: None,
                joined: false,
                local_addr,
                blocked: false,
            }),
            event_tx: tx,
            event_rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// Directly inject an event into the event queue (useful for tests).
    pub async fn inject_event(&self, event: ClusterEvent) -> SdResult<()> {
        self.event_tx
            .send(event)
            .await
            .map_err(|_| SdError::ClusterError)
    }
}

#[async_trait]
impl ClusterDriver for LocalDriver {
    fn name(&self) -> &str {
        "local"
    }

    async fn init(&self, this_node: &SdNode) -> SdResult<()> {
        let mut state = self.state.write().await;
        info!(
            "local cluster driver: init node {}",
            this_node.nid
        );
        state.this_node = Some(this_node.clone());
        Ok(())
    }

    async fn join(&self, node: &SdNode) -> SdResult<()> {
        let mut state = self.state.write().await;
        if state.joined {
            debug!("local cluster driver: already joined, ignoring duplicate join");
            return Ok(());
        }
        state.joined = true;
        let node_clone = node.clone();
        drop(state);

        info!("local cluster driver: node {} joining", node_clone.nid);

        // In single-node mode, a join immediately succeeds and the event
        // is pushed into the channel so that `recv_event` can deliver it.
        self.event_tx
            .send(ClusterEvent::Join(node_clone))
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn leave(&self) -> SdResult<()> {
        let mut state = self.state.write().await;
        if !state.joined {
            return Err(SdError::ClusterError);
        }
        let node = state
            .this_node
            .clone()
            .ok_or(SdError::ClusterError)?;
        state.joined = false;
        drop(state);

        info!("local cluster driver: node {} leaving", node.nid);

        self.event_tx
            .send(ClusterEvent::Leave(node))
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn notify(&self, data: &[u8]) -> SdResult<()> {
        let state = self.state.read().await;
        if !state.joined {
            return Err(SdError::ClusterError);
        }
        drop(state);

        debug!("local cluster driver: notify ({} bytes)", data.len());

        self.event_tx
            .send(ClusterEvent::Notify(data.to_vec()))
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn block(&self) -> SdResult<()> {
        let mut state = self.state.write().await;
        if !state.joined {
            return Err(SdError::ClusterError);
        }
        if state.blocked {
            debug!("local cluster driver: already blocked");
            return Ok(());
        }
        state.blocked = true;
        drop(state);

        debug!("local cluster driver: block");

        self.event_tx
            .send(ClusterEvent::Block)
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn unblock(&self, data: &[u8]) -> SdResult<()> {
        let mut state = self.state.write().await;
        if !state.joined {
            return Err(SdError::ClusterError);
        }
        state.blocked = false;
        drop(state);

        debug!("local cluster driver: unblock ({} bytes)", data.len());

        self.event_tx
            .send(ClusterEvent::Unblock(data.to_vec()))
            .await
            .map_err(|_| SdError::ClusterError)?;

        Ok(())
    }

    async fn get_local_addr(&self) -> SdResult<SocketAddr> {
        let state = self.state.read().await;
        Ok(state.local_addr)
    }

    async fn recv_event(&self) -> SdResult<ClusterEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await.ok_or(SdError::ClusterError)
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
    async fn test_local_driver_name() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);
        assert_eq!(driver.name(), "local");
    }

    #[tokio::test]
    async fn test_local_driver_init_and_join() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);
        let node = make_node(7000);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();

        // Should receive a Join event
        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Join(n) => assert_eq!(n.nid, node.nid),
            other => panic!("expected Join event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_local_driver_leave() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);
        let node = make_node(7000);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();

        // Consume the Join event
        let _ = driver.recv_event().await.unwrap();

        driver.leave().await.unwrap();

        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Leave(n) => assert_eq!(n.nid, node.nid),
            other => panic!("expected Leave event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_local_driver_notify() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);
        let node = make_node(7000);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();
        let _ = driver.recv_event().await.unwrap(); // consume Join

        let payload = b"hello cluster";
        driver.notify(payload).await.unwrap();

        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Notify(data) => assert_eq!(data, payload),
            other => panic!("expected Notify event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_local_driver_block_unblock() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);
        let node = make_node(7000);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();
        let _ = driver.recv_event().await.unwrap(); // consume Join

        driver.block().await.unwrap();

        let event = driver.recv_event().await.unwrap();
        assert!(matches!(event, ClusterEvent::Block));

        let payload = b"unblock result";
        driver.unblock(payload).await.unwrap();

        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Unblock(data) => assert_eq!(data, payload),
            other => panic!("expected Unblock event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_local_driver_get_local_addr() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);

        let result = driver.get_local_addr().await.unwrap();
        assert_eq!(result, addr);
    }

    #[tokio::test]
    async fn test_local_driver_duplicate_join() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);
        let node = make_node(7000);

        driver.init(&node).await.unwrap();
        driver.join(&node).await.unwrap();

        // Second join should succeed silently (no extra event)
        driver.join(&node).await.unwrap();

        // Only one Join event should be in the queue
        let event = driver.recv_event().await.unwrap();
        assert!(matches!(event, ClusterEvent::Join(_)));
    }

    #[tokio::test]
    async fn test_local_driver_leave_without_join() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);
        let node = make_node(7000);

        driver.init(&node).await.unwrap();

        // Should fail because we haven't joined
        let result = driver.leave().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_local_driver_inject_event() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let driver = LocalDriver::new(addr);

        let node = make_node(9999);
        driver
            .inject_event(ClusterEvent::Join(node.clone()))
            .await
            .unwrap();

        let event = driver.recv_event().await.unwrap();
        match event {
            ClusterEvent::Join(n) => assert_eq!(n.nid, node.nid),
            other => panic!("expected Join event, got {:?}", other),
        }
    }
}
