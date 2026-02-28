//! Cluster membership drivers for the sheep daemon.
//!
//! The cluster driver is responsible for node discovery, membership
//! tracking, leader election, and delivering cluster events (join,
//! leave, notify, block/unblock) to the upper layers.
//!
//! Two implementations are provided:
//! - `local`: single-node in-process driver for testing/development.
//! - `sdcluster`: TCP mesh P2P driver for production multi-node clusters.

use async_trait::async_trait;
use sheepdog_proto::error::SdResult;
use sheepdog_proto::node::SdNode;

/// Events delivered from the cluster driver to the daemon.
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// A new node has joined the cluster.
    Join(SdNode),
    /// A node has left or been removed from the cluster.
    Leave(SdNode),
    /// A broadcast notification with opaque payload.
    Notify(Vec<u8>),
    /// The cluster is entering a blocking state for a two-phase update.
    Block,
    /// The blocking state has been released with the result payload.
    Unblock(Vec<u8>),
}

/// Trait that all cluster driver backends must implement.
///
/// The daemon interacts with the cluster through this trait, allowing
/// the underlying transport (local channel, TCP mesh, corosync, etc.)
/// to be swapped transparently.
#[async_trait]
pub trait ClusterDriver: Send + Sync {
    /// Human-readable name of this driver (e.g. "local", "sdcluster").
    fn name(&self) -> &str;

    /// Initialize the driver with the identity of this node.
    async fn init(&self, this_node: &SdNode) -> SdResult<()>;

    /// Request to join the cluster.
    async fn join(&self, node: &SdNode) -> SdResult<()>;

    /// Request to leave the cluster gracefully.
    async fn leave(&self) -> SdResult<()>;

    /// Broadcast an opaque notification to all cluster members.
    async fn notify(&self, data: &[u8]) -> SdResult<()>;

    /// Enter a cluster-wide blocking state (phase 1 of two-phase update).
    async fn block(&self) -> SdResult<()>;

    /// Release the blocking state with a result payload (phase 2).
    async fn unblock(&self, data: &[u8]) -> SdResult<()>;

    /// Return the local address that this driver is bound to.
    async fn get_local_addr(&self) -> SdResult<std::net::SocketAddr>;

    /// Receive the next cluster event. Blocks (async) until one is available.
    async fn recv_event(&self) -> SdResult<ClusterEvent>;
}

pub mod local;
pub mod sdcluster;
