//! Connection pool for reusing TCP connections to peer nodes.

use dashmap::DashMap;
use sheepdog_proto::{NodeId, SdResult};
use std::sync::Arc;
use tokio::net::TcpStream;

/// A pool of TCP connections keyed by NodeId.
pub struct SockfdCache {
    pool: Arc<DashMap<String, Vec<TcpStream>>>,
    max_per_node: usize,
}

impl SockfdCache {
    pub fn new(max_per_node: usize) -> Self {
        Self {
            pool: Arc::new(DashMap::new()),
            max_per_node,
        }
    }

    /// Get a cached connection to the given node, or None.
    pub fn get(&self, nid: &NodeId) -> Option<TcpStream> {
        let key = nid.to_string();
        let mut entry = self.pool.get_mut(&key)?;
        entry.pop()
    }

    /// Return a connection to the pool for reuse.
    pub fn put(&self, nid: &NodeId, stream: TcpStream) {
        let key = nid.to_string();
        let mut entry = self.pool.entry(key).or_insert_with(Vec::new);
        if entry.len() < self.max_per_node {
            entry.push(stream);
        }
        // Drop the stream if pool is full
    }

    /// Get a connection, creating a new one if none cached.
    pub async fn get_or_connect(&self, nid: &NodeId) -> SdResult<TcpStream> {
        if let Some(stream) = self.get(nid) {
            return Ok(stream);
        }
        crate::net::connect_to_addr(nid.socket_addr()).await
    }

    /// Clear all cached connections for a node.
    pub fn clear_node(&self, nid: &NodeId) {
        self.pool.remove(&nid.to_string());
    }

    /// Clear all cached connections.
    pub fn clear_all(&self) {
        self.pool.clear();
    }
}
