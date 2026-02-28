//! Sheepfs configuration.

use std::net::SocketAddr;
use std::time::Duration;

/// Configuration for the sheepfs FUSE filesystem.
pub struct SheepfsConfig {
    /// Address of the sheep daemon to connect to.
    pub sheep_addr: SocketAddr,
    /// Cache timeout for metadata refreshes.
    pub cache_timeout: Duration,
}
