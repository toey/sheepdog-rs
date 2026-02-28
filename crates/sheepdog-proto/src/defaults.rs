//! Tunable operational defaults for the sheepdog system.
//!
//! Protocol-level constants (wire format, object sizes, magic numbers)
//! remain in [`crate::constants`]. This module centralizes the
//! configurable defaults that can be overridden via CLI flags.

// ─── Network / Port Defaults ────────────────────────────────────────────────

/// Default HTTP/S3 API server port.
pub const DEFAULT_HTTP_PORT: u16 = 8000;

/// Default NFS server port (NFSv3).
pub const DEFAULT_NFS_PORT: u16 = 2049;

/// Default NFS MOUNT protocol port.
pub const DEFAULT_NFS_MOUNT_PORT: u16 = 2050;

/// Default NBD (Network Block Device) server port (IANA-reserved).
pub const DEFAULT_NBD_PORT: u16 = 10809;

/// Default DPDK data plane UDP port.
pub const DEFAULT_DPDK_PORT: u16 = 7100;

/// Default cluster communication port offset from the sheep listen port.
/// The cluster driver listens on `listen_port + offset`.
pub const DEFAULT_CLUSTER_PORT_OFFSET: u16 = 1;

/// Default listen port for the shepherd coordination daemon.
pub const DEFAULT_SHEPHERD_PORT: u16 = 7100;

// ─── Performance Tuning ─────────────────────────────────────────────────────

/// Default object cache size in megabytes.
pub const DEFAULT_CACHE_SIZE_MB: u64 = 256;

/// Default journal size in megabytes.
pub const DEFAULT_JOURNAL_SIZE_MB: u64 = 512;

/// Default maximum TCP connections per peer node (connection pool size).
pub const DEFAULT_TCP_MAX_CONNS_PER_NODE: usize = 8;

/// Default number of DPDK RX/TX queues per port.
pub const DEFAULT_DPDK_QUEUES: u16 = 1;

/// Default number of mbufs in the DPDK memory pool.
pub const DEFAULT_DPDK_NR_MBUFS: u32 = 8191;

/// Default per-core mbuf cache size for DPDK.
pub const DEFAULT_DPDK_MBUF_CACHE_SIZE: u32 = 250;

// ─── Recovery Tuning ────────────────────────────────────────────────────────

/// Maximum concurrent recovery operations.
pub const DEFAULT_RECOVERY_MAX_EXEC_COUNT: u32 = 1;

/// Interval between queued recovery work items (milliseconds).
pub const DEFAULT_RECOVERY_QUEUE_WORK_INTERVAL_MS: u64 = 100;

// ─── Cluster Driver Tuning ──────────────────────────────────────────────────

/// Heartbeat interval for the P2P cluster driver (seconds).
pub const DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_SECS: u64 = 5;

/// Heartbeat timeout before declaring a peer dead (seconds).
pub const DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT_SECS: u64 = 15;

/// Maximum size of a single cluster wire message (bytes). 8 MB.
pub const DEFAULT_CLUSTER_MAX_MESSAGE_SIZE: u32 = 8 * 1024 * 1024;

/// Channel buffer size for cluster event queue.
pub const DEFAULT_CLUSTER_EVENT_CHANNEL_SIZE: usize = 512;

/// Channel buffer size for per-peer write queue in the sdcluster driver.
pub const DEFAULT_CLUSTER_PEER_WRITE_CHANNEL_SIZE: usize = 128;

/// Channel buffer size for the local cluster driver event queue.
pub const DEFAULT_LOCAL_EVENT_CHANNEL_SIZE: usize = 256;

// ─── Transport ──────────────────────────────────────────────────────────────

/// Maximum response size for TCP peer transport (bytes). 64 MB.
/// Protects against corrupt length prefixes on the wire.
pub const DEFAULT_TCP_MAX_RESPONSE_SIZE: usize = 64 * 1024 * 1024;

// ─── NBD Server ─────────────────────────────────────────────────────────────

/// Maximum NBD export name length (bytes).
pub const DEFAULT_NBD_MAX_EXPORT_NAME_LEN: usize = 4096;

/// Maximum NBD request payload size (bytes). 32 MB.
pub const DEFAULT_NBD_MAX_PAYLOAD: u32 = 32 * 1024 * 1024;

// ─── VDI ────────────────────────────────────────────────────────────────────

/// Maximum hash collision retries when finding a free VDI ID.
pub const DEFAULT_MAX_VDI_HASH_RETRIES: u32 = 1024;

// ─── Shepherd ───────────────────────────────────────────────────────────────

/// Shepherd heartbeat monitoring interval (seconds).
pub const DEFAULT_SHEPHERD_HEARTBEAT_INTERVAL_SECS: u64 = 5;

/// Shepherd node failure detection timeout (seconds).
pub const DEFAULT_SHEPHERD_FAILURE_TIMEOUT_SECS: u64 = 30;
