//! Sheepdog storage daemon (sheep).
//!
//! This is the main storage daemon that manages data objects, handles
//! client connections, and participates in the cluster.
//!
//! Usage:
//!   sheep [OPTIONS] <DATA_DIR>
//!
//! The daemon creates a tokio async runtime and runs:
//! 1. Client accept loop (TCP listener)
//! 2. Cluster driver (membership management)
//! 3. Recovery worker (background object migration)
//! 4. Optional HTTP/S3 server
//! 5. Optional NFS server
//! 6. Optional NBD export server (for QEMU/qemu-img)

mod cluster;
mod config;
mod daemon;
mod group;
#[cfg(feature = "http")]
mod http;
mod journal;
mod migrate;
mod nbd;
mod nfs;
mod object_cache;
mod object_list_cache;
mod ops;
mod recovery;
mod request;
mod store;
mod vdi;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use sheepdog_proto::constants::SD_LISTEN_PORT;
use sheepdog_proto::node::{NodeId, SdNode};

use crate::cluster::{ClusterDriver, ClusterEvent};
use crate::daemon::SystemInfo;

/// Sheepdog storage daemon
#[derive(Parser, Debug)]
#[command(name = "sheep", version, about = "Sheepdog storage daemon")]
struct Args {
    /// Data directory for object storage
    #[arg(value_name = "DIR")]
    dir: PathBuf,

    /// Listen address
    #[arg(short = 'b', long, default_value = "0.0.0.0")]
    bind_addr: String,

    /// Listen port
    #[arg(short = 'p', long, default_value_t = SD_LISTEN_PORT)]
    port: u16,

    /// Gateway mode (no local storage)
    #[arg(short = 'g', long)]
    gateway: bool,

    /// Number of VNode copies (default from cluster config)
    #[arg(short = 'c', long)]
    copies: Option<u8>,

    /// Enable direct I/O
    #[arg(long)]
    directio: bool,

    /// Journal directory
    #[arg(short = 'j', long)]
    journal: Option<PathBuf>,

    /// Journal size in MB
    #[arg(long, default_value_t = 512)]
    journal_size: u64,

    /// Enable object cache
    #[arg(short = 'w', long)]
    cache: bool,

    /// Object cache size in MB
    #[arg(long, default_value_t = 256)]
    cache_size: u64,

    /// Fault zone ID
    #[arg(short = 'z', long, default_value_t = 0)]
    zone: u32,

    /// Number of virtual nodes
    #[arg(short = 'v', long, default_value_t = 128)]
    vnodes: u16,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'l', long, default_value = "info")]
    log_level: String,

    /// HTTP/S3 port (0 to disable)
    #[arg(long, default_value_t = 8000)]
    http_port: u16,

    /// Enable NFS server
    #[arg(long)]
    nfs: bool,

    /// NFS port (default: 2049)
    #[arg(long, default_value_t = 2049)]
    nfs_port: u16,

    /// NFS MOUNT port (default: 2050)
    #[arg(long, default_value_t = 2050)]
    nfs_mount_port: u16,

    /// Enable NBD export server
    #[arg(long)]
    nbd: bool,

    /// NBD server port (default: 10809)
    #[arg(long, default_value_t = nbd::NBD_DEFAULT_PORT)]
    nbd_port: u16,

    /// Cluster driver to use: "local" (single-node) or "sdcluster" (P2P TCP mesh)
    #[arg(long, default_value = "local")]
    cluster_driver: String,

    /// Seed node addresses for joining an existing cluster (sdcluster driver only).
    /// Format: host:port, can be specified multiple times.
    #[arg(long = "seed", value_name = "HOST:PORT")]
    seeds: Vec<String>,

    /// Cluster communication port offset from listen port (sdcluster driver only)
    #[arg(long, default_value_t = 1)]
    cluster_port_offset: u16,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Initialize logging
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    info!("sheep v{} starting", env!("CARGO_PKG_VERSION"));

    // Parse listen address
    let bind_addr: IpAddr = args
        .bind_addr
        .parse()
        .unwrap_or_else(|_| IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    let listen_addr = SocketAddr::new(bind_addr, args.port);

    // Build this node's identity
    let nid = NodeId::new(bind_addr, args.port);
    let mut this_node = SdNode::new(nid);
    this_node.nr_vnodes = args.vnodes;
    this_node.zone = args.zone;

    // Create data directory
    if let Err(e) = tokio::fs::create_dir_all(&args.dir).await {
        error!("failed to create data directory {}: {}", args.dir.display(), e);
        std::process::exit(1);
    }

    // Create subdirectories
    let obj_dir = args.dir.join("obj");
    let epoch_dir = args.dir.join("epoch");
    for dir in [&obj_dir, &epoch_dir] {
        if let Err(e) = tokio::fs::create_dir_all(dir).await {
            error!("failed to create {}: {}", dir.display(), e);
            std::process::exit(1);
        }
    }

    // Build system info
    let mut sys_info = SystemInfo::new(listen_addr, args.dir.clone(), this_node);
    sys_info.gateway_mode = args.gateway;
    sys_info.use_directio = args.directio;
    sys_info.journal_dir = args.journal;
    sys_info.journal_size = args.journal_size * 1024 * 1024;
    sys_info.object_cache_enabled = args.cache;
    sys_info.object_cache_size = args.cache_size;

    // Try to load existing config (if rejoining)
    match config::load_config(&args.dir).await {
        Ok(cinfo) => {
            info!("loaded existing config: epoch={}", cinfo.epoch);
            sys_info.cinfo = cinfo;
        }
        Err(sheepdog_proto::error::SdError::NotFormatted) => {
            info!("no existing config, starting fresh (waiting for format)");
        }
        Err(e) => {
            error!("failed to load config: {}", e);
            std::process::exit(1);
        }
    }

    let sys = Arc::new(RwLock::new(sys_info));

    // ---------------------------------------------------------------
    // Create cluster driver
    // ---------------------------------------------------------------
    let cluster_driver: Arc<dyn ClusterDriver> = match args.cluster_driver.as_str() {
        "sdcluster" => {
            // Parse seed addresses
            let seed_addrs: Vec<SocketAddr> = args
                .seeds
                .iter()
                .filter_map(|s| {
                    match s.parse::<SocketAddr>() {
                        Ok(addr) => {
                            // Seeds connect on cluster port = port + offset
                            let cluster_addr = SocketAddr::new(
                                addr.ip(),
                                addr.port() + args.cluster_port_offset,
                            );
                            Some(cluster_addr)
                        }
                        Err(e) => {
                            error!("invalid seed address '{}': {}", s, e);
                            None
                        }
                    }
                })
                .collect();

            info!(
                "using sdcluster driver with {} seed(s), port offset {}",
                seed_addrs.len(),
                args.cluster_port_offset
            );

            let driver = cluster::sdcluster::SdClusterDriverBuilder::new()
                .seeds(seed_addrs)
                .port_offset(args.cluster_port_offset)
                .build()
                .await;
            Arc::new(driver)
        }
        "local" | _ => {
            if args.cluster_driver != "local" {
                warn!(
                    "unknown cluster driver '{}', falling back to 'local'",
                    args.cluster_driver
                );
            }
            info!("using local cluster driver (single-node mode)");
            Arc::new(cluster::local::LocalDriver::new(listen_addr))
        }
    };

    // Initialize and join the cluster via the driver
    {
        let s = sys.read().await;
        let this = s.this_node.clone();
        drop(s);

        if let Err(e) = cluster_driver.init(&this).await {
            error!("cluster driver init failed: {}", e);
            std::process::exit(1);
        }

        if let Err(e) = cluster_driver.join(&this).await {
            error!("failed to join cluster: {}", e);
            std::process::exit(1);
        }
    }

    // ---------------------------------------------------------------
    // Spawn cluster event loop
    // ---------------------------------------------------------------
    let sys_cluster = sys.clone();
    let driver_for_loop = cluster_driver.clone();
    tokio::spawn(async move {
        cluster_event_loop(sys_cluster, driver_for_loop).await;
    });

    info!("sheep ready on {}", listen_addr);

    // Spawn the main services
    let sys_accept = sys.clone();
    tokio::spawn(async move {
        if let Err(e) = request::accept_loop(sys_accept).await {
            error!("accept loop failed: {}", e);
        }
    });

    // Spawn HTTP/S3 server
    #[cfg(feature = "http")]
    if args.http_port > 0 {
        let sys_http = sys.clone();
        let http_port = args.http_port;
        tokio::spawn(async move {
            if let Err(e) = http::start_http_server(sys_http, http_port).await {
                error!("HTTP server failed: {}", e);
            }
        });
    }

    // Spawn NFS server
    if args.nfs {
        let sys_nfs = sys.clone();
        let nfs_config = nfs::NfsConfig {
            port: args.nfs_port,
            mount_port: args.nfs_mount_port,
        };
        tokio::spawn(async move {
            if let Err(e) = nfs::start_nfs_server(sys_nfs, nfs_config).await {
                error!("NFS server failed: {}", e);
            }
        });
    }

    // Spawn NBD export server
    if args.nbd {
        let sys_nbd = sys.clone();
        let nbd_port = args.nbd_port;
        tokio::spawn(async move {
            if let Err(e) = nbd::start_nbd_server(sys_nbd, nbd_port).await {
                error!("NBD server failed: {}", e);
            }
        });
    }

    // Wait for shutdown signal
    let shutdown = {
        let s = sys.read().await;
        s.shutdown_notify.clone()
    };

    tokio::select! {
        _ = shutdown.notified() => {
            info!("shutdown signal received");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received, shutting down");
        }
    }

    // ---------------------------------------------------------------
    // Graceful shutdown
    // ---------------------------------------------------------------
    info!("sheep shutting down");

    // Leave the cluster gracefully
    if let Err(e) = cluster_driver.leave().await {
        warn!("cluster leave failed: {}", e);
    }

    {
        let s = sys.read().await;
        s.shutdown_notify.notify_waiters();
    }

    // Save config before exit
    {
        let s = sys.read().await;
        if let Err(e) = config::save_config(&s.dir, &s.cinfo).await {
            error!("failed to save config on shutdown: {}", e);
        }
    }

    info!("sheep stopped");
}

/// Cluster event processing loop.
///
/// Receives events from the cluster driver and dispatches them to the
/// group membership layer, triggering epoch bumps and recovery as needed.
async fn cluster_event_loop(sys: Arc<RwLock<SystemInfo>>, driver: Arc<dyn ClusterDriver>) {
    info!("cluster event loop started (driver={})", driver.name());

    loop {
        let event = match driver.recv_event().await {
            Ok(ev) => ev,
            Err(e) => {
                // Channel closed or driver shut down
                info!("cluster event loop ending: {}", e);
                break;
            }
        };

        match event {
            ClusterEvent::Join(node) => {
                info!("cluster event: node {} joined", node.nid);
                if let Err(e) = group::handle_node_join(sys.clone(), node).await {
                    error!("failed to handle node join: {}", e);
                }
            }
            ClusterEvent::Leave(node) => {
                info!("cluster event: node {} left", node.nid);
                if let Err(e) = group::handle_node_leave(sys.clone(), &node).await {
                    error!("failed to handle node leave: {}", e);
                }
            }
            ClusterEvent::Notify(data) => {
                info!("cluster event: notify ({} bytes)", data.len());
                // Notifications can carry cluster-wide commands (e.g. format,
                // alter-copy). Decode and dispatch as needed.
                handle_cluster_notify(sys.clone(), &data).await;
            }
            ClusterEvent::Block => {
                info!("cluster event: block (two-phase update phase 1)");
                // The cluster is entering a blocking state. Pause new
                // requests that depend on stable cluster state.
            }
            ClusterEvent::Unblock(data) => {
                info!("cluster event: unblock ({} bytes)", data.len());
                // The two-phase update is complete. Resume normal operation.
                handle_cluster_notify(sys.clone(), &data).await;
            }
        }
    }
}

/// Handle a cluster-wide notification payload.
///
/// Notifications carry serialized commands like cluster format, alter-copy,
/// shutdown, etc. This function decodes and applies them.
async fn handle_cluster_notify(sys: Arc<RwLock<SystemInfo>>, data: &[u8]) {
    use sheepdog_proto::node::ClusterStatus;

    // Try to decode as a ClusterNotify message
    #[derive(serde::Deserialize)]
    enum ClusterNotify {
        Format {
            nr_copies: u8,
            copy_policy: u8,
            flags: u16,
            store: String,
            ctime: u64,
        },
        Shutdown,
        AlterCopy {
            nr_copies: u8,
            copy_policy: u8,
        },
        DisableRecovery,
        EnableRecovery,
    }

    let notify: ClusterNotify = match bincode::deserialize(data) {
        Ok(n) => n,
        Err(_) => {
            // Not a recognized notification format; ignore silently
            return;
        }
    };

    match notify {
        ClusterNotify::Format {
            nr_copies,
            copy_policy,
            flags,
            store,
            ctime,
        } => {
            let mut s = sys.write().await;
            s.cinfo.nr_copies = nr_copies;
            s.cinfo.copy_policy = copy_policy;
            s.cinfo.flags = flags;
            s.cinfo.default_store = store;
            s.cinfo.ctime = ctime;
            s.cinfo.status = ClusterStatus::Ok;
            let dir = s.dir.clone();
            let cinfo = s.cinfo.clone();
            drop(s);

            info!(
                "cluster formatted: copies={}, policy={}, flags={:#x}",
                nr_copies, copy_policy, flags
            );

            if let Err(e) = config::save_config(&dir, &cinfo).await {
                error!("failed to save config after format: {}", e);
            }
        }
        ClusterNotify::Shutdown => {
            info!("cluster shutdown notification received");
            let s = sys.read().await;
            s.shutdown_notify.notify_waiters();
        }
        ClusterNotify::AlterCopy {
            nr_copies,
            copy_policy,
        } => {
            let mut s = sys.write().await;
            s.cinfo.nr_copies = nr_copies;
            s.cinfo.copy_policy = copy_policy;
            info!(
                "cluster config updated: copies={}, policy={}",
                nr_copies, copy_policy
            );
        }
        ClusterNotify::DisableRecovery => {
            let mut s = sys.write().await;
            s.disable_recovery = true;
            info!("cluster recovery disabled");
        }
        ClusterNotify::EnableRecovery => {
            let mut s = sys.write().await;
            s.disable_recovery = false;
            info!("cluster recovery enabled");
        }
    }
}
