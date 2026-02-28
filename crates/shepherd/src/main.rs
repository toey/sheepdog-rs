//! Shepherd — cluster coordination daemon.
//!
//! Monitors sheep daemons and provides cluster-wide coordination:
//! - Heartbeat monitoring of all sheep nodes
//! - Automatic restart coordination
//! - Cluster health reporting
//! - Configuration distribution
//!
//! Usage:
//!   shepherd [OPTIONS]

mod handler;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use sheepdog_proto::constants::SD_LISTEN_PORT;
use sheepdog_proto::defaults::{
    DEFAULT_SHEPHERD_FAILURE_TIMEOUT_SECS, DEFAULT_SHEPHERD_HEARTBEAT_INTERVAL_SECS,
    DEFAULT_SHEPHERD_PORT,
};
use tracing::{error, info};

/// Shepherd cluster coordination daemon
#[derive(Parser, Debug)]
#[command(name = "shepherd", version, about = "Sheepdog cluster coordinator")]
struct Args {
    /// Listen address for control connections
    #[arg(short = 'b', long, default_value = "0.0.0.0")]
    bind_addr: String,

    /// Listen port
    #[arg(short = 'p', long, default_value_t = DEFAULT_SHEPHERD_PORT)]
    port: u16,

    /// Sheep daemon port to monitor
    #[arg(long, default_value_t = SD_LISTEN_PORT)]
    sheep_port: u16,

    /// Heartbeat interval in seconds
    #[arg(long, default_value_t = DEFAULT_SHEPHERD_HEARTBEAT_INTERVAL_SECS)]
    heartbeat_interval: u64,

    /// Node failure timeout in seconds (missed heartbeats)
    #[arg(long, default_value_t = DEFAULT_SHEPHERD_FAILURE_TIMEOUT_SECS)]
    failure_timeout: u64,

    /// Log level
    #[arg(short = 'l', long, default_value = "info")]
    log_level: String,
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

    info!("shepherd v{} starting", env!("CARGO_PKG_VERSION"));

    let bind_addr: IpAddr = args
        .bind_addr
        .parse()
        .unwrap_or_else(|_| IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    let listen_addr = SocketAddr::new(bind_addr, args.port);

    let config = handler::ShepherdConfig {
        listen_addr,
        sheep_port: args.sheep_port,
        heartbeat_interval: std::time::Duration::from_secs(args.heartbeat_interval),
        failure_timeout: std::time::Duration::from_secs(args.failure_timeout),
    };

    let shepherd = handler::Shepherd::new(config);

    info!("shepherd listening on {}", listen_addr);

    tokio::select! {
        result = shepherd.run() => {
            if let Err(e) = result {
                error!("shepherd error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received, shutting down");
        }
    }

    info!("shepherd stopped");
}
