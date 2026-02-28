//! Sheepfs — FUSE filesystem for sheepdog VDIs.
//!
//! Mounts sheepdog cluster data as a local filesystem using FUSE.
//! Exposes virtual files for:
//! - VDI volume data (raw block device files)
//! - Cluster node information
//! - VDI metadata and status
//!
//! Usage:
//!   sheepfs [OPTIONS] <MOUNTPOINT>
//!
//! Requires: libfuse (Linux) or macFUSE (macOS)

mod ops;
mod volume;
mod config;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tracing::{error, info};

/// Sheepfs — FUSE filesystem for sheepdog
#[derive(Parser, Debug)]
#[command(name = "sheepfs", version, about = "Sheepdog FUSE filesystem")]
struct Args {
    /// FUSE mount point
    #[arg(value_name = "MOUNTPOINT")]
    mountpoint: PathBuf,

    /// Sheep daemon address
    #[arg(short = 'a', long, default_value = "127.0.0.1")]
    address: String,

    /// Sheep daemon port
    #[arg(short = 'p', long, default_value_t = 7000)]
    port: u16,

    /// Cache timeout in seconds
    #[arg(long, default_value_t = 10)]
    cache_timeout: u64,

    /// Log level
    #[arg(short = 'l', long, default_value = "info")]
    log_level: String,

    /// Run in foreground (don't daemonize)
    #[arg(short = 'f', long)]
    foreground: bool,
}

fn main() {
    let args = Args::parse();

    // Initialize logging
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    info!("sheepfs v{} starting", env!("CARGO_PKG_VERSION"));

    let sheep_addr: SocketAddr = format!("{}:{}", args.address, args.port)
        .parse()
        .unwrap_or_else(|_| {
            error!("invalid sheep address: {}:{}", args.address, args.port);
            std::process::exit(1);
        });

    let config = config::SheepfsConfig {
        sheep_addr,
        cache_timeout: std::time::Duration::from_secs(args.cache_timeout),
    };

    let fs = ops::SheepFs::new(config);

    info!("mounting sheepfs on {}", args.mountpoint.display());

    let mut options = vec![
        fuser::MountOption::FSName("sheepfs".to_string()),
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::RO, // Read-only by default
    ];

    if args.foreground {
        // Run in foreground
        if let Err(e) = fuser::mount2(fs, &args.mountpoint, &options) {
            error!("failed to mount: {}", e);
            std::process::exit(1);
        }
    } else {
        // Daemonize
        options.push(fuser::MountOption::AllowOther);
        if let Err(e) = fuser::mount2(fs, &args.mountpoint, &options) {
            error!("failed to mount: {}", e);
            std::process::exit(1);
        }
    }

    info!("sheepfs unmounted");
}
