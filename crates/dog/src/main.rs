//! Sheepdog CLI admin tool (dog).
//!
//! The `dog` command is the primary administrative interface for managing
//! a sheepdog distributed storage cluster. It communicates with the sheep
//! daemon over TCP using the sheepdog protocol.
//!
//! # Usage
//!
//! ```text
//! dog [OPTIONS] <COMMAND>
//!
//! Commands:
//!   vdi       VDI (Virtual Disk Image) management
//!   node      Cluster node management
//!   cluster   Cluster-wide operations
//!   upgrade   Cluster upgrade utilities
//!
//! Options:
//!   -a, --address <ADDRESS>  Sheep daemon address [default: 127.0.0.1]
//!   -p, --port <PORT>        Sheep daemon port [default: 7000]
//!   -h, --help               Print help
//!   -V, --version            Print version
//! ```

mod cluster;
mod common;
mod node;
mod treeview;
mod upgrade;
mod vdi;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

/// Sheepdog cluster admin tool.
///
/// Manages VDIs, nodes, and cluster configuration by communicating
/// with the sheep daemon over the sheepdog protocol.
#[derive(Parser)]
#[command(name = "dog", version, about = "Sheepdog cluster admin tool")]
struct Cli {
    /// Sheep daemon address
    #[arg(short = 'a', long, default_value = "127.0.0.1")]
    address: String,

    /// Sheep daemon port
    #[arg(short = 'p', long, default_value_t = 7000)]
    port: u16,

    /// Enable verbose/debug logging
    #[arg(short = 'v', long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// VDI (Virtual Disk Image) management
    Vdi(vdi::VdiArgs),
    /// Cluster node management
    Node(node::NodeArgs),
    /// Cluster-wide operations
    Cluster(cluster::ClusterArgs),
    /// Cluster upgrade utilities
    Upgrade(upgrade::UpgradeArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize tracing
    let filter = if cli.verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("warn")
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let addr = &cli.address;
    let port = cli.port;

    match cli.command {
        Commands::Vdi(args) => {
            vdi::run(addr, port, args).await;
        }
        Commands::Node(args) => {
            node::run(addr, port, args).await;
        }
        Commands::Cluster(args) => {
            cluster::run(addr, port, args).await;
        }
        Commands::Upgrade(args) => {
            upgrade::run(addr, port, args).await;
        }
    }
}
