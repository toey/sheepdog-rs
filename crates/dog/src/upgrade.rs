//! Upgrade utilities for the dog CLI.
//!
//! Provides commands for cluster rolling upgrades, epoch log migration,
//! and version checks. Currently a stub for future implementation.

use clap::{Args, Subcommand};

use sheepdog_proto::node::ClusterInfo;
use sheepdog_proto::request::{ResponseResult, SdRequest};

use crate::common::{connect_to_sheep, exit_error, send_request_ok};

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

#[derive(Args)]
pub struct UpgradeArgs {
    #[command(subcommand)]
    pub command: UpgradeCommands,
}

#[derive(Subcommand)]
pub enum UpgradeCommands {
    /// Check if cluster is ready for upgrade
    Check,
    /// Show current and target protocol versions
    Version,
    /// Migrate epoch logs to new format (if needed)
    MigrateEpoch,
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Execute an upgrade subcommand.
pub async fn run(addr: &str, port: u16, args: UpgradeArgs) {
    match args.command {
        UpgradeCommands::Check => {
            upgrade_check(addr, port).await;
        }
        UpgradeCommands::Version => {
            upgrade_version(addr, port).await;
        }
        UpgradeCommands::MigrateEpoch => {
            upgrade_migrate_epoch(addr, port).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Upgrade check
// ---------------------------------------------------------------------------

async fn upgrade_check(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::StatCluster;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if let Ok(cinfo) = bincode::deserialize::<ClusterInfo>(&data) {
                println!("Upgrade Check:");
                println!(
                    "  Cluster protocol version: {}",
                    cinfo.proto_ver
                );
                println!(
                    "  Client protocol version:  {}",
                    sheepdog_proto::constants::SD_PROTO_VER
                );
                println!(
                    "  Internal protocol version: {}",
                    sheepdog_proto::constants::SD_SHEEP_PROTO_VER
                );
                println!("  Cluster epoch:            {}", cinfo.epoch);
                println!("  Cluster nodes:            {}", cinfo.nodes.len());
                println!();

                if cinfo.proto_ver == sheepdog_proto::constants::SD_SHEEP_PROTO_VER {
                    println!("  Cluster is running the current protocol version.");
                    println!("  No upgrade is needed.");
                } else {
                    println!(
                        "  Cluster is running protocol version {}, current is {}.",
                        cinfo.proto_ver,
                        sheepdog_proto::constants::SD_SHEEP_PROTO_VER
                    );
                    println!("  A rolling upgrade may be needed.");
                }
            } else {
                println!("Failed to parse cluster info.");
            }
        }
        Ok(_) => {
            println!("Unexpected response.");
        }
        Err(e) => {
            exit_error(&format!("Failed to check upgrade status: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Upgrade version
// ---------------------------------------------------------------------------

async fn upgrade_version(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    println!("Protocol Versions:");
    println!(
        "  Client (dog):   {}",
        sheepdog_proto::constants::SD_PROTO_VER
    );
    println!(
        "  Internal:       {}",
        sheepdog_proto::constants::SD_SHEEP_PROTO_VER
    );

    let req = SdRequest::StatCluster;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if let Ok(cinfo) = bincode::deserialize::<ClusterInfo>(&data) {
                println!(
                    "  Cluster:        {}",
                    cinfo.proto_ver
                );
            }
        }
        _ => {
            println!("  Cluster:        (unavailable)");
        }
    }
}

// ---------------------------------------------------------------------------
// Migrate epoch logs
// ---------------------------------------------------------------------------

async fn upgrade_migrate_epoch(addr: &str, port: u16) {
    let _ = (addr, port);
    println!("Epoch log migration is not yet implemented.");
    println!("This feature will migrate epoch logs between protocol versions.");
}
