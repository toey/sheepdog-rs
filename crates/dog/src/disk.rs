//! Disk subcommands for the dog CLI.
//!
//! Provides commands to add, remove, and list disks in the multi-disk store.

use bincode;
use clap::{Args, Subcommand};
use tabled::{Table, Tabled};
use tracing;

use sheepdog_proto::node::DiskInfo;
use sheepdog_proto::request::{ResponseResult, SdRequest};

use crate::common::{
    connect_to_sheep, exit_error, format_size, print_success, send_request_ok,
};

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

#[derive(Args)]
pub struct DiskArgs {
    #[command(subcommand)]
    pub command: DiskCommands,
}

#[derive(Subcommand)]
pub enum DiskCommands {
    /// Add a new disk to the multi-disk store
    Add {
        /// Disk path to add
        path: String,
    },
    /// Remove a disk from the multi-disk store
    Remove {
        /// Disk path to remove
        path: String,
    },
    /// List all disks in the multi-disk store
    List,
}

// ---------------------------------------------------------------------------
// Table display types
// ---------------------------------------------------------------------------

#[derive(Tabled)]
struct DiskRow {
    #[tabled(rename = "Disk Id")]
    disk_id: String,
    #[tabled(rename = "Space")]
    space: String,
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Execute a disk subcommand.
pub async fn run(addr: &str, port: u16, args: DiskArgs) {
    match args.command {
        DiskCommands::Add { path } => {
            disk_add(addr, port, &path).await;
        }
        DiskCommands::Remove { path } => {
            disk_remove(addr, port, &path).await;
        }
        DiskCommands::List => {
            disk_list(addr, port).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Disk add
// ---------------------------------------------------------------------------

async fn disk_add(addr: &str, port: u16, path: &str) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::MdPlug {
        path: path.to_string(),
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!("Disk '{}' plugged successfully", path));
        }
        Err(e) => {
            exit_error(&format!("Failed to plug disk '{}': {}", path, e));
        }
    }
}

// ---------------------------------------------------------------------------
// Disk remove
// ---------------------------------------------------------------------------

async fn disk_remove(addr: &str, port: u16, path: &str) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::MdUnplug {
        path: path.to_string(),
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!("Disk '{}' unplugged successfully", path));
        }
        Err(e) => {
            exit_error(&format!("Failed to unplug disk '{}': {}", path, e));
        }
    }
}

// ---------------------------------------------------------------------------
// Disk list
// ---------------------------------------------------------------------------

async fn disk_list(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::MdInfo;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if data.is_empty() {
                println!("No multi-disk information available.");
                return;
            }
            // Try to parse as disk info list
            if let Ok(disks) = bincode::deserialize::<Vec<DiskInfo>>(&data) {
                if disks.is_empty() {
                    println!("No disks configured.");
                    return;
                }

                let rows: Vec<DiskRow> = disks
                    .iter()
                    .map(|d| DiskRow {
                        disk_id: format!("{}", d.disk_id),
                        space: format_size(d.disk_space),
                    })
                    .collect();

                println!("{}", Table::new(&rows));
            } else {
                tracing::error!("Failed to deserialize disk info from response data");
                println!("Failed to parse multi-disk information. Please ensure the server is using multi-disk storage.");
            }
        }
        Ok(_) => {
            println!("No multi-disk information available.");
        }
        Err(e) => {
            exit_error(&format!("Failed to get multi-disk info: {}", e));
        }
    }
}
