//! Node subcommands for the dog CLI.
//!
//! Provides commands to list, inspect, and manage cluster member nodes
//! including multi-disk management and recovery status.

use clap::{Args, Subcommand};
use tabled::{Table, Tabled};

use sheepdog_proto::node::SdNode;
use sheepdog_proto::request::{ResponseResult, SdRequest};

use crate::common::{
    connect_to_sheep, exit_error, format_size, print_success, send_request_ok,
};

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

#[derive(Args)]
pub struct NodeArgs {
    #[command(subcommand)]
    pub command: NodeCommands,
}

#[derive(Subcommand)]
pub enum NodeCommands {
    /// List all cluster nodes
    List {
        /// Show all nodes including offline
        #[arg(short = 'a', long)]
        all: bool,
    },
    /// Show detailed node info
    Info,
    /// Show recovery status
    Recovery,
    /// Multi-disk management
    Md(MdArgs),
    /// Kill this node
    Kill,
    /// Show node statistics
    Stat,
}

#[derive(Args)]
pub struct MdArgs {
    #[command(subcommand)]
    pub command: MdCommands,
}

#[derive(Subcommand)]
pub enum MdCommands {
    /// Show multi-disk info
    Info,
    /// Add a new disk
    Plug {
        /// Disk path
        path: String,
    },
    /// Remove a disk
    Unplug {
        /// Disk path
        path: String,
    },
}

// ---------------------------------------------------------------------------
// Table display types
// ---------------------------------------------------------------------------

#[derive(Tabled)]
struct NodeRow {
    #[tabled(rename = "Id")]
    id: String,
    #[tabled(rename = "Host")]
    host: String,
    #[tabled(rename = "Port")]
    port: u16,
    #[tabled(rename = "VNodes")]
    vnodes: u16,
    #[tabled(rename = "Zone")]
    zone: u32,
    #[tabled(rename = "Space")]
    space: String,
    #[tabled(rename = "Status")]
    status: String,
}

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

/// Execute a node subcommand.
pub async fn run(addr: &str, port: u16, args: NodeArgs) {
    match args.command {
        NodeCommands::List { all: _ } => {
            node_list(addr, port).await;
        }
        NodeCommands::Info => {
            node_info(addr, port).await;
        }
        NodeCommands::Recovery => {
            node_recovery(addr, port).await;
        }
        NodeCommands::Md(md_args) => match md_args.command {
            MdCommands::Info => {
                md_info(addr, port).await;
            }
            MdCommands::Plug { path } => {
                md_plug(addr, port, &path).await;
            }
            MdCommands::Unplug { path } => {
                md_unplug(addr, port, &path).await;
            }
        },
        NodeCommands::Kill => {
            node_kill(addr, port).await;
        }
        NodeCommands::Stat => {
            node_stat(addr, port).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Node list
// ---------------------------------------------------------------------------

async fn node_list(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::GetNodeList;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::NodeList(nodes)) => {
            if nodes.is_empty() {
                println!("No nodes in the cluster.");
                return;
            }

            let rows: Vec<NodeRow> = nodes
                .iter()
                .enumerate()
                .map(|(idx, node)| NodeRow {
                    id: format!("{}", idx),
                    host: node.nid.addr.to_string(),
                    port: node.nid.port,
                    vnodes: node.nr_vnodes,
                    zone: node.zone,
                    space: format_size(node.space),
                    status: "alive".to_string(),
                })
                .collect();

            println!("{}", Table::new(&rows));
            println!();
            println!("Total {} node(s)", nodes.len());
        }
        Ok(resp) => {
            // May also come as Data if serialized
            if let ResponseResult::Data(data) = resp {
                if let Ok(nodes) = bincode::deserialize::<Vec<SdNode>>(&data) {
                    let rows: Vec<NodeRow> = nodes
                        .iter()
                        .enumerate()
                        .map(|(idx, node)| NodeRow {
                            id: format!("{}", idx),
                            host: node.nid.addr.to_string(),
                            port: node.nid.port,
                            vnodes: node.nr_vnodes,
                            zone: node.zone,
                            space: format_size(node.space),
                            status: "alive".to_string(),
                        })
                        .collect();

                    println!("{}", Table::new(&rows));
                    println!();
                    println!("Total {} node(s)", nodes.len());
                } else {
                    println!("Failed to parse node list data.");
                }
            } else {
                println!("Unexpected response.");
            }
        }
        Err(e) => {
            exit_error(&format!("Failed to get node list: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Node info
// ---------------------------------------------------------------------------

async fn node_info(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::StatSheep;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::NodeInfo {
            nr_nodes,
            store_size,
            store_free,
        }) => {
            println!("Node Information:");
            println!("  Address:        {}:{}", addr, port);
            println!("  Cluster nodes:  {}", nr_nodes);
            println!("  Total space:    {}", format_size(store_size));
            println!("  Free space:     {}", format_size(store_free));
            println!(
                "  Used space:     {}",
                format_size(store_size.saturating_sub(store_free))
            );
            if store_size > 0 {
                let usage_pct =
                    ((store_size - store_free) as f64 / store_size as f64) * 100.0;
                println!("  Usage:          {:.1}%", usage_pct);
            }
        }
        Ok(resp) => {
            println!("Unexpected response: {:?}", resp);
        }
        Err(e) => {
            exit_error(&format!("Failed to get node info: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Node recovery
// ---------------------------------------------------------------------------

async fn node_recovery(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::GetRecovery;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if let Ok(epoch) = bincode::deserialize::<u32>(&data) {
                if epoch == 0 {
                    println!("No recovery in progress.");
                } else {
                    println!("Recovery in progress:");
                    println!("  Recovery epoch: {}", epoch);
                }
            } else {
                println!("No recovery in progress.");
            }
        }
        Ok(_) => {
            println!("No recovery in progress.");
        }
        Err(e) => {
            exit_error(&format!("Failed to get recovery status: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-disk management
// ---------------------------------------------------------------------------

async fn md_info(addr: &str, port: u16) {
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
            if let Ok(disks) =
                bincode::deserialize::<Vec<sheepdog_proto::node::DiskInfo>>(&data)
            {
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
                println!("No multi-disk information available.");
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

async fn md_plug(addr: &str, port: u16, path: &str) {
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

async fn md_unplug(addr: &str, port: u16, path: &str) {
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
// Node kill
// ---------------------------------------------------------------------------

async fn node_kill(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::KillNode;
    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!("Node {}:{} killed", addr, port));
        }
        Err(e) => {
            exit_error(&format!("Failed to kill node: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Node stat
// ---------------------------------------------------------------------------

async fn node_stat(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // Get stat info from the sheep
    let req = SdRequest::Stat;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::NodeInfo {
            nr_nodes,
            store_size,
            store_free,
        }) => {
            println!("Node Statistics:");
            println!("  Address:        {}:{}", addr, port);
            println!("  Cluster nodes:  {}", nr_nodes);
            println!("  Store size:     {}", format_size(store_size));
            println!("  Store free:     {}", format_size(store_free));
            println!(
                "  Store used:     {}",
                format_size(store_size.saturating_sub(store_free))
            );
        }
        Ok(ResponseResult::Data(data)) => {
            // May get raw stat data
            println!("Node statistics ({} bytes of data)", data.len());
        }
        Ok(_) => {
            println!("Node statistics retrieved (no details available).");
        }
        Err(e) => {
            exit_error(&format!("Failed to get node stats: {}", e));
        }
    }
}
