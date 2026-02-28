//! Cluster subcommands for the dog CLI.
//!
//! Provides commands to inspect cluster status, format the cluster,
//! manage recovery, and alter cluster-wide settings.

use clap::{Args, Subcommand};

use sheepdog_proto::constants::SD_DEFAULT_COPIES;
use sheepdog_proto::node::ClusterInfo;
use sheepdog_proto::request::{ResponseResult, SdRequest};

use crate::common::{
    cluster_status_str, connect_to_sheep, exit_error, format_size, format_time,
    print_success, send_request_ok,
};

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

#[derive(Args)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub command: ClusterCommands,
}

#[derive(Subcommand)]
pub enum ClusterCommands {
    /// Show cluster status and configuration
    Info,
    /// Format the cluster (initialize the distributed object store)
    Format {
        /// Number of data copies (replicas)
        #[arg(short = 'c', long, default_value_t = SD_DEFAULT_COPIES)]
        copies: u8,
        /// Copy policy (0=replicate, >0=erasure coding)
        #[arg(long, default_value_t = 0)]
        copy_policy: u8,
        /// Enable strict mode
        #[arg(long)]
        strict: bool,
        /// Enable disk mode
        #[arg(long)]
        disk_mode: bool,
        /// Store backend name (default: use sheep's configured store)
        #[arg(short = 's', long, default_value = "")]
        store: String,
    },
    /// Shutdown the entire cluster
    Shutdown,
    /// Control recovery
    Recover(RecoverArgs),
    /// Change default number of copies cluster-wide
    AlterCopy {
        /// New number of copies
        #[arg(short = 'c', long)]
        copies: u8,
        /// Copy policy (0=replicate)
        #[arg(long, default_value_t = 0)]
        copy_policy: u8,
    },
    /// Check cluster health
    Check,
}

#[derive(Args)]
pub struct RecoverArgs {
    #[command(subcommand)]
    pub command: RecoverCommands,
}

#[derive(Subcommand)]
pub enum RecoverCommands {
    /// Enable automatic recovery
    Enable,
    /// Disable automatic recovery
    Disable,
    /// Show recovery status
    Status,
    /// Set recovery parameters
    Set {
        /// Maximum parallel recovery operations
        #[arg(long, default_value_t = 0)]
        max_count: u32,
        /// Work queue interval in ms
        #[arg(long, default_value_t = 0)]
        interval: u64,
        /// Enable throttling
        #[arg(long)]
        throttle: bool,
    },
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Execute a cluster subcommand.
pub async fn run(addr: &str, port: u16, args: ClusterArgs) {
    match args.command {
        ClusterCommands::Info => {
            cluster_info(addr, port).await;
        }
        ClusterCommands::Format {
            copies,
            copy_policy,
            strict,
            disk_mode,
            store,
        } => {
            cluster_format(addr, port, copies, copy_policy, strict, disk_mode, &store).await;
        }
        ClusterCommands::Shutdown => {
            cluster_shutdown(addr, port).await;
        }
        ClusterCommands::Recover(recover_args) => match recover_args.command {
            RecoverCommands::Enable => {
                cluster_recover_enable(addr, port).await;
            }
            RecoverCommands::Disable => {
                cluster_recover_disable(addr, port).await;
            }
            RecoverCommands::Status => {
                cluster_recover_status(addr, port).await;
            }
            RecoverCommands::Set {
                max_count,
                interval,
                throttle,
            } => {
                cluster_recover_set(addr, port, max_count, interval, throttle).await;
            }
        },
        ClusterCommands::AlterCopy {
            copies,
            copy_policy,
        } => {
            cluster_alter_copy(addr, port, copies, copy_policy).await;
        }
        ClusterCommands::Check => {
            cluster_check(addr, port).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster info
// ---------------------------------------------------------------------------

async fn cluster_info(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::StatCluster;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if let Ok(cinfo) = bincode::deserialize::<ClusterInfo>(&data) {
                println!("Cluster Information:");
                println!("  Status:          {}", cluster_status_str(&cinfo.status));
                println!("  Epoch:           {}", cinfo.epoch);
                println!("  Created:         {}", format_time(cinfo.ctime));
                println!("  Default copies:  {}", cinfo.nr_copies);
                println!("  Copy policy:     {}", format_copy_policy(cinfo.copy_policy));
                println!("  Flags:           {}", format_flags(cinfo.flags));
                println!("  Store:           {}", if cinfo.default_store.is_empty() { "(default)" } else { &cinfo.default_store });
                println!(
                    "  Recovery:        {}",
                    if cinfo.disable_recovery {
                        "disabled"
                    } else {
                        "enabled"
                    }
                );
                println!("  Nodes:           {}", cinfo.nodes.len());

                if !cinfo.nodes.is_empty() {
                    println!();
                    println!("  Member nodes:");
                    for (i, node) in cinfo.nodes.iter().enumerate() {
                        println!(
                            "    [{}] {}:{} (vnodes={}, zone={}, space={})",
                            i,
                            node.nid.addr,
                            node.nid.port,
                            node.nr_vnodes,
                            node.zone,
                            format_size(node.space)
                        );
                    }
                }
            } else {
                println!("Failed to parse cluster info.");
            }
        }
        Ok(_) => {
            println!("Unexpected response.");
        }
        Err(e) => {
            exit_error(&format!("Failed to get cluster info: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster format
// ---------------------------------------------------------------------------

async fn cluster_format(
    addr: &str,
    port: u16,
    copies: u8,
    copy_policy: u8,
    strict: bool,
    disk_mode: bool,
    store: &str,
) {
    if copies == 0 {
        exit_error("Number of copies must be at least 1");
    }
    if copies as u16 > sheepdog_proto::constants::SD_MAX_COPIES {
        exit_error(&format!(
            "Number of copies exceeds maximum ({})",
            sheepdog_proto::constants::SD_MAX_COPIES
        ));
    }

    let mut flags: u16 = 0;
    if strict {
        flags |= sheepdog_proto::constants::SD_CLUSTER_FLAG_STRICT;
    }
    if disk_mode {
        flags |= sheepdog_proto::constants::SD_CLUSTER_FLAG_DISKMODE;
    }

    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::MakeFs {
        copies,
        copy_policy,
        flags,
        store: store.to_string(),
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!(
                "Cluster formatted with {} copies (policy: {}, flags: {:#06x})",
                copies,
                format_copy_policy(copy_policy),
                flags
            ));
        }
        Err(e) => {
            exit_error(&format!("Failed to format cluster: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster shutdown
// ---------------------------------------------------------------------------

async fn cluster_shutdown(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::Shutdown;
    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success("Cluster shutdown initiated.");
        }
        Err(e) => {
            // Shutdown may close the connection before we get the response
            // That's normal behavior
            if matches!(e, sheepdog_proto::error::SdError::NetworkError) {
                print_success("Cluster shutdown initiated.");
            } else {
                exit_error(&format!("Failed to shutdown cluster: {}", e));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Recovery control
// ---------------------------------------------------------------------------

async fn cluster_recover_enable(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::EnableRecover;
    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success("Recovery enabled.");
        }
        Err(e) => {
            exit_error(&format!("Failed to enable recovery: {}", e));
        }
    }
}

async fn cluster_recover_disable(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::DisableRecover;
    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success("Recovery disabled.");
        }
        Err(e) => {
            exit_error(&format!("Failed to disable recovery: {}", e));
        }
    }
}

async fn cluster_recover_status(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::GetRecovery;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if let Ok(epoch) = bincode::deserialize::<u32>(&data) {
                if epoch == 0 {
                    println!("Recovery: idle (no recovery in progress)");
                } else {
                    println!("Recovery: in progress (epoch {})", epoch);
                }
            } else {
                println!("Recovery: unknown status");
            }
        }
        Ok(_) => {
            println!("Recovery: idle");
        }
        Err(e) => {
            exit_error(&format!("Failed to get recovery status: {}", e));
        }
    }
}

async fn cluster_recover_set(
    addr: &str,
    port: u16,
    max_exec_count: u32,
    queue_work_interval: u64,
    throttling: bool,
) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::SetRecovery {
        max_exec_count,
        queue_work_interval,
        throttling,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!(
                "Recovery parameters set (max_count={}, interval={}ms, throttle={})",
                max_exec_count, queue_work_interval, throttling
            ));
        }
        Err(e) => {
            exit_error(&format!("Failed to set recovery parameters: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Alter cluster copies
// ---------------------------------------------------------------------------

async fn cluster_alter_copy(addr: &str, port: u16, copies: u8, copy_policy: u8) {
    if copies == 0 {
        exit_error("Number of copies must be at least 1");
    }

    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::AlterClusterCopy {
        copies,
        copy_policy,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!(
                "Cluster default copies changed to {} (policy: {})",
                copies,
                format_copy_policy(copy_policy)
            ));
        }
        Err(e) => {
            exit_error(&format!("Failed to alter cluster copies: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster check
// ---------------------------------------------------------------------------

async fn cluster_check(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // Get cluster info
    let req = SdRequest::StatCluster;
    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if let Ok(cinfo) = bincode::deserialize::<ClusterInfo>(&data) {
                println!("Cluster Health Check:");
                println!("  Status:          {}", cluster_status_str(&cinfo.status));
                println!("  Epoch:           {}", cinfo.epoch);
                println!("  Nodes:           {}", cinfo.nodes.len());
                println!("  Default copies:  {}", cinfo.nr_copies);

                // Check if there are enough nodes for the replication level
                if (cinfo.nodes.len() as u8) < cinfo.nr_copies {
                    println!();
                    println!(
                        "  WARNING: Not enough nodes ({}) for {} copies!",
                        cinfo.nodes.len(),
                        cinfo.nr_copies
                    );
                    println!(
                        "           Need at least {} nodes for full redundancy.",
                        cinfo.nr_copies
                    );
                } else {
                    println!();
                    println!("  All checks passed.");
                }

                // Check recovery status
                let recovery_req = SdRequest::GetRecovery;
                if let Ok(ResponseResult::Data(rdata)) =
                    send_request_ok(&mut stream, recovery_req).await
                {
                    if let Ok(epoch) = bincode::deserialize::<u32>(&rdata) {
                        if epoch > 0 {
                            println!("  NOTE: Recovery is in progress (epoch {})", epoch);
                        }
                    }
                }
            } else {
                println!("Failed to parse cluster info for health check.");
            }
        }
        Ok(_) => {
            println!("Unexpected response during health check.");
        }
        Err(e) => {
            exit_error(&format!("Cluster health check failed: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_copy_policy(policy: u8) -> &'static str {
    if policy == 0 {
        "replicate"
    } else {
        "erasure coding"
    }
}

fn format_flags(flags: u16) -> String {
    let mut parts = Vec::new();
    if flags & sheepdog_proto::constants::SD_CLUSTER_FLAG_STRICT != 0 {
        parts.push("strict");
    }
    if flags & sheepdog_proto::constants::SD_CLUSTER_FLAG_DISKMODE != 0 {
        parts.push("diskmode");
    }
    if flags & sheepdog_proto::constants::SD_CLUSTER_FLAG_USE_LOCK != 0 {
        parts.push("lock");
    }
    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(", ")
    }
}
