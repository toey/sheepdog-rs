//! iSCSI target management commands for the `dog` CLI.

use clap::{Args, Subcommand};
use sheepdog_proto::request::{ResponseResult, SdRequest};

use crate::common::{connect_to_sheep, exit_error, print_success, send_request_ok};

#[derive(Args, Debug)]
pub struct IscsiArgs {
    #[command(subcommand)]
    pub command: IscsiCommand,
}

#[derive(Subcommand, Debug)]
pub enum IscsiCommand {
    /// Create a new iSCSI target
    Create(IscsiCreateArgs),
    /// List all iSCSI targets
    List,
    /// Delete an iSCSI target
    Delete(IscsiDeleteArgs),
}

#[derive(Args, Debug)]
pub struct IscsiCreateArgs {
    /// Target name (IQN format)
    #[arg(long)]
    pub target_name: String,

    /// Target alias (optional)
    #[arg(long)]
    pub target_alias: Option<String>,

    /// VDI ID backed by this LUN
    #[arg(long)]
    pub vid: u32,

    /// VDI size in bytes
    #[arg(long)]
    pub size: u64,

    /// Block size (default: 512)
    #[arg(long, default_value = "512")]
    pub block_size: u32,

    /// CHAP username (optional)
    #[arg(long)]
    pub chap_username: Option<String>,

    /// CHAP secret (optional)
    #[arg(long)]
    pub chap_secret: Option<String>,
}

#[derive(Args, Debug)]
pub struct IscsiDeleteArgs {
    /// Target name to delete
    #[arg(long)]
    pub target_name: String,
}

/// Execute an iSCSI subcommand.
pub async fn run(address: &str, port: u16, args: IscsiArgs) {
    match args.command {
        IscsiCommand::Create(create_args) => {
            iscsi_create(address, port, create_args).await;
        }
        IscsiCommand::List => {
            iscsi_list(address, port).await;
        }
        IscsiCommand::Delete(delete_args) => {
            iscsi_delete(address, port, delete_args).await;
        }
    }
}

async fn iscsi_create(addr: &str, port: u16, args: IscsiCreateArgs) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::IscsiCreate {
        target_name: args.target_name,
        target_alias: args.target_alias,
        vid: args.vid,
        size: args.size,
        block_size: args.block_size,
        chap_username: args.chap_username,
        chap_secret: args.chap_secret,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Success) => {
            print_success("iSCSI target created successfully");
        }
        Ok(resp) => {
            exit_error(&format!("Unexpected response: {:?}", resp));
        }
        Err(e) => {
            exit_error(&format!("Error: {}", e));
        }
    }
}

async fn iscsi_list(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::IscsiList;

    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::IscsiList(targets)) => {
            if targets.is_empty() {
                println!("No iSCSI targets found.");
            } else {
                for target in targets {
                    println!(
                        "Target: {} (VID: {})",
                        target.target_name, target.vid
                    );
                }
            }
        }
        Ok(resp) => {
            exit_error(&format!("Unexpected response: {:?}", resp));
        }
        Err(e) => {
            exit_error(&format!("Failed to list iSCSI targets: {}", e));
        }
    }
}

async fn iscsi_delete(addr: &str, port: u16, args: IscsiDeleteArgs) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::IscsiDelete {
        target_name: args.target_name,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Success) => {
            print_success("iSCSI target deleted successfully");
        }
        Ok(resp) => {
            exit_error(&format!("Unexpected response: {:?}", resp));
        }
        Err(e) => {
            exit_error(&format!("Failed to delete iSCSI target: {}", e));
        }
    }
}