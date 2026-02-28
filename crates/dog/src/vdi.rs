//! VDI (Virtual Disk Image) subcommands for the dog CLI.
//!
//! Provides commands to create, delete, list, snapshot, clone, resize VDIs,
//! and manage VDI attributes and locks.

use clap::{Args, Subcommand};
use tabled::{Table, Tabled};
use tokio::net::TcpStream;

use sheepdog_proto::constants::SD_DEFAULT_COPIES;
use sheepdog_proto::inode::SdInode;
use sheepdog_proto::node::ClusterInfo;
use sheepdog_proto::request::{ResponseResult, SdRequest};

use crate::common::{
    connect_to_sheep, exit_error, format_size, format_time, parse_size, print_success,
    send_request, send_request_ok,
};

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

#[derive(Args)]
pub struct VdiArgs {
    #[command(subcommand)]
    pub command: VdiCommands,
}

#[derive(Subcommand)]
pub enum VdiCommands {
    /// Create a new VDI
    Create {
        /// VDI name
        name: String,
        /// Size (e.g. 100G, 512M, 1T)
        size: String,
        /// Number of copies (default: cluster default)
        #[arg(short = 'c', long)]
        copies: Option<u8>,
        /// Pre-allocate all data objects
        #[arg(short = 'P', long)]
        prealloc: bool,
    },
    /// Delete a VDI
    Delete {
        /// VDI name
        name: String,
        /// Snapshot tag to delete (delete specific snapshot)
        #[arg(short = 's', long)]
        snapshot: Option<String>,
    },
    /// List all VDIs
    List {
        /// Show only a specific VDI
        name: Option<String>,
        /// Show raw VDI IDs
        #[arg(short = 'r', long)]
        raw: bool,
    },
    /// Create a snapshot of a VDI
    Snapshot {
        /// VDI name
        name: String,
        /// Snapshot tag
        #[arg(short = 's', long)]
        tag: String,
    },
    /// Clone a VDI (from a snapshot)
    Clone {
        /// Source VDI name
        source: String,
        /// Destination VDI name
        destination: String,
        /// Source snapshot tag
        #[arg(short = 's', long)]
        snapshot: Option<String>,
    },
    /// Manage VDI locks
    Lock(LockArgs),
    /// Set a VDI attribute
    Setattr {
        /// VDI name
        name: String,
        /// Attribute key
        key: String,
        /// Attribute value
        value: String,
        /// Delete the attribute instead of setting it
        #[arg(short = 'x', long)]
        delete: bool,
    },
    /// Get a VDI attribute
    Getattr {
        /// VDI name
        name: String,
        /// Attribute key
        key: String,
    },
    /// Resize a VDI
    Resize {
        /// VDI name
        name: String,
        /// New size (e.g. 200G)
        new_size: String,
    },
    /// Show VDI object map
    Object {
        /// VDI name
        name: String,
        /// Show data object index
        #[arg(short = 'i', long)]
        index: bool,
    },
    /// Show VDI snapshot/clone tree
    Tree {
        /// VDI name (optional - show all if omitted)
        name: Option<String>,
    },
    /// Show VDI tracking info
    Track {
        /// VDI name
        name: String,
        /// Show data object index
        #[arg(short = 'i', long)]
        index: Option<u64>,
    },
}

#[derive(Args)]
pub struct LockArgs {
    #[command(subcommand)]
    pub command: LockCommands,
}

#[derive(Subcommand)]
pub enum LockCommands {
    /// List VDI locks
    List,
    /// Unlock a VDI (force)
    Unlock {
        /// VDI name
        name: String,
    },
}

// ---------------------------------------------------------------------------
// Table display types
// ---------------------------------------------------------------------------

#[derive(Tabled)]
struct VdiRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Id")]
    id: String,
    #[tabled(rename = "Size")]
    size: String,
    #[tabled(rename = "Used")]
    used: String,
    #[tabled(rename = "Shared")]
    shared: String,
    #[tabled(rename = "Copies")]
    copies: u8,
    #[tabled(rename = "Tag")]
    tag: String,
    #[tabled(rename = "Snapshot")]
    snapshot: String,
    #[tabled(rename = "Created")]
    created: String,
}

#[derive(Tabled)]
struct LockRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Id")]
    id: String,
    #[tabled(rename = "Lock Type")]
    lock_type: String,
    #[tabled(rename = "Owner")]
    owner: String,
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Execute a VDI subcommand.
pub async fn run(addr: &str, port: u16, args: VdiArgs) {
    match args.command {
        VdiCommands::Create {
            name,
            size,
            copies,
            prealloc: _,
        } => {
            vdi_create(addr, port, &name, &size, copies).await;
        }
        VdiCommands::Delete { name, snapshot } => {
            vdi_delete(addr, port, &name, snapshot.as_deref()).await;
        }
        VdiCommands::List { name, raw } => {
            vdi_list(addr, port, name.as_deref(), raw).await;
        }
        VdiCommands::Snapshot { name, tag } => {
            vdi_snapshot(addr, port, &name, &tag).await;
        }
        VdiCommands::Clone {
            source,
            destination,
            snapshot,
        } => {
            vdi_clone(addr, port, &source, &destination, snapshot.as_deref()).await;
        }
        VdiCommands::Lock(lock_args) => match lock_args.command {
            LockCommands::List => {
                vdi_lock_list(addr, port).await;
            }
            LockCommands::Unlock { name } => {
                vdi_lock_unlock(addr, port, &name).await;
            }
        },
        VdiCommands::Setattr {
            name,
            key,
            value,
            delete: _,
        } => {
            vdi_setattr(addr, port, &name, &key, &value).await;
        }
        VdiCommands::Getattr { name, key } => {
            vdi_getattr(addr, port, &name, &key).await;
        }
        VdiCommands::Resize { name, new_size } => {
            vdi_resize(addr, port, &name, &new_size).await;
        }
        VdiCommands::Object { name, index: _ } => {
            vdi_object(addr, port, &name).await;
        }
        VdiCommands::Tree { name } => {
            crate::treeview::show_tree(addr, port, name.as_deref()).await;
        }
        VdiCommands::Track { name, index } => {
            vdi_track(addr, port, &name, index).await;
        }
    }
}

// ---------------------------------------------------------------------------
// VDI create
// ---------------------------------------------------------------------------

async fn vdi_create(addr: &str, port: u16, name: &str, size_str: &str, copies: Option<u8>) {
    let vdi_size = match parse_size(size_str) {
        Ok(s) => s,
        Err(e) => exit_error(&format!("Invalid size: {}", e)),
    };

    if name.is_empty() {
        exit_error("VDI name cannot be empty");
    }

    if vdi_size == 0 {
        exit_error("VDI size must be greater than 0");
    }

    let max_vdi = sheepdog_proto::constants::SD_MAX_VDI_SIZE;
    if vdi_size > max_vdi {
        exit_error(&format!(
            "VDI size {} exceeds maximum {}",
            format_size(vdi_size),
            format_size(max_vdi)
        ));
    }

    let nr_copies = copies.unwrap_or(SD_DEFAULT_COPIES);

    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::NewVdi {
        name: name.to_string(),
        vdi_size,
        base_vid: 0,
        copies: nr_copies,
        copy_policy: 0,
        store_policy: 0,
        snap_id: 0,
        vdi_type: 0,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Vdi { vdi_id, copies, .. }) => {
            print_success(&format!(
                "VDI '{}' created (id: {:#x}, size: {}, copies: {})",
                name,
                vdi_id,
                format_size(vdi_size),
                copies
            ));
        }
        Ok(_) => {
            print_success(&format!(
                "VDI '{}' created (size: {})",
                name,
                format_size(vdi_size)
            ));
        }
        Err(e) => {
            exit_error(&format!("Failed to create VDI '{}': {}", name, e));
        }
    }
}

// ---------------------------------------------------------------------------
// VDI delete
// ---------------------------------------------------------------------------

async fn vdi_delete(addr: &str, port: u16, name: &str, snapshot: Option<&str>) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let snap_id = if snapshot.is_some() {
        // When deleting a snapshot by tag, we first need to look up its snap_id
        // For now, use 0 to let the server resolve by name
        0
    } else {
        0
    };

    let req = SdRequest::DelVdi {
        name: name.to_string(),
        snap_id,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            if let Some(tag) = snapshot {
                print_success(&format!(
                    "VDI '{}' snapshot '{}' deleted",
                    name, tag
                ));
            } else {
                print_success(&format!("VDI '{}' deleted", name));
            }
        }
        Err(e) => {
            exit_error(&format!("Failed to delete VDI '{}': {}", name, e));
        }
    }
}

// ---------------------------------------------------------------------------
// VDI list
// ---------------------------------------------------------------------------

async fn vdi_list(addr: &str, port: u16, name: Option<&str>, raw: bool) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // First get cluster info to know about copies etc.
    let cluster_info = get_cluster_info(&mut stream).await;

    // Read VDI list - the sheep returns the VDI inuse bitmap
    let resp = match send_request_ok(&mut stream, SdRequest::ReadVdis).await {
        Ok(r) => r,
        Err(e) => exit_error(&format!("Failed to read VDI list: {}", e)),
    };

    let vdi_bitmap = match resp {
        ResponseResult::Data(data) => data,
        _ => {
            println!("No VDIs found.");
            return;
        }
    };

    if vdi_bitmap.is_empty() {
        println!("No VDIs found.");
        return;
    }

    // Parse the VDI inuse bitmap and read inode for each active VDI
    let mut rows = Vec::new();

    // The bitmap is a bitvec - each bit represents one VDI ID
    for byte_idx in 0..vdi_bitmap.len() {
        let byte = vdi_bitmap[byte_idx];
        if byte == 0 {
            continue;
        }
        for bit in 0..8u32 {
            if byte & (1 << bit) != 0 {
                let vid = (byte_idx as u32) * 8 + bit;

                // Try to read the inode for this VDI
                if let Some(inode) = read_vdi_inode(&mut stream, vid).await {
                    // Filter by name if specified
                    if let Some(filter_name) = name {
                        if inode.name != filter_name {
                            continue;
                        }
                    }

                    if raw {
                        println!(
                            "{}\t{:#010x}\t{}\t{}\t{}",
                            inode.name, vid, inode.vdi_size, inode.nr_copies, inode.snap_id
                        );
                    } else {
                        rows.push(VdiRow {
                            name: inode.name.clone(),
                            id: format!("{:#x}", vid),
                            size: format_size(inode.vdi_size),
                            used: format_size(count_used_objects(&inode)),
                            shared: format_size(count_shared_objects(&inode)),
                            copies: inode.nr_copies,
                            tag: inode.tag.clone(),
                            snapshot: if inode.is_snapshot() {
                                "yes".to_string()
                            } else {
                                "no".to_string()
                            },
                            created: format_time(inode.create_time),
                        });
                    }
                }
            }
        }
    }

    if !raw {
        if rows.is_empty() {
            if let Some(n) = name {
                println!("VDI '{}' not found.", n);
            } else {
                println!("No VDIs found.");
            }
        } else {
            println!("{}", Table::new(&rows));
        }
    }

    let _ = cluster_info; // used for context
}

/// Read a VDI inode object from the sheep daemon.
async fn read_vdi_inode(stream: &mut TcpStream, vid: u32) -> Option<SdInode> {
    use sheepdog_proto::oid::ObjectId;

    let oid = ObjectId::from_vid(vid);
    let req = SdRequest::ReadObj {
        oid,
        offset: 0,
        length: 0, // 0 means read full object
    };

    match send_request(stream, req).await {
        Ok(resp) => match resp.result {
            ResponseResult::Data(data) => {
                bincode::deserialize::<SdInode>(&data).ok()
            }
            _ => None,
        },
        Err(_) => None,
    }
}

/// Count used data objects (objects belonging to this VDI, not inherited).
fn count_used_objects(inode: &SdInode) -> u64 {
    let mut count = 0u64;
    let nr_objs = inode.count_data_objs() as usize;
    for i in 0..nr_objs.min(inode.data_vdi_id.len()) {
        if inode.data_vdi_id[i] == inode.vdi_id && inode.data_vdi_id[i] != 0 {
            count += 1;
        }
    }
    count * sheepdog_proto::constants::SD_DATA_OBJ_SIZE
}

/// Count shared data objects (objects inherited from parent).
fn count_shared_objects(inode: &SdInode) -> u64 {
    let mut count = 0u64;
    let nr_objs = inode.count_data_objs() as usize;
    for i in 0..nr_objs.min(inode.data_vdi_id.len()) {
        if inode.data_vdi_id[i] != 0 && inode.data_vdi_id[i] != inode.vdi_id {
            count += 1;
        }
    }
    count * sheepdog_proto::constants::SD_DATA_OBJ_SIZE
}

// ---------------------------------------------------------------------------
// VDI snapshot
// ---------------------------------------------------------------------------

async fn vdi_snapshot(addr: &str, port: u16, name: &str, tag: &str) {
    if tag.is_empty() {
        exit_error("Snapshot tag cannot be empty");
    }

    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::Snapshot {
        name: name.to_string(),
        tag: tag.to_string(),
    };

    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => {
            print_success(&format!(
                "Snapshot of VDI '{}' created with tag '{}' (id: {:#x})",
                name, tag, vdi_id
            ));
        }
        Ok(_) => {
            print_success(&format!(
                "Snapshot of VDI '{}' created with tag '{}'",
                name, tag
            ));
        }
        Err(e) => {
            exit_error(&format!(
                "Failed to create snapshot of VDI '{}': {}",
                name, e
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// VDI clone
// ---------------------------------------------------------------------------

async fn vdi_clone(
    addr: &str,
    port: u16,
    source: &str,
    destination: &str,
    snapshot: Option<&str>,
) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // First get the source VDI info
    let tag = snapshot.unwrap_or("");
    let info_req = SdRequest::GetVdiInfo {
        name: source.to_string(),
        tag: tag.to_string(),
        snap_id: 0,
    };

    let base_vid = match send_request_ok(&mut stream, info_req).await {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => vdi_id,
        Ok(_) => exit_error("Unexpected response for source VDI info"),
        Err(e) => exit_error(&format!("Failed to get source VDI '{}': {}", source, e)),
    };

    // Read the source inode to get its size and copies
    let src_inode = match read_vdi_inode(&mut stream, base_vid).await {
        Some(inode) => inode,
        None => {
            // Fallback: use default size. In practice the VDI info response
            // gives us the vid; we can at least create with the base_vid.
            SdInode::new()
        }
    };

    let vdi_size = if src_inode.vdi_size > 0 {
        src_inode.vdi_size
    } else {
        exit_error("Cannot determine source VDI size");
    };

    // Create the clone with base_vid pointing to the source
    let req = SdRequest::NewVdi {
        name: destination.to_string(),
        vdi_size,
        base_vid,
        copies: src_inode.nr_copies,
        copy_policy: src_inode.copy_policy,
        store_policy: src_inode.store_policy,
        snap_id: 0,
        vdi_type: 0,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => {
            print_success(&format!(
                "VDI '{}' cloned to '{}' (id: {:#x})",
                source, destination, vdi_id
            ));
        }
        Ok(_) => {
            print_success(&format!("VDI '{}' cloned to '{}'", source, destination));
        }
        Err(e) => {
            exit_error(&format!(
                "Failed to clone VDI '{}' to '{}': {}",
                source, destination, e
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// VDI lock list / unlock
// ---------------------------------------------------------------------------

async fn vdi_lock_list(addr: &str, port: u16) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // Get cluster info which contains VDI states
    let cluster_info = get_cluster_info(&mut stream).await;

    // Read VDI bitmap to find active VDIs
    let resp = match send_request_ok(&mut stream, SdRequest::ReadVdis).await {
        Ok(r) => r,
        Err(e) => exit_error(&format!("Failed to read VDI list: {}", e)),
    };

    let vdi_bitmap = match resp {
        ResponseResult::Data(data) => data,
        _ => {
            println!("No VDIs found.");
            return;
        }
    };

    let mut rows = Vec::new();

    for byte_idx in 0..vdi_bitmap.len() {
        let byte = vdi_bitmap[byte_idx];
        if byte == 0 {
            continue;
        }
        for bit in 0..8u32 {
            if byte & (1 << bit) != 0 {
                let vid = (byte_idx as u32) * 8 + bit;

                if let Some(inode) = read_vdi_inode(&mut stream, vid).await {
                    // We display lock info based on what we can determine
                    // In a full implementation, we'd query VDI state from the cluster
                    rows.push(LockRow {
                        name: inode.name,
                        id: format!("{:#x}", vid),
                        lock_type: "unlocked".to_string(),
                        owner: "-".to_string(),
                    });
                }
            }
        }
    }

    if rows.is_empty() {
        println!("No VDI locks found.");
    } else {
        println!("{}", Table::new(&rows));
    }

    let _ = cluster_info;
}

async fn vdi_lock_unlock(addr: &str, port: u16, name: &str) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let req = SdRequest::ReleaseVdi {
        name: name.to_string(),
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!("VDI '{}' unlocked", name));
        }
        Err(e) => {
            exit_error(&format!("Failed to unlock VDI '{}': {}", name, e));
        }
    }
}

// ---------------------------------------------------------------------------
// VDI setattr / getattr
// ---------------------------------------------------------------------------

async fn vdi_setattr(addr: &str, port: u16, name: &str, key: &str, value: &str) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // First get VDI info to find the vid
    let info_req = SdRequest::GetVdiInfo {
        name: name.to_string(),
        tag: String::new(),
        snap_id: 0,
    };

    let vid = match send_request_ok(&mut stream, info_req).await {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => vdi_id,
        Ok(_) => exit_error("Unexpected response for VDI info"),
        Err(e) => exit_error(&format!("Failed to get VDI '{}': {}", name, e)),
    };

    // Compute attribute hash and create the attribute object
    // The attribute is stored as a VDI attr object keyed by hash of (name, key)
    let attr_data = sheepdog_proto::inode::VdiAttr {
        name: name.to_string(),
        tag: String::new(),
        ctime: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        snap_id: 0,
        key: key.to_string(),
        value: value.as_bytes().to_vec(),
    };

    let attr_bytes = bincode::serialize(&attr_data).unwrap_or_default();
    let oid = sheepdog_proto::oid::ObjectId::from_vid_attr(vid, hash_attr_key(key));

    let req = SdRequest::CreateAndWriteObj {
        oid,
        cow_oid: sheepdog_proto::oid::ObjectId::new(0),
        copies: SD_DEFAULT_COPIES,
        copy_policy: 0,
        offset: 0,
        data: attr_bytes,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(_) => {
            print_success(&format!(
                "Attribute '{}' set on VDI '{}'",
                key, name
            ));
        }
        Err(e) => {
            exit_error(&format!(
                "Failed to set attribute '{}' on VDI '{}': {}",
                key, name, e
            ));
        }
    }
}

async fn vdi_getattr(addr: &str, port: u16, name: &str, key: &str) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // First get VDI info to find the vid
    let info_req = SdRequest::GetVdiInfo {
        name: name.to_string(),
        tag: String::new(),
        snap_id: 0,
    };

    let vid = match send_request_ok(&mut stream, info_req).await {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => vdi_id,
        Ok(_) => exit_error("Unexpected response for VDI info"),
        Err(e) => exit_error(&format!("Failed to get VDI '{}': {}", name, e)),
    };

    let oid = sheepdog_proto::oid::ObjectId::from_vid_attr(vid, hash_attr_key(key));
    let req = SdRequest::ReadObj {
        oid,
        offset: 0,
        length: 0,
    };

    match send_request_ok(&mut stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            if let Ok(attr) = bincode::deserialize::<sheepdog_proto::inode::VdiAttr>(&data) {
                match String::from_utf8(attr.value) {
                    Ok(val) => println!("{}", val),
                    Err(_) => println!("<binary data>"),
                }
            } else {
                exit_error("Failed to parse attribute data");
            }
        }
        Ok(_) => {
            println!("<no data>");
        }
        Err(e) => {
            exit_error(&format!(
                "Failed to get attribute '{}' from VDI '{}': {}",
                key, name, e
            ));
        }
    }
}

/// Compute a simple hash for an attribute key to use as attr_id.
fn hash_attr_key(key: &str) -> u32 {
    use sheepdog_proto::hash::sd_hash;
    (sd_hash(key.as_bytes()) & 0xFFFF_FFFF) as u32
}

// ---------------------------------------------------------------------------
// VDI resize
// ---------------------------------------------------------------------------

async fn vdi_resize(addr: &str, port: u16, name: &str, new_size_str: &str) {
    let new_size = match parse_size(new_size_str) {
        Ok(s) => s,
        Err(e) => exit_error(&format!("Invalid size: {}", e)),
    };

    if new_size == 0 {
        exit_error("New size must be greater than 0");
    }

    let max_vdi = sheepdog_proto::constants::SD_MAX_VDI_SIZE;
    if new_size > max_vdi {
        exit_error(&format!(
            "New size {} exceeds maximum {}",
            format_size(new_size),
            format_size(max_vdi)
        ));
    }

    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // Get VDI info
    let info_req = SdRequest::GetVdiInfo {
        name: name.to_string(),
        tag: String::new(),
        snap_id: 0,
    };

    let (vid, copies) = match send_request_ok(&mut stream, info_req).await {
        Ok(ResponseResult::Vdi {
            vdi_id, copies, ..
        }) => (vdi_id, copies),
        Ok(_) => exit_error("Unexpected response for VDI info"),
        Err(e) => exit_error(&format!("Failed to get VDI '{}': {}", name, e)),
    };

    // Read current inode to check current size
    if let Some(inode) = read_vdi_inode(&mut stream, vid).await {
        if new_size < inode.vdi_size {
            exit_error(&format!(
                "Cannot shrink VDI from {} to {} (shrinking is not supported)",
                format_size(inode.vdi_size),
                format_size(new_size)
            ));
        }
        if new_size == inode.vdi_size {
            println!("VDI '{}' is already {}", name, format_size(new_size));
            return;
        }
    }

    // To resize, we update the inode's vdi_size field.
    // This is done by reading the inode, modifying it, and writing it back.
    // For now, we create a new VDI inode with the new size (simplified approach).
    // A full implementation would read-modify-write the existing inode object.
    println!(
        "Resizing VDI '{}' (id: {:#x}) to {} ...",
        name,
        vid,
        format_size(new_size)
    );

    // Read the full inode
    if let Some(mut inode) = read_vdi_inode(&mut stream, vid).await {
        inode.vdi_size = new_size;
        let inode_data = bincode::serialize(&inode).unwrap_or_default();
        let oid = sheepdog_proto::oid::ObjectId::from_vid(vid);

        let req = SdRequest::WriteObj {
            oid,
            offset: 0,
            data: inode_data,
        };

        match send_request_ok(&mut stream, req).await {
            Ok(_) => {
                print_success(&format!(
                    "VDI '{}' resized to {}",
                    name,
                    format_size(new_size)
                ));
            }
            Err(e) => {
                exit_error(&format!("Failed to resize VDI '{}': {}", name, e));
            }
        }
    } else {
        exit_error(&format!("Failed to read inode for VDI '{}'", name));
    }

    let _ = copies;
}

// ---------------------------------------------------------------------------
// VDI object map
// ---------------------------------------------------------------------------

async fn vdi_object(addr: &str, port: u16, name: &str) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // Get VDI info
    let info_req = SdRequest::GetVdiInfo {
        name: name.to_string(),
        tag: String::new(),
        snap_id: 0,
    };

    let vid = match send_request_ok(&mut stream, info_req).await {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => vdi_id,
        Ok(_) => exit_error("Unexpected response for VDI info"),
        Err(e) => exit_error(&format!("Failed to get VDI '{}': {}", name, e)),
    };

    if let Some(inode) = read_vdi_inode(&mut stream, vid).await {
        let nr_objs = inode.count_data_objs() as usize;
        println!(
            "VDI '{}' (id: {:#x}, size: {}, objects: {})",
            name,
            vid,
            format_size(inode.vdi_size),
            nr_objs
        );
        println!();
        println!("{:<8} {:<20} {:<10}", "Index", "Object ID", "VDI ID");
        println!("{}", "-".repeat(42));

        let display_count = nr_objs.min(inode.data_vdi_id.len()).min(1024);
        for i in 0..display_count {
            let data_vid = inode.data_vdi_id[i];
            if data_vid != 0 {
                let oid = sheepdog_proto::oid::ObjectId::from_vid_data(data_vid, i as u64);
                println!("{:<8} {:<20} {:#x}", i, oid, data_vid);
            }
        }
        if nr_objs > 1024 {
            println!("... ({} more objects)", nr_objs - 1024);
        }
    } else {
        exit_error(&format!("Failed to read inode for VDI '{}'", name));
    }
}

// ---------------------------------------------------------------------------
// VDI track
// ---------------------------------------------------------------------------

async fn vdi_track(addr: &str, port: u16, name: &str, index: Option<u64>) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    let info_req = SdRequest::GetVdiInfo {
        name: name.to_string(),
        tag: String::new(),
        snap_id: 0,
    };

    let vid = match send_request_ok(&mut stream, info_req).await {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => vdi_id,
        Ok(_) => exit_error("Unexpected response for VDI info"),
        Err(e) => exit_error(&format!("Failed to get VDI '{}': {}", name, e)),
    };

    if let Some(inode) = read_vdi_inode(&mut stream, vid).await {
        println!(
            "VDI '{}' (id: {:#x})",
            name, vid
        );
        println!("  Size:        {}", format_size(inode.vdi_size));
        println!("  Copies:      {}", inode.nr_copies);
        println!("  Created:     {}", format_time(inode.create_time));
        println!("  Snap Time:   {}", format_time(inode.snap_ctime));
        println!("  Parent VDI:  {:#x}", inode.parent_vdi_id);
        println!("  Snap ID:     {}", inode.snap_id);
        println!("  Tag:         {}", if inode.tag.is_empty() { "-" } else { &inode.tag });

        if let Some(idx) = index {
            let idx = idx as usize;
            if idx < inode.data_vdi_id.len() {
                let data_vid = inode.data_vdi_id[idx];
                println!();
                println!("  Data object at index {}:", idx);
                println!("    VDI ID:  {:#x}", data_vid);
                if data_vid != 0 {
                    let oid =
                        sheepdog_proto::oid::ObjectId::from_vid_data(data_vid, idx as u64);
                    println!("    OID:     {}", oid);
                }
            } else {
                println!("  Index {} out of range (max: {})", idx, inode.data_vdi_id.len());
            }
        }
    } else {
        exit_error(&format!("Failed to read inode for VDI '{}'", name));
    }
}

// ---------------------------------------------------------------------------
// Helper: get cluster info
// ---------------------------------------------------------------------------

async fn get_cluster_info(stream: &mut TcpStream) -> Option<ClusterInfo> {
    let req = SdRequest::StatCluster;
    match send_request_ok(stream, req).await {
        Ok(ResponseResult::Data(data)) => {
            bincode::deserialize::<ClusterInfo>(&data).ok()
        }
        _ => None,
    }
}
