//! VDI tree visualization.
//!
//! Displays snapshot/clone relationships as an ASCII tree, showing
//! parent-child relationships between VDI versions.

use sheepdog_proto::inode::SdInode;
use sheepdog_proto::request::{ResponseResult, SdRequest};
use tokio::net::TcpStream;

use crate::common::{
    connect_to_sheep, exit_error, format_size, format_time, send_request, send_request_ok,
};

/// A node in the VDI tree.
struct TreeNode {
    vid: u32,
    name: String,
    tag: String,
    is_snapshot: bool,
    vdi_size: u64,
    create_time: u64,
    children: Vec<TreeNode>,
}

/// Display the VDI snapshot/clone tree.
pub async fn show_tree(addr: &str, port: u16, name: Option<&str>) {
    let mut stream = match connect_to_sheep(addr, port).await {
        Ok(s) => s,
        Err(_) => exit_error("Failed to connect to sheep daemon"),
    };

    // Read VDI bitmap
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

    // Collect all inodes
    let mut inodes: Vec<SdInode> = Vec::new();

    for byte_idx in 0..vdi_bitmap.len() {
        let byte = vdi_bitmap[byte_idx];
        if byte == 0 {
            continue;
        }
        for bit in 0..8u32 {
            if byte & (1 << bit) != 0 {
                let vid = (byte_idx as u32) * 8 + bit;
                if let Some(inode) = read_vdi_inode(&mut stream, vid).await {
                    // Filter by name if specified
                    if let Some(filter_name) = name {
                        if inode.name != filter_name {
                            continue;
                        }
                    }
                    inodes.push(inode);
                }
            }
        }
    }

    if inodes.is_empty() {
        if let Some(n) = name {
            println!("VDI '{}' not found.", n);
        } else {
            println!("No VDIs found.");
        }
        return;
    }

    // Build tree(s) - find root nodes (those without a parent or whose parent is 0)
    let all_vids: Vec<u32> = inodes.iter().map(|i| i.vdi_id).collect();
    let mut roots = Vec::new();

    for inode in &inodes {
        if inode.parent_vdi_id == 0 || !all_vids.contains(&inode.parent_vdi_id) {
            let tree = build_tree(inode.vdi_id, &inodes);
            roots.push(tree);
        }
    }

    // Print each tree
    for root in &roots {
        print_tree(root, "", true);
    }
}

/// Build a tree node from an inode and its children.
fn build_tree(vid: u32, inodes: &[SdInode]) -> TreeNode {
    let inode = inodes.iter().find(|i| i.vdi_id == vid);

    let (name, tag, is_snapshot, vdi_size, create_time) = match inode {
        Some(i) => (
            i.name.clone(),
            i.tag.clone(),
            i.is_snapshot(),
            i.vdi_size,
            i.create_time,
        ),
        None => (
            format!("unknown-{:#x}", vid),
            String::new(),
            false,
            0,
            0,
        ),
    };

    // Find children (inodes whose parent_vdi_id matches this vid)
    let child_vids: Vec<u32> = inodes
        .iter()
        .filter(|i| i.parent_vdi_id == vid && i.vdi_id != vid)
        .map(|i| i.vdi_id)
        .collect();

    let children: Vec<TreeNode> = child_vids
        .iter()
        .map(|&cvid| build_tree(cvid, inodes))
        .collect();

    TreeNode {
        vid,
        name,
        tag,
        is_snapshot,
        vdi_size,
        create_time,
        children,
    }
}

/// Print a tree with ASCII art connectors.
fn print_tree(node: &TreeNode, prefix: &str, is_last: bool) {
    let connector = if prefix.is_empty() {
        ""
    } else if is_last {
        "`-- "
    } else {
        "|-- "
    };

    let kind = if node.is_snapshot { "s" } else { "c" };
    let tag_display = if node.tag.is_empty() {
        String::new()
    } else {
        format!(" (tag: {})", node.tag)
    };

    println!(
        "{}{}[{}] {} ({:#x}) {}{} [{}]",
        prefix,
        connector,
        kind,
        node.name,
        node.vid,
        format_size(node.vdi_size),
        tag_display,
        format_time(node.create_time),
    );

    let child_prefix = if prefix.is_empty() {
        "".to_string()
    } else if is_last {
        format!("{}    ", prefix)
    } else {
        format!("{}|   ", prefix)
    };

    for (i, child) in node.children.iter().enumerate() {
        let child_is_last = i == node.children.len() - 1;
        print_tree(child, &child_prefix, child_is_last);
    }
}

/// Read a VDI inode object from the sheep daemon.
async fn read_vdi_inode(stream: &mut TcpStream, vid: u32) -> Option<SdInode> {
    use sheepdog_proto::oid::ObjectId;

    let oid = ObjectId::from_vid(vid);
    let req = SdRequest::ReadObj {
        oid,
        offset: 0,
        length: 0,
    };

    match send_request(stream, req).await {
        Ok(resp) => match resp.result {
            ResponseResult::Data(data) => bincode::deserialize::<SdInode>(&data).ok(),
            _ => None,
        },
        Err(_) => None,
    }
}
