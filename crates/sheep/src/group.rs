//! Cluster membership and epoch management.
//!
//! Manages node join/leave events, epoch transitions, and cluster state
//! consistency. In the C version this was `group.c` (~1,400 lines).
//!
//! Key concepts:
//! - **Epoch**: monotonically increasing cluster generation number.
//!   Changes when any node joins or leaves.
//! - **VNode Info**: snapshot of the consistent hash ring at a given epoch.
//!   Used to route objects to the correct nodes.
//! - **Recovery**: when epochs change, objects may need to be migrated
//!   to new locations on the hash ring.

use sheepdog_proto::error::SdResult;
use sheepdog_proto::node::{ClusterStatus, SdNode};
use sheepdog_core::consistent_hash::VNodeInfo;
use tracing::{info, warn, error};

use crate::config;
use crate::daemon::SharedSys;

/// Handle a node join event.
///
/// Called when the cluster driver detects a new node joining.
/// Updates the node list, bumps epoch, and triggers recovery if needed.
pub async fn handle_node_join(sys: SharedSys, new_node: SdNode) -> SdResult<()> {
    let mut s = sys.write().await;

    // Check if node is already in the list (rejoin)
    let existing = s
        .cinfo
        .nodes
        .iter()
        .position(|n| n.nid == new_node.nid);

    if let Some(idx) = existing {
        info!("node {} rejoining (updating entry)", new_node.nid);
        s.cinfo.nodes[idx] = new_node;
    } else {
        info!("node {} joining cluster", new_node.nid);
        s.cinfo.nodes.push(new_node);
        s.cinfo.nodes.sort(); // Keep sorted for deterministic hash ring
    }

    // If cluster is in WaitForJoin and this node makes quorum, transition to Ok
    if s.cinfo.status == ClusterStatus::WaitForJoin {
        // For now, a single node is sufficient
        s.cinfo.status = ClusterStatus::Ok;
        info!("cluster status: Ok (enough nodes joined)");
    }

    // Bump epoch
    s.bump_epoch();
    let epoch = s.epoch();
    let dir = s.dir.clone();
    let cinfo = s.cinfo.clone();

    drop(s); // Release write lock before I/O

    // Persist epoch log
    let epoch_log = config::build_epoch_log(&cinfo);
    if let Err(e) = config::save_epoch_log(&dir, &epoch_log).await {
        error!("failed to save epoch log: {}", e);
    }
    if let Err(e) = config::save_config(&dir, &cinfo).await {
        error!("failed to save config: {}", e);
    }

    info!("epoch {} with {} nodes", epoch, cinfo.nodes.len());

    // TODO: trigger recovery for the new epoch
    // start_recovery(sys, epoch).await;

    Ok(())
}

/// Handle a node leave event.
///
/// Called when the cluster driver detects a node failure or departure.
pub async fn handle_node_leave(sys: SharedSys, left_node: &SdNode) -> SdResult<()> {
    let mut s = sys.write().await;

    let pos = s
        .cinfo
        .nodes
        .iter()
        .position(|n| n.nid == left_node.nid);

    if let Some(idx) = pos {
        info!("node {} left cluster", left_node.nid);
        s.cinfo.nodes.remove(idx);
    } else {
        warn!("node {} left but wasn't in node list", left_node.nid);
        return Ok(());
    }

    if s.cinfo.nodes.is_empty() {
        warn!("all nodes have left, cluster shutting down");
        s.cinfo.status = ClusterStatus::Shutdown;
        s.shutdown_notify.notify_waiters();
        return Ok(());
    }

    // Bump epoch
    s.bump_epoch();
    let epoch = s.epoch();
    let dir = s.dir.clone();
    let cinfo = s.cinfo.clone();

    drop(s);

    // Persist epoch log
    let epoch_log = config::build_epoch_log(&cinfo);
    if let Err(e) = config::save_epoch_log(&dir, &epoch_log).await {
        error!("failed to save epoch log: {}", e);
    }
    if let Err(e) = config::save_config(&dir, &cinfo).await {
        error!("failed to save config: {}", e);
    }

    info!("epoch {} with {} nodes (after leave)", epoch, cinfo.nodes.len());

    // TODO: trigger recovery for the new epoch
    // start_recovery(sys, epoch).await;

    Ok(())
}

/// Build a VNodeInfo from the current cluster state.
pub async fn get_vnode_info(sys: &SharedSys) -> VNodeInfo {
    let s = sys.read().await;
    VNodeInfo::new(&s.cinfo.nodes)
}

/// Get the VNodeInfo for a specific historical epoch.
pub async fn get_vnode_info_epoch(sys: &SharedSys, epoch: u32) -> SdResult<VNodeInfo> {
    let dir = {
        let s = sys.read().await;
        s.dir.clone()
    };

    let nodes = config::get_epoch_nodes(&dir, epoch).await?;
    Ok(VNodeInfo::new(&nodes))
}

/// Update the cluster status.
pub async fn update_cluster_status(sys: SharedSys, status: ClusterStatus) {
    let mut s = sys.write().await;
    let old = s.cinfo.status;
    s.cinfo.status = status;
    info!("cluster status: {:?} -> {:?}", old, status);
}

/// Check if this node is the cluster leader.
///
/// The leader is the node with the smallest NodeId (deterministic).
pub async fn is_leader(sys: &SharedSys) -> bool {
    let s = sys.read().await;
    if s.cinfo.nodes.is_empty() {
        return true;
    }
    let min_node = s.cinfo.nodes.iter().min_by_key(|n| &n.nid);
    match min_node {
        Some(leader) => leader.nid == s.this_node.nid,
        None => true,
    }
}

/// Get the number of current cluster members.
pub async fn nr_nodes(sys: &SharedSys) -> usize {
    let s = sys.read().await;
    s.cinfo.nodes.len()
}

/// Find a node by its NodeId string.
pub async fn find_node(sys: &SharedSys, nid: &str) -> Option<SdNode> {
    let s = sys.read().await;
    s.cinfo.nodes.iter().find(|n| n.nid.to_string() == nid).cloned()
}

/// Build initial cluster info for a fresh start.
pub fn initial_cluster_info(this_node: &SdNode) -> SdResult<()> {
    // The cluster starts in WaitForFormat state.
    // The first `dog cluster format` command will transition it to Ok.
    Ok(())
}
