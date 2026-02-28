//! Object recovery state machine.
//!
//! When the cluster epoch changes (a node joins or leaves), the consistent
//! hash ring is rebalanced and objects may need to migrate between nodes.
//! The recovery worker manages this background process.
//!
//! ## Recovery flow
//!
//! 1. **Idle** - Normal operation, no recovery in progress.
//! 2. **Preparing** - A new epoch has been detected. The worker compares
//!    the old and new hash rings to build a list of objects that need to
//!    move to or from this node.
//! 3. **InProgress** - Objects are being fetched from peers or removed
//!    from local storage as the ring rebalance dictates.
//! 4. **Complete** - All objects have been moved. The worker transitions
//!    back to Idle.
//!
//! If another epoch change occurs during recovery, the current recovery
//! is aborted and restarted with the new epoch.

use std::collections::HashSet;
use std::sync::Arc;

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::node::SdNode;
use sheepdog_proto::oid::ObjectId;
use sheepdog_core::consistent_hash::VNodeInfo;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info, warn};

use crate::daemon::SharedSys;
use crate::store::StoreDriver;

/// Current state of the recovery process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryState {
    /// No recovery in progress.
    Idle,
    /// Preparing the recovery plan (comparing hash rings).
    Preparing,
    /// Actively moving objects.
    InProgress,
    /// Recovery complete for this epoch.
    Complete,
}

impl std::fmt::Display for RecoveryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryState::Idle => write!(f, "idle"),
            RecoveryState::Preparing => write!(f, "preparing"),
            RecoveryState::InProgress => write!(f, "in_progress"),
            RecoveryState::Complete => write!(f, "complete"),
        }
    }
}

/// Progress tracker for a recovery operation.
#[derive(Debug, Clone)]
pub struct RecoveryProgress {
    /// Total number of objects to process.
    pub total_objs: u64,
    /// Number of objects recovered so far.
    pub recovered_objs: u64,
    /// Number of objects that failed to recover.
    pub failed_objs: u64,
    /// Number of objects removed (no longer owned by this node).
    pub removed_objs: u64,
}

impl RecoveryProgress {
    fn new() -> Self {
        Self {
            total_objs: 0,
            recovered_objs: 0,
            failed_objs: 0,
            removed_objs: 0,
        }
    }

    /// Percentage complete (0.0 to 100.0).
    pub fn percent(&self) -> f64 {
        if self.total_objs == 0 {
            return 100.0;
        }
        ((self.recovered_objs + self.removed_objs) as f64 / self.total_objs as f64) * 100.0
    }
}

/// An object that needs to be recovered (fetched from a peer node).
#[derive(Debug, Clone)]
struct RecoveryItem {
    /// Object ID to recover.
    oid: ObjectId,
    /// EC index for erasure coding.
    ec_index: u8,
    /// Node that should have the object.
    source: Arc<SdNode>,
}

/// The background recovery worker.
///
/// Monitors epoch changes and drives the recovery state machine.
pub struct RecoveryWorker {
    /// Shared daemon state.
    sys: SharedSys,
    /// Current recovery state.
    state: RwLock<RecoveryState>,
    /// Recovery progress.
    progress: RwLock<RecoveryProgress>,
    /// The epoch we are recovering for.
    recovery_epoch: RwLock<u32>,
    /// Signal to abort the current recovery.
    abort_notify: Notify,
    /// Signal that recovery is complete.
    complete_notify: Notify,
    /// The store driver for local object I/O.
    store: Arc<dyn StoreDriver>,
}

impl RecoveryWorker {
    /// Create a new RecoveryWorker.
    pub fn new(sys: SharedSys, store: Arc<dyn StoreDriver>) -> Self {
        Self {
            sys,
            state: RwLock::new(RecoveryState::Idle),
            progress: RwLock::new(RecoveryProgress::new()),
            recovery_epoch: RwLock::new(0),
            abort_notify: Notify::new(),
            complete_notify: Notify::new(),
            store,
        }
    }

    /// Get the current recovery state.
    pub async fn state(&self) -> RecoveryState {
        *self.state.read().await
    }

    /// Get the current recovery progress.
    pub async fn progress(&self) -> RecoveryProgress {
        self.progress.read().await.clone()
    }

    /// Get the epoch currently being recovered.
    pub async fn epoch(&self) -> u32 {
        *self.recovery_epoch.read().await
    }

    /// Start recovery for a new epoch.
    ///
    /// If recovery is already in progress, it is aborted and restarted.
    pub async fn start_recovery(&self, new_epoch: u32) -> SdResult<()> {
        let current_state = *self.state.read().await;

        if current_state == RecoveryState::InProgress
            || current_state == RecoveryState::Preparing
        {
            info!(
                "recovery: aborting epoch {} for new epoch {}",
                *self.recovery_epoch.read().await,
                new_epoch
            );
            self.abort_notify.notify_waiters();
        }

        *self.state.write().await = RecoveryState::Preparing;
        *self.recovery_epoch.write().await = new_epoch;
        *self.progress.write().await = RecoveryProgress::new();

        info!("recovery: starting for epoch {}", new_epoch);
        Ok(())
    }

    /// Prepare the recovery plan by comparing old and new hash rings.
    ///
    /// Identifies objects that this node should now own but doesn't have,
    /// and objects that this node no longer owns and should remove.
    pub async fn prepare(
        &self,
        old_vnode_info: &VNodeInfo,
        new_vnode_info: &VNodeInfo,
        nr_copies: usize,
    ) -> SdResult<(Vec<RecoveryItem>, Vec<ObjectId>)> {
        let sys = self.sys.read().await;
        let this_nid = sys.this_node.nid.to_string();
        drop(sys);

        // Get list of all local objects
        let local_oids = self.store.get_obj_list().await?;
        let local_set: HashSet<ObjectId> = local_oids.iter().copied().collect();

        let mut to_remove: Vec<ObjectId> = Vec::new();

        // Check which local objects are no longer owned by this node
        for &oid in &local_oids {
            let new_nodes = new_vnode_info.oid_to_nodes(oid, nr_copies);
            let owned = new_nodes.iter().any(|n| n.nid.to_string() == this_nid);

            if !owned {
                to_remove.push(oid);
            }
        }

        // Check which objects should now be on this node but aren't
        // We need to know all objects in the cluster. Since we can't enumerate
        // the entire object space, we look at objects that were on nodes in
        // the old ring that now map to us.
        //
        // In practice, the caller should provide a list of potentially affected
        // objects. For now, we check objects that were on old nodes.
        for &oid in &local_oids {
            let old_nodes = old_vnode_info.oid_to_nodes(oid, nr_copies);
            let new_nodes = new_vnode_info.oid_to_nodes(oid, nr_copies);

            // Check if any new target node doesn't have this object
            // (i.e., it just became responsible and needs a copy)
            for node in &new_nodes {
                let nid_str = node.nid.to_string();
                if nid_str == this_nid {
                    continue; // Skip ourselves
                }
                let was_target = old_nodes.iter().any(|n| n.nid.to_string() == nid_str);
                if !was_target {
                    // This node is newly responsible; it may not have the object.
                    // The peer node will request it from us via the peer protocol.
                    debug!(
                        "recovery: node {} is newly responsible for {}",
                        nid_str, oid
                    );
                }
            }
        }

        // Query peers for their object lists to find objects that now map to us
        let mut to_fetch: Vec<RecoveryItem> = Vec::new();
        let new_nodes = new_vnode_info.nodes();
        let epoch = *self.recovery_epoch.read().await;

        for node in &new_nodes {
            let nid_str = node.nid.to_string();
            if nid_str == this_nid {
                continue;
            }

            let peer_oids = match self.query_peer_obj_list(node, epoch).await {
                Ok(oids) => oids,
                Err(e) => {
                    warn!("recovery: failed to query obj list from {}: {}", node.nid, e);
                    continue;
                }
            };

            for oid in peer_oids {
                let target_nodes = new_vnode_info.oid_to_nodes(oid, nr_copies);
                let should_be_here = target_nodes.iter().any(|n| n.nid.to_string() == this_nid);

                if should_be_here && !local_set.contains(&oid) {
                    let ec_index = target_nodes
                        .iter()
                        .position(|n| n.nid.to_string() == this_nid)
                        .unwrap_or(0) as u8;

                    to_fetch.push(RecoveryItem {
                        oid,
                        ec_index,
                        source: node.clone(),
                    });
                }
            }
        }

        debug!(
            "recovery: plan: fetch={}, remove={}",
            to_fetch.len(),
            to_remove.len()
        );

        Ok((to_fetch, to_remove))
    }

    /// Execute the recovery plan.
    ///
    /// Fetches missing objects from peers and removes objects no longer
    /// owned by this node.
    pub async fn execute(
        &self,
        to_fetch: Vec<RecoveryItem>,
        to_remove: Vec<ObjectId>,
    ) -> SdResult<()> {
        let total = to_fetch.len() as u64 + to_remove.len() as u64;
        {
            let mut progress = self.progress.write().await;
            progress.total_objs = total;
        }

        *self.state.write().await = RecoveryState::InProgress;

        // Fetch missing objects from peers
        for item in &to_fetch {
            // Check for abort
            if *self.state.read().await != RecoveryState::InProgress {
                info!("recovery: aborted during fetch phase");
                return Ok(());
            }

            match self.fetch_object(item).await {
                Ok(()) => {
                    let mut progress = self.progress.write().await;
                    progress.recovered_objs += 1;
                }
                Err(e) => {
                    warn!("recovery: failed to fetch {} from {}: {}", item.oid, item.source.nid, e);
                    let mut progress = self.progress.write().await;
                    progress.failed_objs += 1;
                }
            }
        }

        // Remove objects no longer owned
        for oid in &to_remove {
            if *self.state.read().await != RecoveryState::InProgress {
                info!("recovery: aborted during remove phase");
                return Ok(());
            }

            match self.store.remove(*oid, 0).await {
                Ok(()) => {
                    let mut progress = self.progress.write().await;
                    progress.removed_objs += 1;
                    debug!("recovery: removed {}", oid);
                }
                Err(e) => {
                    warn!("recovery: failed to remove {}: {}", oid, e);
                    let mut progress = self.progress.write().await;
                    progress.failed_objs += 1;
                }
            }
        }

        *self.state.write().await = RecoveryState::Complete;
        self.complete_notify.notify_waiters();

        let progress = self.progress.read().await;
        info!(
            "recovery: complete (recovered={}, removed={}, failed={})",
            progress.recovered_objs, progress.removed_objs, progress.failed_objs
        );

        Ok(())
    }

    /// Fetch a single object from a peer node.
    ///
    /// Connects to the peer, sends a ReadPeer request, receives the object
    /// data, and writes it to the local store.
    async fn fetch_object(&self, item: &RecoveryItem) -> SdResult<()> {
        use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse, ResponseResult};
        use sheepdog_proto::constants::SD_SHEEP_PROTO_VER;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = item.source.nid.socket_addr();
        debug!(
            "recovery: fetching {} (ec={}) from {}",
            item.oid, item.ec_index, addr
        );

        // Connect to peer
        let mut stream = sheepdog_core::net::connect_to_addr(addr).await?;

        // Build ReadPeer request
        let epoch = self.sys.read().await.epoch();
        let header = RequestHeader {
            proto_ver: SD_SHEEP_PROTO_VER,
            epoch,
            id: 0,
        };
        let req = SdRequest::ReadPeer {
            oid: item.oid,
            ec_index: item.ec_index,
            offset: 0,
            length: 0, // 0 means read entire object
        };

        // Send request (length-prefixed bincode)
        let req_data = bincode::serialize(&(header, req))
            .map_err(|_| SdError::SystemError)?;
        stream
            .write_u32(req_data.len() as u32)
            .await
            .map_err(|_| SdError::NetworkError)?;
        stream
            .write_all(&req_data)
            .await
            .map_err(|_| SdError::NetworkError)?;

        // Read response
        let resp_len = stream
            .read_u32()
            .await
            .map_err(|_| SdError::NetworkError)? as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|_| SdError::NetworkError)?;

        let response: SdResponse =
            bincode::deserialize(&resp_buf).map_err(|_| SdError::SystemError)?;

        // Extract data from response
        let data = match response.result {
            ResponseResult::Data(d) => d,
            ResponseResult::Error(e) => return Err(e),
            _ => return Err(SdError::InvalidParms),
        };

        if data.is_empty() {
            return Err(SdError::NoObj);
        }

        // Write to local store
        self.store
            .create_and_write(item.oid, item.ec_index, &data)
            .await?;

        info!(
            "recovery: fetched {} ({} bytes) from {}",
            item.oid,
            data.len(),
            addr
        );
        Ok(())
    }

    /// Query a peer node for its list of stored object IDs.
    async fn query_peer_obj_list(
        &self,
        node: &SdNode,
        tgt_epoch: u32,
    ) -> SdResult<Vec<ObjectId>> {
        use sheepdog_proto::constants::SD_SHEEP_PROTO_VER;
        use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse, ResponseResult};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = node.nid.socket_addr();
        debug!("recovery: querying obj list from {}", addr);

        let mut stream = sheepdog_core::net::connect_to_addr(addr).await?;

        let epoch = self.sys.read().await.epoch();
        let header = RequestHeader {
            proto_ver: SD_SHEEP_PROTO_VER,
            epoch,
            id: 0,
        };
        let req = SdRequest::GetObjList { tgt_epoch };

        let req_data = bincode::serialize(&(header, req))
            .map_err(|_| SdError::SystemError)?;
        stream
            .write_u32(req_data.len() as u32)
            .await
            .map_err(|_| SdError::NetworkError)?;
        stream
            .write_all(&req_data)
            .await
            .map_err(|_| SdError::NetworkError)?;

        let resp_len = stream
            .read_u32()
            .await
            .map_err(|_| SdError::NetworkError)? as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|_| SdError::NetworkError)?;

        let response: SdResponse =
            bincode::deserialize(&resp_buf).map_err(|_| SdError::SystemError)?;

        match response.result {
            ResponseResult::Data(data) => {
                let raw_oids: Vec<u64> =
                    bincode::deserialize(&data).unwrap_or_default();
                let oids: Vec<ObjectId> = raw_oids
                    .into_iter()
                    .map(ObjectId::new)
                    .collect();
                debug!("recovery: got {} objects from {}", oids.len(), addr);
                Ok(oids)
            }
            ResponseResult::Error(e) => Err(e),
            _ => Ok(Vec::new()),
        }
    }

    /// Run the recovery worker loop.
    ///
    /// Waits for epoch change notifications and drives recovery.
    pub async fn run(self: Arc<Self>, shutdown: Arc<Notify>) {
        info!("recovery: worker started");

        let epoch_notify = {
            let sys = self.sys.read().await;
            sys.epoch_notify.clone()
        };

        loop {
            tokio::select! {
                _ = epoch_notify.notified() => {
                    let (epoch, nr_copies, disabled) = {
                        let sys = self.sys.read().await;
                        (sys.epoch(), sys.cinfo.nr_copies as usize, sys.disable_recovery)
                    };

                    if disabled {
                        debug!("recovery: disabled, skipping epoch {}", epoch);
                        continue;
                    }

                    if let Err(e) = self.start_recovery(epoch).await {
                        error!("recovery: failed to start for epoch {}: {}", epoch, e);
                        continue;
                    }

                    // Build hash ring info
                    // We need both old and new vnode info
                    let prev_epoch = if epoch > 1 { epoch - 1 } else { 0 };

                    let new_vnodes = crate::group::get_vnode_info(&self.sys).await;

                    let old_vnodes = if prev_epoch > 0 {
                        match crate::group::get_vnode_info_epoch(&self.sys, prev_epoch).await {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("recovery: no vnode info for epoch {}: {}", prev_epoch, e);
                                *self.state.write().await = RecoveryState::Idle;
                                continue;
                            }
                        }
                    } else {
                        // First epoch — nothing to recover from
                        *self.state.write().await = RecoveryState::Idle;
                        continue;
                    };

                    // Prepare
                    let (to_fetch, to_remove) = match self.prepare(&old_vnodes, &new_vnodes, nr_copies).await {
                        Ok(plan) => plan,
                        Err(e) => {
                            error!("recovery: prepare failed: {}", e);
                            *self.state.write().await = RecoveryState::Idle;
                            continue;
                        }
                    };

                    if to_fetch.is_empty() && to_remove.is_empty() {
                        info!("recovery: nothing to do for epoch {}", epoch);
                        *self.state.write().await = RecoveryState::Idle;
                        continue;
                    }

                    // Execute
                    if let Err(e) = self.execute(to_fetch, to_remove).await {
                        error!("recovery: execution failed: {}", e);
                    }

                    *self.state.write().await = RecoveryState::Idle;
                }
                _ = shutdown.notified() => {
                    info!("recovery: worker shutting down");
                    break;
                }
            }
        }
    }

    /// Wait for the current recovery to complete.
    pub async fn wait_for_completion(&self) {
        let state = *self.state.read().await;
        if state == RecoveryState::Idle || state == RecoveryState::Complete {
            return;
        }
        self.complete_notify.notified().await;
    }

    /// Abort any in-progress recovery.
    pub async fn abort(&self) {
        let state = *self.state.read().await;
        if state == RecoveryState::InProgress || state == RecoveryState::Preparing {
            info!("recovery: abort requested");
            self.abort_notify.notify_waiters();
            *self.state.write().await = RecoveryState::Idle;
        }
    }
}

/// Check if an object should be on this node according to the hash ring.
pub fn is_obj_local(
    oid: ObjectId,
    this_nid: &str,
    vnode_info: &VNodeInfo,
    nr_copies: usize,
) -> bool {
    let nodes = vnode_info.oid_to_nodes(oid, nr_copies);
    nodes.iter().any(|n| n.nid.to_string() == this_nid)
}

/// Compute the set of objects that should migrate due to an epoch change.
///
/// Returns two sets:
/// - `incoming`: objects that should now be on this node but may not be.
/// - `outgoing`: objects currently on this node that should move elsewhere.
pub fn compute_migration_sets(
    local_oids: &[ObjectId],
    this_nid: &str,
    old_vinfo: &VNodeInfo,
    new_vinfo: &VNodeInfo,
    nr_copies: usize,
) -> (Vec<ObjectId>, Vec<ObjectId>) {
    let mut incoming = Vec::new();
    let mut outgoing = Vec::new();

    for &oid in local_oids {
        let old_local = is_obj_local(oid, this_nid, old_vinfo, nr_copies);
        let new_local = is_obj_local(oid, this_nid, new_vinfo, nr_copies);

        match (old_local, new_local) {
            (true, false) => outgoing.push(oid),
            (false, true) => incoming.push(oid),
            _ => {} // No change needed
        }
    }

    (incoming, outgoing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_state_display() {
        assert_eq!(RecoveryState::Idle.to_string(), "idle");
        assert_eq!(RecoveryState::Preparing.to_string(), "preparing");
        assert_eq!(RecoveryState::InProgress.to_string(), "in_progress");
        assert_eq!(RecoveryState::Complete.to_string(), "complete");
    }

    #[test]
    fn test_recovery_progress() {
        let mut progress = RecoveryProgress::new();
        assert_eq!(progress.percent(), 100.0); // 0/0 = 100%

        progress.total_objs = 100;
        progress.recovered_objs = 25;
        progress.removed_objs = 25;
        assert!((progress.percent() - 50.0).abs() < 0.01);
    }
}
