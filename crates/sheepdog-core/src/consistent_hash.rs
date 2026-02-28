//! Consistent hashing with virtual nodes (vnodes) for data placement.

use sheepdog_proto::hash::sd_hash_next;
use sheepdog_proto::node::SdNode;
use sheepdog_proto::oid::ObjectId;
use sheepdog_proto::hash::sd_hash_oid;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A virtual node on the hash ring.
#[derive(Debug, Clone)]
pub struct VNode {
    /// Position on the hash ring
    pub hash: u64,
    /// Physical node this vnode belongs to
    pub node: Arc<SdNode>,
}

/// Snapshot of the vnode distribution — immutable, reference-counted.
#[derive(Debug, Clone)]
pub struct VNodeInfo {
    /// Virtual nodes sorted by hash (BTreeMap keys are the hash values)
    vnodes: BTreeMap<u64, Arc<SdNode>>,
    /// Physical nodes by NodeId string
    nodes: BTreeMap<String, Arc<SdNode>>,
    /// Number of fault zones
    pub nr_zones: usize,
}

impl VNodeInfo {
    /// Build a VNodeInfo from a list of physical nodes.
    pub fn new(nodes: &[SdNode]) -> Self {
        let mut vnodes = BTreeMap::new();
        let mut node_map = BTreeMap::new();
        let mut zones = std::collections::HashSet::new();

        for node in nodes {
            let arc_node = Arc::new(node.clone());
            let nid_str = node.nid.to_string();
            node_map.insert(nid_str.clone(), arc_node.clone());
            zones.insert(node.zone);

            // Generate virtual nodes
            let mut hash = sd_hash_oid(
                u64::from_ne_bytes({
                    let mut buf = [0u8; 8];
                    let addr_bytes = nid_str.as_bytes();
                    let copy_len = addr_bytes.len().min(8);
                    buf[..copy_len].copy_from_slice(&addr_bytes[..copy_len]);
                    buf
                }),
            );

            for _ in 0..node.nr_vnodes {
                vnodes.insert(hash, arc_node.clone());
                hash = sd_hash_next(hash);
            }
        }

        Self {
            vnodes,
            nodes: node_map,
            nr_zones: zones.len(),
        }
    }

    /// Find the `nr_copies` nodes responsible for the given object.
    ///
    /// Uses zone-aware placement: avoids placing multiple replicas in the
    /// same fault zone when possible.
    pub fn oid_to_nodes(&self, oid: ObjectId, nr_copies: usize) -> Vec<Arc<SdNode>> {
        if self.vnodes.is_empty() {
            return Vec::new();
        }

        let hash = sd_hash_oid(oid.raw());
        let mut result: Vec<Arc<SdNode>> = Vec::with_capacity(nr_copies);
        let mut zones_used = std::collections::HashSet::new();

        // Walk the ring from the hash point
        let iter = self
            .vnodes
            .range(hash..)
            .chain(self.vnodes.iter())
            .map(|(_, node)| node);

        for node in iter {
            if result.len() >= nr_copies {
                break;
            }

            // Zone-aware: prefer different zones
            let nid_str = node.nid.to_string();
            let already_selected = result.iter().any(|n| n.nid.to_string() == nid_str);
            if already_selected {
                continue;
            }

            // If we haven't filled all zones yet, prefer a new zone
            if zones_used.len() < self.nr_zones && zones_used.contains(&node.zone) {
                continue;
            }

            zones_used.insert(node.zone);
            result.push(node.clone());
        }

        // If zone-aware didn't fill all slots, relax the constraint
        if result.len() < nr_copies {
            let iter2 = self
                .vnodes
                .range(hash..)
                .chain(self.vnodes.iter())
                .map(|(_, node)| node);

            for node in iter2 {
                if result.len() >= nr_copies {
                    break;
                }
                let nid_str = node.nid.to_string();
                if !result.iter().any(|n| n.nid.to_string() == nid_str) {
                    result.push(node.clone());
                }
            }
        }

        result
    }

    /// Get the total number of physical nodes.
    pub fn nr_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Get all physical nodes.
    pub fn nodes(&self) -> Vec<Arc<SdNode>> {
        self.nodes.values().cloned().collect()
    }

    /// Find a node by its NodeId string.
    pub fn find_node(&self, nid: &str) -> Option<Arc<SdNode>> {
        self.nodes.get(nid).cloned()
    }
}
