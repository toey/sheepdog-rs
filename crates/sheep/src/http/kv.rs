//! Key-value storage operations for HTTP API.
//!
//! Maps bucket/key pairs to sheepdog VDI objects.
//! Each bucket is a VDI, and keys are stored as data objects within it.

use std::collections::BTreeMap;
use std::sync::Arc;

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::hash::sd_hash;
use sheepdog_proto::oid::ObjectId;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Bucket metadata.
#[derive(Debug, Clone)]
pub struct Bucket {
    /// VDI ID backing this bucket.
    pub vid: u32,
    /// Bucket name.
    pub name: String,
    /// Creation time (epoch seconds).
    pub created: u64,
}

/// Key-value store backed by sheepdog objects.
pub struct KvStore {
    /// Bucket name → Bucket metadata.
    buckets: Arc<RwLock<BTreeMap<String, Bucket>>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Register a bucket (VDI) in the KV store.
    pub async fn create_bucket(&self, name: &str, vid: u32) -> SdResult<()> {
        let mut buckets = self.buckets.write().await;
        if buckets.contains_key(name) {
            return Err(SdError::VdiExist);
        }
        buckets.insert(
            name.to_string(),
            Bucket {
                vid,
                name: name.to_string(),
                created: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            },
        );
        Ok(())
    }

    /// Delete a bucket.
    pub async fn delete_bucket(&self, name: &str) -> SdResult<()> {
        let mut buckets = self.buckets.write().await;
        if buckets.remove(name).is_none() {
            return Err(SdError::NoVdi);
        }
        Ok(())
    }

    /// Get bucket info.
    pub async fn get_bucket(&self, name: &str) -> SdResult<Bucket> {
        let buckets = self.buckets.read().await;
        buckets
            .get(name)
            .cloned()
            .ok_or(SdError::NoVdi)
    }

    /// List all buckets.
    pub async fn list_buckets(&self) -> Vec<Bucket> {
        let buckets = self.buckets.read().await;
        buckets.values().cloned().collect()
    }

    /// Compute the object ID for a key within a bucket.
    pub fn key_to_oid(vid: u32, key: &str) -> ObjectId {
        let hash = sd_hash(key.as_bytes());
        let data_idx = (hash % (1 << 20)) as u64; // Index within inode data array
        ObjectId::from_vid_data(vid, data_idx)
    }

    /// Check if a bucket exists.
    pub async fn bucket_exists(&self, name: &str) -> bool {
        let buckets = self.buckets.read().await;
        buckets.contains_key(name)
    }
}
