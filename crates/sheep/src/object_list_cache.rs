//! Object list cache per epoch.
//!
//! Caches the mapping from epoch number to the list of object IDs stored
//! on this node at that epoch. This avoids expensive directory scans when
//! the object list hasn't changed.
//!
//! The cache is typically populated during recovery preparation and
//! invalidated when objects are added or removed.

use std::collections::HashMap;
use std::sync::RwLock;

use sheepdog_proto::oid::ObjectId;
use tracing::debug;

/// Cache of object ID lists, keyed by epoch.
pub struct ObjectListCache {
    /// Epoch -> list of OIDs.
    cache: RwLock<HashMap<u32, Vec<ObjectId>>>,
    /// Maximum number of epochs to cache.
    max_epochs: usize,
}

impl ObjectListCache {
    /// Create a new cache that retains at most `max_epochs` entries.
    pub fn new(max_epochs: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::with_capacity(max_epochs)),
            max_epochs,
        }
    }

    /// Look up the object list for a given epoch.
    pub fn get(&self, epoch: u32) -> Option<Vec<ObjectId>> {
        self.cache.read().unwrap().get(&epoch).cloned()
    }

    /// Store the object list for a given epoch.
    ///
    /// If the cache exceeds `max_epochs`, the oldest entry is evicted.
    pub fn put(&self, epoch: u32, oids: Vec<ObjectId>) {
        let mut cache = self.cache.write().unwrap();

        // Evict oldest if over capacity
        if cache.len() >= self.max_epochs && !cache.contains_key(&epoch) {
            if let Some(&oldest) = cache.keys().min() {
                cache.remove(&oldest);
                debug!("obj_list_cache: evicted epoch {}", oldest);
            }
        }

        debug!("obj_list_cache: cached {} objects for epoch {}", oids.len(), epoch);
        cache.insert(epoch, oids);
    }

    /// Invalidate the cache for a specific epoch.
    pub fn invalidate(&self, epoch: u32) {
        self.cache.write().unwrap().remove(&epoch);
    }

    /// Invalidate all cached entries.
    pub fn invalidate_all(&self) {
        self.cache.write().unwrap().clear();
        debug!("obj_list_cache: cleared all entries");
    }

    /// Check if an epoch is cached.
    pub fn contains(&self, epoch: u32) -> bool {
        self.cache.read().unwrap().contains_key(&epoch)
    }

    /// Get the number of cached epochs.
    pub fn len(&self) -> usize {
        self.cache.read().unwrap().len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.read().unwrap().is_empty()
    }

    /// Get all cached epoch numbers.
    pub fn epochs(&self) -> Vec<u32> {
        self.cache.read().unwrap().keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_obj_list_cache_basic() {
        let cache = ObjectListCache::new(5);
        assert!(cache.is_empty());

        let oids = vec![ObjectId::new(1), ObjectId::new(2)];
        cache.put(1, oids.clone());

        assert!(cache.contains(1));
        assert_eq!(cache.get(1).unwrap(), oids);
        assert!(cache.get(2).is_none());
    }

    #[test]
    fn test_obj_list_cache_eviction() {
        let cache = ObjectListCache::new(3);

        for epoch in 1..=3 {
            cache.put(epoch, vec![ObjectId::new(epoch as u64)]);
        }
        assert_eq!(cache.len(), 3);

        // Adding a 4th should evict the oldest (epoch 1)
        cache.put(4, vec![ObjectId::new(4)]);
        assert_eq!(cache.len(), 3);
        assert!(!cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
        assert!(cache.contains(4));
    }

    #[test]
    fn test_obj_list_cache_invalidate() {
        let cache = ObjectListCache::new(5);
        cache.put(1, vec![ObjectId::new(1)]);
        cache.put(2, vec![ObjectId::new(2)]);

        cache.invalidate(1);
        assert!(!cache.contains(1));
        assert!(cache.contains(2));

        cache.invalidate_all();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_obj_list_cache_update() {
        let cache = ObjectListCache::new(5);

        cache.put(1, vec![ObjectId::new(10)]);
        cache.put(1, vec![ObjectId::new(20), ObjectId::new(30)]);

        let oids = cache.get(1).unwrap();
        assert_eq!(oids.len(), 2);
        assert_eq!(oids[0], ObjectId::new(20));
    }
}
