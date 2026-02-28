//! Write-back LRU object cache.
//!
//! Caches recently accessed object data in memory to reduce disk I/O.
//! Write operations go to the cache first and are flushed to the backing
//! store in the background. Reads check the cache before falling through
//! to disk.
//!
//! The cache uses:
//! - `dashmap` for concurrent access from multiple async tasks
//! - `lru::LruCache` for eviction ordering (wrapped in a mutex)
//! - A background flush task that periodically writes dirty entries

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use dashmap::DashMap;
use lru::LruCache;
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use tracing::{debug, info, warn};

use crate::store::StoreDriver;

/// Key for cache entries: (ObjectId, ec_index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub oid: ObjectId,
    pub ec_index: u8,
}

impl CacheKey {
    pub fn new(oid: ObjectId, ec_index: u8) -> Self {
        Self { oid, ec_index }
    }
}

/// A single cache entry.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Cached object data.
    pub data: Vec<u8>,
    /// Whether this entry has been modified since last flush.
    pub dirty: bool,
    /// Monotonic access counter for debugging/stats.
    pub last_access: u64,
}

/// Statistics about cache usage.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Total number of entries currently in cache.
    pub entries: usize,
    /// Maximum number of entries.
    pub capacity: usize,
    /// Total number of cache hits.
    pub hits: u64,
    /// Total number of cache misses.
    pub misses: u64,
    /// Number of dirty entries awaiting flush.
    pub dirty: usize,
    /// Total bytes of cached data.
    pub bytes_cached: u64,
    /// Number of evictions since startup.
    pub evictions: u64,
    /// Number of flush operations since startup.
    pub flushes: u64,
}

/// Write-back LRU object cache.
///
/// Thread-safe: uses DashMap for data storage and a Mutex-protected
/// LruCache for tracking access order and eviction.
pub struct ObjectCache {
    /// Cached data, keyed by (oid, ec_index).
    data: DashMap<CacheKey, CacheEntry>,

    /// LRU tracker: maps CacheKey to insertion order.
    /// Protected by a mutex since LruCache is not Sync.
    lru: Mutex<LruCache<CacheKey, ()>>,

    /// Maximum number of entries in the cache.
    capacity: usize,

    /// Monotonically increasing access counter.
    access_counter: AtomicU64,

    /// Hit counter.
    hits: AtomicU64,

    /// Miss counter.
    misses: AtomicU64,

    /// Eviction counter.
    evictions: AtomicU64,

    /// Flush counter.
    flush_count: AtomicU64,
}

impl ObjectCache {
    /// Create a new cache with the given maximum number of entries.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "cache capacity must be > 0");
        Self {
            data: DashMap::with_capacity(capacity),
            lru: Mutex::new(LruCache::new(capacity)),
            capacity,
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
        }
    }

    /// Look up an object in the cache.
    ///
    /// Returns `Some(data)` if found, `None` if not cached.
    /// Updates LRU ordering on hit.
    pub fn get(&self, oid: ObjectId, ec_index: u8) -> Option<Vec<u8>> {
        let key = CacheKey::new(oid, ec_index);

        if let Some(mut entry) = self.data.get_mut(&key) {
            // Update access time
            let access = self.access_counter.fetch_add(1, Ordering::Relaxed);
            entry.last_access = access;

            // Promote in LRU
            if let Ok(mut lru) = self.lru.lock() {
                lru.get(&key);
            }

            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.data.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert or update an object in the cache.
    ///
    /// If the cache is full, the least recently used clean entry is evicted.
    /// If all entries are dirty, the oldest dirty entry is force-flushed
    /// (returned for the caller to persist) before eviction.
    ///
    /// Returns an evicted dirty entry that must be written to disk, if any.
    pub fn put(
        &self,
        oid: ObjectId,
        ec_index: u8,
        data: Vec<u8>,
        dirty: bool,
    ) -> Option<(CacheKey, Vec<u8>)> {
        let key = CacheKey::new(oid, ec_index);
        let access = self.access_counter.fetch_add(1, Ordering::Relaxed);

        let entry = CacheEntry {
            data,
            dirty,
            last_access: access,
        };

        // Check if we need to evict
        let evicted = if self.data.len() >= self.capacity && !self.data.contains_key(&key) {
            self.evict_one()
        } else {
            None
        };

        // Insert/update
        self.data.insert(key, entry);
        if let Ok(mut lru) = self.lru.lock() {
            lru.put(key, ());
        }

        evicted
    }

    /// Evict one entry from the cache to make room.
    ///
    /// Prefers evicting clean entries. If no clean entry is found, evicts
    /// the LRU dirty entry and returns it for flushing.
    fn evict_one(&self) -> Option<(CacheKey, Vec<u8>)> {
        let mut lru = self.lru.lock().ok()?;

        // Try to find a clean entry to evict
        // Walk from LRU end towards MRU end
        let keys: Vec<CacheKey> = lru.iter().map(|(k, _)| *k).collect();
        for key in &keys {
            if let Some(entry) = self.data.get(key) {
                if !entry.dirty {
                    // Found a clean entry to evict
                    drop(entry);
                    self.data.remove(key);
                    lru.pop(key);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                    debug!("cache: evicted clean entry {}", key.oid);
                    return None;
                }
            }
        }

        // All entries are dirty; evict the LRU entry and return its data
        if let Some((key, _)) = lru.pop_lru() {
            if let Some((_, entry)) = self.data.remove(&key) {
                self.evictions.fetch_add(1, Ordering::Relaxed);
                debug!("cache: force-evicted dirty entry {}", key.oid);
                return Some((key, entry.data));
            }
        }

        None
    }

    /// Mark a cached entry as clean (after it has been flushed to disk).
    pub fn mark_clean(&self, oid: ObjectId, ec_index: u8) {
        let key = CacheKey::new(oid, ec_index);
        if let Some(mut entry) = self.data.get_mut(&key) {
            entry.dirty = false;
        }
    }

    /// Collect all dirty entries for a specific VDI.
    ///
    /// Returns the keys and data of dirty entries whose OID belongs
    /// to the given VDI. The entries remain in cache but are marked clean.
    pub fn flush_vid(&self, vid: u32) -> Vec<(CacheKey, Vec<u8>)> {
        let mut flushed = Vec::new();

        // Iterate all entries, find dirty ones matching this VDI
        for mut entry in self.data.iter_mut() {
            let key = *entry.key();
            if key.oid.to_vid() == vid && entry.dirty {
                flushed.push((key, entry.data.clone()));
                entry.dirty = false;
            }
        }

        if !flushed.is_empty() {
            self.flush_count.fetch_add(1, Ordering::Relaxed);
            debug!("cache: flushed {} entries for vid {}", flushed.len(), vid);
        }

        flushed
    }

    /// Collect all dirty entries from the entire cache.
    ///
    /// Returns the keys and data. Entries are marked clean in the cache.
    pub fn flush_all(&self) -> Vec<(CacheKey, Vec<u8>)> {
        let mut flushed = Vec::new();

        for mut entry in self.data.iter_mut() {
            if entry.dirty {
                let key = *entry.key();
                flushed.push((key, entry.data.clone()));
                entry.dirty = false;
            }
        }

        if !flushed.is_empty() {
            self.flush_count.fetch_add(1, Ordering::Relaxed);
            info!("cache: flushed {} dirty entries", flushed.len());
        }

        flushed
    }

    /// Write all dirty entries to the given store driver.
    ///
    /// This is the main flush path: iterates dirty entries, writes each
    /// to disk via the store driver, and marks them clean.
    pub async fn flush_to_store(&self, store: &dyn StoreDriver) -> SdResult<()> {
        let dirty_entries = self.flush_all();

        for (key, data) in dirty_entries {
            // Try create_and_write first; if exists, fall back to write-at-0
            match store.create_and_write(key.oid, key.ec_index, &data).await {
                Ok(()) => {}
                Err(SdError::OidExist) => {
                    store.write(key.oid, key.ec_index, 0, &data).await?;
                }
                Err(e) => {
                    // Mark dirty again since flush failed
                    if let Some(mut entry) = self.data.get_mut(&key) {
                        entry.dirty = true;
                    }
                    warn!("cache: failed to flush {}: {}", key.oid, e);
                    return Err(e);
                }
            }
        }

        store.flush().await?;
        Ok(())
    }

    /// Remove an entry from the cache entirely.
    pub fn purge(&self, oid: ObjectId, ec_index: u8) -> Option<CacheEntry> {
        let key = CacheKey::new(oid, ec_index);
        if let Ok(mut lru) = self.lru.lock() {
            lru.pop(&key);
        }
        self.data.remove(&key).map(|(_, v)| v)
    }

    /// Remove all entries from the cache. Returns dirty entries.
    pub fn purge_all(&self) -> Vec<(CacheKey, CacheEntry)> {
        let mut evicted = Vec::new();

        // Collect all keys first to avoid holding iterator while modifying
        let keys: Vec<CacheKey> = self.data.iter().map(|e| *e.key()).collect();

        for key in keys {
            if let Some((k, entry)) = self.data.remove(&key) {
                if entry.dirty {
                    evicted.push((k, entry));
                }
            }
        }

        if let Ok(mut lru) = self.lru.lock() {
            lru.clear();
        }

        evicted
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let dirty = self.data.iter().filter(|e| e.dirty).count();
        let bytes_cached: u64 = self.data.iter().map(|e| e.data.len() as u64).sum();

        CacheStats {
            entries: self.data.len(),
            capacity: self.capacity,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            dirty,
            bytes_cached,
            evictions: self.evictions.load(Ordering::Relaxed),
            flushes: self.flush_count.load(Ordering::Relaxed),
        }
    }

    /// Check if the cache contains an entry.
    pub fn contains(&self, oid: ObjectId, ec_index: u8) -> bool {
        self.data.contains_key(&CacheKey::new(oid, ec_index))
    }

    /// Get the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Spawn a background flush task that periodically writes dirty entries
/// to the store.
///
/// The task runs until the `shutdown` notify is triggered.
pub async fn run_flush_loop(
    cache: std::sync::Arc<ObjectCache>,
    store: std::sync::Arc<dyn StoreDriver>,
    interval: std::time::Duration,
    shutdown: std::sync::Arc<tokio::sync::Notify>,
) {
    info!(
        "cache: starting flush loop (interval={:?})",
        interval
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                if let Err(e) = cache.flush_to_store(store.as_ref()).await {
                    warn!("cache: flush loop error: {}", e);
                }
            }
            _ = shutdown.notified() => {
                info!("cache: flush loop shutting down, final flush");
                if let Err(e) = cache.flush_to_store(store.as_ref()).await {
                    warn!("cache: final flush error: {}", e);
                }
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_ops() {
        let cache = ObjectCache::new(10);
        let oid = ObjectId::new(0x0000_002a_0000_0001);

        // Miss
        assert!(cache.get(oid, 0).is_none());

        // Put
        let evicted = cache.put(oid, 0, b"hello".to_vec(), false);
        assert!(evicted.is_none());

        // Hit
        let data = cache.get(oid, 0).unwrap();
        assert_eq!(data, b"hello");

        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_dirty_flush() {
        let cache = ObjectCache::new(10);
        let oid1 = ObjectId::from_vid_data(10, 1);
        let oid2 = ObjectId::from_vid_data(10, 2);
        let oid3 = ObjectId::from_vid_data(20, 1);

        cache.put(oid1, 0, b"data1".to_vec(), true);
        cache.put(oid2, 0, b"data2".to_vec(), true);
        cache.put(oid3, 0, b"data3".to_vec(), true);

        // Flush vid 10 only
        let flushed = cache.flush_vid(10);
        assert_eq!(flushed.len(), 2);

        // After flush, entries should be clean
        let stats = cache.stats();
        assert_eq!(stats.dirty, 1); // Only oid3 (vid=20) is still dirty
    }

    #[test]
    fn test_cache_flush_all() {
        let cache = ObjectCache::new(10);

        for i in 0..5u64 {
            let oid = ObjectId::new(i);
            cache.put(oid, 0, vec![i as u8; 100], true);
        }

        let flushed = cache.flush_all();
        assert_eq!(flushed.len(), 5);

        let stats = cache.stats();
        assert_eq!(stats.dirty, 0);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = ObjectCache::new(3);

        // Fill the cache
        for i in 0..3u64 {
            let oid = ObjectId::new(i);
            cache.put(oid, 0, vec![i as u8; 10], false);
        }

        // Adding a 4th should evict one
        let oid4 = ObjectId::new(99);
        let evicted = cache.put(oid4, 0, b"new".to_vec(), false);
        assert!(evicted.is_none()); // Clean eviction, no data returned

        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_cache_dirty_eviction() {
        let cache = ObjectCache::new(2);

        // Fill with dirty entries
        let oid1 = ObjectId::new(1);
        let oid2 = ObjectId::new(2);
        cache.put(oid1, 0, b"dirty1".to_vec(), true);
        cache.put(oid2, 0, b"dirty2".to_vec(), true);

        // Adding a 3rd should force-evict the LRU dirty entry
        let oid3 = ObjectId::new(3);
        let evicted = cache.put(oid3, 0, b"dirty3".to_vec(), true);

        // Should get back the evicted dirty entry
        assert!(evicted.is_some());
        let (key, data) = evicted.unwrap();
        assert_eq!(key.oid, oid1); // LRU entry
        assert_eq!(data, b"dirty1");
    }

    #[test]
    fn test_cache_purge() {
        let cache = ObjectCache::new(10);
        let oid = ObjectId::new(42);

        cache.put(oid, 0, b"data".to_vec(), true);
        assert!(cache.contains(oid, 0));

        let entry = cache.purge(oid, 0).unwrap();
        assert!(entry.dirty);
        assert!(!cache.contains(oid, 0));
    }

    #[test]
    fn test_cache_purge_all() {
        let cache = ObjectCache::new(10);

        for i in 0..5u64 {
            cache.put(ObjectId::new(i), 0, vec![0; 10], i % 2 == 0);
        }

        let dirty = cache.purge_all();
        // Entries 0, 2, 4 are dirty
        assert_eq!(dirty.len(), 3);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_stats() {
        let cache = ObjectCache::new(100);

        for i in 0..10u64 {
            cache.put(ObjectId::new(i), 0, vec![0; 50], i < 5);
        }

        let stats = cache.stats();
        assert_eq!(stats.entries, 10);
        assert_eq!(stats.capacity, 100);
        assert_eq!(stats.dirty, 5);
        assert_eq!(stats.bytes_cached, 500);
    }
}
