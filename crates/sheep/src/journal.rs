//! Write-ahead journal using memory-mapped files.
//!
//! The journal provides crash recovery for the object store. Before any
//! write reaches the backing store, it is first appended to the journal.
//! On startup after an unclean shutdown, the journal is replayed to
//! reconstruct any writes that were in-flight.
//!
//! ## Journal file format
//!
//! The journal is a fixed-size memory-mapped file. Entries are appended
//! sequentially. Each entry has the following layout:
//!
//! ```text
//! +----------+----------+----------+----------+--------+
//! | magic(4) | oid (8)  | offset(8)| len (4)  | data.. |
//! +----------+----------+----------+----------+--------+
//! ```
//!
//! A header at offset 0 stores:
//! ```text
//! +----------+----------+----------+
//! | magic(4) | head (8) | tail (8) |
//! +----------+----------+----------+
//! ```
//!
//! Writes advance the `tail` pointer. After a successful flush to the
//! backing store, the `head` pointer is advanced to match `tail`,
//! effectively marking all entries as committed.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use memmap2::MmapMut;
use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use tracing::{debug, error, info, warn};

/// Magic number for journal header validation.
const JOURNAL_MAGIC: u32 = 0x534A_524E; // "SJRN"

/// Magic number for journal entries.
const ENTRY_MAGIC: u32 = 0x4A45_4E54; // "JENT"

/// Size of the journal header.
const HEADER_SIZE: usize = 20; // magic(4) + head(8) + tail(8)

/// Size of an entry header (before the data payload).
const ENTRY_HEADER_SIZE: usize = 24; // magic(4) + oid(8) + offset(8) + len(4)

/// A single journal entry, parsed from the mmap.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// Object ID this write targets.
    pub oid: ObjectId,
    /// Byte offset within the object.
    pub offset: u64,
    /// Write payload data.
    pub data: Vec<u8>,
}

/// Write-ahead journal backed by a memory-mapped file.
pub struct Journal {
    /// Memory-mapped journal file.
    mmap: Mutex<MmapMut>,
    /// Path to the journal file (for diagnostics).
    path: PathBuf,
    /// Total size of the journal file in bytes.
    size: usize,
    /// Whether the journal has been initialized.
    initialized: AtomicBool,
}

impl Journal {
    /// Open or create a journal file at `path` with the given `size` in bytes.
    ///
    /// If the file already exists, it is opened and validated. If it does
    /// not exist, it is created and initialized with an empty header.
    pub fn open(path: &Path, size: usize) -> SdResult<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| {
                error!("journal: failed to open {}: {}", path.display(), e);
                SdError::Eio
            })?;

        // Ensure the file is the right size
        let metadata = file.metadata().map_err(|_| SdError::Eio)?;
        if metadata.len() < size as u64 {
            file.set_len(size as u64).map_err(|e| {
                error!("journal: failed to set size: {}", e);
                SdError::Eio
            })?;
        }

        // Memory-map the file
        let mmap = unsafe {
            MmapMut::map_mut(&file).map_err(|e| {
                error!("journal: failed to mmap {}: {}", path.display(), e);
                SdError::Eio
            })?
        };

        let journal = Self {
            mmap: Mutex::new(mmap),
            path: path.to_path_buf(),
            size,
            initialized: AtomicBool::new(false),
        };

        // Initialize header if new
        {
            let mmap = journal.mmap.lock().unwrap();
            let magic = u32::from_le_bytes([mmap[0], mmap[1], mmap[2], mmap[3]]);
            if magic != JOURNAL_MAGIC {
                drop(mmap);
                journal.init_header()?;
                info!("journal: initialized new journal at {}", path.display());
            } else {
                info!("journal: opened existing journal at {}", path.display());
            }
        }

        journal.initialized.store(true, Ordering::Release);
        Ok(journal)
    }

    /// Initialize the journal header with empty state.
    fn init_header(&self) -> SdResult<()> {
        let mut mmap = self.mmap.lock().unwrap();

        // Write magic
        mmap[0..4].copy_from_slice(&JOURNAL_MAGIC.to_le_bytes());
        // head = HEADER_SIZE (start of data area)
        mmap[4..12].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        // tail = HEADER_SIZE (empty journal)
        mmap[12..20].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());

        mmap.flush().map_err(|e| {
            error!("journal: failed to flush header: {}", e);
            SdError::Eio
        })?;

        Ok(())
    }

    /// Read the current head pointer (start of uncommitted entries).
    fn read_head(mmap: &MmapMut) -> u64 {
        u64::from_le_bytes(mmap[4..12].try_into().unwrap())
    }

    /// Read the current tail pointer (end of written entries).
    fn read_tail(mmap: &MmapMut) -> u64 {
        u64::from_le_bytes(mmap[12..20].try_into().unwrap())
    }

    /// Write the head pointer.
    fn write_head(mmap: &mut MmapMut, head: u64) {
        mmap[4..12].copy_from_slice(&head.to_le_bytes());
    }

    /// Write the tail pointer.
    fn write_tail(mmap: &mut MmapMut, tail: u64) {
        mmap[12..20].copy_from_slice(&tail.to_le_bytes());
    }

    /// Append a write entry to the journal.
    ///
    /// Returns `SdError::NoSpace` if the journal is full.
    pub fn append(&self, oid: ObjectId, offset: u64, data: &[u8]) -> SdResult<()> {
        let entry_size = ENTRY_HEADER_SIZE + data.len();
        let mut mmap = self.mmap.lock().unwrap();
        let tail = Self::read_tail(&mmap) as usize;

        // Check if there's room
        if tail + entry_size > self.size {
            warn!("journal: no space (tail={}, need={}, size={})", tail, entry_size, self.size);
            return Err(SdError::NoSpace);
        }

        // Write entry header
        let pos = tail;
        mmap[pos..pos + 4].copy_from_slice(&ENTRY_MAGIC.to_le_bytes());
        mmap[pos + 4..pos + 12].copy_from_slice(&oid.raw().to_le_bytes());
        mmap[pos + 12..pos + 20].copy_from_slice(&offset.to_le_bytes());
        mmap[pos + 20..pos + 24].copy_from_slice(&(data.len() as u32).to_le_bytes());
        mmap[pos + 24..pos + 24 + data.len()].copy_from_slice(data);

        // Update tail
        let new_tail = (tail + entry_size) as u64;
        Self::write_tail(&mut mmap, new_tail);

        debug!("journal: appended entry for {} (offset={}, len={})", oid, offset, data.len());
        Ok(())
    }

    /// Read all uncommitted entries from the journal (for replay).
    ///
    /// Walks from `head` to `tail` parsing entries.
    pub fn read_entries(&self) -> SdResult<Vec<JournalEntry>> {
        let mmap = self.mmap.lock().unwrap();
        let head = Self::read_head(&mmap) as usize;
        let tail = Self::read_tail(&mmap) as usize;

        let mut entries = Vec::new();
        let mut pos = head;

        while pos + ENTRY_HEADER_SIZE <= tail {
            // Read entry magic
            let magic = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
            if magic != ENTRY_MAGIC {
                warn!("journal: bad entry magic at offset {}", pos);
                break;
            }

            let oid_raw = u64::from_le_bytes(mmap[pos + 4..pos + 12].try_into().unwrap());
            let offset = u64::from_le_bytes(mmap[pos + 12..pos + 20].try_into().unwrap());
            let data_len = u32::from_le_bytes(mmap[pos + 20..pos + 24].try_into().unwrap()) as usize;

            if pos + ENTRY_HEADER_SIZE + data_len > tail {
                warn!("journal: truncated entry at offset {}", pos);
                break;
            }

            let data = mmap[pos + 24..pos + 24 + data_len].to_vec();
            entries.push(JournalEntry {
                oid: ObjectId::new(oid_raw),
                offset,
                data,
            });

            pos += ENTRY_HEADER_SIZE + data_len;
        }

        debug!("journal: read {} entries from journal", entries.len());
        Ok(entries)
    }

    /// Replay all journal entries by calling the provided callback.
    ///
    /// After successful replay, the journal is reset (head = tail).
    pub async fn replay<F>(&self, mut apply: F) -> SdResult<usize>
    where
        F: FnMut(JournalEntry) -> SdResult<()>,
    {
        let entries = self.read_entries()?;
        let count = entries.len();

        if count == 0 {
            debug!("journal: nothing to replay");
            return Ok(0);
        }

        info!("journal: replaying {} entries", count);

        for entry in entries {
            apply(entry)?;
        }

        // Reset the journal after successful replay
        self.commit()?;

        info!("journal: replay complete ({} entries)", count);
        Ok(count)
    }

    /// Commit: advance head to match tail, marking all entries as committed.
    ///
    /// This is called after the backing store has been flushed successfully.
    pub fn commit(&self) -> SdResult<()> {
        let mut mmap = self.mmap.lock().unwrap();
        let tail = Self::read_tail(&mmap);
        Self::write_head(&mut mmap, tail);

        mmap.flush().map_err(|e| {
            error!("journal: failed to flush on commit: {}", e);
            SdError::Eio
        })?;

        debug!("journal: committed (head=tail={})", tail);
        Ok(())
    }

    /// Reset the journal, discarding all entries.
    pub fn reset(&self) -> SdResult<()> {
        let mut mmap = self.mmap.lock().unwrap();
        Self::write_head(&mut mmap, HEADER_SIZE as u64);
        Self::write_tail(&mut mmap, HEADER_SIZE as u64);

        mmap.flush().map_err(|e| {
            error!("journal: failed to flush on reset: {}", e);
            SdError::Eio
        })?;

        info!("journal: reset");
        Ok(())
    }

    /// Check if the journal has uncommitted entries.
    pub fn has_uncommitted(&self) -> bool {
        let mmap = self.mmap.lock().unwrap();
        Self::read_head(&mmap) != Self::read_tail(&mmap)
    }

    /// Get the number of bytes used in the journal.
    pub fn used_bytes(&self) -> usize {
        let mmap = self.mmap.lock().unwrap();
        Self::read_tail(&mmap) as usize - HEADER_SIZE
    }

    /// Get the total capacity of the journal in bytes.
    pub fn capacity(&self) -> usize {
        self.size - HEADER_SIZE
    }

    /// Get the journal file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_journal_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sheep_test_journal_{}", name))
    }

    #[test]
    fn test_journal_create_and_open() {
        let path = test_journal_path("create");
        let _ = std::fs::remove_file(&path);

        let journal = Journal::open(&path, 1024 * 1024).unwrap();
        assert!(!journal.has_uncommitted());
        assert_eq!(journal.used_bytes(), 0);

        drop(journal);

        // Re-open should work
        let journal2 = Journal::open(&path, 1024 * 1024).unwrap();
        assert!(!journal2.has_uncommitted());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_journal_append_and_read() {
        let path = test_journal_path("append");
        let _ = std::fs::remove_file(&path);

        let journal = Journal::open(&path, 1024 * 1024).unwrap();

        let oid1 = ObjectId::new(0x42);
        let oid2 = ObjectId::new(0x43);

        journal.append(oid1, 0, b"hello").unwrap();
        journal.append(oid2, 100, b"world").unwrap();

        assert!(journal.has_uncommitted());

        let entries = journal.read_entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].oid, oid1);
        assert_eq!(entries[0].offset, 0);
        assert_eq!(entries[0].data, b"hello");
        assert_eq!(entries[1].oid, oid2);
        assert_eq!(entries[1].offset, 100);
        assert_eq!(entries[1].data, b"world");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_journal_commit() {
        let path = test_journal_path("commit");
        let _ = std::fs::remove_file(&path);

        let journal = Journal::open(&path, 1024 * 1024).unwrap();

        journal.append(ObjectId::new(1), 0, b"data").unwrap();
        assert!(journal.has_uncommitted());

        journal.commit().unwrap();
        assert!(!journal.has_uncommitted());

        // After commit, entries should be empty
        let entries = journal.read_entries().unwrap();
        assert_eq!(entries.len(), 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_journal_reset() {
        let path = test_journal_path("reset");
        let _ = std::fs::remove_file(&path);

        let journal = Journal::open(&path, 1024 * 1024).unwrap();

        journal.append(ObjectId::new(1), 0, b"data").unwrap();
        journal.reset().unwrap();

        assert!(!journal.has_uncommitted());
        assert_eq!(journal.used_bytes(), 0);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_journal_replay() {
        let path = test_journal_path("replay");
        let _ = std::fs::remove_file(&path);

        let journal = Journal::open(&path, 1024 * 1024).unwrap();

        journal.append(ObjectId::new(1), 0, b"first").unwrap();
        journal.append(ObjectId::new(2), 10, b"second").unwrap();

        let mut replayed = Vec::new();
        let count = journal
            .replay(|entry| {
                replayed.push(entry);
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(count, 2);
        assert_eq!(replayed.len(), 2);
        assert!(!journal.has_uncommitted()); // Should be committed after replay

        let _ = std::fs::remove_file(&path);
    }
}
