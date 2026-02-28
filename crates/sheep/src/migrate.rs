//! Data migration utilities.
//!
//! Provides helpers for moving objects between store backends, used during
//! store backend migration (e.g., switching from "plain" to "tree" layout).
//!
//! Migration is performed object-by-object, reading from the source store
//! and writing to the destination store. Progress is reported via a callback.

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::oid::ObjectId;
use tracing::{debug, info, warn};

use crate::store::StoreDriver;

/// Progress information for a migration operation.
#[derive(Debug, Clone)]
pub struct MigrationProgress {
    /// Total number of objects to migrate.
    pub total: u64,
    /// Number of objects successfully migrated.
    pub migrated: u64,
    /// Number of objects that failed to migrate.
    pub failed: u64,
    /// Number of objects skipped (already exist in destination).
    pub skipped: u64,
}

impl MigrationProgress {
    fn new(total: u64) -> Self {
        Self {
            total,
            migrated: 0,
            failed: 0,
            skipped: 0,
        }
    }

    /// Percentage complete.
    pub fn percent(&self) -> f64 {
        if self.total == 0 {
            return 100.0;
        }
        ((self.migrated + self.skipped + self.failed) as f64 / self.total as f64) * 100.0
    }
}

/// Migrate all objects from one store driver to another.
///
/// Reads each object from `src` and writes it to `dst`. Objects that
/// already exist in `dst` are skipped. The `on_progress` callback is
/// invoked after each object for progress reporting.
///
/// Returns the final migration progress.
pub async fn migrate_store<F>(
    src: &dyn StoreDriver,
    dst: &dyn StoreDriver,
    mut on_progress: F,
) -> SdResult<MigrationProgress>
where
    F: FnMut(&MigrationProgress),
{
    info!(
        "migrate: starting migration from '{}' to '{}'",
        src.name(),
        dst.name()
    );

    // Get all object IDs from source
    let oids = src.get_obj_list().await?;
    let total = oids.len() as u64;
    let mut progress = MigrationProgress::new(total);

    info!("migrate: {} objects to process", total);

    for oid in oids {
        // Check if already exists in destination (ec_index=0)
        if dst.exist(oid, 0).await {
            progress.skipped += 1;
            debug!("migrate: skipped {} (already exists)", oid);
            on_progress(&progress);
            continue;
        }

        // Read the full object from source
        let obj_size = oid.obj_size() as usize;
        match src.read(oid, 0, 0, obj_size).await {
            Ok(data) => {
                // Write to destination
                match dst.create_and_write(oid, 0, &data).await {
                    Ok(()) => {
                        progress.migrated += 1;
                        debug!("migrate: copied {} ({} bytes)", oid, data.len());
                    }
                    Err(SdError::OidExist) => {
                        progress.skipped += 1;
                        debug!("migrate: skipped {} (race: already exists)", oid);
                    }
                    Err(e) => {
                        progress.failed += 1;
                        warn!("migrate: failed to write {} to dst: {}", oid, e);
                    }
                }
            }
            Err(e) => {
                progress.failed += 1;
                warn!("migrate: failed to read {} from src: {}", oid, e);
            }
        }

        on_progress(&progress);
    }

    info!(
        "migrate: complete (migrated={}, skipped={}, failed={})",
        progress.migrated, progress.skipped, progress.failed
    );

    if progress.failed > 0 {
        warn!("migrate: {} objects failed to migrate", progress.failed);
    }

    Ok(progress)
}

/// Migrate a single object from source to destination.
///
/// Returns Ok(true) if migrated, Ok(false) if skipped.
pub async fn migrate_object(
    src: &dyn StoreDriver,
    dst: &dyn StoreDriver,
    oid: ObjectId,
    ec_index: u8,
) -> SdResult<bool> {
    if dst.exist(oid, ec_index).await {
        return Ok(false);
    }

    let obj_size = oid.obj_size() as usize;
    let data = src.read(oid, ec_index, 0, obj_size).await?;

    match dst.create_and_write(oid, ec_index, &data).await {
        Ok(()) => Ok(true),
        Err(SdError::OidExist) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Verify that all objects in source exist in destination.
///
/// Returns a list of OIDs missing from the destination.
pub async fn verify_migration(
    src: &dyn StoreDriver,
    dst: &dyn StoreDriver,
) -> SdResult<Vec<ObjectId>> {
    let src_oids = src.get_obj_list().await?;
    let mut missing = Vec::new();

    for oid in src_oids {
        if !dst.exist(oid, 0).await {
            missing.push(oid);
        }
    }

    if missing.is_empty() {
        info!("migrate: verification passed");
    } else {
        warn!("migrate: {} objects missing from destination", missing.len());
    }

    Ok(missing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_progress() {
        let mut p = MigrationProgress::new(100);
        assert_eq!(p.percent(), 0.0);

        p.migrated = 50;
        p.skipped = 20;
        p.failed = 5;
        assert!((p.percent() - 75.0).abs() < 0.01);
    }

    #[test]
    fn test_migration_progress_empty() {
        let p = MigrationProgress::new(0);
        assert_eq!(p.percent(), 100.0);
    }
}
