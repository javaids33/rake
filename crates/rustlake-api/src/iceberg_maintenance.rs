//! Iceberg table maintenance — compaction, snapshot expiry, orphan file cleanup.
//!
//! Operates on `IcebergTableState` to produce new snapshots or prune old ones.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::Serialize;

use crate::iceberg_metadata::{self, IcebergTableState, SnapshotInfo};
use crate::iceberg_writer::DataFileInfo;

/// Status report for a table's maintenance health.
#[derive(Debug, Clone, Serialize)]
pub struct MaintenanceStatus {
    /// Total number of data files across all live snapshots.
    pub total_files: usize,
    /// Average file size in bytes.
    pub avg_file_size_bytes: u64,
    /// Number of files smaller than the target size.
    pub small_file_count: usize,
    /// Fragmentation score: 0.0 (perfect) to 1.0 (highly fragmented).
    pub fragmentation_score: f64,
    /// Number of snapshots.
    pub snapshot_count: usize,
    /// Oldest snapshot timestamp.
    pub oldest_snapshot_ms: Option<i64>,
    /// Recommended actions.
    pub recommendations: Vec<String>,
}

/// Result of a compaction operation.
#[derive(Debug, Clone, Serialize)]
pub struct CompactionResult {
    /// Number of input files that were compacted.
    pub input_files: usize,
    /// Number of output files after compaction.
    pub output_files: usize,
    /// Total rows rewritten.
    pub rows_rewritten: u64,
    /// New metadata path.
    pub metadata_path: String,
}

/// Result of snapshot expiration.
#[derive(Debug, Clone, Serialize)]
pub struct ExpireResult {
    /// Number of snapshots expired.
    pub expired_count: usize,
    /// Snapshot IDs that were removed.
    pub expired_ids: Vec<i64>,
    /// Number of manifest files deleted.
    pub manifests_deleted: usize,
    /// New metadata path.
    pub metadata_path: String,
}

/// Result of orphan file cleanup.
#[derive(Debug, Clone, Serialize)]
pub struct OrphanCleanupResult {
    /// Number of orphan files found.
    pub orphan_files_found: usize,
    /// Number of orphan files deleted.
    pub orphan_files_deleted: usize,
    /// Total bytes reclaimed.
    pub bytes_reclaimed: u64,
}

/// Compute maintenance status for a table.
pub fn compute_maintenance_status(
    state: &IcebergTableState,
    target_file_size_bytes: u64,
) -> MaintenanceStatus {
    // Collect all data files from all snapshots
    let all_files: Vec<&DataFileInfo> = state
        .snapshots
        .iter()
        .flat_map(|s| s.data_files.iter())
        .collect();

    let total_files = all_files.len();
    let total_size: u64 = all_files.iter().map(|f| f.file_size).sum();
    let avg_file_size = if total_files > 0 {
        total_size / total_files as u64
    } else {
        0
    };
    let small_file_count = all_files
        .iter()
        .filter(|f| f.file_size < target_file_size_bytes / 2)
        .count();

    let fragmentation_score = if total_files > 0 {
        (small_file_count as f64 / total_files as f64).min(1.0)
    } else {
        0.0
    };

    let oldest_snapshot_ms = state.snapshots.iter().map(|s| s.timestamp_ms).min();

    let mut recommendations = Vec::new();
    if fragmentation_score > 0.5 {
        recommendations.push("High fragmentation — run compaction to merge small files".to_string());
    }
    if state.snapshots.len() > 100 {
        recommendations.push(format!(
            "{} snapshots — consider expiring old snapshots",
            state.snapshots.len()
        ));
    }
    if small_file_count > 10 {
        recommendations.push(format!(
            "{} files below target size — compaction recommended",
            small_file_count
        ));
    }

    MaintenanceStatus {
        total_files,
        avg_file_size_bytes: avg_file_size,
        small_file_count,
        fragmentation_score,
        snapshot_count: state.snapshots.len(),
        oldest_snapshot_ms,
        recommendations,
    }
}

/// Compact small Parquet files into larger ones.
///
/// Reads all data files from the current snapshot, merges them into files of
/// approximately `target_file_size_mb` MB, writes new files, and creates a new
/// "replace" snapshot.
pub async fn compact_table(
    store: &Arc<dyn ObjectStore>,
    state: &IcebergTableState,
    target_file_size_mb: u64,
    schema: &arrow::datatypes::Schema,
) -> Result<CompactionResult, String> {
    let target_bytes = target_file_size_mb * 1024 * 1024;

    // Collect all unique data file paths from all snapshots
    let mut all_file_paths: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for snap in &state.snapshots {
        for df in &snap.data_files {
            if seen.insert(df.file_path.clone()) {
                all_file_paths.push(df.file_path.clone());
            }
        }
    }

    if all_file_paths.is_empty() {
        return Err("No data files to compact".into());
    }

    let input_files = all_file_paths.len();

    // Read all files and collect batches
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut total_rows: u64 = 0;

    for file_path in &all_file_paths {
        let data = store
            .get(&ObjectPath::from(file_path.as_str()))
            .await
            .map_err(|e| format!("Failed to read {}: {}", file_path, e))?
            .bytes()
            .await
            .map_err(|e| format!("Failed to read bytes from {}: {}", file_path, e))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(data)
            .map_err(|e| format!("Parquet reader error for {}: {}", file_path, e))?
            .build()
            .map_err(|e| format!("Parquet build error for {}: {}", file_path, e))?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("Batch read error: {}", e))?;
            total_rows += batch.num_rows() as u64;
            all_batches.push(batch);
        }
    }

    if all_batches.is_empty() {
        return Err("No data found in files to compact".into());
    }

    // Write compacted files
    let now = chrono::Utc::now();
    let date_part = now.format("%Y-%m-%d").to_string();
    let prefix = iceberg_metadata::strip_s3_prefix_pub(&state.location);
    let mut new_data_files: Vec<DataFileInfo> = Vec::new();
    let mut current_buf: Vec<RecordBatch> = Vec::new();
    let mut current_rows: u64 = 0;
    let mut file_counter = 0u32;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .set_bloom_filter_enabled(true)
        .build();

    for batch in all_batches {
        let batch_rows = batch.num_rows() as u64;
        current_buf.push(batch);
        current_rows += batch_rows;

        // Estimate: if accumulated rows would produce a file near target size, flush
        let estimated_size = current_rows * 100; // rough estimate: 100 bytes/row avg
        if estimated_size >= target_bytes {
            file_counter += 1;
            let df = write_compacted_file(
                store,
                &prefix,
                &date_part,
                file_counter,
                &current_buf,
                schema,
                &props,
            )
            .await?;
            new_data_files.push(df);
            current_buf.clear();
            current_rows = 0;
        }
    }

    // Flush remaining
    if !current_buf.is_empty() {
        file_counter += 1;
        let df = write_compacted_file(
            store,
            &prefix,
            &date_part,
            file_counter,
            &current_buf,
            schema,
            &props,
        )
        .await?;
        new_data_files.push(df);
    }

    // Create a new "replace" snapshot
    let metadata_path =
        iceberg_metadata::append_snapshot(store, state, &new_data_files, schema, "replace").await?;

    Ok(CompactionResult {
        input_files,
        output_files: new_data_files.len(),
        rows_rewritten: total_rows,
        metadata_path,
    })
}

async fn write_compacted_file(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    date_part: &str,
    file_counter: u32,
    batches: &[RecordBatch],
    schema: &arrow::datatypes::Schema,
    props: &WriterProperties,
) -> Result<DataFileInfo, String> {
    let mut parquet_buf = Vec::new();
    let mut row_count = 0u64;
    {
        let mut writer =
            ArrowWriter::try_new(&mut parquet_buf, Arc::new(schema.clone()), Some(props.clone()))
                .map_err(|e| format!("Parquet writer init: {}", e))?;
        for batch in batches {
            row_count += batch.num_rows() as u64;
            writer
                .write(batch)
                .map_err(|e| format!("Parquet write: {}", e))?;
        }
        writer
            .close()
            .map_err(|e| format!("Parquet close: {}", e))?;
    }

    let file_size = parquet_buf.len() as u64;
    let file_name = format!(
        "{}/{}/compacted-{:04}.parquet",
        prefix, date_part, file_counter
    );

    store
        .put(
            &ObjectPath::from(file_name.as_str()),
            object_store::PutPayload::from(parquet_buf),
        )
        .await
        .map_err(|e| format!("S3 PUT compacted file: {}", e))?;

    Ok(DataFileInfo {
        file_path: file_name,
        file_size,
        row_count,
    })
}

/// Expire old snapshots, keeping at least `retain_last_n` and removing those
/// older than `older_than_ms`.
pub async fn expire_snapshots(
    store: &Arc<dyn ObjectStore>,
    state: &IcebergTableState,
    older_than_ms: i64,
    retain_last_n: usize,
) -> Result<ExpireResult, String> {
    if state.snapshots.len() <= retain_last_n {
        return Ok(ExpireResult {
            expired_count: 0,
            expired_ids: vec![],
            manifests_deleted: 0,
            metadata_path: String::new(),
        });
    }

    // Sort snapshots by timestamp, keep the last N, expire the rest if old enough
    let mut sorted_snaps: Vec<&SnapshotInfo> = state.snapshots.iter().collect();
    sorted_snaps.sort_by_key(|s| s.timestamp_ms);

    let cutoff_index = if sorted_snaps.len() > retain_last_n {
        sorted_snaps.len() - retain_last_n
    } else {
        0
    };

    let mut expired_ids = Vec::new();
    let mut manifests_deleted = 0usize;

    for snap in &sorted_snaps[..cutoff_index] {
        if snap.timestamp_ms < older_than_ms {
            expired_ids.push(snap.snapshot_id);

            // Try to delete the manifest list file
            let manifest_path = snap
                .manifest_list_path
                .strip_prefix(&state.location)
                .unwrap_or(&snap.manifest_list_path);
            let manifest_key =
                iceberg_metadata::strip_s3_prefix_pub(&format!("{}{}", state.location, manifest_path));
            if store
                .delete(&ObjectPath::from(manifest_key.as_str()))
                .await
                .is_ok()
            {
                manifests_deleted += 1;
            }
        }
    }

    if expired_ids.is_empty() {
        return Ok(ExpireResult {
            expired_count: 0,
            expired_ids: vec![],
            manifests_deleted: 0,
            metadata_path: String::new(),
        });
    }

    // Write new metadata without expired snapshots
    let now_ms = chrono::Utc::now().timestamp_millis();
    let new_version = state.metadata_version + 1;
    let mut new_metadata = state.metadata.clone();

    if let Some(snaps) = new_metadata.get_mut("snapshots").and_then(|v| v.as_array_mut()) {
        snaps.retain(|s| {
            let sid = s.get("snapshot-id").and_then(|v| v.as_i64()).unwrap_or(0);
            !expired_ids.contains(&sid)
        });
    }

    if let Some(log) = new_metadata.get_mut("snapshot-log").and_then(|v| v.as_array_mut()) {
        log.retain(|entry| {
            let sid = entry
                .get("snapshot-id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            !expired_ids.contains(&sid)
        });
    }

    new_metadata["last-updated-ms"] = serde_json::json!(now_ms);

    if let Some(mlog) = new_metadata.get_mut("metadata-log").and_then(|v| v.as_array_mut()) {
        mlog.push(serde_json::json!({
            "timestamp-ms": state.metadata.get("last-updated-ms").and_then(|v| v.as_i64()).unwrap_or(now_ms),
            "metadata-file": format!("{}/metadata/v{}.metadata.json", state.location, state.metadata_version),
        }));
    }

    let metadata_json = serde_json::to_string_pretty(&new_metadata)
        .map_err(|e| format!("Metadata JSON: {}", e))?;
    let prefix = iceberg_metadata::strip_s3_prefix_pub(&state.location);
    let metadata_path = format!("{}/metadata/v{}.metadata.json", prefix, new_version);
    store
        .put(
            &ObjectPath::from(metadata_path.as_str()),
            object_store::PutPayload::from(metadata_json.as_bytes().to_vec()),
        )
        .await
        .map_err(|e| format!("S3 PUT metadata: {}", e))?;

    tracing::info!(
        table = %state.table_uuid,
        expired = expired_ids.len(),
        manifests_deleted = manifests_deleted,
        "Snapshots expired (v{})",
        new_version
    );

    Ok(ExpireResult {
        expired_count: expired_ids.len(),
        expired_ids,
        manifests_deleted,
        metadata_path,
    })
}

/// Remove orphan files that are not referenced by any snapshot's manifests.
pub async fn remove_orphan_files(
    store: &Arc<dyn ObjectStore>,
    state: &IcebergTableState,
    data_prefix: &str,
) -> Result<OrphanCleanupResult, String> {
    // Collect all referenced file paths from all snapshots
    let mut referenced: HashSet<String> = HashSet::new();
    for snap in &state.snapshots {
        for df in &snap.data_files {
            referenced.insert(df.file_path.clone());
        }
        // Also keep manifest list files
        if !snap.manifest_list_path.is_empty() {
            referenced.insert(snap.manifest_list_path.clone());
        }
    }

    // List all files under the data prefix
    let list = store
        .list(Some(&ObjectPath::from(data_prefix)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| format!("Failed to list files under {}: {}", data_prefix, e))?;

    let mut orphan_files_found = 0usize;
    let mut orphan_files_deleted = 0usize;
    let mut bytes_reclaimed = 0u64;

    for item in &list {
        let path_str = item.location.to_string();

        // Skip metadata files
        if path_str.contains("/metadata/") {
            continue;
        }

        // Only consider .parquet files
        if !path_str.ends_with(".parquet") {
            continue;
        }

        // Check if this file is referenced
        if !referenced.contains(&path_str) {
            orphan_files_found += 1;
            let size = item.size;

            if store.delete(&item.location).await.is_ok() {
                orphan_files_deleted += 1;
                bytes_reclaimed += size as u64;
            }
        }
    }

    tracing::info!(
        table = %state.table_uuid,
        found = orphan_files_found,
        deleted = orphan_files_deleted,
        bytes = bytes_reclaimed,
        "Orphan files cleaned up"
    );

    Ok(OrphanCleanupResult {
        orphan_files_found,
        orphan_files_deleted,
        bytes_reclaimed,
    })
}
