//! S3-based table format discovery — agnostic scanner for any bucket.
//!
//! Scans S3 warehouse paths to discover tables in any format:
//! - **Iceberg**: `metadata/v*.metadata.json` (Trino, Spark, Flink, Databricks)
//! - **Delta Lake**: `_delta_log/*.json` (Databricks, Spark)
//! - **Hudi**: `.hoodie/hoodie.properties` (Apache Hudi)
//! - **Raw Parquet**: directories containing `*.parquet` files (no metadata)
//!
//! Handles both hierarchical warehouses (db.db/table/) and flat layouts (table/).
//!
//! Performance: S3 ListObjectsV2 ~50ms per 1000 objects.
//! For 500 tables → ~5-10s with 16 concurrent S3 operations.

use futures::stream::{self, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Concurrent S3 operations for directory listing (phase 1-2).
const DISCOVERY_DIR_PARALLELISM: usize = 16;
/// Concurrent S3 operations for metadata reads (phase 3).
const DISCOVERY_META_PARALLELISM: usize = 24;
/// Maximum directory depth to recurse when scanning for tables.
/// Trino/Hive warehouses can be: schema/namespace/table-uuid/ (3 levels),
/// and some have even deeper layouts.
const MAX_SCAN_DEPTH: usize = 5;

/// Table format detected from S3 directory layout.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TableFormat {
    Iceberg,
    Delta,
    Hudi,
    Parquet,
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableFormat::Iceberg => write!(f, "iceberg"),
            TableFormat::Delta => write!(f, "delta"),
            TableFormat::Hudi => write!(f, "hudi"),
            TableFormat::Parquet => write!(f, "parquet"),
        }
    }
}

/// A discovered table from S3 scanning (any format).
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveredTable {
    pub database: String,
    pub table_name: String,
    pub s3_location: String,
    pub format: TableFormat,
    pub metadata_location: Option<String>,
    pub column_count: usize,
    pub columns: Vec<ColumnInfo>,
    pub format_version: Option<i64>,
    pub partition_fields: Vec<String>,
    pub snapshot_count: usize,
    pub last_updated_ms: Option<i64>,
    /// Table type from metadata properties (e.g., "MATERIALIZED_VIEW", "VIEW").
    #[serde(default)]
    pub table_type: String,
    /// Arbitrary properties from table metadata.
    #[serde(default)]
    pub properties: std::collections::HashMap<String, String>,
}

// Keep the old name as an alias for backward compatibility in routes.rs
pub type IcebergTableInfo = DiscoveredTable;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub ordinal: i32,
}

// Keep old name for backward compat
pub type IcebergColumnInfo = ColumnInfo;

/// Scan progress event emitted during discovery.
#[derive(Debug, Clone, Serialize)]
pub struct ScanProgress {
    pub phase: String,
    pub detail: String,
    pub databases_found: usize,
    pub tables_found: usize,
    pub tables_scanned: usize,
    pub total_to_scan: usize,
    pub elapsed_ms: u128,
    /// Formats found so far.
    pub formats: std::collections::HashMap<String, usize>,
}

/// Result of scanning a warehouse.
#[derive(Debug, Clone, Serialize)]
pub struct WarehouseScanResult {
    pub warehouse_path: String,
    pub databases: Vec<String>,
    pub tables: Vec<DiscoveredTable>,
    pub total_tables: usize,
    pub scan_duration_ms: u128,
    /// Breakdown by format.
    pub format_counts: std::collections::HashMap<String, usize>,
}

/// Build an S3 ObjectStore from credentials.
pub fn build_s3_store(
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    endpoint: Option<&str>,
) -> Result<Arc<dyn ObjectStore>, String> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_allow_http(true);
    if let Some(ep) = endpoint {
        if !ep.is_empty() {
            builder = builder.with_endpoint(ep);
        }
    }
    let store = builder.build()
        .map_err(|e| format!("Failed to build S3 store for bucket '{}': {}", bucket, e))?;
    Ok(Arc::new(store))
}

/// Scan an S3 warehouse path to discover tables in all supported formats.
///
/// Sends progress updates via the optional `progress_tx` channel.
/// The caller can use this to emit SSE events to the frontend.
pub async fn scan_warehouse(
    store: &Arc<dyn ObjectStore>,
    warehouse_prefix: &str,
) -> Result<WarehouseScanResult, String> {
    scan_warehouse_with_progress(store, warehouse_prefix, None).await
}

/// Scan with progress reporting. Pass a channel sender to receive live updates.
pub async fn scan_warehouse_with_progress(
    store: &Arc<dyn ObjectStore>,
    warehouse_prefix: &str,
    progress_tx: Option<mpsc::UnboundedSender<ScanProgress>>,
) -> Result<WarehouseScanResult, String> {
    let start = std::time::Instant::now();
    let prefix = normalize_prefix(warehouse_prefix);

    let emit = |phase: &str, detail: &str, dbs: usize, found: usize, scanned: usize, total: usize, formats: &std::collections::HashMap<String, usize>| {
        if let Some(ref tx) = progress_tx {
            let _ = tx.send(ScanProgress {
                phase: phase.to_string(),
                detail: detail.to_string(),
                databases_found: dbs,
                tables_found: found,
                tables_scanned: scanned,
                total_to_scan: total,
                elapsed_ms: start.elapsed().as_millis(),
                formats: formats.clone(),
            });
        }
    };

    let empty_formats: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    // ── Phase 1: Discover top-level directories ──────────────────────
    emit("listing", "Listing bucket directories...", 0, 0, 0, 0, &empty_formats);

    let top_level_dirs = list_directories(store, &prefix).await?;
    let top_level_files = list_files_with_dirs(store, &prefix).await?;

    tracing::info!(count = top_level_dirs.len(), files = top_level_files.files.len(), prefix = %prefix, "S3 scan: discovered top-level entries");

    // Determine structure: is this a hierarchical warehouse (db/table/) or flat (table/)?
    // Check if top-level dirs contain metadata/ or _delta_log/ — if so, it's flat layout.
    // We probe the first few dirs in parallel to decide.
    let mut databases: Vec<String> = Vec::new();
    let mut flat_tables: Vec<(String, String)> = Vec::new(); // (dir_name, path)

    if top_level_dirs.is_empty() {
        // Bucket root has no subdirs — check for parquet files at root
        emit("complete", "Empty bucket — no tables found", 0, 0, 0, 0, &empty_formats);
        return Ok(WarehouseScanResult {
            warehouse_path: prefix,
            databases: vec![],
            tables: vec![],
            total_tables: 0,
            scan_duration_ms: start.elapsed().as_millis(),
            format_counts: empty_formats,
        });
    }

    emit("probing", &format!("Probing {} directories for table formats...", top_level_dirs.len()), 0, 0, 0, top_level_dirs.len(), &empty_formats);

    // Probe top-level dirs to detect layout
    let probe_results: Vec<(String, DirProbe)> = stream::iter(top_level_dirs.iter().cloned())
        .map(|dir| {
            let store = store.clone();
            let prefix = prefix.clone();
            async move {
                let dir_path = format!("{}{}/", prefix, dir);
                let probe = probe_directory(&store, &dir_path).await;
                (dir, probe)
            }
        })
        .buffer_unordered(DISCOVERY_DIR_PARALLELISM)
        .collect()
        .await;

    for (dir, probe) in &probe_results {
        if probe.is_table {
            // This top-level dir IS a table (flat layout)
            flat_tables.push((dir.clone(), format!("{}{}/", prefix, dir)));
        } else if probe.has_subdirs {
            // This dir has subdirs — treat as a database
            databases.push(dir.clone());
        }
        // Dirs with neither metadata nor subdirs are ignored (e.g., _spark_metadata, logs)
    }

    emit(
        "discovering",
        &format!("{} databases, {} top-level tables found", databases.len(), flat_tables.len()),
        databases.len(),
        flat_tables.len(),
        0,
        0,
        &empty_formats,
    );

    // ── Phase 2: Recursively discover table dirs within each database ─
    // Trino/Hive warehouses can have deep hierarchies:
    //   catalog/schema/table-uuid/metadata/
    // We recurse into non-table directories up to MAX_SCAN_DEPTH.
    //
    // The "database" label composes intermediate path segments with underscores:
    //   consumer/app_104387_rtef/mars-{uuid}/ → database = "app_104387_rtef", table = "mars"
    //   (consumer is treated as catalog, app_104387_rtef as schema/database)
    //   If only 2-level: sales.db/orders/ → database = "sales", table = "orders"

    let mut all_table_paths: Vec<(String, String, String)> = Vec::new(); // (db, table, path)

    // Add flat (top-level) tables
    for (table_name, path) in &flat_tables {
        all_table_paths.push(("default".to_string(), table_name.clone(), path.clone()));
    }

    // Recursively explore databases to find table directories
    if !databases.is_empty() {
        let mut dirs_to_explore: Vec<(Vec<String>, String)> = Vec::new(); // (path_segments, full_path)
        for db_name in &databases {
            dirs_to_explore.push((
                vec![db_name.clone()],
                format!("{}{}/", prefix, db_name),
            ));
        }

        // BFS-style recursive exploration: each level checks subdirs for tables vs deeper hierarchy
        let mut depth = 1;
        while !dirs_to_explore.is_empty() && depth < MAX_SCAN_DEPTH {
            let explore_results: Vec<(Vec<String>, String, Vec<String>, Vec<String>)> =
                stream::iter(dirs_to_explore.into_iter())
                    .map(|(segments, dir_path)| {
                        let store = store.clone();
                        async move {
                            let subdirs = list_directories(&store, &dir_path).await.unwrap_or_default();
                            // Probe each subdir to see if it's a table or needs deeper traversal
                            let mut table_names = Vec::new();
                            let mut deeper_dirs = Vec::new();
                            let probes: Vec<(String, DirProbe)> = stream::iter(subdirs.into_iter())
                                .map(|subdir| {
                                    let store = store.clone();
                                    let dp = dir_path.clone();
                                    async move {
                                        let sub_path = format!("{}{}/", dp, subdir);
                                        let probe = probe_directory(&store, &sub_path).await;
                                        (subdir, probe)
                                    }
                                })
                                .buffer_unordered(DISCOVERY_DIR_PARALLELISM)
                                .collect()
                                .await;
                            for (subdir, probe) in probes {
                                if probe.is_table {
                                    table_names.push(subdir);
                                } else if probe.has_subdirs {
                                    deeper_dirs.push(subdir);
                                }
                            }
                            (segments, dir_path, table_names, deeper_dirs)
                        }
                    })
                    .buffer_unordered(DISCOVERY_DIR_PARALLELISM)
                    .collect()
                    .await;

            let mut next_explore: Vec<(Vec<String>, String)> = Vec::new();
            for (segments, dir_path, table_names, deeper_dirs) in explore_results {
                // For tables found here: compose database name from path segments
                // If segments = ["consumer", "app_104387_rtef"], database = "app_104387_rtef"
                //   (skip the catalog-level prefix, use the deepest non-table segment)
                // If segments = ["sales.db"], database = "sales" (strip .db suffix)
                let db_name = compose_database_name(&segments);

                for table_dir in table_names {
                    let table_path = format!("{}{}/", dir_path, table_dir);
                    // Strip UUID suffix from table dir name
                    let clean_name = strip_uuid_suffix(&table_dir);
                    all_table_paths.push((db_name.clone(), clean_name, table_path));
                }

                // Queue deeper dirs for next iteration
                for deeper in deeper_dirs {
                    let mut new_segments = segments.clone();
                    new_segments.push(deeper.clone());
                    next_explore.push((
                        new_segments,
                        format!("{}{}/", dir_path, deeper),
                    ));
                }
            }

            dirs_to_explore = next_explore;
            depth += 1;
        }
    }

    let total_to_scan = all_table_paths.len();
    emit(
        "scanning",
        &format!("Scanning {} directories for table metadata...", total_to_scan),
        databases.len(),
        0,
        0,
        total_to_scan,
        &empty_formats,
    );

    tracing::info!(
        databases = databases.len(),
        flat_tables = flat_tables.len(),
        total_dirs = total_to_scan,
        "S3 scan: starting multi-format table discovery"
    );

    // ── Phase 3: Discover tables in all formats (high parallelism) ───
    // We use a counter + progress sender to report real-time progress.
    let scanned_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let found_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let format_counts = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::<String, usize>::new()));

    let table_infos: Vec<Option<DiscoveredTable>> = stream::iter(all_table_paths)
        .map(|(db, table, path)| {
            let store = store.clone();
            let scanned = scanned_counter.clone();
            let found = found_counter.clone();
            let fmt_counts = format_counts.clone();
            let ptx = progress_tx.clone();
            let total = total_to_scan;
            let db_count = databases.len();
            let start_time = start;
            async move {
                let result = discover_table(&store, &db, &table, &path).await;

                let s = scanned.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if let Some(ref tbl) = result {
                    found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mut fc = fmt_counts.lock().await;
                    *fc.entry(tbl.format.to_string()).or_insert(0) += 1;

                    // Emit progress every table found, or every 10 scanned
                    if let Some(ref tx) = ptx {
                        let f = found.load(std::sync::atomic::Ordering::Relaxed);
                        let _ = tx.send(ScanProgress {
                            phase: "scanning".to_string(),
                            detail: format!("Found {} ({}.{})", tbl.format, db, table),
                            databases_found: db_count,
                            tables_found: f,
                            tables_scanned: s,
                            total_to_scan: total,
                            elapsed_ms: start_time.elapsed().as_millis(),
                            formats: fc.clone(),
                        });
                    }
                } else if s % 10 == 0 || s == total {
                    // Progress update even for non-tables (every 10)
                    if let Some(ref tx) = ptx {
                        let f = found.load(std::sync::atomic::Ordering::Relaxed);
                        let fc = fmt_counts.lock().await;
                        let _ = tx.send(ScanProgress {
                            phase: "scanning".to_string(),
                            detail: format!("Scanned {}/{} directories...", s, total),
                            databases_found: db_count,
                            tables_found: f,
                            tables_scanned: s,
                            total_to_scan: total,
                            elapsed_ms: start_time.elapsed().as_millis(),
                            formats: fc.clone(),
                        });
                    }
                }

                result
            }
        })
        .buffer_unordered(DISCOVERY_META_PARALLELISM)
        .collect()
        .await;

    let tables: Vec<DiscoveredTable> = table_infos.into_iter().flatten().collect();
    let total_tables = tables.len();

    let final_format_counts = {
        let guard = format_counts.lock().await;
        guard.clone()
    };

    // Collect all unique database names from discovered tables
    let mut db_names: Vec<String> = tables.iter()
        .map(|t| t.database.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    db_names.sort();

    let duration = start.elapsed().as_millis();
    tracing::info!(
        tables = total_tables,
        databases = db_names.len(),
        elapsed_ms = duration,
        "S3 multi-format scan complete"
    );

    emit(
        "complete",
        &format!("Done! {} tables in {}ms", total_tables, duration),
        db_names.len(),
        total_tables,
        total_to_scan,
        total_to_scan,
        &final_format_counts,
    );

    Ok(WarehouseScanResult {
        warehouse_path: prefix,
        databases: db_names,
        tables,
        total_tables,
        scan_duration_ms: duration,
        format_counts: final_format_counts,
    })
}

/// Result of probing a directory to determine if it's a table or database.
#[derive(Debug)]
struct DirProbe {
    is_table: bool,
    has_subdirs: bool,
}

/// Quick probe: check if a directory contains table metadata markers or subdirectories.
async fn probe_directory(store: &Arc<dyn ObjectStore>, dir_path: &str) -> DirProbe {
    // ListWithDelimiter gives us immediate children (files + subdirs)
    let obj_prefix = ObjectPath::from(dir_path);
    let result = match store.list_with_delimiter(Some(&obj_prefix)).await {
        Ok(r) => r,
        Err(_) => return DirProbe { is_table: false, has_subdirs: false },
    };

    let subdir_names: Vec<String> = result.common_prefixes.iter()
        .filter_map(|p| {
            let s = p.to_string();
            let trimmed = s.trim_end_matches('/');
            trimmed.rsplit('/').next().map(|n| n.to_string())
        })
        .collect();

    // Check for format markers in subdirectory names
    let has_metadata = subdir_names.iter().any(|d| d == "metadata");
    let has_delta_log = subdir_names.iter().any(|d| d == "_delta_log");
    let has_hoodie = subdir_names.iter().any(|d| d == ".hoodie");
    let has_data_dir = subdir_names.iter().any(|d| d == "data");

    // Check file names for parquet/orc at this level
    let file_names: Vec<String> = result.objects.iter()
        .filter_map(|o| {
            let s = o.location.to_string();
            s.rsplit('/').next().map(|n| n.to_string())
        })
        .collect();
    let has_parquet_files = file_names.iter().any(|f| f.ends_with(".parquet") || f.ends_with(".snappy.parquet"));

    let is_table = has_metadata || has_delta_log || has_hoodie || has_parquet_files || (has_data_dir && !has_metadata);
    let has_subdirs = !subdir_names.is_empty();

    DirProbe { is_table, has_subdirs }
}

/// Discover a single table from its S3 path — tries all formats.
async fn discover_table(
    store: &Arc<dyn ObjectStore>,
    database: &str,
    table_name: &str,
    table_path: &str,
) -> Option<DiscoveredTable> {
    // Try formats in priority order: Iceberg → Delta → Hudi → raw Parquet
    if let Some(t) = discover_iceberg_table(store, database, table_name, table_path).await {
        return Some(t);
    }
    if let Some(t) = discover_delta_table(store, database, table_name, table_path).await {
        return Some(t);
    }
    if let Some(t) = discover_hudi_table(store, database, table_name, table_path).await {
        return Some(t);
    }
    if let Some(t) = discover_parquet_dir(store, database, table_name, table_path).await {
        return Some(t);
    }
    None
}

// ── Iceberg Discovery ──────────────────────────────────────────────

/// Discover an Iceberg table from metadata/v*.metadata.json.
async fn discover_iceberg_table(
    store: &Arc<dyn ObjectStore>,
    database: &str,
    table_name: &str,
    table_path: &str,
) -> Option<DiscoveredTable> {
    let metadata_prefix = format!("{}metadata/", table_path);
    let meta_files = list_files(store, &metadata_prefix).await.ok()?;

    if meta_files.is_empty() {
        return None;
    }

    let metadata_file = find_latest_metadata(&meta_files)?;
    let metadata_path = format!("{}{}", metadata_prefix, metadata_file);

    let metadata = read_json_file(store, &metadata_path).await.ok()?;

    let (columns, format_version) = parse_iceberg_schema(&metadata);
    let partition_fields = parse_partition_spec(&metadata);
    let snapshot_count = metadata.get("snapshots")
        .and_then(|s| s.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let last_updated = metadata.get("last-updated-ms")
        .and_then(|v| v.as_i64());

    let properties = parse_iceberg_properties(&metadata);
    let table_type = properties.get("table_type")
        .or_else(|| properties.get("table-type"))
        .cloned()
        .unwrap_or_default();

    Some(DiscoveredTable {
        database: database.to_string(),
        table_name: table_name.to_string(),
        s3_location: table_path.trim_end_matches('/').to_string(),
        format: TableFormat::Iceberg,
        metadata_location: Some(metadata_path),
        column_count: columns.len(),
        columns,
        format_version: Some(format_version),
        partition_fields,
        snapshot_count,
        last_updated_ms: last_updated,
        table_type,
        properties,
    })
}

// ── Delta Lake Discovery ───────────────────────────────────────────

/// Discover a Delta Lake table from `_delta_log/` directory.
///
/// Delta tables store their metadata in `_delta_log/*.json` files.
/// The latest JSON commit file contains the schema and partition info.
async fn discover_delta_table(
    store: &Arc<dyn ObjectStore>,
    database: &str,
    table_name: &str,
    table_path: &str,
) -> Option<DiscoveredTable> {
    let delta_prefix = format!("{}_delta_log/", table_path);
    let files = list_files(store, &delta_prefix).await.ok()?;

    if files.is_empty() {
        return None;
    }

    // Find the latest commit JSON (highest numbered: 00000000000000000000.json)
    let mut json_files: Vec<&str> = files.iter()
        .filter(|f| f.ends_with(".json"))
        .map(|s| s.as_str())
        .collect();
    json_files.sort();

    let latest_commit = json_files.last()?;
    let commit_path = format!("{}{}", delta_prefix, latest_commit);

    let commit_data = read_json_lines(store, &commit_path).await.ok()?;

    // Parse Delta commit log — it's newline-delimited JSON
    // Look for "metaData" action which contains schema
    let mut columns = Vec::new();
    let mut partition_cols: Vec<String> = Vec::new();
    let mut properties = std::collections::HashMap::new();
    let mut format_version: i64 = 1;

    for entry in &commit_data {
        if let Some(protocol) = entry.get("protocol") {
            format_version = protocol.get("minReaderVersion")
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
        }
        if let Some(meta) = entry.get("metaData") {
            // Schema is a JSON string inside the metaData
            if let Some(schema_str) = meta.get("schemaString").and_then(|s| s.as_str()) {
                if let Ok(schema_json) = serde_json::from_str::<serde_json::Value>(schema_str) {
                    if let Some(fields) = schema_json.get("fields").and_then(|f| f.as_array()) {
                        for (i, field) in fields.iter().enumerate() {
                            let name = field.get("name").and_then(|n| n.as_str()).unwrap_or("unknown").to_string();
                            let dtype = delta_type_to_string(field.get("type"));
                            let nullable = field.get("nullable").and_then(|n| n.as_bool()).unwrap_or(true);
                            columns.push(ColumnInfo { name, data_type: dtype, nullable, ordinal: i as i32 });
                        }
                    }
                }
            }
            // Partition columns
            if let Some(parts) = meta.get("partitionColumns").and_then(|p| p.as_array()) {
                partition_cols = parts.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
            }
            // Properties (description, delta.enableChangeDataFeed, etc.)
            if let Some(props) = meta.get("configuration").and_then(|c| c.as_object()) {
                for (k, v) in props {
                    if let Some(vs) = v.as_str() {
                        properties.insert(k.clone(), vs.to_string());
                    }
                }
            }
        }
    }

    // Count commits as "snapshots"
    let snapshot_count = json_files.len();

    Some(DiscoveredTable {
        database: database.to_string(),
        table_name: table_name.to_string(),
        s3_location: table_path.trim_end_matches('/').to_string(),
        format: TableFormat::Delta,
        metadata_location: Some(commit_path),
        column_count: columns.len(),
        columns,
        format_version: Some(format_version),
        partition_fields: partition_cols,
        snapshot_count,
        last_updated_ms: None,
        table_type: String::new(),
        properties,
    })
}

/// Convert Delta type to string.
fn delta_type_to_string(type_val: Option<&serde_json::Value>) -> String {
    match type_val {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Object(obj)) => {
            if let Some(t) = obj.get("type").and_then(|v| v.as_str()) {
                match t {
                    "struct" => "struct".to_string(),
                    "array" => {
                        let elem = obj.get("elementType")
                            .map(|e| delta_type_to_string(Some(e)))
                            .unwrap_or_else(|| "unknown".to_string());
                        format!("array<{}>", elem)
                    }
                    "map" => {
                        let key = obj.get("keyType")
                            .map(|k| delta_type_to_string(Some(k)))
                            .unwrap_or_else(|| "string".to_string());
                        let val = obj.get("valueType")
                            .map(|v| delta_type_to_string(Some(v)))
                            .unwrap_or_else(|| "string".to_string());
                        format!("map<{}, {}>", key, val)
                    }
                    _ => t.to_string(),
                }
            } else {
                "unknown".to_string()
            }
        }
        _ => "unknown".to_string(),
    }
}

// ── Hudi Discovery ─────────────────────────────────────────────────

/// Discover an Apache Hudi table from `.hoodie/` directory.
async fn discover_hudi_table(
    store: &Arc<dyn ObjectStore>,
    database: &str,
    table_name: &str,
    table_path: &str,
) -> Option<DiscoveredTable> {
    let hoodie_prefix = format!("{}.hoodie/", table_path);
    let files = list_files(store, &hoodie_prefix).await.ok()?;

    if files.is_empty() {
        return None;
    }

    // Read hoodie.properties if available
    let mut properties = std::collections::HashMap::new();
    if files.iter().any(|f| f == "hoodie.properties") {
        let props_path = format!("{}hoodie.properties", hoodie_prefix);
        if let Ok(bytes) = read_file_bytes(store, &props_path).await {
            if let Ok(text) = String::from_utf8(bytes) {
                for line in text.lines() {
                    let line = line.trim();
                    if line.starts_with('#') || line.is_empty() { continue; }
                    if let Some(eq) = line.find('=') {
                        let key = line[..eq].trim().to_string();
                        let val = line[eq+1..].trim().to_string();
                        properties.insert(key, val);
                    }
                }
            }
        }
    }

    let table_type = properties.get("hoodie.table.type")
        .cloned()
        .unwrap_or_else(|| "COPY_ON_WRITE".to_string());

    // Try to read schema from .hoodie/latest commit metadata
    // For now, return empty columns — schema inference for Hudi is complex
    let commit_files: Vec<&String> = files.iter()
        .filter(|f| f.ends_with(".commit") || f.ends_with(".deltacommit"))
        .collect();

    Some(DiscoveredTable {
        database: database.to_string(),
        table_name: table_name.to_string(),
        s3_location: table_path.trim_end_matches('/').to_string(),
        format: TableFormat::Hudi,
        metadata_location: Some(hoodie_prefix),
        column_count: 0,
        columns: vec![],
        format_version: None,
        partition_fields: vec![],
        snapshot_count: commit_files.len(),
        last_updated_ms: None,
        table_type,
        properties,
    })
}

// ── Raw Parquet Discovery ──────────────────────────────────────────

/// Discover a directory of Parquet files (no formal table format).
/// Common in Spark/EMR output, Athena CTAS, Glue ETL, etc.
async fn discover_parquet_dir(
    store: &Arc<dyn ObjectStore>,
    database: &str,
    table_name: &str,
    table_path: &str,
) -> Option<DiscoveredTable> {
    // Check for .parquet files at this level or in data/ subdir
    let listing = list_files_with_dirs(store, table_path).await.ok()?;

    let parquet_files: Vec<&String> = listing.files.iter()
        .filter(|f| f.ends_with(".parquet") || f.ends_with(".snappy.parquet"))
        .collect();

    // Also check data/ subdir
    let data_parquet = if listing.dirs.contains(&"data".to_string()) {
        let data_prefix = format!("{}data/", table_path);
        list_files(store, &data_prefix).await.ok()
            .map(|files| files.into_iter().filter(|f| f.ends_with(".parquet") || f.ends_with(".snappy.parquet")).count())
            .unwrap_or(0)
    } else {
        0
    };

    let total_parquet = parquet_files.len() + data_parquet;
    if total_parquet == 0 {
        return None;
    }

    // Check for Hive-style partitioning (subdirs like year=2024/)
    let partition_dirs: Vec<String> = listing.dirs.iter()
        .filter(|d| d.contains('='))
        .cloned()
        .collect();
    let partition_fields: Vec<String> = partition_dirs.iter()
        .filter_map(|d| d.split('=').next().map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut properties = std::collections::HashMap::new();
    properties.insert("file_count".to_string(), total_parquet.to_string());
    if !partition_dirs.is_empty() {
        properties.insert("partitioning".to_string(), "hive".to_string());
    }

    Some(DiscoveredTable {
        database: database.to_string(),
        table_name: table_name.to_string(),
        s3_location: table_path.trim_end_matches('/').to_string(),
        format: TableFormat::Parquet,
        metadata_location: None,
        column_count: 0, // Would need to read a parquet file to get schema
        columns: vec![],
        format_version: None,
        partition_fields,
        snapshot_count: total_parquet,
        last_updated_ms: None,
        table_type: String::new(),
        properties,
    })
}

// ── Name cleaning helpers ──────────────────────────────────────────

/// Strip UUID suffix from table directory names.
///
/// Iceberg catalogs (Trino, Spark, Nessie) often create table directories with
/// UUID suffixes: `mars-8c47cb78483c464aaceba11d21658a04`. This function
/// strips the UUID portion and returns just the clean table name: `mars`.
///
/// Pattern: `{name}-{32-hex-char-uuid}` → `{name}`
///
/// If the name doesn't match the UUID pattern, it's returned as-is.
fn strip_uuid_suffix(name: &str) -> String {
    // UUID hex pattern: 32 lowercase hex chars (no dashes in dir names)
    // e.g., "mars-8c47cb78483c464aaceba11d21658a04"
    if let Some(dash_pos) = name.rfind('-') {
        let suffix = &name[dash_pos + 1..];
        // Check if suffix looks like a UUID (32 hex chars, no dashes)
        if suffix.len() == 32 && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            let clean = &name[..dash_pos];
            if !clean.is_empty() {
                return clean.to_string();
            }
        }
        // Also check for UUID with dashes: 8-4-4-4-12 format (36 chars)
        // e.g., "table-8c47cb78-4834-c464-aace-ba11d21658a0"
        // This is less common in S3 paths but worth handling
    }

    // Check for standard UUID format appended after a dash: name-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    // Total UUID with dashes = 36 chars
    if name.len() > 37 {
        // Try to find pattern where last 36 chars after a dash form a UUID
        let potential_split = name.len() - 36;
        if potential_split > 0 && name.as_bytes()[potential_split - 1] == b'-' {
            let suffix = &name[potential_split..];
            // Check UUID pattern: 8-4-4-4-12
            let parts: Vec<&str> = suffix.split('-').collect();
            if parts.len() == 5
                && parts[0].len() == 8
                && parts[1].len() == 4
                && parts[2].len() == 4
                && parts[3].len() == 4
                && parts[4].len() == 12
                && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_hexdigit()))
            {
                let clean = &name[..potential_split - 1];
                if !clean.is_empty() {
                    return clean.to_string();
                }
            }
        }
    }

    name.to_string()
}

/// Compose a database/schema name from the path segments leading to a table.
///
/// Handles various warehouse layouts:
/// - `["sales.db"]` → `"sales"` (strip .db suffix)
/// - `["consumer", "app_104387_rtef"]` → `"app_104387_rtef"` (catalog/schema → use schema)
/// - `["warehouse", "catalog", "schema"]` → `"catalog_schema"` (deep → join non-root segments)
/// - `["default"]` → `"default"`
fn compose_database_name(segments: &[String]) -> String {
    if segments.is_empty() {
        return "default".to_string();
    }

    // Single segment: just clean it
    if segments.len() == 1 {
        return segments[0].trim_end_matches(".db").to_string();
    }

    // Multiple segments: skip the first (treated as catalog/warehouse root),
    // join the rest with underscores. This maps:
    //   ["consumer", "app_104387_rtef"] → "app_104387_rtef"
    //   ["warehouse", "ns1", "ns2"] → "ns1_ns2"
    let meaningful: Vec<&str> = segments[1..].iter()
        .map(|s| s.trim_end_matches(".db"))
        .collect();

    if meaningful.is_empty() {
        segments[0].trim_end_matches(".db").to_string()
    } else {
        meaningful.join("_")
    }
}

// ── File / Directory helpers ───────────────────────────────────────

struct DirListing {
    files: Vec<String>,
    dirs: Vec<String>,
}

/// List both files and directories at a prefix.
async fn list_files_with_dirs(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<DirListing, String> {
    let obj_prefix = ObjectPath::from(prefix);
    let result = store.list_with_delimiter(Some(&obj_prefix)).await
        .map_err(|e| format!("S3 list '{}': {}", prefix, e))?;

    let dirs: Vec<String> = result.common_prefixes.into_iter()
        .filter_map(|p| {
            let s = p.to_string();
            let trimmed = s.trim_end_matches('/');
            trimmed.rsplit('/').next().map(|n| n.to_string())
        })
        .collect();

    let files: Vec<String> = result.objects.into_iter()
        .filter_map(|meta| {
            let s = meta.location.to_string();
            s.rsplit('/').next().map(|n| n.to_string())
        })
        .collect();

    Ok(DirListing { files, dirs })
}

/// List immediate subdirectories under a prefix.
async fn list_directories(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<Vec<String>, String> {
    let obj_prefix = ObjectPath::from(prefix);
    let result = store.list_with_delimiter(Some(&obj_prefix)).await
        .map_err(|e| format!("S3 list '{}': {}", prefix, e))?;

    let dirs: Vec<String> = result.common_prefixes.into_iter()
        .filter_map(|p| {
            let path_str = p.to_string();
            let trimmed = path_str.trim_end_matches('/');
            trimmed.rsplit('/').next().map(|s| s.to_string())
        })
        .filter(|d| !d.is_empty() && !d.starts_with('.') && !d.starts_with('_'))
        .collect();

    Ok(dirs)
}

/// List files (not directories) under a prefix.
async fn list_files(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<Vec<String>, String> {
    let obj_prefix = ObjectPath::from(prefix);
    let result = store.list_with_delimiter(Some(&obj_prefix)).await
        .map_err(|e| format!("S3 list files '{}': {}", prefix, e))?;

    let files: Vec<String> = result.objects.into_iter()
        .filter_map(|meta| {
            let path_str = meta.location.to_string();
            path_str.rsplit('/').next().map(|s| s.to_string())
        })
        .collect();

    Ok(files)
}

/// Read a JSON file from S3.
async fn read_json_file(
    store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<serde_json::Value, String> {
    let obj_path = ObjectPath::from(path);
    let result = store.get(&obj_path).await
        .map_err(|e| format!("Failed to read {}: {}", path, e))?;
    let bytes = result.bytes().await
        .map_err(|e| format!("Failed to read bytes from {}: {}", path, e))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| format!("Failed to parse {}: {}", path, e))
}

/// Read a newline-delimited JSON file (Delta commit log format).
async fn read_json_lines(
    store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let obj_path = ObjectPath::from(path);
    let result = store.get(&obj_path).await
        .map_err(|e| format!("Failed to read {}: {}", path, e))?;
    let bytes = result.bytes().await
        .map_err(|e| format!("Failed to read bytes from {}: {}", path, e))?;
    let text = String::from_utf8_lossy(&bytes);
    let entries: Vec<serde_json::Value> = text.lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    Ok(entries)
}

/// Read raw file bytes from S3.
async fn read_file_bytes(
    store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<Vec<u8>, String> {
    let obj_path = ObjectPath::from(path);
    let result = store.get(&obj_path).await
        .map_err(|e| format!("Failed to read {}: {}", path, e))?;
    let bytes = result.bytes().await
        .map_err(|e| format!("Failed to read bytes from {}: {}", path, e))?;
    Ok(bytes.to_vec())
}

// Keep old name for backward compat
pub async fn read_iceberg_metadata(
    store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<serde_json::Value, String> {
    read_json_file(store, path).await
}

// ── Iceberg-specific parsing ───────────────────────────────────────

/// Find the latest v*.metadata.json file from a list of metadata files.
fn find_latest_metadata(files: &[String]) -> Option<String> {
    if files.contains(&"version-hint.text".to_string()) {
        // Would need to read version-hint.text to get the version number
        // Fall through to version number scanning
    }

    let mut best_version: i64 = -1;
    let mut best_file: Option<String> = None;

    for file in files {
        if file.ends_with(".metadata.json") {
            let version = file.split('.').next()
                .and_then(|prefix| {
                    if let Some(num_str) = prefix.strip_prefix('v') {
                        num_str.parse::<i64>().ok()
                    } else {
                        prefix.split('-').next().and_then(|n| n.parse::<i64>().ok())
                    }
                });
            if let Some(v) = version {
                if v > best_version {
                    best_version = v;
                    best_file = Some(file.clone());
                }
            }
        }
    }

    if best_file.is_none() {
        best_file = files.iter()
            .filter(|f| f.ends_with(".metadata.json"))
            .max()
            .cloned();
    }

    best_file
}

/// Parse Iceberg schema from metadata JSON.
fn parse_iceberg_schema(metadata: &serde_json::Value) -> (Vec<ColumnInfo>, i64) {
    let format_version = metadata.get("format-version")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);

    let schema = if format_version >= 2 {
        let schema_id = metadata.get("current-schema-id").and_then(|v| v.as_i64()).unwrap_or(0);
        metadata.get("schemas")
            .and_then(|s| s.as_array())
            .and_then(|schemas| {
                schemas.iter().find(|s| {
                    s.get("schema-id").and_then(|id| id.as_i64()).unwrap_or(-1) == schema_id
                })
            })
            .or_else(|| metadata.get("schema"))
    } else {
        metadata.get("schema")
    };

    let columns = schema
        .and_then(|s| s.get("fields"))
        .and_then(|f| f.as_array())
        .map(|fields| {
            fields.iter().enumerate().map(|(i, field)| {
                let name = field.get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let data_type = iceberg_type_to_string(field.get("type"));
                let required = field.get("required")
                    .and_then(|r| r.as_bool())
                    .unwrap_or(false);
                ColumnInfo {
                    name,
                    data_type,
                    nullable: !required,
                    ordinal: i as i32,
                }
            }).collect()
        })
        .unwrap_or_default();

    (columns, format_version)
}

/// Convert Iceberg type JSON to a string representation.
fn iceberg_type_to_string(type_val: Option<&serde_json::Value>) -> String {
    match type_val {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Object(obj)) => {
            if let Some(t) = obj.get("type").and_then(|v| v.as_str()) {
                match t {
                    "struct" => "struct".to_string(),
                    "list" => {
                        let elem = obj.get("element")
                            .map(|e| iceberg_type_to_string(Some(e)))
                            .unwrap_or_else(|| "unknown".to_string());
                        format!("list<{}>", elem)
                    }
                    "map" => {
                        let key = obj.get("key")
                            .map(|k| iceberg_type_to_string(Some(k)))
                            .unwrap_or_else(|| "string".to_string());
                        let val = obj.get("value")
                            .map(|v| iceberg_type_to_string(Some(v)))
                            .unwrap_or_else(|| "string".to_string());
                        format!("map<{}, {}>", key, val)
                    }
                    _ => t.to_string(),
                }
            } else {
                "unknown".to_string()
            }
        }
        _ => "unknown".to_string(),
    }
}

/// Parse partition spec from Iceberg metadata.
fn parse_partition_spec(metadata: &serde_json::Value) -> Vec<String> {
    let spec = metadata.get("partition-specs")
        .and_then(|specs| specs.as_array())
        .and_then(|specs| {
            let default_id = metadata.get("default-spec-id").and_then(|v| v.as_i64()).unwrap_or(0);
            specs.iter().find(|s| s.get("spec-id").and_then(|id| id.as_i64()).unwrap_or(-1) == default_id)
        })
        .and_then(|s| s.get("fields"))
        .or_else(|| metadata.get("partition-spec"))
        .and_then(|f| f.as_array());

    spec.map(|fields| {
        fields.iter().filter_map(|f| {
            let name = f.get("source-id").and_then(|v| v.as_i64())
                .map(|id| format!("col_{}", id))
                .or_else(|| f.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()));
            let transform = f.get("transform").and_then(|t| t.as_str()).unwrap_or("identity");
            name.map(|n| {
                if transform == "identity" { n } else { format!("{}({})", transform, n) }
            })
        }).collect()
    }).unwrap_or_default()
}

/// Extract properties map from Iceberg metadata JSON.
fn parse_iceberg_properties(metadata: &serde_json::Value) -> std::collections::HashMap<String, String> {
    metadata.get("properties")
        .and_then(|p| p.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| {
                    v.as_str().map(|s| (k.clone(), s.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Normalize a warehouse prefix to ensure it ends with '/'.
fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_start_matches("s3://");
    let path_only = if let Some(idx) = trimmed.find('/') {
        &trimmed[idx + 1..]
    } else {
        ""
    };
    if path_only.is_empty() {
        String::new()
    } else if path_only.ends_with('/') {
        path_only.to_string()
    } else {
        format!("{}/", path_only)
    }
}

// ── Trino warehouse location helpers ───────────────────────────────

/// Try to get warehouse location from Trino catalog properties.
pub async fn get_warehouse_location_from_trino(
    rest: &crate::trino_client::TrinoRestClient,
    catalog_name: &str,
) -> Option<String> {
    let sql = format!(
        "SELECT property_value FROM system.metadata.catalog_properties WHERE catalog_name = '{}' AND property_name = 'warehouse'",
        catalog_name
    );
    if let Ok(rows) = rest.query(&sql, "system").await {
        if let Some(row) = rows.first() {
            if let Some(val) = row.first().and_then(|v| v.as_str()) {
                let location = val.trim().to_string();
                if !location.is_empty() {
                    tracing::info!(catalog = %catalog_name, warehouse = %location, "Got warehouse location from Trino catalog properties");
                    return Some(location);
                }
            }
        }
    }

    let schema_sql = format!("SHOW SCHEMAS FROM \"{}\"", catalog_name);
    if let Ok(rows) = rest.query(&schema_sql, catalog_name).await {
        for row in &rows {
            if let Some(schema) = row.first().and_then(|v| v.as_str()) {
                let schema = schema.trim();
                if schema == "information_schema" || schema == "pg_catalog" { continue; }
                let show_schema_sql = format!("SHOW CREATE SCHEMA \"{}\".\"{}\"", catalog_name, schema);
                if let Ok(create_result) = rest.query(&show_schema_sql, catalog_name).await {
                    let ddl: String = create_result.iter()
                        .filter_map(|r| r.first().and_then(|v| v.as_str()))
                        .collect::<Vec<_>>().join("\n").to_lowercase();
                    if let Some(loc) = extract_s3_location(&ddl) {
                        if let Some(warehouse) = derive_warehouse_from_schema_location(&loc) {
                            return Some(warehouse);
                        }
                        return Some(loc);
                    }
                }
                break;
            }
        }
    }

    if let Ok(rows) = rest.query(&format!("SHOW SCHEMAS FROM \"{}\"", catalog_name), catalog_name).await {
        for row in &rows {
            if let Some(schema) = row.first().and_then(|v| v.as_str()) {
                let schema = schema.trim();
                if schema == "information_schema" || schema == "pg_catalog" { continue; }
                let tables_sql = format!("SHOW TABLES FROM \"{}\".\"{}\"", catalog_name, schema);
                if let Ok(table_rows) = rest.query(&tables_sql, catalog_name).await {
                    if let Some(table_row) = table_rows.first() {
                        if let Some(table_name) = table_row.first().and_then(|v| v.as_str()) {
                            let show_sql = format!("SHOW CREATE TABLE \"{}\".\"{}\".\"{}\"", catalog_name, schema, table_name.trim());
                            if let Ok(create_result) = rest.query(&show_sql, catalog_name).await {
                                let ddl: String = create_result.iter()
                                    .filter_map(|r| r.first().and_then(|v| v.as_str()))
                                    .collect::<Vec<_>>().join("\n").to_lowercase();
                                if let Some(loc) = extract_s3_location(&ddl) {
                                    if let Some(warehouse) = derive_warehouse_from_location(&loc) {
                                        return Some(warehouse);
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            }
        }
    }

    tracing::warn!(catalog = %catalog_name, "Could not determine warehouse location");
    None
}

fn extract_s3_location(ddl: &str) -> Option<String> {
    for pattern in &["external_location = '", "location = '", "'s3://", "'s3a://"] {
        if let Some(idx) = ddl.find(pattern) {
            let start = if pattern.starts_with('\'') { idx + 1 } else { idx + pattern.len() };
            let rest = &ddl[start..];
            if let Some(end) = rest.find('\'') {
                let loc = rest[..end].trim().to_string();
                if loc.starts_with("s3://") || loc.starts_with("s3a://") {
                    return Some(loc);
                }
            }
        }
    }
    None
}

fn derive_warehouse_from_location(location: &str) -> Option<String> {
    let parts: Vec<&str> = location.trim_end_matches('/').rsplitn(3, '/').collect();
    if parts.len() >= 3 {
        Some(format!("{}/", parts[2]))
    } else {
        None
    }
}

fn derive_warehouse_from_schema_location(location: &str) -> Option<String> {
    let trimmed = location.trim_end_matches('/');
    if let Some(idx) = trimmed.rfind('/') {
        let parent = &trimmed[..idx];
        if parent.starts_with("s3://") || parent.starts_with("s3a://") {
            return Some(format!("{}/", parent));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_uuid_suffix_32hex() {
        // Trino-style: table-<32 hex chars>
        assert_eq!(
            strip_uuid_suffix("mars-8c47cb78483c464aaceba11d21658a04"),
            "mars"
        );
        assert_eq!(
            strip_uuid_suffix("mars_ccb_new-abcdef0123456789abcdef0123456789"),
            "mars_ccb_new"
        );
    }

    #[test]
    fn test_strip_uuid_suffix_dashed_uuid() {
        // Standard UUID format: table-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        assert_eq!(
            strip_uuid_suffix("orders-8c47cb78-4834-c464-aace-ba11d21658a0"),
            "orders"
        );
    }

    #[test]
    fn test_strip_uuid_no_uuid() {
        // Regular names without UUIDs — returned unchanged
        assert_eq!(strip_uuid_suffix("orders"), "orders");
        assert_eq!(strip_uuid_suffix("tpch_lineitem"), "tpch_lineitem");
        assert_eq!(strip_uuid_suffix("app_104387_rtef"), "app_104387_rtef");
        assert_eq!(strip_uuid_suffix("sales.db"), "sales.db");
    }

    #[test]
    fn test_strip_uuid_short_hex_not_uuid() {
        // Short hex suffix that isn't a UUID — preserved
        assert_eq!(strip_uuid_suffix("table-abc123"), "table-abc123");
        assert_eq!(strip_uuid_suffix("data-v2"), "data-v2");
    }

    #[test]
    fn test_compose_database_name_single() {
        assert_eq!(compose_database_name(&[s("sales.db")]), "sales");
        assert_eq!(compose_database_name(&[s("default")]), "default");
        assert_eq!(compose_database_name(&[s("analytics")]), "analytics");
    }

    #[test]
    fn test_compose_database_name_catalog_schema() {
        // Trino: catalog/schema → use schema as database name
        assert_eq!(
            compose_database_name(&[s("consumer"), s("app_104387_rtef")]),
            "app_104387_rtef"
        );
        assert_eq!(
            compose_database_name(&[s("consumer"), s("airflow_metrics")]),
            "airflow_metrics"
        );
    }

    #[test]
    fn test_compose_database_name_deep() {
        // 3+ levels: join non-root segments
        assert_eq!(
            compose_database_name(&[s("warehouse"), s("ns1"), s("ns2")]),
            "ns1_ns2"
        );
    }

    #[test]
    fn test_compose_database_name_empty() {
        assert_eq!(compose_database_name(&[]), "default");
    }

    fn s(val: &str) -> String {
        val.to_string()
    }
}
