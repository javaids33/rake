//! S3-based Iceberg table discovery.
//!
//! Scans an S3 warehouse path directly to discover Iceberg tables
//! without routing through Trino's SQL engine. This is dramatically faster:
//! - S3 ListObjectsV2: ~50ms per 1000 objects
//! - Trino SHOW CREATE TABLE: ~200-500ms per table (parse → plan → execute → poll)
//!
//! For a warehouse with 100 tables, S3 discovery takes ~2s vs Trino ~30-60s.
//!
//! Layout expected:
//! ```text
//! s3://bucket/warehouse/
//!   database_name/
//!     table_name/
//!       metadata/
//!         v1.metadata.json     ← Iceberg table metadata (schema, partitions)
//!         00000-....avro       ← manifest lists
//!       data/
//!         *.parquet            ← data files
//! ```

use futures::stream::{self, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::Serialize;
use std::sync::Arc;

/// Maximum concurrent S3 operations during discovery.
const S3_DISCOVERY_PARALLELISM: usize = 8;

/// A discovered Iceberg table from S3 scanning.
#[derive(Debug, Clone, Serialize)]
pub struct IcebergTableInfo {
    pub database: String,
    pub table_name: String,
    pub s3_location: String,
    pub metadata_location: Option<String>,
    pub column_count: usize,
    pub columns: Vec<IcebergColumnInfo>,
    pub format_version: Option<i64>,
    pub partition_fields: Vec<String>,
    pub snapshot_count: usize,
    pub last_updated_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IcebergColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub ordinal: i32,
}

/// Result of scanning a warehouse.
#[derive(Debug, Clone, Serialize)]
pub struct WarehouseScanResult {
    pub warehouse_path: String,
    pub databases: Vec<String>,
    pub tables: Vec<IcebergTableInfo>,
    pub total_tables: usize,
    pub scan_duration_ms: u128,
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

/// Scan an S3 warehouse path to discover Iceberg databases and tables.
///
/// This is the fast path — no Trino queries, pure S3 ListObjects.
pub async fn scan_warehouse(
    store: &Arc<dyn ObjectStore>,
    warehouse_prefix: &str,
) -> Result<WarehouseScanResult, String> {
    let start = std::time::Instant::now();
    let prefix = normalize_prefix(warehouse_prefix);

    // Step 1: List top-level directories = databases
    let databases = list_directories(store, &prefix).await?;
    tracing::info!(count = databases.len(), prefix = %prefix, "S3 scan: discovered databases");

    if databases.is_empty() {
        return Ok(WarehouseScanResult {
            warehouse_path: prefix,
            databases: vec![],
            tables: vec![],
            total_tables: 0,
            scan_duration_ms: start.elapsed().as_millis(),
        });
    }

    // Step 2: List tables within each database (parallel, 8 concurrent)
    let db_tables: Vec<(String, Vec<String>)> = stream::iter(databases.iter().cloned())
        .map(|db| {
            let store = store.clone();
            let prefix = prefix.clone();
            async move {
                let db_prefix = format!("{}{}/", prefix, db);
                let tables = list_directories(&store, &db_prefix).await.unwrap_or_default();
                (db, tables)
            }
        })
        .buffer_unordered(S3_DISCOVERY_PARALLELISM)
        .collect()
        .await;

    // Step 3: For each table, check for metadata/ dir and read metadata.json (parallel, 8 concurrent)
    let mut all_table_paths: Vec<(String, String, String)> = Vec::new(); // (db, table, path)
    for (db, tables) in &db_tables {
        for table in tables {
            let path = format!("{}{}/{}/", prefix, db, table);
            all_table_paths.push((db.clone(), table.clone(), path));
        }
    }

    let table_infos: Vec<Option<IcebergTableInfo>> = stream::iter(all_table_paths)
        .map(|(db, table, path)| {
            let store = store.clone();
            async move {
                discover_iceberg_table(&store, &db, &table, &path).await
            }
        })
        .buffer_unordered(S3_DISCOVERY_PARALLELISM)
        .collect()
        .await;

    let tables: Vec<IcebergTableInfo> = table_infos.into_iter().flatten().collect();
    let total_tables = tables.len();
    let db_names: Vec<String> = db_tables.iter().map(|(db, _)| db.clone()).collect();

    tracing::info!(
        tables = total_tables,
        databases = db_names.len(),
        elapsed_ms = start.elapsed().as_millis(),
        "S3 Iceberg warehouse scan complete"
    );

    Ok(WarehouseScanResult {
        warehouse_path: prefix,
        databases: db_names,
        tables,
        total_tables,
        scan_duration_ms: start.elapsed().as_millis(),
    })
}

/// Discover a single Iceberg table from its S3 path.
/// Checks for metadata/ directory and reads the latest metadata.json.
async fn discover_iceberg_table(
    store: &Arc<dyn ObjectStore>,
    database: &str,
    table_name: &str,
    table_path: &str,
) -> Option<IcebergTableInfo> {
    let metadata_prefix = format!("{}metadata/", table_path);

    // List metadata files to find the latest version-hint or v*.metadata.json
    let meta_files = list_files(store, &metadata_prefix).await.ok()?;

    if meta_files.is_empty() {
        // No metadata/ directory — might not be an Iceberg table
        return None;
    }

    // Find the latest metadata JSON file
    // Iceberg convention: v{N}.metadata.json where higher N = newer
    let metadata_file = find_latest_metadata(&meta_files)?;
    let metadata_path = format!("{}{}", metadata_prefix, metadata_file);

    // Read and parse the metadata JSON
    let metadata = read_iceberg_metadata(store, &metadata_path).await.ok()?;

    // Extract schema from metadata
    let (columns, format_version) = parse_iceberg_schema(&metadata);
    let partition_fields = parse_partition_spec(&metadata);
    let snapshot_count = metadata.get("snapshots")
        .and_then(|s| s.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let last_updated = metadata.get("last-updated-ms")
        .and_then(|v| v.as_i64());

    let s3_location = table_path.trim_end_matches('/').to_string();

    Some(IcebergTableInfo {
        database: database.to_string(),
        table_name: table_name.to_string(),
        s3_location,
        metadata_location: Some(metadata_path),
        column_count: columns.len(),
        columns: columns.clone(),
        format_version: Some(format_version),
        partition_fields,
        snapshot_count,
        last_updated_ms: last_updated,
    })
}

/// Read and parse an Iceberg metadata JSON file from S3.
async fn read_iceberg_metadata(
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

/// Find the latest v*.metadata.json file from a list of metadata files.
fn find_latest_metadata(files: &[String]) -> Option<String> {
    // First check for version-hint.text
    if files.contains(&"version-hint.text".to_string()) {
        // Would need to read version-hint.text to get the version number
        // For now, fall through to version number scanning
    }

    // Find highest version v{N}.metadata.json
    let mut best_version: i64 = -1;
    let mut best_file: Option<String> = None;

    for file in files {
        if file.ends_with(".metadata.json") {
            // Extract version number from v{N}.metadata.json or {N}-{uuid}.metadata.json
            let version = file.split('.').next()
                .and_then(|prefix| {
                    // Try v{N} format
                    if let Some(num_str) = prefix.strip_prefix('v') {
                        num_str.parse::<i64>().ok()
                    } else {
                        // Try {N}-{uuid} format (e.g., "00001-abc123")
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

    // Fallback: just pick the last metadata.json alphabetically
    if best_file.is_none() {
        best_file = files.iter()
            .filter(|f| f.ends_with(".metadata.json"))
            .max()
            .cloned();
    }

    best_file
}

/// Parse Iceberg schema from metadata JSON.
/// Returns (columns, format_version).
fn parse_iceberg_schema(metadata: &serde_json::Value) -> (Vec<IcebergColumnInfo>, i64) {
    let format_version = metadata.get("format-version")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);

    // Iceberg v2: schema is in "schemas" array, current-schema-id points to the active one
    // Iceberg v1: schema is in "schema" object directly
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
                IcebergColumnInfo {
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
            // Complex type: struct, list, map
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
    // v2: "partition-specs" array + "default-spec-id"
    // v1: "partition-spec" array
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

/// List immediate subdirectories under a prefix.
async fn list_directories(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<Vec<String>, String> {
    let obj_prefix = ObjectPath::from(prefix);
    let result = store.list_with_delimiter(Some(&obj_prefix)).await
        .map_err(|e| format!("S3 list '{}': {}", prefix, e))?;

    let dirs: Vec<String> = result.common_prefixes.into_iter()
        .filter_map(|p| {
            let path_str = p.to_string();
            // Extract the last directory component
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

/// Normalize a warehouse prefix to ensure it ends with '/'.
fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_start_matches("s3://");
    // Remove bucket name if present
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

/// Try to get warehouse location from Trino catalog properties.
/// This is a single fast query vs scanning for each table.
pub async fn get_warehouse_location_from_trino(
    rest: &crate::trino_client::TrinoRestClient,
    catalog_name: &str,
) -> Option<String> {
    // Try catalog_properties system table
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

    // Fallback 1: SHOW CREATE SCHEMA — Iceberg schemas often have a location property
    let schema_sql = format!("SHOW SCHEMAS FROM \"{}\"", catalog_name);
    if let Ok(rows) = rest.query(&schema_sql, catalog_name).await {
        for row in &rows {
            if let Some(schema) = row.first().and_then(|v| v.as_str()) {
                let schema = schema.trim();
                if schema == "information_schema" || schema == "pg_catalog" { continue; }

                let show_schema_sql = format!("SHOW CREATE SCHEMA \"{}\".\"{}\"", catalog_name, schema);
                tracing::info!(catalog = %catalog_name, schema = %schema, "Trying SHOW CREATE SCHEMA for warehouse location");
                if let Ok(create_result) = rest.query(&show_schema_sql, catalog_name).await {
                    let ddl: String = create_result.iter()
                        .filter_map(|r| r.first().and_then(|v| v.as_str()))
                        .collect::<Vec<_>>().join("\n").to_lowercase();
                    tracing::debug!(catalog = %catalog_name, schema = %schema, ddl = %ddl, "SHOW CREATE SCHEMA result");

                    // Extract s3 location from schema DDL
                    if let Some(loc) = extract_s3_location(&ddl) {
                        // Schema location is usually s3://bucket/warehouse/db — parent is warehouse
                        if let Some(warehouse) = derive_warehouse_from_schema_location(&loc) {
                            tracing::info!(catalog = %catalog_name, warehouse = %warehouse, "Derived warehouse from SHOW CREATE SCHEMA");
                            return Some(warehouse);
                        }
                        // If we can't derive parent, the schema location itself might be the warehouse
                        tracing::info!(catalog = %catalog_name, warehouse = %loc, "Using schema location as warehouse");
                        return Some(loc);
                    }
                }
                break; // Only try the first real schema
            }
        }
    }

    // Fallback 2: SHOW CREATE TABLE on one table — extract location, strip table/db segments
    tracing::info!(catalog = %catalog_name, "SHOW CREATE SCHEMA didn't yield location, trying SHOW CREATE TABLE on one table");
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
                                        tracing::info!(catalog = %catalog_name, warehouse = %warehouse, "Derived warehouse from SHOW CREATE TABLE");
                                        return Some(warehouse);
                                    }
                                }
                            }
                        }
                    }
                }
                break; // Only try the first schema
            }
        }
    }

    tracing::warn!(catalog = %catalog_name, "Could not determine warehouse location from any method");
    None
}

/// Extract S3 location from DDL text. Handles both s3:// and s3a:// prefixes.
fn extract_s3_location(ddl: &str) -> Option<String> {
    for pattern in &["external_location = '", "location = '", "'s3://", "'s3a://"] {
        if let Some(idx) = ddl.find(pattern) {
            let start = if pattern.starts_with('\'') {
                idx + 1
            } else {
                idx + pattern.len()
            };
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

/// Derive warehouse root from a table location.
/// e.g., s3://bucket/warehouse/db/table → s3://bucket/warehouse/
fn derive_warehouse_from_location(location: &str) -> Option<String> {
    let parts: Vec<&str> = location.trim_end_matches('/').rsplitn(3, '/').collect();
    if parts.len() >= 3 {
        // parts[2] = s3://bucket/warehouse, parts[1] = db, parts[0] = table
        Some(format!("{}/", parts[2]))
    } else {
        None
    }
}

/// Derive warehouse root from a schema location.
/// Schema locations are one level above table: s3://bucket/warehouse/db → s3://bucket/warehouse/
fn derive_warehouse_from_schema_location(location: &str) -> Option<String> {
    let trimmed = location.trim_end_matches('/');
    if let Some(idx) = trimmed.rfind('/') {
        let parent = &trimmed[..idx];
        // Make sure we still have the s3:// prefix and at least a bucket
        if parent.starts_with("s3://") || parent.starts_with("s3a://") {
            return Some(format!("{}/", parent));
        }
    }
    None
}
