//! DuckDB OLAP accelerator engine.
//!
//! Wraps a DuckDB in-memory connection behind `spawn_blocking` (DuckDB's C FFI
//! is `!Send`). All data exchange uses Arrow IPC as a version bridge between
//! DuckDB's arrow (v56) and our workspace arrow (v57). The IPC format is stable
//! across versions, so this bridge is zero-overhead for correctness.

use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use rustlake_core::config::DuckDbEngineConfig;
use rustlake_core::{Result, RustLakeError};

/// DuckDB-backed OLAP engine for heavy analytical workloads.
///
/// The underlying `duckdb::Connection` is `!Send`, so all access goes through
/// `tokio::task::spawn_blocking` with an `Arc<Mutex<..>>` guard.
pub struct DuckDbEngine {
    conn: Arc<Mutex<duckdb::Connection>>,
}

impl DuckDbEngine {
    /// Create a new in-memory DuckDB engine with the given config.
    pub fn new(config: &DuckDbEngineConfig) -> Result<Self> {
        let conn = duckdb::Connection::open_in_memory()
            .map_err(|e| RustLakeError::DuckDb(format!("Failed to open DuckDB: {}", e)))?;

        // Apply PRAGMAs from config
        if let Some(ref limit) = config.memory_limit {
            conn.execute_batch(&format!("SET memory_limit = '{}';", limit))
                .map_err(|e| {
                    RustLakeError::DuckDb(format!("Failed to set memory_limit: {}", e))
                })?;
        }
        if let Some(threads) = config.threads {
            conn.execute_batch(&format!("SET threads = {};", threads))
                .map_err(|e| RustLakeError::DuckDb(format!("Failed to set threads: {}", e)))?;
        }

        // Install and load S3/Iceberg/Delta extensions for direct lake access
        // These are bundled with DuckDB 1.4+ when using the "bundled" feature
        let extensions = ["httpfs", "iceberg", "delta", "parquet"];
        for ext in &extensions {
            match conn.execute_batch(&format!("INSTALL {}; LOAD {};", ext, ext)) {
                Ok(_) => tracing::info!(extension = ext, "DuckDB: extension loaded"),
                Err(e) => tracing::debug!(extension = ext, error = %e, "DuckDB: extension not available (non-fatal)"),
            }
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Execute a SQL query and return Arrow RecordBatches.
    ///
    /// Internally: DuckDB query_arrow → DuckDB's arrow IPC bytes → our arrow RecordBatch.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let conn = self.conn.clone();
        let query = query.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| RustLakeError::DuckDb(format!("Lock poisoned: {}", e)))?;

            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| RustLakeError::DuckDb(format!("Prepare failed: {}", e)))?;

            // query_arrow returns DuckDB's arrow RecordBatch (arrow v56)
            let duck_batches: Vec<duckdb::arrow::record_batch::RecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| RustLakeError::DuckDb(format!("Query failed: {}", e)))?
                .collect();

            if duck_batches.is_empty() {
                return Ok(Vec::new());
            }

            // Bridge: DuckDB arrow v56 → IPC bytes → our arrow v57
            // Step 1: Serialize using duckdb-ipc (arrow-ipc v56, unified with DuckDB's arrow)
            let ipc_bytes = {
                let schema = duck_batches[0].schema();
                let mut buf = Vec::new();
                {
                    let mut writer =
                        duckdb_ipc::writer::StreamWriter::try_new(&mut buf, &schema)
                            .map_err(|e| {
                                RustLakeError::DuckDb(format!("DuckDB IPC write init: {}", e))
                            })?;
                    for batch in &duck_batches {
                        writer.write(batch).map_err(|e| {
                            RustLakeError::DuckDb(format!("DuckDB IPC write batch: {}", e))
                        })?;
                    }
                    writer.finish().map_err(|e| {
                        RustLakeError::DuckDb(format!("DuckDB IPC write finish: {}", e))
                    })?;
                }
                buf
            };

            // Step 2: Deserialize using our arrow IPC reader
            let cursor = std::io::Cursor::new(ipc_bytes);
            let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
                .map_err(|e| RustLakeError::DuckDb(format!("IPC read init: {}", e)))?;

            let batches: std::result::Result<Vec<RecordBatch>, _> = reader.collect();
            let batches =
                batches.map_err(|e| RustLakeError::DuckDb(format!("IPC read batch: {}", e)))?;

            Ok(batches)
        })
        .await
        .map_err(|e| RustLakeError::DuckDb(format!("spawn_blocking panicked: {}", e)))?
    }

    /// Register Arrow RecordBatches as a named DuckDB table.
    ///
    /// Internally: our arrow RecordBatch → IPC bytes → DuckDB's arrow → Appender insert.
    pub async fn register_arrow_table(
        &self,
        name: &str,
        batches: &[RecordBatch],
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let conn = self.conn.clone();
        let name = name.to_string();
        let schema = batches[0].schema();

        // Step 1: Serialize our batches to IPC bytes using our arrow_ipc
        let ipc_bytes = batches_to_ipc(batches)?;

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| RustLakeError::DuckDb(format!("Lock poisoned: {}", e)))?;

            // DROP + CREATE table using DDL derived from our schema
            let ddl = arrow_schema_to_duckdb_ddl(&name, &schema);
            conn.execute_batch(&format!("DROP TABLE IF EXISTS \"{}\"; {}", name, ddl))
                .map_err(|e| {
                    RustLakeError::DuckDb(format!(
                        "Failed to create table '{}': {}",
                        name, e
                    ))
                })?;

            // Step 2: Read IPC bytes as DuckDB's arrow batches (via duckdb-ipc v56)
            let cursor = std::io::Cursor::new(&ipc_bytes);
            let reader =
                duckdb_ipc::reader::StreamReader::try_new(cursor, None).map_err(|e| {
                    RustLakeError::DuckDb(format!("DuckDB IPC read failed: {}", e))
                })?;

            // Step 3: Insert via Appender (uses DuckDB's arrow types)
            let mut appender = conn.appender(&name).map_err(|e| {
                RustLakeError::DuckDb(format!("Appender for '{}' failed: {}", name, e))
            })?;

            for batch_result in reader {
                let batch = batch_result
                    .map_err(|e| RustLakeError::DuckDb(format!("IPC batch read: {}", e)))?;
                appender.append_record_batch(batch).map_err(|e| {
                    RustLakeError::DuckDb(format!("Appender insert failed: {}", e))
                })?;
            }

            appender
                .flush()
                .map_err(|e| RustLakeError::DuckDb(format!("Appender flush failed: {}", e)))?;

            Ok(())
        })
        .await
        .map_err(|e| RustLakeError::DuckDb(format!("spawn_blocking panicked: {}", e)))?
    }

    /// Sync multiple tables from DataFusion into DuckDB.
    /// Returns the number of tables successfully synced.
    pub async fn sync_tables(
        &self,
        tables: Vec<(String, Vec<RecordBatch>)>,
    ) -> Result<usize> {
        let mut synced = 0usize;
        for (name, batches) in tables {
            match self.register_arrow_table(&name, &batches).await {
                Ok(()) => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    tracing::debug!(table = %name, rows, "DuckDB: synced table");
                    synced += 1;
                }
                Err(e) => {
                    tracing::warn!(table = %name, error = %e, "DuckDB: failed to sync table");
                }
            }
        }
        Ok(synced)
    }

    /// Configure S3 credentials so DuckDB can directly read S3/Iceberg/Delta tables.
    pub async fn configure_s3(&self, access_key: &str, secret_key: &str, region: &str, endpoint: Option<&str>) -> Result<()> {
        let conn = self.conn.clone();
        let sql = format!(
            "SET s3_access_key_id='{}'; SET s3_secret_access_key='{}'; SET s3_region='{}';{}{}",
            access_key, secret_key, region,
            endpoint.map(|ep| format!(" SET s3_endpoint='{}';", ep.trim_start_matches("http://").trim_start_matches("https://"))).unwrap_or_default(),
            if endpoint.map(|ep| ep.starts_with("http://")).unwrap_or(false) { " SET s3_use_ssl=false; SET s3_url_style='path';" } else { "" },
        );

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| RustLakeError::DuckDb(format!("Lock: {}", e)))?;
            conn.execute_batch(&sql).map_err(|e| RustLakeError::DuckDb(format!("S3 config: {}", e)))?;
            Ok(())
        }).await.map_err(|e| RustLakeError::DuckDb(format!("spawn_blocking: {}", e)))?
    }

    /// Query an S3 Parquet file directly (no DataFusion sync needed).
    /// Returns "direct" execution mode — DuckDB reads from S3 itself.
    pub async fn query_s3_parquet(&self, s3_path: &str) -> Result<Vec<RecordBatch>> {
        let sql = format!("SELECT * FROM read_parquet('{}')", s3_path);
        self.sql(&sql).await
    }

    /// Query an Iceberg table directly from S3 metadata.
    /// Requires the iceberg extension to be loaded.
    pub async fn query_iceberg(&self, metadata_path: &str) -> Result<Vec<RecordBatch>> {
        let sql = format!("SELECT * FROM iceberg_scan('{}')", metadata_path);
        self.sql(&sql).await
    }

    /// Get DuckDB version string.
    pub fn version(&self) -> String {
        let conn = self.conn.lock().ok();
        match conn {
            Some(c) => {
                let mut stmt = match c.prepare("SELECT version()") {
                    Ok(s) => s,
                    Err(_) => return "unknown".to_string(),
                };
                let mut rows = match stmt.query([]) {
                    Ok(r) => r,
                    Err(_) => return "unknown".to_string(),
                };
                match rows.next() {
                    Ok(Some(row)) => {
                        row.get::<_, String>(0).unwrap_or_else(|_| "unknown".to_string())
                    }
                    _ => "unknown".to_string(),
                }
            }
            None => "unknown".to_string(),
        }
    }
}

/// Serialize our workspace arrow RecordBatches to IPC stream bytes.
fn batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| RustLakeError::DuckDb(format!("IPC write init: {}", e)))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| RustLakeError::DuckDb(format!("IPC write: {}", e)))?;
        }
        writer
            .finish()
            .map_err(|e| RustLakeError::DuckDb(format!("IPC finish: {}", e)))?;
    }
    Ok(buf)
}

/// Generate a DuckDB CREATE TABLE DDL statement from an Arrow schema.
fn arrow_schema_to_duckdb_ddl(table_name: &str, schema: &Schema) -> String {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let duckdb_type = arrow_type_to_duckdb(f.data_type());
            format!("\"{}\" {}", f.name(), duckdb_type)
        })
        .collect();

    format!("CREATE TABLE \"{}\" ({});", table_name, cols.join(", "))
}

/// Map Arrow DataType to DuckDB SQL type string.
fn arrow_type_to_duckdb(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 | DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Interval(_) => "INTERVAL",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
        _ => "VARCHAR", // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_arrow_schema_to_duckdb_ddl() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let ddl = arrow_schema_to_duckdb_ddl("test_table", &schema);
        assert!(ddl.contains("CREATE TABLE"));
        assert!(ddl.contains("BIGINT"));
        assert!(ddl.contains("VARCHAR"));
    }

    #[tokio::test]
    async fn test_duckdb_engine_basic() {
        let config = DuckDbEngineConfig {
            enabled: true,
            memory_limit: None,
            threads: None,
        };
        let engine = DuckDbEngine::new(&config).unwrap();
        let batches = engine.sql("SELECT 1 + 1 AS result").await.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_register_and_query() {
        let config = DuckDbEngineConfig {
            enabled: true,
            memory_limit: None,
            threads: None,
        };
        let engine = DuckDbEngine::new(&config).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();

        engine
            .register_arrow_table("users", &[batch])
            .await
            .unwrap();

        let result = engine
            .sql("SELECT COUNT(*) as cnt FROM users")
            .await
            .unwrap();
        assert_eq!(result[0].num_rows(), 1);
    }
}
