//! S3 Parquet sink for CDC pipelines.
//!
//! Writes Arrow RecordBatches to Parquet files on S3. Each flush creates a new
//! Parquet file with a timestamped name. Buffers multiple batches before writing
//! to amortize S3 PUT overhead.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Configuration for an S3 Parquet sink.
pub struct ParquetSinkConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix within the bucket (e.g., "cdc/orders-stream").
    pub prefix: String,
    /// S3 endpoint URL (for MinIO/custom S3).
    pub endpoint: Option<String>,
    /// AWS access key.
    pub access_key: String,
    /// AWS secret key.
    pub secret_key: String,
    /// AWS region.
    pub region: String,
}

impl ParquetSinkConfig {
    /// Parse an S3 URI like "s3://bucket/prefix/path" into bucket + prefix.
    pub fn from_s3_uri(uri: &str, endpoint: Option<String>, access_key: String, secret_key: String, region: String) -> Result<Self, String> {
        let stripped = uri.strip_prefix("s3://").or_else(|| uri.strip_prefix("s3a://"))
            .ok_or_else(|| format!("Invalid S3 URI: {}", uri))?;
        let slash_pos = stripped.find('/').unwrap_or(stripped.len());
        let bucket = stripped[..slash_pos].to_string();
        let prefix = if slash_pos < stripped.len() {
            stripped[slash_pos + 1..].trim_end_matches('/').to_string()
        } else {
            String::new()
        };
        Ok(Self { bucket, prefix, endpoint, access_key, secret_key, region })
    }
}

/// A sink that writes Arrow RecordBatches to Parquet files on S3.
///
/// Buffers batches in memory and flushes when the buffer reaches `flush_threshold`
/// rows or when `flush()` is called explicitly.
pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    flush_threshold: usize,
    buffer: Vec<RecordBatch>,
    buffer_rows: usize,
    files_written: u64,
    total_rows_written: u64,
    /// Track data files written for Iceberg metadata generation
    data_files: Vec<crate::iceberg_writer::DataFileInfo>,
    /// Last flushed schema (for Iceberg metadata)
    last_schema: Option<Arc<arrow::datatypes::Schema>>,
    /// Bucket name for Iceberg metadata
    bucket: String,
}

impl ParquetSink {
    /// Create a new ParquetSink from config.
    pub fn new(config: &ParquetSinkConfig, flush_threshold: usize) -> Result<Self, String> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .with_allow_http(true);

        if let Some(ref ep) = config.endpoint {
            if !ep.is_empty() {
                builder = builder.with_endpoint(ep);
                // MinIO and S3-compatible stores need path-style requests
                builder = builder.with_virtual_hosted_style_request(false);
            }
        }

        let store = builder.build()
            .map_err(|e| format!("Failed to build S3 store: {}", e))?;

        tracing::info!(
            bucket = %config.bucket, prefix = %config.prefix,
            flush_threshold = flush_threshold,
            "ParquetSink created"
        );

        Ok(Self {
            store: Arc::new(store),
            prefix: config.prefix.clone(),
            flush_threshold,
            buffer: Vec::new(),
            buffer_rows: 0,
            files_written: 0,
            total_rows_written: 0,
            data_files: Vec::new(),
            last_schema: None,
            bucket: config.bucket.clone(),
        })
    }

    /// Add a batch to the buffer. Flushes automatically if threshold is reached.
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        let rows = batch.num_rows();
        self.buffer.push(batch);
        self.buffer_rows += rows;

        if self.buffer_rows >= self.flush_threshold {
            self.flush().await?;
        }
        Ok(())
    }

    /// Flush buffered batches to a Parquet file on S3.
    pub async fn flush(&mut self) -> Result<(), String> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let schema = self.buffer[0].schema();
        let rows = self.buffer_rows;

        // Write to in-memory Parquet buffer with optimized settings
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_bloom_filter_enabled(true)
            .set_max_row_group_size(1024 * 1024) // 1M rows per row group for better pruning
            .set_write_batch_size(8192)
            .build();

        let mut parquet_buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut parquet_buf, schema.clone(), Some(props))
                .map_err(|e| format!("Parquet writer init: {}", e))?;

            for batch in &self.buffer {
                writer.write(batch)
                    .map_err(|e| format!("Parquet write batch: {}", e))?;
            }

            writer.close()
                .map_err(|e| format!("Parquet close: {}", e))?;
        }

        let file_size = parquet_buf.len();
        let parquet_payload = object_store::PutPayload::from(parquet_buf);

        // Generate timestamped file name
        let now = chrono::Utc::now();
        let date_part = now.format("%Y-%m-%d").to_string();
        let time_part = now.format("%H%M%S").to_string();
        self.files_written += 1;
        let file_name = format!(
            "{}/{}/batch-{}-{:04}.parquet",
            self.prefix, date_part, time_part, self.files_written
        );

        // Upload to S3
        let path = ObjectPath::from(file_name.as_str());
        self.store.put(&path, parquet_payload)
            .await
            .map_err(|e| format!("S3 PUT failed for '{}': {}", file_name, e))?;

        self.total_rows_written += rows as u64;

        // Track for Iceberg metadata
        self.data_files.push(crate::iceberg_writer::DataFileInfo {
            file_path: file_name.clone(),
            file_size: file_size as u64,
            row_count: rows as u64,
        });
        self.last_schema = Some(schema);

        tracing::info!(
            file = %file_name,
            rows = rows,
            size_bytes = file_size,
            total_files = self.files_written,
            total_rows = self.total_rows_written,
            "ParquetSink: flushed to S3"
        );

        self.buffer.clear();
        self.buffer_rows = 0;
        Ok(())
    }

    /// Get total files written.
    pub fn files_written(&self) -> u64 {
        self.files_written
    }

    /// Get total rows written.
    pub fn total_rows_written(&self) -> u64 {
        self.total_rows_written
    }

    /// Write Iceberg v2 metadata to S3 for the data files written so far.
    /// This makes the table discoverable by Trino, Spark, and Iceberg-aware engines.
    ///
    /// If `existing_state` is provided, appends a new snapshot to the existing table.
    /// Otherwise, creates a new table with its first snapshot.
    pub async fn finalize_iceberg(&self) -> Result<String, String> {
        self.finalize_iceberg_incremental(None).await
    }

    /// Write Iceberg metadata, optionally appending to an existing table state.
    pub async fn finalize_iceberg_incremental(
        &self,
        existing_state: Option<&crate::iceberg_metadata::IcebergTableState>,
    ) -> Result<String, String> {
        if self.data_files.is_empty() {
            return Err("No data files to create Iceberg metadata for".into());
        }
        let schema = self.last_schema.as_ref()
            .ok_or_else(|| "No schema available for Iceberg metadata".to_string())?;

        match existing_state {
            Some(state) => {
                // Append new snapshot to existing table
                crate::iceberg_metadata::append_snapshot(
                    &self.store,
                    state,
                    &self.data_files,
                    schema,
                    "append",
                ).await
            }
            None => {
                // Create new table with first snapshot
                let table_uuid = uuid::Uuid::new_v4().to_string();
                crate::iceberg_writer::write_iceberg_metadata(
                    &self.store,
                    &self.prefix,
                    schema,
                    &self.data_files,
                    &table_uuid,
                    &self.bucket,
                ).await
            }
        }
    }

    /// Validate a batch against quality gates before writing.
    /// Returns Ok(()) if validation passes or no gates configured,
    /// Err with failure details if validation fails.
    pub fn validate_batch_quality(
        &self,
        batch: &RecordBatch,
        gate: Option<&crate::quality_gates::QualityGate>,
    ) -> Result<(), String> {
        if let Some(gate) = gate {
            let result = crate::quality_gates::validate_batch(gate, batch);
            if !result.passed {
                let failures: Vec<String> = result.failures.iter()
                    .map(|f| format!("{}: {}", f.check, f.message))
                    .collect();
                return Err(format!("Quality gate failed: {}", failures.join("; ")));
            }
        }
        Ok(())
    }

    /// Get a reference to the data files written so far.
    pub fn data_files(&self) -> &[crate::iceberg_writer::DataFileInfo] {
        &self.data_files
    }

    /// Get the underlying ObjectStore.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Get the prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Get the S3 URI prefix where Parquet files are written (for ListingTable registration).
    #[allow(dead_code)]
    pub fn s3_data_uri(&self, bucket: &str) -> String {
        format!("s3://{}/{}/", bucket, self.prefix)
    }

    /// Get the bucket name.
    #[allow(dead_code)]
    pub fn bucket(&self) -> &str {
        // Extract from store — we stored it in config but not on self.
        // Return empty for now; caller should use the config bucket.
        ""
    }

    /// Get a clone of the underlying ObjectStore for DataFusion registration.
    #[allow(dead_code)]
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }
}
