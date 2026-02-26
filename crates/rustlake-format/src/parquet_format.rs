//! Raw Parquet format — reads Parquet files directly without a table format layer.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rustlake_core::{Result, RustLakeError};

use crate::{FormatType, PartitionSpec, SnapshotInfo, TableFormat};

/// A table backed by raw Parquet files in a directory.
pub struct ParquetTableFormat {
    /// Path to the Parquet file or directory.
    path: String,
    /// Cached schema (populated on first access).
    schema: tokio::sync::OnceCell<SchemaRef>,
}

impl ParquetTableFormat {
    /// Create a new Parquet format table from a file path.
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            schema: tokio::sync::OnceCell::new(),
        }
    }
}

#[async_trait]
impl TableFormat for ParquetTableFormat {
    fn format_type(&self) -> FormatType {
        FormatType::Parquet
    }

    async fn schema(&self) -> Result<SchemaRef> {
        let path = self.path.clone();
        let schema = self
            .schema
            .get_or_try_init(|| async {
                // Read schema from the Parquet file metadata
                let file = tokio::fs::File::open(&path).await.map_err(|e| {
                    RustLakeError::Storage(format!("Failed to open '{}': {}", path, e))
                })?;
                let reader =
                    parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(file)
                        .await
                        .map_err(|e| {
                            RustLakeError::Storage(format!(
                                "Failed to read Parquet metadata: {}",
                                e
                            ))
                        })?;
                Ok(reader.schema().clone()) as Result<SchemaRef>
            })
            .await?;
        Ok(schema.clone())
    }

    async fn snapshots(&self) -> Result<Vec<SnapshotInfo>> {
        // Raw Parquet has no snapshots — return a single synthetic one
        Ok(vec![SnapshotInfo {
            snapshot_id: 0,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            manifest_list: self.path.clone(),
            summary: HashMap::from([("format".to_string(), "parquet".to_string())]),
        }])
    }

    async fn current_snapshot_id(&self) -> Result<Option<i64>> {
        Ok(Some(0))
    }

    async fn scan(
        &self,
        _projection: Option<Vec<String>>,
        _filters: Option<Vec<String>>,
        _limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let file = tokio::fs::File::open(&self.path).await.map_err(|e| {
            RustLakeError::Storage(format!("Failed to open '{}': {}", self.path, e))
        })?;

        let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|e| RustLakeError::Storage(format!("Parquet reader error: {}", e)))?;

        let mut stream = builder.build().map_err(|e| {
            RustLakeError::Storage(format!("Failed to build Parquet stream: {}", e))
        })?;

        use futures::StreamExt;
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| RustLakeError::Storage(format!("Parquet read error: {}", e)))?;
            batches.push(batch);
        }

        Ok(batches)
    }

    async fn append(&self, _batches: Vec<RecordBatch>) -> Result<SnapshotInfo> {
        Err(RustLakeError::Engine(
            "Append not supported for raw Parquet format".into(),
        ))
    }

    async fn partition_spec(&self) -> Result<Option<PartitionSpec>> {
        Ok(None)
    }

    async fn properties(&self) -> Result<HashMap<String, String>> {
        Ok(HashMap::from([
            ("format".to_string(), "parquet".to_string()),
            ("path".to_string(), self.path.clone()),
        ]))
    }
}
