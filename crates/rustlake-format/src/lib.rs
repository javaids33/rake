//! Unified table format layer for RustLake.
//!
//! Wraps Iceberg, Delta, Lance, and raw Parquet formats behind a common
//! [`TableFormat`] trait so the engine layer doesn't care which format
//! backs a table.

pub mod parquet_format;
pub mod snapshot;

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rustlake_core::Result;

/// Metadata about a table snapshot (point-in-time version).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SnapshotInfo {
    /// Unique identifier for this snapshot.
    pub snapshot_id: i64,
    /// Unix timestamp in milliseconds when this snapshot was created.
    pub timestamp_ms: i64,
    /// Path to the manifest list file for this snapshot.
    pub manifest_list: String,
    /// Key-value summary metadata (e.g., record count, added files).
    pub summary: std::collections::HashMap<String, String>,
}

/// Partition specification for a table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionSpec {
    /// Ordered list of partition fields.
    pub fields: Vec<PartitionField>,
}

/// A single partition field.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionField {
    /// Name of the source column to partition on.
    pub source_column: String,
    /// Transform to apply to the source column value.
    pub transform: PartitionTransform,
    /// Name of the resulting partition field.
    pub name: String,
}

/// Partition transform types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PartitionTransform {
    /// No transformation; partition on the raw column value.
    Identity,
    /// Extract the year from a date/timestamp column.
    Year,
    /// Extract the month from a date/timestamp column.
    Month,
    /// Extract the day from a date/timestamp column.
    Day,
    /// Extract the hour from a timestamp column.
    Hour,
    /// Hash the value into the specified number of buckets.
    Bucket(u32),
    /// Truncate the value to the specified width.
    Truncate(u32),
}

/// Format type identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FormatType {
    /// Apache Iceberg table format.
    Iceberg,
    /// Delta Lake table format.
    Delta,
    /// Lance columnar format (optimized for vector/AI workloads).
    Lance,
    /// Raw Parquet files (no table format metadata).
    Parquet,
}

impl std::fmt::Display for FormatType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Iceberg => write!(f, "iceberg"),
            Self::Delta => write!(f, "delta"),
            Self::Lance => write!(f, "lance"),
            Self::Parquet => write!(f, "parquet"),
        }
    }
}

/// Unified interface for all table formats.
///
/// Implementations handle format-specific details (metadata parsing,
/// snapshot management, schema evolution) while exposing a common API
/// to the engine layer.
#[async_trait]
pub trait TableFormat: Send + Sync {
    /// Get the format type.
    fn format_type(&self) -> FormatType;

    /// Get the current table schema.
    async fn schema(&self) -> Result<SchemaRef>;

    /// List available snapshots (versions) of this table.
    async fn snapshots(&self) -> Result<Vec<SnapshotInfo>>;

    /// Get the current snapshot ID.
    async fn current_snapshot_id(&self) -> Result<Option<i64>>;

    /// Scan the table and return data as Arrow RecordBatches.
    async fn scan(
        &self,
        projection: Option<Vec<String>>,
        filters: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>>;

    /// Append data to the table.
    async fn append(&self, batches: Vec<RecordBatch>) -> Result<SnapshotInfo>;

    /// Get the partition spec.
    async fn partition_spec(&self) -> Result<Option<PartitionSpec>>;

    /// Get table properties as key-value pairs.
    async fn properties(&self) -> Result<std::collections::HashMap<String, String>>;
}

/// Create a DataFusion TableProvider from a TableFormat implementation.
pub fn as_table_provider(
    _format: Arc<dyn TableFormat>,
) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
    // TODO: Implement a bridge from TableFormat to DataFusion TableProvider
    Err(rustlake_core::RustLakeError::Engine(
        "TableFormat → TableProvider bridge not yet implemented".into(),
    ))
}
