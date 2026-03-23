//! Streaming ingestion engine for RustLake.
//!
//! Provides connectors for Kafka, CDC (MongoDB, Postgres), and custom sources.
//! Materializes streaming data into Iceberg tables via `rustlake-format`.

pub mod connector;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod pipeline;
#[cfg(feature = "kafka")]
pub mod schema_registry;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustlake_core::Result;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// A source of streaming records.
#[async_trait]
pub trait StreamSource: Send + Sync {
    /// Get the name of this source.
    fn name(&self) -> &str;

    /// Start consuming records. Returns a receiver for incoming batches.
    async fn start(&self) -> Result<tokio::sync::mpsc::Receiver<Result<RecordBatch>>>;

    /// Stop the source.
    async fn stop(&self) -> Result<()>;

    /// Get the current consumer lag (if applicable).
    async fn lag(&self) -> Result<Option<u64>>;
}

/// A sink for writing streaming records.
#[async_trait]
pub trait StreamSink: Send + Sync {
    /// Get the name of this sink.
    fn name(&self) -> &str;

    /// Write a batch of records to the sink.
    async fn write(&self, batch: RecordBatch) -> Result<()>;

    /// Flush any buffered records.
    async fn flush(&self) -> Result<()>;

    /// Commit the current checkpoint offset.
    async fn commit(&self) -> Result<()>;
}

/// Metrics for a running stream pipeline.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamMetrics {
    /// Total records read from the source.
    pub records_ingested: u64,
    /// Total records written to the sink.
    pub records_written: u64,
    /// Total bytes read from the source.
    pub bytes_ingested: u64,
    /// Number of RecordBatch micro-batches processed.
    pub batches_processed: u64,
    /// Total errors encountered during pipeline execution.
    pub errors: u64,
    /// Current consumer lag in milliseconds (if available).
    pub lag_ms: Option<u64>,
}

/// A single streaming event representing e-commerce activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    /// Unique event identifier (UUID).
    pub event_id: String,
    /// Event type (e.g., "page_view", "purchase", "add_to_cart").
    pub event_type: String,
    /// Customer identifier who triggered the event.
    pub customer_id: u32,
    /// Browser session identifier.
    pub session_id: String,
    /// Product identifier, if applicable.
    pub product_id: Option<u32>,
    /// Page URL path where the event occurred.
    pub page: String,
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Additional event-specific properties as JSON.
    pub properties: serde_json::Value,
}

/// Aggregated streaming metrics tracked across all pipelines.
pub struct StreamingMetrics {
    /// Total events ingested across all pipelines.
    pub events_ingested: AtomicU64,
    /// Total bytes processed across all pipelines.
    pub bytes_processed: AtomicU64,
    /// Computed events per second (snapshot).
    pub events_per_second: std::sync::RwLock<f64>,
    /// Number of active pipelines.
    pub active_pipelines: std::sync::RwLock<usize>,
    /// Timestamp of the last event received.
    pub last_event_time: std::sync::RwLock<Option<DateTime<Utc>>>,
}

impl StreamingMetrics {
    /// Create a new `StreamingMetrics` instance with zeroed counters.
    pub fn new() -> Self {
        Self {
            events_ingested: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            events_per_second: std::sync::RwLock::new(0.0),
            active_pipelines: std::sync::RwLock::new(0),
            last_event_time: std::sync::RwLock::new(None),
        }
    }

    /// Record a batch of ingested events.
    pub fn record_ingestion(&self, event_count: u64, byte_count: u64, eps: f64) {
        self.events_ingested
            .fetch_add(event_count, Ordering::Relaxed);
        self.bytes_processed
            .fetch_add(byte_count, Ordering::Relaxed);
        if let Ok(mut rate) = self.events_per_second.write() {
            *rate = eps;
        }
        if let Ok(mut ts) = self.last_event_time.write() {
            *ts = Some(Utc::now());
        }
    }

    /// Get a serializable snapshot of the current metrics.
    pub fn snapshot(&self) -> StreamingMetricsSnapshot {
        StreamingMetricsSnapshot {
            events_ingested: self.events_ingested.load(Ordering::Relaxed),
            bytes_processed: self.bytes_processed.load(Ordering::Relaxed),
            events_per_second: self.events_per_second.read().map(|r| *r).unwrap_or(0.0),
            active_pipelines: self.active_pipelines.read().map(|r| *r).unwrap_or(0),
            last_event_time: self.last_event_time.read().ok().and_then(|r| *r),
        }
    }
}

impl Default for StreamingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A serializable snapshot of streaming metrics.
#[derive(Debug, Clone, Serialize)]
pub struct StreamingMetricsSnapshot {
    /// Total events ingested since startup.
    pub events_ingested: u64,
    /// Total bytes processed since startup.
    pub bytes_processed: u64,
    /// Current throughput in events per second.
    pub events_per_second: f64,
    /// Number of currently active pipelines.
    pub active_pipelines: usize,
    /// Timestamp of the last event received, if any.
    pub last_event_time: Option<DateTime<Utc>>,
}
