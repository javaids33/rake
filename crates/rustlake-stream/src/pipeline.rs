//! Stream processing pipeline — connects sources to sinks with optional transforms.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rustlake_core::Result;
use tracing;

use crate::{StreamMetrics, StreamSink, StreamSource, StreamingMetrics};

/// A streaming pipeline that reads from a source and writes to a sink.
pub struct StreamPipeline {
    name: String,
    source: Arc<dyn StreamSource>,
    sink: Arc<dyn StreamSink>,
    metrics: Arc<PipelineMetrics>,
    global_metrics: Option<Arc<StreamingMetrics>>,
}

struct PipelineMetrics {
    records_ingested: AtomicU64,
    records_written: AtomicU64,
    bytes_ingested: AtomicU64,
    batches_processed: AtomicU64,
    errors: AtomicU64,
}

impl StreamPipeline {
    /// Create a new pipeline connecting source to sink.
    pub fn new(name: &str, source: Arc<dyn StreamSource>, sink: Arc<dyn StreamSink>) -> Self {
        Self {
            name: name.to_string(),
            source,
            sink,
            metrics: Arc::new(PipelineMetrics {
                records_ingested: AtomicU64::new(0),
                records_written: AtomicU64::new(0),
                bytes_ingested: AtomicU64::new(0),
                batches_processed: AtomicU64::new(0),
                errors: AtomicU64::new(0),
            }),
            global_metrics: None,
        }
    }

    /// Create a new pipeline with global streaming metrics tracking.
    pub fn with_metrics(
        name: &str,
        source: Arc<dyn StreamSource>,
        sink: Arc<dyn StreamSink>,
        global_metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            name: name.to_string(),
            source,
            sink,
            metrics: Arc::new(PipelineMetrics {
                records_ingested: AtomicU64::new(0),
                records_written: AtomicU64::new(0),
                bytes_ingested: AtomicU64::new(0),
                batches_processed: AtomicU64::new(0),
                errors: AtomicU64::new(0),
            }),
            global_metrics: Some(global_metrics),
        }
    }

    /// Run the pipeline until stopped or error.
    pub async fn run(&self) -> Result<()> {
        tracing::info!(pipeline = %self.name, source = %self.source.name(), sink = %self.sink.name(), "Starting stream pipeline");

        // Mark pipeline as active in global metrics
        if let Some(ref gm) = self.global_metrics {
            if let Ok(mut count) = gm.active_pipelines.write() {
                *count += 1;
            }
        }

        let mut rx = self.source.start().await?;

        while let Some(batch_result) = rx.recv().await {
            match batch_result {
                Ok(batch) => {
                    let num_rows = batch.num_rows() as u64;
                    self.metrics
                        .records_ingested
                        .fetch_add(num_rows, Ordering::Relaxed);

                    self.sink.write(batch).await.inspect_err(|_| {
                        self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                    })?;

                    self.metrics
                        .records_written
                        .fetch_add(num_rows, Ordering::Relaxed);
                    self.metrics
                        .batches_processed
                        .fetch_add(1, Ordering::Relaxed);

                    // Update global metrics if available
                    if let Some(ref gm) = self.global_metrics {
                        gm.record_ingestion(num_rows, 0, num_rows as f64);
                    }
                }
                Err(e) => {
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(pipeline = %self.name, error = %e, "Stream error");
                }
            }
        }

        self.sink.flush().await?;
        self.sink.commit().await?;

        // Mark pipeline as inactive in global metrics
        if let Some(ref gm) = self.global_metrics {
            if let Ok(mut count) = gm.active_pipelines.write() {
                *count = count.saturating_sub(1);
            }
        }

        tracing::info!(pipeline = %self.name, "Pipeline stopped");
        Ok(())
    }

    /// Get current pipeline metrics.
    pub fn metrics(&self) -> StreamMetrics {
        StreamMetrics {
            records_ingested: self.metrics.records_ingested.load(Ordering::Relaxed),
            records_written: self.metrics.records_written.load(Ordering::Relaxed),
            bytes_ingested: self.metrics.bytes_ingested.load(Ordering::Relaxed),
            batches_processed: self.metrics.batches_processed.load(Ordering::Relaxed),
            errors: self.metrics.errors.load(Ordering::Relaxed),
            lag_ms: None,
        }
    }
}
