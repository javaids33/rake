//! MongoDB Change Streams CDC source.
//!
//! Connects to a MongoDB collection (or database) and watches for changes via
//! Change Streams. Each change event is converted to an Arrow RecordBatch for
//! downstream processing. Supports resume tokens for fault-tolerant restart.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{StringBuilder, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::ChangeStreamOptions;
use mongodb::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};

use crate::mongodb_conn::MongoConnParams;

/// Configuration for a MongoDB CDC source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSourceConfig {
    /// Reference to an existing MongoDB connection ID.
    #[serde(default)]
    pub connection_id: String,
    /// Database name to watch.
    pub database: String,
    /// Collection name, or "*" to watch all collections in the database.
    #[serde(default = "default_collection")]
    pub collection: String,
    /// Full document mode: "updateLookup", "whenAvailable", "required", or "default".
    #[serde(default = "default_full_document")]
    pub full_document: String,
    /// Optional aggregation pipeline filters for the change stream.
    #[serde(default)]
    pub pipeline: Vec<Document>,
}

fn default_collection() -> String {
    "*".to_string()
}

fn default_full_document() -> String {
    "updateLookup".to_string()
}

/// Arrow schema for CDC change events.
pub fn cdc_event_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("operation_type", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("namespace_db", DataType::Utf8, true),
        Field::new("namespace_coll", DataType::Utf8, true),
        Field::new("document", DataType::Utf8, true),
        Field::new("update_description", DataType::Utf8, true),
    ]))
}

/// Convert an `OperationType` to its string representation.
fn operation_type_to_string(op: &OperationType) -> String {
    match op {
        OperationType::Insert => "insert".to_string(),
        OperationType::Update => "update".to_string(),
        OperationType::Replace => "replace".to_string(),
        OperationType::Delete => "delete".to_string(),
        OperationType::Drop => "drop".to_string(),
        OperationType::Rename => "rename".to_string(),
        OperationType::DropDatabase => "dropDatabase".to_string(),
        OperationType::Invalidate => "invalidate".to_string(),
        OperationType::Other(s) => s.clone(),
        // Forward-compatible catch-all for future variants
        _ => "unknown".to_string(),
    }
}

/// Convert a single `ChangeStreamEvent<Document>` into an Arrow RecordBatch row.
fn event_to_record_batch(
    schema: &SchemaRef,
    events: &[ChangeStreamEvent<Document>],
) -> Result<RecordBatch, String> {
    let n = events.len();

    let mut id_builder = StringBuilder::with_capacity(n, n * 64);
    let mut op_builder = StringBuilder::with_capacity(n, n * 16);
    let mut ts_builder = TimestampMicrosecondArray::builder(n);
    let mut ns_db_builder = StringBuilder::with_capacity(n, n * 32);
    let mut ns_coll_builder = StringBuilder::with_capacity(n, n * 32);
    let mut doc_builder = StringBuilder::with_capacity(n, n * 512);
    let mut update_builder = StringBuilder::with_capacity(n, n * 256);

    for event in events {
        // Resume token as JSON string for the _id column
        let id_str = serde_json::to_string(&event.id).unwrap_or_default();
        id_builder.append_value(&id_str);

        // Operation type
        op_builder.append_value(operation_type_to_string(&event.operation_type));

        // Timestamp from cluster_time (Timestamp { time, increment })
        if let Some(ref ts) = event.cluster_time {
            // cluster_time.time is seconds since epoch
            let micros = ts.time as i64 * 1_000_000;
            ts_builder.append_value(micros);
        } else if let Some(ref wt) = event.wall_time {
            // wall_time is a bson::DateTime — millis since epoch
            ts_builder.append_value(wt.timestamp_millis() * 1000);
        } else {
            ts_builder.append_null();
        }

        // Namespace
        if let Some(ref ns) = event.ns {
            ns_db_builder.append_value(&ns.db);
            match &ns.coll {
                Some(c) => ns_coll_builder.append_value(c),
                None => ns_coll_builder.append_null(),
            }
        } else {
            ns_db_builder.append_null();
            ns_coll_builder.append_null();
        }

        // Full document as JSON
        match &event.full_document {
            Some(doc) => doc_builder.append_value(
                serde_json::to_string(doc).unwrap_or_default(),
            ),
            None => doc_builder.append_null(),
        }

        // Update description as JSON
        match &event.update_description {
            Some(ud) => update_builder.append_value(
                serde_json::to_string(ud).unwrap_or_default(),
            ),
            None => update_builder.append_null(),
        }
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(op_builder.finish()),
            Arc::new(ts_builder.finish().with_timezone("UTC")),
            Arc::new(ns_db_builder.finish()),
            Arc::new(ns_coll_builder.finish()),
            Arc::new(doc_builder.finish()),
            Arc::new(update_builder.finish()),
        ],
    )
    .map_err(|e| format!("Failed to create CDC RecordBatch: {}", e))
}

/// A running MongoDB CDC source that watches a collection/database for changes.
///
/// Spawns a background tokio task that reads from a MongoDB Change Stream
/// and sends Arrow RecordBatches to a channel. Tracks resume tokens for
/// fault-tolerant restart.
pub struct MongoDbCdcSource {
    /// Whether the CDC source is currently running.
    running: Arc<AtomicBool>,
    /// Last known resume token for fault-tolerant restart.
    last_resume_token: Arc<RwLock<Option<ResumeToken>>>,
}

impl MongoDbCdcSource {
    /// Start a new MongoDB CDC source.
    ///
    /// Connects to the specified MongoDB database/collection and begins watching
    /// for change events. Returns a receiver channel for consuming CDC events
    /// as Arrow RecordBatches.
    ///
    /// # Arguments
    /// * `params` - MongoDB connection parameters
    /// * `config` - CDC source configuration (database, collection, pipeline, etc.)
    /// * `resume_token` - Optional resume token to continue from a previous position
    pub async fn start(
        params: &MongoConnParams,
        config: &CdcSourceConfig,
        resume_token: Option<ResumeToken>,
    ) -> Result<(Self, mpsc::Receiver<Result<RecordBatch, String>>), String> {
        let client = params.build_client().await?;
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, String>>(64);

        let running = Arc::new(AtomicBool::new(true));
        let last_resume_token = Arc::new(RwLock::new(resume_token.clone()));

        let schema = cdc_event_schema();
        let database = config.database.clone();
        let collection = config.collection.clone();
        let full_document_mode = config.full_document.clone();
        let pipeline: Vec<Document> = config.pipeline.clone();
        let running_clone = running.clone();
        let token_clone = last_resume_token.clone();
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            if let Err(e) = run_change_stream(
                client,
                &database,
                &collection,
                &full_document_mode,
                pipeline,
                resume_token,
                schema,
                running_clone,
                token_clone,
                tx_clone,
            )
            .await
            {
                tracing::error!(error = %e, "MongoDB CDC source failed");
            }
        });

        Ok((
            Self {
                running,
                last_resume_token,
            },
            rx,
        ))
    }

    /// Stop the CDC source.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check whether the CDC source is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get the last known resume token for fault-tolerant restart.
    #[allow(dead_code)]
    pub async fn last_resume_token(&self) -> Option<ResumeToken> {
        self.last_resume_token.read().await.clone()
    }
}

/// Internal: run the change stream loop.
async fn run_change_stream(
    client: Client,
    database: &str,
    collection: &str,
    full_document_mode: &str,
    pipeline: Vec<Document>,
    resume_token: Option<ResumeToken>,
    schema: SchemaRef,
    running: Arc<AtomicBool>,
    last_resume_token: Arc<RwLock<Option<ResumeToken>>>,
    tx: mpsc::Sender<Result<RecordBatch, String>>,
) -> Result<(), String> {
    use mongodb::options::FullDocumentType;

    // Build change stream options
    let fd_type = match full_document_mode {
        "updateLookup" => Some(FullDocumentType::UpdateLookup),
        "whenAvailable" => Some(FullDocumentType::WhenAvailable),
        "required" => Some(FullDocumentType::Required),
        _ => None,
    };
    let resume = resume_token.clone();
    let options = ChangeStreamOptions::builder()
        .full_document(fd_type)
        .resume_after(resume)
        .build();

    // Open change stream at collection or database level
    if collection == "*" {
        // Database-level watch — all collections
        let db = client.database(database);
        let mut stream = db
            .watch(pipeline, options)
            .await
            .map_err(|e| format!("Failed to open database change stream: {}", e))?;

        let mut batch_buffer: Vec<ChangeStreamEvent<Document>> = Vec::new();
        let batch_size = 100;
        let flush_interval = tokio::time::Duration::from_millis(500);

        while running.load(Ordering::SeqCst) {
            // Try to get next event with a timeout
            match tokio::time::timeout(flush_interval, stream.try_next()).await {
                Ok(Ok(Some(event))) => {
                    // Save resume token
                    {
                        let mut token = last_resume_token.write().await;
                        *token = Some(event.id.clone());
                    }
                    batch_buffer.push(event);

                    // Flush when batch is full
                    if batch_buffer.len() >= batch_size {
                        let batch = event_to_record_batch(&schema, &batch_buffer)?;
                        batch_buffer.clear();
                        if tx.send(Ok(batch)).await.is_err() {
                            break; // receiver dropped
                        }
                    }
                }
                Ok(Ok(None)) => {
                    // Stream ended
                    break;
                }
                Ok(Err(e)) => {
                    let _ = tx
                        .send(Err(format!("Change stream error: {}", e)))
                        .await;
                    break;
                }
                Err(_) => {
                    // Timeout — flush partial batch if any
                    if !batch_buffer.is_empty() {
                        let batch = event_to_record_batch(&schema, &batch_buffer)?;
                        batch_buffer.clear();
                        if tx.send(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }

        // Flush remaining events
        if !batch_buffer.is_empty() {
            if let Ok(batch) = event_to_record_batch(&schema, &batch_buffer) {
                let _ = tx.send(Ok(batch)).await;
            }
        }
    } else {
        // Collection-level watch
        let db = client.database(database);
        let coll = db.collection::<Document>(collection);
        let mut stream = coll
            .watch(pipeline, options)
            .await
            .map_err(|e| format!("Failed to open collection change stream: {}", e))?;

        let mut batch_buffer: Vec<ChangeStreamEvent<Document>> = Vec::new();
        let batch_size = 100;
        let flush_interval = tokio::time::Duration::from_millis(500);

        while running.load(Ordering::SeqCst) {
            match tokio::time::timeout(flush_interval, stream.try_next()).await {
                Ok(Ok(Some(event))) => {
                    {
                        let mut token = last_resume_token.write().await;
                        *token = Some(event.id.clone());
                    }
                    batch_buffer.push(event);

                    if batch_buffer.len() >= batch_size {
                        let batch = event_to_record_batch(&schema, &batch_buffer)?;
                        batch_buffer.clear();
                        if tx.send(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    let _ = tx
                        .send(Err(format!("Change stream error: {}", e)))
                        .await;
                    break;
                }
                Err(_) => {
                    if !batch_buffer.is_empty() {
                        let batch = event_to_record_batch(&schema, &batch_buffer)?;
                        batch_buffer.clear();
                        if tx.send(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }

        if !batch_buffer.is_empty() {
            if let Ok(batch) = event_to_record_batch(&schema, &batch_buffer) {
                let _ = tx.send(Ok(batch)).await;
            }
        }
    }

    tracing::info!(database = %database, collection = %collection, "MongoDB CDC source stopped");
    Ok(())
}
