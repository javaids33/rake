use std::sync::atomic::AtomicU64;
use std::time::Instant;

use chrono::{DateTime, Utc};
use rustlake_engine::RustLakeContext;
use rustlake_stream::{StreamEvent, StreamingMetrics};
use rustlake_vector::embedding::SimpleEmbeddingGenerator;
use rustlake_vector::search::VectorIndex;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Maximum number of query history entries to retain in memory.
const MAX_QUERY_HISTORY: usize = 1000;

/// Maximum number of stream events to retain in the circular buffer.
const MAX_STREAM_EVENTS: usize = 10000;

/// Maximum number of engine performance records to retain.
const MAX_ENGINE_HISTORY: usize = 500;

/// Maximum number of chat messages to retain in memory.
const MAX_CHAT_MESSAGES: usize = 500;

/// Path to the persistent chat log file.
const CHAT_LOG_PATH: &str = "feedback.jsonl";

/// Path to the persistent user transforms file.
const TRANSFORMS_PATH: &str = "user_transforms.jsonl";

/// Path to the persistent scheduled jobs file.
const JOBS_PATH: &str = "scheduled_jobs.jsonl";

/// Path to the persistent connections file.
const CONNECTIONS_PATH: &str = "connections.jsonl";

/// A single entry in the query history log.
#[derive(Debug, Clone, Serialize)]
pub struct QueryHistoryEntry {
    /// Unique query execution ID.
    pub query_id: Uuid,
    /// The SQL query that was executed.
    pub sql: String,
    /// Classified query type (e.g., "OLAP", "Interactive").
    pub query_type: String,
    /// Number of rows returned.
    pub row_count: usize,
    /// Execution duration in milliseconds.
    pub duration_ms: u128,
    /// When the query was executed.
    pub timestamp: DateTime<Utc>,
    /// Execution status ("success" or "error").
    pub status: String,
    /// Error message, if the query failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Which engine executed this query ("DataFusion" or "DuckDB").
    #[serde(default = "default_engine_name")]
    pub engine: String,
}

#[allow(dead_code)] // Used by serde(default) on QueryHistoryEntry::engine
fn default_engine_name() -> String {
    "DataFusion".to_string()
}

/// A chat message in the feedback channel.
///
/// Supports multiple sender types for two-way communication:
/// - `"user"` — feedback from UI users or Claude Web
/// - `"developer"` — responses from Claude Code after completing work
/// - `"system"` — automated status messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Unique message ID.
    pub id: Uuid,
    /// The message text.
    pub message: String,
    /// Who sent this: "user", "developer", or "system".
    pub sender: String,
    /// Category for user messages: "bug", "feature", "general".
    /// For developer messages: "completed", "in_progress", "info".
    pub category: String,
    /// When the message was sent.
    pub timestamp: DateTime<Utc>,
}

/// Legacy alias kept for backwards-compatible deserialization of old feedback.jsonl entries.
#[derive(Deserialize)]
struct LegacyFeedbackEntry {
    id: Uuid,
    message: String,
    category: String,
    timestamp: DateTime<Utc>,
}

/// Flexible entry for developer-appended feedback.jsonl entries that may use
/// integer IDs, `"source"` instead of `"sender"`, or other variations.
#[derive(Deserialize)]
struct FlexibleFeedbackEntry {
    #[serde(default)]
    id: Option<serde_json::Value>,
    message: String,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    sender: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    category: Option<String>,
    timestamp: DateTime<Utc>,
}

/// A registered external database connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionEntry {
    /// Unique connection ID.
    pub id: String,
    /// User-assigned connection name.
    pub name: String,
    /// Connection type (e.g., "postgres").
    pub conn_type: String,
    /// Database host.
    pub host: String,
    /// Database port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Database username.
    pub username: String,
    /// Connection status: "connected" or "error".
    pub status: String,
    /// Tables discovered in the database.
    pub tables: Vec<String>,
    /// When the connection was established.
    pub created_at: DateTime<Utc>,
    /// How this connection was added: "bootstrap" (auto) or "user" (manual).
    pub source: String,
    /// Sync status for async onboarding: "ready", "syncing", "error".
    #[serde(default = "default_sync_ready")]
    pub sync_status: String,
    /// Error message if sync failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_error: Option<String>,
    /// Progress detail for syncing connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_progress: Option<String>,
    /// Authentication method: "scram" (default), "aws_iam", "x509", "connection_string".
    #[serde(default = "default_auth_method")]
    pub auth_method: String,
    /// Raw connection string (for auth_method = "connection_string").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_string: Option<String>,
}

fn default_sync_ready() -> String {
    "ready".to_string()
}

fn default_auth_method() -> String {
    "scram".to_string()
}

/// A user-created transform model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTransform {
    pub name: String,
    pub sql: String,
    pub depends_on: Vec<String>,
    pub materialization: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
}

/// Configuration for event-based triggers (e.g., file arrival).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfig {
    /// Event type (e.g., "file_arrival").
    pub event_type: String,
    /// Watch path (e.g., "uploads/", "s3://bucket/incoming/").
    pub path: String,
    /// Optional glob pattern to filter files (e.g., "*.csv").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_pattern: Option<String>,
}

/// A dedicated job cluster for resource isolation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCluster {
    /// Cluster name (e.g., "default", "high-memory", "gpu").
    pub name: String,
    /// Maximum number of concurrent jobs on this cluster.
    pub max_concurrent: u32,
    /// Human-readable description.
    pub description: String,
}

/// A scheduled job entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledJob {
    pub id: String,
    pub name: String,
    pub job_type: String,
    pub cron: String,
    pub target: String,
    pub enabled: bool,
    pub last_run: Option<DateTime<Utc>>,
    pub next_run: Option<DateTime<Utc>>,
    pub last_status: Option<String>,
    pub created_at: DateTime<Utc>,
    /// Target engine: "auto", "datafusion", "duckdb", or "polars".
    #[serde(default = "default_auto_engine")]
    pub engine: String,
    /// Trigger type: "time", "event", or "continuous".
    #[serde(default = "default_trigger_type")]
    pub trigger_type: String,
    /// Configuration for event-based triggers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_config: Option<EventConfig>,
    /// Job cluster name (e.g., "default", "high-memory").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// Maximum execution time in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// Number of retries on failure.
    #[serde(default)]
    pub retries: u32,
    /// User-defined tags for filtering.
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_auto_engine() -> String {
    "auto".to_string()
}

fn default_trigger_type() -> String {
    "time".to_string()
}

/// A user-created streaming pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingPipeline {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub source_config: serde_json::Value,
    pub transform_sql: Option<String>,
    pub sink_table: String,
    pub status: String,
    pub events_processed: u64,
    pub created_at: DateTime<Utc>,
}

/// A single job execution record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunEntry {
    pub job_id: String,
    pub job_name: String,
    pub target: String,
    pub status: String,
    pub duration_ms: u128,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// Maximum number of job run history entries to retain.
const MAX_JOB_RUNS: usize = 500;

/// S3/MinIO object storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub name: String,
    pub endpoint: String,
    pub access_key: String,
    #[serde(skip_serializing)]
    #[allow(dead_code)] // Written on creation, read when initiating S3 operations
    pub secret_key: String,
    pub bucket: String,
    pub region: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    /// Iceberg tables discovered via S3 warehouse scan.
    #[serde(default)]
    pub tables: Vec<String>,
    /// Table type info from Iceberg metadata properties (table_name → table_type, e.g. "MATERIALIZED_VIEW").
    #[serde(default)]
    pub table_types: std::collections::HashMap<String, String>,
    /// Table format info: table_name → format (e.g. "iceberg", "delta", "parquet", "hudi").
    #[serde(default)]
    pub table_formats: std::collections::HashMap<String, String>,
    /// Sync status for async discovery: "ready", "syncing", "error".
    #[serde(default = "default_sync_ready")]
    pub sync_status: String,
    /// Error message if sync/discovery failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_error: Option<String>,
    /// Current scan progress phase and detail for real-time UI updates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_progress: Option<String>,
    /// Scan progress detail (e.g., "Found iceberg (sales.orders)").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_detail: Option<String>,
    /// Tables scanned so far (numerator for progress bar).
    #[serde(default)]
    pub scan_scanned: usize,
    /// Total directories to scan (denominator for progress bar).
    #[serde(default)]
    pub scan_total: usize,
    /// Tables found so far during scan.
    #[serde(default)]
    pub scan_found: usize,
    /// Elapsed scan time in ms.
    #[serde(default)]
    pub scan_elapsed_ms: u128,
    /// Format breakdown during/after scan.
    #[serde(default)]
    pub format_counts: std::collections::HashMap<String, usize>,
}

/// S3 credentials for a set of buckets, fetched from the credentials API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BucketCreds {
    pub account_id: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub region: String,
}

/// Fetch S3 credentials from an external API and build a bucket→creds map.
///
/// The API returns JSON like:
/// ```json
/// {
///   "account-uuid": {
///     "access_key": "...",
///     "secret_key": "...",
///     "session_token": null,
///     "bucket_names": ["bucket-a", "bucket-b"]
///   }
/// }
/// ```
pub async fn fetch_s3_credentials_from_api(
    url: &str,
) -> Result<std::collections::HashMap<String, S3BucketCreds>, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("HTTP client build failed: {}", e))?;

    let resp = client.get(url)
        .send().await
        .map_err(|e| format!("Failed to fetch S3 credentials from {}: {}", url, e))?;

    if !resp.status().is_success() {
        return Err(format!("S3 credentials API returned HTTP {}", resp.status()));
    }

    let body: serde_json::Value = resp.json().await
        .map_err(|e| format!("Failed to parse S3 credentials JSON: {}", e))?;

    let mut bucket_map: std::collections::HashMap<String, S3BucketCreds> = std::collections::HashMap::new();
    let region = std::env::var("RUSTLAKE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    if let Some(obj) = body.as_object() {
        for (account_id, account_data) in obj {
            let access_key = account_data.get("access_key")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let secret_key = account_data.get("secret_key")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let session_token = account_data.get("session_token")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if access_key.is_empty() || secret_key.is_empty() {
                tracing::warn!(account_id = %account_id, "Skipping account: missing access_key or secret_key");
                continue;
            }

            let buckets = account_data.get("bucket_names")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let creds = S3BucketCreds {
                account_id: account_id.clone(),
                access_key,
                secret_key,
                session_token,
                region: region.clone(),
            };

            for bucket in &buckets {
                bucket_map.insert(bucket.clone(), creds.clone());
            }

            tracing::info!(
                account_id = %account_id,
                buckets = buckets.len(),
                bucket_names = ?buckets,
                "Loaded S3 credentials for {} buckets",
                buckets.len()
            );
        }
    }

    Ok(bucket_map)
}

/// A table discovered from a Trino Iceberg catalog for migration to Rake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTable {
    /// Connection ID for the source Trino instance.
    pub conn_id: String,
    /// Trino catalog name (e.g., "iceberg", "hive").
    pub catalog: String,
    /// Schema within the catalog.
    pub schema_name: String,
    /// Table name.
    pub table_name: String,
    /// Table format: "iceberg", "hive", "delta", "jdbc", "tpch", etc.
    pub format: String,
    /// S3 location of the table data (e.g., "s3://bucket/warehouse/table").
    pub location: Option<String>,
    /// Hive Metastore URI if applicable (e.g., "thrift://host:9083").
    pub metastore_uri: Option<String>,
    /// Number of columns in the table.
    pub column_count: usize,
    /// Approximate row count (if known).
    pub row_count: Option<u64>,
    /// Whether this table has been registered in Rake's DataFusion catalog.
    pub registered_in_rake: bool,
    /// Name of the table in Rake's catalog (e.g., "iceberg.schema.table").
    pub rake_table_name: Option<String>,
    /// Discovery/registration status: "discovered", "registered", "ready", "error".
    pub status: String,
    /// Error message if status is "error".
    pub error: Option<String>,
}

/// Per-engine result from a migration comparison run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineResult {
    pub engine: String,
    pub duration_ms: u64,
    pub row_count: usize,
    pub status: String,
    pub error: Option<String>,
    /// Data path used for this engine result: "via_trino", "s3_direct", "in_memory".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// A migration comparison entry (Trino vs local engines).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationComparison {
    pub id: String,
    pub sql: String,
    pub results: Vec<EngineResult>,
    pub winner: String,
    pub speedup: f64,
    pub data_match: bool,
    pub timestamp: DateTime<Utc>,
}

/// A record of engine performance for a single query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineLatencyRecord {
    /// Engine name: "DataFusion", "DuckDB", "Polars", "Trino".
    pub engine: String,
    /// Classified query type: "scan_aggregate", "join", "complex_join", "point_lookup", "ordered_scan", "full_scan".
    pub query_type: String,
    /// Execution duration in milliseconds.
    pub duration_ms: u64,
    /// Number of rows returned.
    pub row_count: usize,
    /// Approximate data size in bytes.
    pub data_size_bytes: u64,
    /// Data path used: "s3_direct", "in_memory", "via_trino", "trino_native".
    pub path: String,
    /// ISO 8601 timestamp.
    pub timestamp: String,
}

/// Tracks per-engine performance history for adaptive routing decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnginePerformanceTracker {
    /// Per-engine latency history records.
    pub history: Vec<EngineLatencyRecord>,
}

impl EnginePerformanceTracker {
    /// Create an empty performance tracker.
    pub fn new() -> Self {
        Self { history: Vec::new() }
    }

    /// Record an engine execution result.
    pub fn record(&mut self, record: EngineLatencyRecord) {
        self.history.push(record);
        if self.history.len() > MAX_ENGINE_HISTORY {
            let drain_count = self.history.len() - MAX_ENGINE_HISTORY;
            self.history.drain(..drain_count);
        }
    }

    /// Average latency in ms for a given engine and query type.
    /// Returns None if no history exists for the combination.
    pub fn avg_latency(&self, engine: &str, query_type: &str) -> Option<f64> {
        let matching: Vec<u64> = self.history.iter()
            .filter(|r| r.engine == engine && r.query_type == query_type && r.duration_ms > 0)
            .map(|r| r.duration_ms)
            .collect();
        if matching.is_empty() {
            None
        } else {
            Some(matching.iter().sum::<u64>() as f64 / matching.len() as f64)
        }
    }

    /// Check if we have any history for a given query type.
    pub fn has_history_for(&self, query_type: &str) -> bool {
        self.history.iter().any(|r| r.query_type == query_type)
    }
}

/// An engine recommendation for a query, including strategy and explanation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRecommendation {
    /// Orchestration strategy: "single_engine", "scan_handoff", "parallel_fanout".
    pub strategy: String,
    /// Recommended primary engine.
    pub primary_engine: String,
    /// Explanation of why this engine/strategy was chosen.
    pub reason: String,
    /// Estimated speedup vs Trino baseline.
    pub estimated_speedup: f64,
    /// For scan_handoff: which engine scans the data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_engine: Option<String>,
    /// For scan_handoff: which engine processes/joins the data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_engine: Option<String>,
}

/// An alternative orchestration strategy shown alongside the primary recommendation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlternativeStrategy {
    /// Strategy name.
    pub strategy: String,
    /// Human-readable description.
    pub description: String,
    /// When to use this strategy.
    pub when: String,
}

/// Shared application state for the API server.
pub struct AppState {
    /// The query engine context shared across all requests.
    pub ctx: RwLock<RustLakeContext>,
    /// Federated data provider registry (Postgres, MySQL, SQLite, etc.).
    pub provider_registry: crate::providers::ProviderRegistry,
    /// Optional DuckDB OLAP accelerator engine.
    #[cfg(feature = "duckdb")]
    pub duckdb_engine: Option<rustlake_engine::duckdb_engine::DuckDbEngine>,
    /// Optional Polars DataFrame engine.
    #[cfg(feature = "polars")]
    pub polars_engine: Option<rustlake_engine::polars_engine::PolarsEngine>,
    /// Live metrics from the Arrow Flight gRPC server (None if Flight is disabled).
    pub flight_metrics: Option<rustlake_flight::server::FlightMetrics>,
    /// Coordinator handle for cluster management (None if not a coordinator).
    pub coordinator: Option<std::sync::Arc<rustlake_flight::coordinator::Coordinator>>,
    /// In-memory log of recent query executions.
    pub query_history: RwLock<Vec<QueryHistoryEntry>>,
    /// Server startup time (for uptime calculation).
    pub start_time: Instant,
    /// Total number of queries executed since startup.
    pub query_count: AtomicU64,
    /// Circular buffer of recent stream events (max `MAX_STREAM_EVENTS`).
    pub stream_events: RwLock<Vec<StreamEvent>>,
    /// Aggregated streaming metrics.
    pub stream_metrics: StreamingMetrics,
    /// In-memory vector index for semantic search.
    pub vector_index: RwLock<VectorIndex>,
    /// Embedding generator used for vector operations.
    pub embedding_generator: SimpleEmbeddingGenerator,
    /// In-memory chat message log (feedback + developer responses).
    pub chat_messages: RwLock<Vec<ChatMessage>>,
    /// External database connections.
    pub connections: RwLock<Vec<ConnectionEntry>>,
    /// Connection passwords keyed by connection ID (not serialized).
    pub connection_passwords: RwLock<std::collections::HashMap<String, String>>,
    /// User-created transforms (stored alongside built-in defaults).
    pub user_transforms: RwLock<Vec<UserTransform>>,
    /// Scheduled jobs (cron-based).
    pub scheduled_jobs: RwLock<Vec<ScheduledJob>>,
    /// User-created streaming pipelines.
    pub streaming_pipelines: RwLock<Vec<StreamingPipeline>>,
    /// S3/MinIO storage configurations.
    pub s3_configs: RwLock<Vec<S3Config>>,
    /// Job execution history log.
    pub job_runs: RwLock<Vec<JobRunEntry>>,
    /// Dedicated job clusters for resource isolation.
    pub job_clusters: RwLock<Vec<JobCluster>>,
    /// User-defined table descriptions (table_name → description).
    pub table_descriptions: RwLock<std::collections::HashMap<String, String>>,
    /// User-defined column descriptions (table_name.column_name → description).
    pub column_descriptions: RwLock<std::collections::HashMap<String, String>>,
    /// Data quality alert rules.
    pub quality_rules: RwLock<Vec<crate::routes::QualityRule>>,
    /// Uploaded dbt project (at most one at a time).
    pub dbt_project: RwLock<Option<crate::routes::DbtProject>>,
    /// Benchmark run results (TPC-H query timings).
    pub benchmark_results: RwLock<Vec<crate::routes::BenchmarkResult>>,
    /// DuckDB-backed Trino metadata cache (shared across all Trino connections).
    #[cfg(feature = "duckdb")]
    pub trino_cache: Option<std::sync::Arc<crate::trino_client::TrinoCache>>,
    /// Active Trino connections keyed by connection ID.
    pub trino_connections: RwLock<std::collections::HashMap<String, std::sync::Arc<crate::trino_client::TrinoConnection>>>,
    /// Tables discovered from Trino for migration comparison.
    pub migration_tables: RwLock<Vec<MigrationTable>>,
    /// Migration comparison results (Trino vs local engines).
    pub migration_comparisons: RwLock<Vec<MigrationComparison>>,
    /// S3 credentials for migration (bucket_name -> S3BucketCreds).
    /// Auto-populated from RUSTLAKE_S3_CREDENTIALS_URL or manually via POST /api/v1/migration/credentials.
    pub migration_s3_creds: RwLock<std::collections::HashMap<String, S3BucketCreds>>,
    /// Engine performance tracker for adaptive routing decisions.
    pub engine_tracker: RwLock<EnginePerformanceTracker>,
    /// Read-only tables from migration — DDL/DML blocked to protect source data.
    pub read_only_tables: RwLock<std::collections::HashSet<String>>,
    /// Encrypted credential store for persisting passwords and S3 credentials.
    pub credential_store: crate::credential_store::CredentialStore,
    /// Active MongoDB CDC sources keyed by pipeline ID.
    pub cdc_sources: RwLock<std::collections::HashMap<String, std::sync::Arc<crate::mongodb_cdc::MongoDbCdcSource>>>,
}

impl AppState {
    /// Whether DuckDB is available as an OLAP accelerator.
    pub fn duckdb_available(&self) -> bool {
        #[cfg(feature = "duckdb")]
        {
            self.duckdb_engine.is_some()
        }
        #[cfg(not(feature = "duckdb"))]
        {
            false
        }
    }

    /// Whether Polars is available as a DataFrame engine.
    pub fn polars_available(&self) -> bool {
        #[cfg(feature = "polars")]
        {
            self.polars_engine.is_some()
        }
        #[cfg(not(feature = "polars"))]
        {
            false
        }
    }

    /// Create a new AppState with an empty vector index.
    #[allow(dead_code)]
    pub fn new(ctx: RustLakeContext) -> Self {
        let embedding_generator = SimpleEmbeddingGenerator::new(128);
        Self {
            ctx: RwLock::new(ctx),
            provider_registry: crate::providers::ProviderRegistry::new(),
            #[cfg(feature = "duckdb")]
            duckdb_engine: None,
            #[cfg(feature = "polars")]
            polars_engine: None,
            flight_metrics: None,
            coordinator: None,
            query_history: RwLock::new(Vec::new()),
            start_time: Instant::now(),
            query_count: AtomicU64::new(0),
            stream_events: RwLock::new(Vec::new()),
            stream_metrics: StreamingMetrics::new(),
            vector_index: RwLock::new(VectorIndex::new(128)),
            embedding_generator,
            chat_messages: RwLock::new(Vec::new()),
            connections: RwLock::new(Vec::new()),
            connection_passwords: RwLock::new(std::collections::HashMap::new()),
            user_transforms: RwLock::new(Vec::new()),
            scheduled_jobs: RwLock::new(Vec::new()),
            streaming_pipelines: RwLock::new(Vec::new()),
            s3_configs: RwLock::new(Vec::new()),
            job_runs: RwLock::new(Vec::new()),
            job_clusters: RwLock::new(default_job_clusters()),
            table_descriptions: RwLock::new(std::collections::HashMap::new()),
            column_descriptions: RwLock::new(std::collections::HashMap::new()),
            quality_rules: RwLock::new(Vec::new()),
            dbt_project: RwLock::new(None),
            benchmark_results: RwLock::new(Vec::new()),
            #[cfg(feature = "duckdb")]
            trino_cache: {
                match crate::trino_client::TrinoCache::new("trino_cache.duckdb") {
                    Ok(c) => { tracing::info!("Trino DuckDB cache initialized"); Some(std::sync::Arc::new(c)) }
                    Err(e) => { tracing::warn!("Trino cache init failed: {}", e); None }
                }
            },
            trino_connections: RwLock::new(std::collections::HashMap::new()),
            migration_tables: RwLock::new(Vec::new()),
            migration_comparisons: RwLock::new(Vec::new()),
            migration_s3_creds: RwLock::new(std::collections::HashMap::new()),
            engine_tracker: RwLock::new(EnginePerformanceTracker::new()),
            read_only_tables: RwLock::new(std::collections::HashSet::new()),
            credential_store: crate::credential_store::CredentialStore::new(),
            cdc_sources: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Create a new AppState with a pre-populated vector index.
    pub fn with_vector_index(ctx: RustLakeContext, index: VectorIndex) -> Self {
        let dimensions = index.dimensions();
        let embedding_generator = SimpleEmbeddingGenerator::new(dimensions);
        Self {
            ctx: RwLock::new(ctx),
            provider_registry: crate::providers::ProviderRegistry::new(),
            #[cfg(feature = "duckdb")]
            duckdb_engine: None,
            #[cfg(feature = "polars")]
            polars_engine: None,
            flight_metrics: None,
            coordinator: None,
            query_history: RwLock::new(Vec::new()),
            start_time: Instant::now(),
            query_count: AtomicU64::new(0),
            stream_events: RwLock::new(Vec::new()),
            stream_metrics: StreamingMetrics::new(),
            vector_index: RwLock::new(index),
            embedding_generator,
            chat_messages: RwLock::new(Vec::new()),
            connections: RwLock::new(Vec::new()),
            connection_passwords: RwLock::new(std::collections::HashMap::new()),
            user_transforms: RwLock::new(Vec::new()),
            scheduled_jobs: RwLock::new(Vec::new()),
            streaming_pipelines: RwLock::new(Vec::new()),
            s3_configs: RwLock::new(Vec::new()),
            job_runs: RwLock::new(Vec::new()),
            job_clusters: RwLock::new(default_job_clusters()),
            table_descriptions: RwLock::new(std::collections::HashMap::new()),
            column_descriptions: RwLock::new(std::collections::HashMap::new()),
            quality_rules: RwLock::new(Vec::new()),
            dbt_project: RwLock::new(None),
            benchmark_results: RwLock::new(Vec::new()),
            #[cfg(feature = "duckdb")]
            trino_cache: {
                match crate::trino_client::TrinoCache::new("trino_cache.duckdb") {
                    Ok(c) => Some(std::sync::Arc::new(c)),
                    Err(e) => { tracing::warn!("Trino cache init failed: {}", e); None }
                }
            },
            trino_connections: RwLock::new(std::collections::HashMap::new()),
            migration_tables: RwLock::new(Vec::new()),
            migration_comparisons: RwLock::new(Vec::new()),
            migration_s3_creds: RwLock::new(std::collections::HashMap::new()),
            engine_tracker: RwLock::new(EnginePerformanceTracker::new()),
            read_only_tables: RwLock::new(std::collections::HashSet::new()),
            credential_store: crate::credential_store::CredentialStore::new(),
            cdc_sources: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Record a chat message (from any sender).
    /// Persists to `feedback.jsonl` on disk and trims in-memory if over limit.
    pub async fn record_chat_message(&self, msg: ChatMessage) {
        if let Err(e) = append_message_to_file(&msg) {
            tracing::error!(error = %e, "Failed to persist chat message to disk");
        }

        let mut messages = self.chat_messages.write().await;
        messages.push(msg);
        if messages.len() > MAX_CHAT_MESSAGES {
            let drain_count = messages.len() - MAX_CHAT_MESSAGES;
            messages.drain(..drain_count);
        }
    }

    /// Record a query execution in the history log.
    /// Trims oldest entries when the history exceeds `MAX_QUERY_HISTORY`.
    pub async fn record_query(&self, entry: QueryHistoryEntry) {
        let mut history = self.query_history.write().await;
        history.push(entry);
        if history.len() > MAX_QUERY_HISTORY {
            let drain_count = history.len() - MAX_QUERY_HISTORY;
            history.drain(..drain_count);
        }
    }

    /// Add a user transform and persist to disk.
    /// Add a connection and persist to disk.
    pub async fn add_connection_entry(&self, entry: ConnectionEntry) {
        if let Err(e) = append_json_line(CONNECTIONS_PATH, &entry) {
            tracing::error!(error = %e, "Failed to persist connection to disk");
        }
        self.connections.write().await.push(entry);
    }

    /// Update a connection entry in place and rewrite persistence file.
    pub async fn update_connection_entry(&self, id: &str, f: impl FnOnce(&mut ConnectionEntry)) {
        let mut connections = self.connections.write().await;
        if let Some(entry) = connections.iter_mut().find(|c| c.id == id) {
            f(entry);
        }
        if let Err(e) = rewrite_json_lines(CONNECTIONS_PATH, &*connections) {
            tracing::error!(error = %e, "Failed to rewrite connections file");
        }
    }

    /// Remove a connection and rewrite the persistence file.
    pub async fn remove_connection_entry(&self, id: &str) -> bool {
        let mut connections = self.connections.write().await;
        let before = connections.len();
        connections.retain(|c| c.id != id);
        if connections.len() < before {
            if let Err(e) = rewrite_json_lines(CONNECTIONS_PATH, &*connections) {
                tracing::error!(error = %e, "Failed to rewrite connections file");
            }
            true
        } else {
            false
        }
    }

    /// Store a connection password (in-memory + encrypted on disk).
    pub async fn store_password(&self, id: String, password: String) -> Option<String> {
        if let Err(e) = self.credential_store.store_password(&id, &password) {
            tracing::warn!(error = %e, conn_id = %id, "Failed to persist encrypted password");
        }
        self.connection_passwords.write().await.insert(id, password)
    }

    pub async fn add_user_transform(&self, ut: UserTransform) {
        if let Err(e) = append_json_line(TRANSFORMS_PATH, &ut) {
            tracing::error!(error = %e, "Failed to persist user transform to disk");
        }
        self.user_transforms.write().await.push(ut);
    }

    /// Remove a user transform and rewrite the persistence file.
    pub async fn remove_user_transform(&self, name: &str) -> bool {
        let mut transforms = self.user_transforms.write().await;
        let before = transforms.len();
        transforms.retain(|t| t.name != name);
        if transforms.len() == before {
            return false;
        }
        if let Err(e) = rewrite_json_lines(TRANSFORMS_PATH, &*transforms) {
            tracing::error!(error = %e, "Failed to rewrite transforms file");
        }
        true
    }

    /// Add a scheduled job and persist to disk.
    pub async fn add_scheduled_job(&self, job: ScheduledJob) {
        if let Err(e) = append_json_line(JOBS_PATH, &job) {
            tracing::error!(error = %e, "Failed to persist scheduled job to disk");
        }
        self.scheduled_jobs.write().await.push(job);
    }

    /// Remove a scheduled job and rewrite the persistence file.
    pub async fn remove_scheduled_job(&self, id: &str) -> bool {
        let mut jobs = self.scheduled_jobs.write().await;
        let before = jobs.len();
        jobs.retain(|j| j.id != id);
        if jobs.len() == before {
            return false;
        }
        if let Err(e) = rewrite_json_lines(JOBS_PATH, &*jobs) {
            tracing::error!(error = %e, "Failed to rewrite jobs file");
        }
        true
    }

    /// Update a scheduled job's state and rewrite the persistence file.
    pub async fn persist_jobs(&self) {
        let jobs = self.scheduled_jobs.read().await;
        if let Err(e) = rewrite_json_lines(JOBS_PATH, &*jobs) {
            tracing::error!(error = %e, "Failed to persist jobs to disk");
        }
    }

    /// Record a job execution in the history log.
    pub async fn record_job_run(&self, entry: JobRunEntry) {
        let mut runs = self.job_runs.write().await;
        runs.push(entry);
        if runs.len() > MAX_JOB_RUNS {
            let drain_count = runs.len() - MAX_JOB_RUNS;
            runs.drain(..drain_count);
        }
    }

    /// Seed a database connection entry directly (used by bootstrap, bypasses HTTP layer).
    /// Returns `true` if a new connection was added, `false` if one with the same name already exists.
    pub async fn seed_connection(&self, entry: ConnectionEntry, password: String) -> bool {
        let mut connections = self.connections.write().await;
        if connections.iter().any(|c| c.name == entry.name) {
            return false;
        }
        let id = entry.id.clone();
        connections.push(entry);
        drop(connections);
        self.connection_passwords.write().await.insert(id, password);
        true
    }

    /// Seed a streaming pipeline if one with the same name doesn't already exist.
    pub async fn seed_pipeline(&self, pipeline: StreamingPipeline) -> bool {
        let mut pipelines = self.streaming_pipelines.write().await;
        if pipelines.iter().any(|p| p.name == pipeline.name) {
            return false;
        }
        pipelines.push(pipeline);
        true
    }

    /// Seed a scheduled job if one with the same name doesn't already exist.
    /// Persists to disk.
    pub async fn seed_scheduled_job(&self, job: ScheduledJob) -> bool {
        let jobs = self.scheduled_jobs.read().await;
        if jobs.iter().any(|j| j.name == job.name) {
            return false;
        }
        drop(jobs);
        self.add_scheduled_job(job).await;
        true
    }

    /// Seed a user transform if one with the same name doesn't already exist.
    /// Persists to disk.
    pub async fn seed_user_transform(&self, ut: UserTransform) -> bool {
        let transforms = self.user_transforms.read().await;
        if transforms.iter().any(|t| t.name == ut.name) {
            return false;
        }
        drop(transforms);
        self.add_user_transform(ut).await;
        true
    }

    /// Append stream events to the circular buffer.
    /// Trims oldest entries when the buffer exceeds `MAX_STREAM_EVENTS`.
    pub async fn append_stream_events(&self, events: Vec<StreamEvent>) {
        let mut buffer = self.stream_events.write().await;
        buffer.extend(events);
        if buffer.len() > MAX_STREAM_EVENTS {
            let drain_count = buffer.len() - MAX_STREAM_EVENTS;
            buffer.drain(..drain_count);
        }
    }
}

/// Append a chat message as a JSON line to the persistent log file.
fn append_message_to_file(msg: &ChatMessage) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(CHAT_LOG_PATH)?;
    let line = serde_json::to_string(msg)
        .map_err(std::io::Error::other)?;
    writeln!(file, "{}", line)?;
    Ok(())
}

/// Append a serializable item as a JSON line to a file.
fn append_json_line<T: Serialize>(path: &str, item: &T) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let line = serde_json::to_string(item).map_err(std::io::Error::other)?;
    writeln!(file, "{}", line)?;
    Ok(())
}

/// Rewrite a JSONL file with the full list of items.
fn rewrite_json_lines<T: Serialize>(path: &str, items: &[T]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    for item in items {
        let line = serde_json::to_string(item).map_err(std::io::Error::other)?;
        writeln!(file, "{}", line)?;
    }
    Ok(())
}

/// Load connections from the JSONL persistence file.
pub fn load_connections_from_file() -> Vec<ConnectionEntry> {
    let contents = match std::fs::read_to_string(CONNECTIONS_PATH) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    contents
        .lines()
        .filter_map(|line| serde_json::from_str::<ConnectionEntry>(line).ok())
        .collect()
}

/// Load user transforms from the JSONL persistence file.
pub fn load_user_transforms_from_file() -> Vec<UserTransform> {
    let contents = match std::fs::read_to_string(TRANSFORMS_PATH) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    contents
        .lines()
        .filter_map(|line| serde_json::from_str::<UserTransform>(line).ok())
        .collect()
}

/// Load scheduled jobs from the JSONL persistence file.
pub fn load_scheduled_jobs_from_file() -> Vec<ScheduledJob> {
    let contents = match std::fs::read_to_string(JOBS_PATH) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    contents
        .lines()
        .filter_map(|line| serde_json::from_str::<ScheduledJob>(line).ok())
        .collect()
}

/// Load previously persisted chat messages from the JSONL log file.
/// Handles both new `ChatMessage` format and legacy `FeedbackEntry` format.
pub fn load_chat_messages_from_file() -> Vec<ChatMessage> {
    let contents = match std::fs::read_to_string(CHAT_LOG_PATH) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    contents
        .lines()
        .filter_map(|line| {
            // Try new format first (UUID id + sender field)
            if let Ok(msg) = serde_json::from_str::<ChatMessage>(line) {
                return Some(msg);
            }
            // Try legacy format (UUID id, no sender)
            if let Ok(legacy) = serde_json::from_str::<LegacyFeedbackEntry>(line) {
                return Some(ChatMessage {
                    id: legacy.id,
                    message: legacy.message,
                    sender: "user".to_string(),
                    category: legacy.category,
                    timestamp: legacy.timestamp,
                });
            }
            // Flexible fallback: integer IDs, source/role instead of sender
            if let Ok(flex) = serde_json::from_str::<FlexibleFeedbackEntry>(line) {
                let id = match &flex.id {
                    Some(serde_json::Value::String(s)) => s.parse::<Uuid>().unwrap_or_else(|_| Uuid::new_v4()),
                    _ => Uuid::new_v4(),
                };
                let sender = flex.sender
                    .or(flex.source)
                    .or(flex.role)
                    .unwrap_or_else(|| "user".to_string());
                return Some(ChatMessage {
                    id,
                    message: flex.message,
                    sender,
                    category: flex.category.unwrap_or_else(|| "general".to_string()),
                    timestamp: flex.timestamp,
                });
            }
            None
        })
        .collect()
}

/// Built-in job clusters for resource isolation.
fn default_job_clusters() -> Vec<JobCluster> {
    vec![
        JobCluster {
            name: "default".to_string(),
            max_concurrent: 4,
            description: "General purpose — transforms, ingestion, snapshots".to_string(),
        },
        JobCluster {
            name: "high-memory".to_string(),
            max_concurrent: 2,
            description: "Large transforms, full-table compaction, heavy joins".to_string(),
        },
        JobCluster {
            name: "gpu".to_string(),
            max_concurrent: 1,
            description: "AI/vector workloads — embedding generation, model inference".to_string(),
        },
    ]
}
