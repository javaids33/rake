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

/// Maximum number of chat messages to retain in memory.
const MAX_CHAT_MESSAGES: usize = 500;

/// Path to the persistent chat log file.
const CHAT_LOG_PATH: &str = "feedback.jsonl";

/// Path to the persistent user transforms file.
const TRANSFORMS_PATH: &str = "user_transforms.jsonl";

/// Path to the persistent scheduled jobs file.
const JOBS_PATH: &str = "scheduled_jobs.jsonl";

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
#[derive(Debug, Clone, Serialize)]
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
    pub secret_key: String,
    pub bucket: String,
    pub region: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

/// Shared application state for the API server.
pub struct AppState {
    /// The query engine context shared across all requests.
    pub ctx: RwLock<RustLakeContext>,
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
}

impl AppState {
    /// Create a new AppState with an empty vector index.
    #[allow(dead_code)]
    pub fn new(ctx: RustLakeContext) -> Self {
        let embedding_generator = SimpleEmbeddingGenerator::new(128);
        Self {
            ctx: RwLock::new(ctx),
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
        }
    }

    /// Create a new AppState with a pre-populated vector index.
    pub fn with_vector_index(ctx: RustLakeContext, index: VectorIndex) -> Self {
        let dimensions = index.dimensions();
        let embedding_generator = SimpleEmbeddingGenerator::new(dimensions);
        Self {
            ctx: RwLock::new(ctx),
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
