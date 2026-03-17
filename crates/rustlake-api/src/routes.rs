use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, RecordBatch};
use axum::extract::{Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use rustlake_router::{EngineTarget, QueryClassifier, QueryType};
use rustlake_stream::connector::SimulatedSource;
use rustlake_stream::StreamingMetricsSnapshot;
use rustlake_transform::{Model, ModelConfig, SqlCompiler};

use crate::state::{
    AlternativeStrategy, AppState, ChatMessage, ConnectionEntry, EngineLatencyRecord,
    EngineRecommendation, EngineResult, EventConfig, JobRunEntry, MigrationComparison,
    MigrationTable, QueryHistoryEntry, S3BucketCreds, S3Config, ScheduledJob, StreamingPipeline,
    UserTransform,
};

// ── Request / Response types ───────────────────────────────────────

/// Request body for the SQL execution endpoint.
#[derive(Deserialize)]
pub struct SqlRequest {
    /// The SQL query string to execute.
    pub sql: String,
    /// Target engine: "auto" (default), "datafusion", "duckdb", or "polars".
    #[serde(default = "default_engine_choice")]
    pub engine: String,
}

fn default_engine_choice() -> String {
    "auto".to_string()
}

/// Response body for the SQL execution endpoint.
#[derive(Serialize)]
pub struct SqlResponse {
    /// Unique identifier for this query execution.
    pub query_id: String,
    /// Column names from the result schema.
    pub columns: Vec<String>,
    /// Result rows as JSON objects.
    pub rows: Vec<serde_json::Value>,
    /// Number of rows returned.
    pub row_count: usize,
    /// Classified query type (e.g., "OLAP", "Interactive").
    pub query_type: String,
    /// Total query execution time in milliseconds.
    pub duration_ms: u128,
    /// Time spent parsing and classifying the SQL (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_ms: Option<u128>,
    /// Time spent executing the query (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec_ms: Option<u128>,
    /// Which engine executed the query ("DataFusion" or "DuckDB").
    pub engine: String,
}

/// JSON error response body returned for failed requests.
#[derive(Serialize)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
}

/// Response body listing all registered tables.
#[derive(Serialize)]
pub struct TableListResponse {
    /// Names of all registered tables.
    pub tables: Vec<String>,
}

/// Request body for registering a new table from a file path.
#[derive(Deserialize)]
pub struct RegisterTableRequest {
    /// Table name to register under.
    pub name: String,
    /// File path to the data source (CSV or Parquet).
    pub path: String,
    /// File format: "csv", "parquet", or "auto" (detect from extension).
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_format() -> String {
    "auto".to_string()
}

/// Response body for the health check endpoint.
#[derive(Serialize)]
pub struct HealthResponse {
    /// Service status (always "ok" if the server is responding).
    pub status: String,
    /// RustLake API version.
    pub version: String,
    /// Query engine name.
    pub engine: String,
}

/// Query parameters for the query history endpoint.
#[derive(Deserialize)]
pub struct HistoryQuery {
    /// Maximum number of history entries to return. Defaults to 50.
    pub limit: Option<usize>,
}

/// Schema information for a single column.
#[derive(Serialize)]
pub struct ColumnSchema {
    /// Column name.
    pub name: String,
    /// Arrow data type as a string (e.g., "Int64", "Utf8").
    pub data_type: String,
    /// Whether the column allows null values.
    pub nullable: bool,
}

/// Response for the table schema endpoint.
#[derive(Serialize)]
pub struct TableSchemaResponse {
    /// Name of the table.
    pub table: String,
    /// Schema columns with types and nullability.
    pub columns: Vec<ColumnSchema>,
}

/// Response for the table preview endpoint.
#[derive(Serialize)]
pub struct TablePreviewResponse {
    /// Name of the table.
    pub table: String,
    /// Column names.
    pub columns: Vec<String>,
    /// Preview rows as JSON objects (up to 100).
    pub rows: Vec<serde_json::Value>,
    /// Number of rows returned.
    pub row_count: usize,
}

/// Per-column statistics.
#[derive(Serialize)]
pub struct ColumnStats {
    /// Column name.
    pub name: String,
    /// Arrow data type as a string.
    pub data_type: String,
    /// Minimum value in the column (if computable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Value>,
    /// Maximum value in the column (if computable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Value>,
    /// Number of null values in the column.
    pub null_count: u64,
}

/// Response for the table stats endpoint.
#[derive(Serialize)]
pub struct TableStatsResponse {
    /// Name of the table.
    pub table: String,
    /// Total number of rows.
    pub row_count: usize,
    /// Total number of columns.
    pub column_count: usize,
    /// Per-column statistics.
    pub columns: Vec<ColumnStats>,
}

/// Response for the system info endpoint.
#[derive(Serialize)]
pub struct SystemInfoResponse {
    /// RustLake version string.
    pub version: String,
    /// Query engine name.
    pub engine: String,
    /// Server uptime in seconds since startup.
    pub uptime_seconds: u64,
    /// Total number of SQL queries executed.
    pub total_queries: u64,
    /// Apache Arrow library version.
    pub arrow_version: String,
    /// DataFusion query engine version.
    pub datafusion_version: String,
}

/// Response for the system resources endpoint.
#[derive(Serialize)]
pub struct SystemResourcesResponse {
    /// Number of logical CPU cores available.
    pub cpu_cores: usize,
    /// Total physical memory in bytes.
    pub total_memory_bytes: u64,
    /// Engine memory limit from config (None = unbounded).
    pub engine_memory_limit: Option<usize>,
    /// Rows per Arrow RecordBatch.
    pub batch_size: usize,
    /// Target partition count for DataFusion parallelism.
    pub target_partitions: usize,
    /// Number of Tokio worker threads.
    pub tokio_workers: usize,
    /// Whether distributed (multi-node) mode is active.
    pub distributed_mode: bool,
    /// Current status of the Flight gRPC server.
    pub flight_status: String,
    /// Role of this node in the cluster topology.
    pub node_role: String,
}

/// Response for the Flight server info endpoint.
#[derive(Serialize)]
pub struct FlightInfoResponse {
    /// Protocol name (e.g., "Arrow Flight RPC").
    pub protocol: String,
    /// Bind host for the Flight gRPC server.
    pub host: String,
    /// gRPC port the Flight server listens on.
    pub port: u16,
    /// Current server status ("running", "stopped", "disabled").
    pub status: String,
    /// Maximum gRPC message size in bytes.
    pub max_message_size: usize,
    /// List of supported Flight capabilities.
    pub capabilities: Vec<String>,
    /// Apache Arrow version used by the server.
    pub arrow_version: String,
    /// Number of active Flight gRPC connections.
    pub active_clients: u64,
    /// Total queries served via Flight since startup.
    pub queries_served: u64,
    /// BI tools and clients known to be compatible.
    pub supported_clients: Vec<String>,
}

/// Detailed Flight server status for the /api/v1/flight/status endpoint.
#[derive(Serialize)]
pub struct FlightStatusResponse {
    pub enabled: bool,
    pub running: bool,
    pub host: String,
    pub port: u16,
    pub active_connections: u64,
    pub queries_served: u64,
}

/// Response for the system metrics endpoint (real-time OS metrics).
#[derive(Serialize)]
pub struct SystemMetricsResponse {
    /// CPU usage percentage (0.0 - 100.0).
    pub cpu_usage_percent: f64,
    /// Memory used in bytes.
    pub memory_used_bytes: u64,
    /// Total memory in bytes.
    pub memory_total_bytes: u64,
    /// Memory usage percentage.
    pub memory_usage_percent: f64,
    /// Disk used in bytes (root partition).
    pub disk_used_bytes: u64,
    /// Disk total in bytes (root partition).
    pub disk_total_bytes: u64,
    /// Disk usage percentage.
    pub disk_usage_percent: f64,
    /// Load average (1 min).
    pub load_avg_1m: f64,
    /// Load average (5 min).
    pub load_avg_5m: f64,
    /// Active query count (approximate).
    pub active_queries: u64,
    /// Total queries since startup.
    pub total_queries: u64,
    /// Queries per second (last minute).
    pub queries_per_second: f64,
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
}

/// Response for query cost estimation.
#[derive(Serialize)]
pub struct QueryEstimateResponse {
    /// The SQL query.
    pub sql: String,
    /// Estimated row count to process.
    pub estimated_rows: u64,
    /// Estimated bytes to scan.
    pub estimated_bytes: u64,
    /// Human-readable estimated scan size.
    pub estimated_scan_size: String,
    /// Number of partitions to read.
    pub partitions: usize,
    /// Cost rating: "low", "medium", "high".
    pub cost_rating: String,
    /// Tables referenced in the query.
    pub tables_referenced: Vec<String>,
    /// Planning notes.
    pub notes: Vec<String>,
}

/// Request for connection test.
#[derive(Deserialize)]
pub struct ConnectionTestRequest {
    /// Connection type: "postgres", "mysql", "s3", etc.
    pub conn_type: String,
    /// Host or endpoint.
    pub host: String,
    /// Port number.
    pub port: Option<u16>,
    /// Database name (if applicable).
    pub database: Option<String>,
    /// Username.
    pub username: Option<String>,
    /// Password.
    pub password: Option<String>,
}

/// Response for connection test.
#[derive(Serialize)]
pub struct ConnectionTestResponse {
    /// Whether the connection succeeded.
    pub success: bool,
    /// Human-readable status message.
    pub message: String,
    /// Latency of the test in milliseconds.
    pub latency_ms: Option<u128>,
    /// Server version (if available).
    pub server_version: Option<String>,
    /// Tables found (for database connections).
    pub tables_found: Option<usize>,
    /// Validation level: "full" (protocol handshake), "tcp" (port reachable), "dns" (host resolves), "config" (fields valid)
    pub validation_level: String,
    /// Individual checks performed and their results.
    pub checks: Vec<ConnectionCheck>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectionCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

/// A single transform model in the transforms list.
#[derive(Serialize)]
pub struct TransformEntry {
    /// Transform model name.
    pub name: String,
    /// Raw SQL template (may contain ref/source macros).
    pub sql: String,
    /// Names of upstream dependencies.
    pub depends_on: Vec<String>,
    /// Materialization strategy (e.g., "view", "table").
    pub materialization: String,
    /// Human-readable description of the transform.
    pub description: String,
}

/// Response for the transforms list endpoint.
#[derive(Serialize)]
pub struct TransformsResponse {
    /// List of available transform models.
    pub transforms: Vec<TransformEntry>,
}

/// A node in the lineage DAG.
#[derive(Serialize)]
pub struct LineageNode {
    /// Node identifier (e.g., "raw.orders", "fct_revenue").
    pub id: String,
    /// Node type (e.g., "source", "staging", "fact", "report").
    #[serde(rename = "type")]
    pub node_type: String,
    /// Data format if applicable (e.g., "csv", "parquet").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Materialization strategy if applicable (e.g., "view", "table").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialization: Option<String>,
}

/// An edge in the lineage DAG.
#[derive(Serialize)]
pub struct LineageEdge {
    /// Source node ID.
    pub from: String,
    /// Target node ID.
    pub to: String,
}

/// Response for the lineage endpoint.
#[derive(Serialize)]
pub struct LineageResponse {
    /// All nodes in the lineage DAG.
    pub nodes: Vec<LineageNode>,
    /// Directed edges representing data flow between nodes.
    pub edges: Vec<LineageEdge>,
}

/// Response for the transform run endpoint.
#[derive(Serialize)]
pub struct TransformRunResponse {
    /// Name of the executed transform.
    pub transform: String,
    /// The SQL after ref/source macro resolution.
    pub compiled_sql: String,
    /// Column names from the result schema.
    pub columns: Vec<String>,
    /// Result rows as JSON objects.
    pub rows: Vec<serde_json::Value>,
    /// Number of rows returned.
    pub row_count: usize,
    /// Execution time in milliseconds.
    pub duration_ms: u128,
}

/// Response for the EXPLAIN plan endpoint.
#[derive(Serialize)]
pub struct ExplainResponse {
    /// Original SQL query.
    pub sql: String,
    /// Logical plan as a string.
    pub logical_plan: String,
    /// Physical plan as a string.
    pub physical_plan: String,
    /// Plan nodes for tree visualization.
    pub nodes: Vec<PlanNode>,
}

/// A single node in the query plan tree.
#[derive(Serialize, Clone)]
pub struct PlanNode {
    /// Node identifier.
    pub id: usize,
    /// Operator name (e.g., "TableScan", "Filter", "HashAggregate").
    pub operator: String,
    /// Detail string (e.g., filter expression, table name).
    pub detail: String,
    /// Estimated row count from plan (if available).
    pub estimated_rows: Option<usize>,
    /// Parent node ID (None for root).
    pub parent: Option<usize>,
    /// Depth in the tree (0 = root).
    pub depth: usize,
}

/// Per-table quality check result.
#[derive(Serialize, Clone)]
pub struct TableQualityCheck {
    /// Table name.
    pub table: String,
    /// Total row count.
    pub row_count: usize,
    /// Number of columns.
    pub column_count: usize,
    /// Per-column null percentages.
    pub null_percentages: Vec<ColumnNullInfo>,
    /// Overall health: "healthy", "warning", or "critical".
    pub health: String,
    /// List of issues found.
    pub issues: Vec<String>,
    /// When this check was performed.
    pub checked_at: String,
}

/// Null info per column.
#[derive(Serialize, Clone)]
pub struct ColumnNullInfo {
    /// Column name.
    pub name: String,
    /// Data type.
    pub data_type: String,
    /// Null count.
    pub null_count: u64,
    /// Total rows.
    pub total_rows: usize,
    /// Null percentage.
    pub null_pct: f64,
}

/// Quality checks response.
#[derive(Serialize)]
pub struct QualityChecksResponse {
    /// Per-table checks.
    pub checks: Vec<TableQualityCheck>,
    /// Summary counts.
    pub healthy_count: usize,
    pub warning_count: usize,
    pub critical_count: usize,
    pub total_tables: usize,
}

/// A quality alert rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityRule {
    pub id: String,
    pub table_name: String,
    pub rule_type: String,
    pub threshold: f64,
    pub enabled: bool,
    pub created_at: String,
}

/// DAG response for the scheduler.
#[derive(Serialize)]
pub struct SchedulerDagResponse {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<DagEdge>,
}

#[derive(Serialize)]
pub struct DagNode {
    pub id: String,
    pub name: String,
    pub job_type: String,
    pub status: String,
    pub cron: String,
    pub enabled: bool,
    pub last_run: Option<String>,
}

#[derive(Serialize)]
pub struct DagEdge {
    pub from: String,
    pub to: String,
    pub label: Option<String>,
}

/// dbt model definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtModel {
    pub name: String,
    pub sql: String,
    pub depends_on: Vec<String>,
    pub materialization: String,
    pub description: String,
    pub schema_name: Option<String>,
    pub tags: Vec<String>,
}

/// dbt project state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtProject {
    pub name: String,
    pub version: String,
    pub models: Vec<DbtModel>,
    pub sources: Vec<DbtSource>,
    pub uploaded_at: String,
}

/// dbt source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtSource {
    pub name: String,
    pub schema_name: String,
    pub tables: Vec<String>,
}

/// Response for dbt model run.
#[derive(Serialize)]
pub struct DbtRunResponse {
    pub model: String,
    pub status: String,
    pub compiled_sql: String,
    pub row_count: usize,
    pub duration_ms: u128,
    pub error: Option<String>,
}

/// Response for dbt run-all.
#[derive(Serialize)]
pub struct DbtRunAllResponse {
    pub results: Vec<DbtRunResponse>,
    pub total_duration_ms: u128,
    pub success_count: usize,
    pub failure_count: usize,
}

// ── Routes ─────────────────────────────────────────────────────────

/// Build the Axum router with all API routes.
pub fn api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(dashboard))
        .route("/health", get(health))
        .route("/api/v1/sql", post(execute_sql))
        .route("/api/v1/sql/compare", post(compare_sql))
        .route("/api/v1/sql/explain", post(explain_sql))
        .route("/api/v1/tables", get(list_tables))
        .route("/api/v1/tables/register", post(register_table))
        .route("/api/v1/query/history", get(query_history))
        .route("/api/v1/tables/{name}/schema", get(table_schema))
        .route("/api/v1/tables/{name}/preview", get(table_preview))
        .route("/api/v1/tables/{name}/stats", get(table_stats))
        .route("/api/v1/system/info", get(system_info))
        .route("/api/v1/system/resources", get(system_resources))
        // Flight server info + status
        .route("/api/v1/flight/info", get(flight_info))
        .route("/api/v1/flight/status", get(flight_status))
        // Transform / lineage endpoints
        .route(
            "/api/v1/transforms",
            get(list_transforms).post(create_transform),
        )
        .route("/api/v1/lineage", get(lineage))
        .route("/api/v1/transforms/{name}/run", post(run_transform))
        .route("/api/v1/transforms/{name}", delete(delete_transform))
        // Vector/semantic search endpoints
        .route("/api/v1/vector/index", post(vector_index_documents))
        .route("/api/v1/vector/search", post(vector_search))
        .route("/api/v1/vector/status", get(vector_status))
        // Streaming endpoints
        .route("/api/v1/stream/ingest", post(stream_ingest))
        .route("/api/v1/stream/status", get(stream_status))
        .route("/api/v1/stream/events", get(stream_events))
        // File upload
        .route("/api/v1/upload", post(upload_file))
        // External database connections
        .route(
            "/api/v1/connections",
            post(add_connection).get(list_connections),
        )
        .route("/api/v1/connections/{id}", delete(delete_connection).put(update_connection))
        .route("/api/v1/connections/{id}/status", get(connection_sync_status))
        .route(
            "/api/v1/connections/{id}/register/{table}",
            post(register_external_table),
        )
        // Chat / Feedback endpoints
        .route(
            "/api/v1/feedback",
            post(submit_chat_message).get(list_chat_messages),
        )
        .route("/api/v1/feedback/respond", post(submit_chat_message))
        .route("/api/v1/feedback/messages", get(list_chat_messages))
        // Scheduling endpoints
        .route(
            "/api/v1/schedules",
            get(list_schedules).post(create_schedule),
        )
        .route("/api/v1/schedules/{id}", get(get_schedule).put(update_schedule).delete(delete_schedule))
        .route("/api/v1/schedules/{id}/run", post(run_schedule))
        .route("/api/v1/schedules/runs", get(list_job_runs))
        // Job clusters
        .route("/api/v1/clusters", get(list_clusters))
        // Cluster topology (distributed execution)
        .route("/api/v1/cluster/topology", get(cluster_topology))
        .route("/api/v1/cluster/workers", get(list_workers))
        // Table metadata + deregister
        .route("/api/v1/tables/{name}", delete(deregister_table))
        .route("/api/v1/tables/{name}/description", put(update_table_description).get(get_table_description))
        // Streaming pipeline endpoints
        .route(
            "/api/v1/streaming/pipelines",
            get(list_pipelines).post(create_pipeline),
        )
        .route("/api/v1/streaming/pipelines/{id}", delete(delete_pipeline))
        .route("/api/v1/streaming/pipelines/{id}/start", post(start_pipeline))
        .route("/api/v1/streaming/pipelines/{id}/stop", post(stop_pipeline))
        .route("/api/v1/streaming/pipelines/import", post(import_pipelines))
        // S3/Object storage config
        .route(
            "/api/v1/storage/s3",
            get(list_s3_configs).post(add_s3_config),
        )
        .route("/api/v1/storage/s3/{id}", put(update_s3_config).delete(delete_s3_config))
        // Quality checks
        .route("/api/v1/quality/checks", get(quality_checks))
        .route(
            "/api/v1/quality/rules",
            get(list_quality_rules).post(create_quality_rule),
        )
        .route("/api/v1/quality/rules/{id}", delete(delete_quality_rule))
        // Scheduler DAG
        .route("/api/v1/schedules/dag", get(scheduler_dag))
        // dbt integration
        .route("/api/v1/dbt/upload", post(dbt_upload))
        .route("/api/v1/dbt/project", get(dbt_project_info))
        .route("/api/v1/dbt/models", get(list_dbt_models))
        .route("/api/v1/dbt/run/{name}", post(run_dbt_model))
        .route("/api/v1/dbt/run-all", post(run_all_dbt_models))
        // System metrics (real-time OS metrics)
        .route("/api/v1/system/metrics", get(system_metrics))
        // Query cost estimation
        .route("/api/v1/sql/estimate", post(estimate_query))
        // Connection testing
        .route("/api/v1/connections/test", post(test_connection))
        // Bulk import/export
        .route("/api/v1/connections/import", post(import_connections))
        .route("/api/v1/connections/export", get(export_connections))
        // Bootstrap (auto-connect demo services)
        .route("/api/v1/bootstrap", post(run_bootstrap))
        .route("/api/v1/bootstrap/status", get(bootstrap_status))
        // Benchmarks (TPC-H)
        .route("/api/v1/benchmarks/queries", get(list_benchmark_queries))
        .route("/api/v1/benchmarks/run", post(run_benchmark_query))
        .route("/api/v1/benchmarks/results", get(list_benchmark_results))
        .route("/api/v1/benchmarks/compare", post(compare_benchmark))
        // Engine info
        .route("/api/v1/engines", get(list_engines))
        .route("/api/v1/engines/sync", post(engines_sync))
        // Provider info
        .route("/api/v1/providers", get(list_providers))
        // Trino catalog browse (DuckDB-cached)
        .route("/api/v1/trino/{conn_id}/browse", get(trino_browse))
        .route("/api/v1/trino/{conn_id}/columns", get(trino_columns))
        .route("/api/v1/trino/{conn_id}/preview", get(trino_preview))
        .route("/api/v1/trino/{conn_id}/query", post(trino_query))
        .route("/api/v1/trino/{conn_id}/refresh", post(trino_refresh))
        .route("/api/v1/trino/{conn_id}/stats", get(trino_stats))
        // Migration: Iceberg catalog migration (Trino → Rake)
        .route("/api/v1/migration/{conn_id}/discover", post(migration_discover))
        .route("/api/v1/migration/{conn_id}/register", post(migration_register))
        .route("/api/v1/migration/credentials", post(migration_credentials))
        .route("/api/v1/migration/compare", post(migration_compare))
        .route("/api/v1/migration/{conn_id}/tables", get(migration_tables))
        .route("/api/v1/migration/comparisons", get(migration_comparisons))
        // Server-Sent Events — replaces polling for metrics/health/tables
        .route("/api/v1/events", get(event_stream))
        // WebSocket
        .route("/api/v1/ws", get(crate::ws::ws_handler))
}

// ── Handlers ───────────────────────────────────────────────────────

async fn dashboard() -> axum::response::Html<String> {
    // Serve dashboard.html from the project root
    let html = std::fs::read_to_string("dashboard.html").unwrap_or_else(|_| {
        "<h1>dashboard.html not found</h1><p>Run from the project root directory.</p>".to_string()
    });
    axum::response::Html(html)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        engine: "DataFusion".to_string(),
    })
}

/// GET /api/v1/events — Server-Sent Events stream.
///
/// Pushes a combined `status` event every 5 seconds containing health, system
/// metrics, table count, and engine info. Replaces 3+ polling intervals on the
/// frontend with a single persistent connection. Also pushes one-off events
/// for connection sync status changes.
async fn event_stream(
    State(state): State<Arc<AppState>>,
) -> Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = async_stream::stream! {
        // Track last-seen connection sync statuses so we can push deltas
        let mut last_sync: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        // Track last-seen sync_progress so we can push trino_scan events on change
        let mut last_progress: std::collections::HashMap<String, String> = std::collections::HashMap::new();

        // Subscribe to real-time pipeline events from CDC consumers
        let mut pipeline_rx = state.pipeline_events_tx.subscribe();

        loop {
            // ── Drain any pending pipeline events (non-blocking) ─────
            loop {
                match pipeline_rx.try_recv() {
                    Ok(evt) => {
                        let payload = serde_json::to_string(&evt).unwrap_or_default();
                        yield Ok(Event::default().event("pipeline_event").data(payload));
                    }
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => break,
                    Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                }
            }
            // ── Build combined status payload ─────────────────────
            let total_queries = state.query_count.load(Ordering::Relaxed);
            let uptime = state.start_time.elapsed().as_secs();
            let (load_1m, _load_5m, cpu_percent) = get_load_average();
            let (mem_used, mem_total) = get_memory_usage();
            let mem_pct = if mem_total > 0 { (mem_used as f64 / mem_total as f64) * 100.0 } else { 0.0 };

            // Table count (lightweight — just counts, doesn't serialize all table info)
            let table_count = {
                let ctx = state.ctx.read().await;
                ctx.list_tables().await.map(|t| t.len()).unwrap_or(0)
            };

            // Engine info
            let mut engines_arr = vec![serde_json::json!({
                "name": "DataFusion", "version": "51", "status": "running"
            })];
            #[cfg(feature = "duckdb")]
            if let Some(ref dk) = state.duckdb_engine {
                engines_arr.push(serde_json::json!({
                    "name": "DuckDB", "version": dk.version(), "status": "running"
                }));
            }
            #[cfg(feature = "polars")]
            if let Some(ref pl) = state.polars_engine {
                engines_arr.push(serde_json::json!({
                    "name": "Polars", "version": pl.version(), "status": "running"
                }));
            }

            let payload = serde_json::json!({
                "health": "ok",
                "cpu": (cpu_percent * 10.0).round() / 10.0,
                "mem_used": mem_used,
                "mem_total": mem_total,
                "mem_pct": (mem_pct * 10.0).round() / 10.0,
                "load_1m": load_1m,
                "total_queries": total_queries,
                "uptime": uptime,
                "tables": table_count,
                "engines": engines_arr,
            });

            yield Ok(Event::default().event("status").data(payload.to_string()));

            // ── Check connection sync status changes ──────────────
            {
                let conns = state.connections.read().await;
                for conn in conns.iter() {
                    let prev = last_sync.get(&conn.id).map(|s| s.as_str()).unwrap_or("");
                    if conn.sync_status != prev {
                        last_sync.insert(conn.id.clone(), conn.sync_status.clone());
                        let sync_event = serde_json::json!({
                            "id": conn.id,
                            "sync_status": conn.sync_status,
                            "sync_error": conn.sync_error,
                            "tables": conn.tables,
                            "table_count": conn.tables.len(),
                        });
                        yield Ok(Event::default().event("connection_sync").data(sync_event.to_string()));
                    }

                    // ── Check sync progress changes (Trino scan phases) ──
                    let progress = conn.sync_progress.clone().unwrap_or_default();
                    let prev_progress = last_progress.get(&conn.id).map(|s| s.as_str()).unwrap_or("");
                    if progress != prev_progress {
                        last_progress.insert(conn.id.clone(), progress.clone());
                        let scan_event = serde_json::json!({
                            "id": conn.id,
                            "phase": conn.sync_progress,
                            "sync_status": conn.sync_status,
                        });
                        yield Ok(Event::default().event("trino_scan").data(scan_event.to_string()));
                    }
                }
            }

            // ── Check S3 scan progress changes ────────────────────
            {
                let s3_configs = state.s3_configs.read().await;
                for cfg in s3_configs.iter() {
                    if cfg.sync_status == "syncing" {
                        let scan_key = format!("s3:{}", cfg.name);
                        let current_progress = format!(
                            "{}:{}:{}:{}",
                            cfg.scan_progress.as_deref().unwrap_or(""),
                            cfg.scan_scanned,
                            cfg.scan_found,
                            cfg.scan_elapsed_ms,
                        );
                        let prev = last_progress.get(&scan_key).map(|s| s.as_str()).unwrap_or("");
                        if current_progress != prev {
                            last_progress.insert(scan_key, current_progress);
                            let scan_event = serde_json::json!({
                                "name": cfg.name,
                                "phase": cfg.scan_progress,
                                "detail": cfg.scan_detail,
                                "scanned": cfg.scan_scanned,
                                "total": cfg.scan_total,
                                "found": cfg.scan_found,
                                "elapsed_ms": cfg.scan_elapsed_ms,
                                "formats": cfg.format_counts,
                                "sync_status": cfg.sync_status,
                            });
                            yield Ok(Event::default().event("s3_scan").data(scan_event.to_string()));
                        }
                    }
                }
            }

            // ── Check pipeline status changes ──────────────────────
            {
                let pipelines = state.streaming_pipelines.read().await;
                for p in pipelines.iter() {
                    if p.status == "running" || p.status == "snapshotting" {
                        let key = format!("pipe:{}", p.id);
                        let current = format!("{}:{}:{}", p.status, p.events_processed, p.sink_table);
                        let prev = last_progress.get(&key).map(|s| s.as_str()).unwrap_or("");
                        if current != prev {
                            last_progress.insert(key, current);
                            let event = serde_json::json!({
                                "id": p.id,
                                "name": p.name,
                                "status": p.status,
                                "events_processed": p.events_processed,
                                "source_type": p.source_type,
                                "sink_table": p.sink_table,
                            });
                            yield Ok(Event::default().event("pipeline_status").data(event.to_string()));
                        }
                    }
                }
            }

            // Adaptive sleep: 2s when pipelines are running (for real-time event counts), 10s otherwise
            let has_running = {
                let pipelines = state.streaming_pipelines.read().await;
                pipelines.iter().any(|p| p.status == "running" || p.status == "snapshotting")
            };
            let sleep_secs = if has_running { 2 } else { 10 };
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn execute_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> std::result::Result<Json<SqlResponse>, (StatusCode, Json<ErrorResponse>)> {
    let query_id = Uuid::new_v4();
    let start = Instant::now();

    tracing::info!(sql = %req.sql, %query_id, engine_choice = %req.engine, "Received SQL request");

    // Log available schemas for debugging table resolution
    {
        let ctx = state.ctx.read().await;
        let df_ctx = ctx.datafusion_ctx();
        if let Some(catalog) = df_ctx.catalog("datafusion") {
            let schemas = catalog.schema_names();
            tracing::info!(%query_id, available_schemas = ?schemas, "DataFusion catalog schemas");
        }
    }

    // Classify the query (parse + classify timing)
    let parse_start = Instant::now();
    let classification = QueryClassifier::classify_with_engine(&req.sql)
        .unwrap_or(rustlake_router::ClassificationResult {
            query_type: QueryType::Olap,
            engine: EngineTarget::Either,
        });
    let query_type = classification.query_type;
    let parse_ms = parse_start.elapsed().as_millis();
    tracing::info!(query_type = %query_type, recommended_engine = %classification.engine, parse_ms, "Query classified");

    // Block DDL/DML on read-only migration tables
    if matches!(query_type, QueryType::Ddl | QueryType::Dml) {
        let read_only = state.read_only_tables.read().await;
        if !read_only.is_empty() {
            let sql_upper = req.sql.trim().to_uppercase();
            // Extract table name from common DDL/DML patterns
            let target_table = extract_target_table(&sql_upper);
            if let Some(table) = target_table {
                let table_lower = table.to_lowercase();
                if read_only.iter().any(|ro| table_lower == ro.to_lowercase() || table_lower.ends_with(&format!(".{}", ro.to_lowercase()))) {
                    return Err((StatusCode::FORBIDDEN, Json(ErrorResponse {
                        error: format!("Table '{}' is read-only (migrated from Iceberg catalog). Write operations are blocked to protect source data during migration comparison.", table),
                    })));
                }
            }
        }
    }

    // Handle CTAS: CREATE TABLE <name> AS SELECT ...
    let sql_upper = req.sql.trim().to_uppercase();
    if sql_upper.starts_with("CREATE TABLE") && sql_upper.contains(" AS ") {
        return handle_ctas(state, req.sql, query_id, query_type, parse_ms, start).await;
    }

    // Determine target engine: explicit override > classifier recommendation
    let engine_name = determine_engine(&state, &req.engine, &classification.engine);

    tracing::debug!(
        %query_id,
        engine = engine_name,
        duckdb_available = state.duckdb_available(),
        polars_available = state.polars_available(),
        "Engine selected"
    );

    // Execute via the selected engine
    let exec_start = Instant::now();
    let result = match engine_name {
        "DuckDB" => execute_via_duckdb(&state, &req.sql).await,
        "Polars" => execute_via_polars(&state, &req.sql).await,
        _ => {
            let ctx = state.ctx.read().await;
            ctx.sql(&req.sql).await.map_err(|e| e.to_string())
        }
    };
    let exec_ms = exec_start.elapsed().as_millis();
    let duration_ms = start.elapsed().as_millis();

    // Increment query counter
    state.query_count.fetch_add(1, Ordering::Relaxed);

    let batches = match result {
        Ok(batches) => batches,
        Err(e) => {
            // If an alternative engine failed, try fallback to DataFusion
            if engine_name != "DataFusion" {
                tracing::warn!(engine = engine_name, error = %e, "Engine failed, falling back to DataFusion");
                let fallback_start = Instant::now();
                let ctx = state.ctx.read().await;
                match ctx.sql(&req.sql).await {
                    Ok(batches) => {
                        let fallback_ms = fallback_start.elapsed().as_millis();
                        let duration_ms = start.elapsed().as_millis();
                        return finish_sql_response(
                            &state, query_id, &req.sql, query_type, "DataFusion",
                            batches, parse_ms, fallback_ms, duration_ms,
                        ).await;
                    }
                    Err(fallback_err) => {
                        tracing::error!(error = %fallback_err, "DataFusion fallback also failed");
                        let duration_ms = start.elapsed().as_millis();
                        state
                            .record_query(QueryHistoryEntry {
                                query_id,
                                sql: req.sql.clone(),
                                query_type: query_type.to_string(),
                                row_count: 0,
                                duration_ms,
                                timestamp: Utc::now(),
                                status: "error".to_string(),
                                error: Some(fallback_err.to_string()),
                                engine: "DataFusion".to_string(),
                            })
                            .await;
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse { error: fallback_err.to_string() }),
                        ));
                    }
                }
            }

            tracing::error!(error = %e, "Query execution failed");
            state
                .record_query(QueryHistoryEntry {
                    query_id,
                    sql: req.sql.clone(),
                    query_type: query_type.to_string(),
                    row_count: 0,
                    duration_ms,
                    timestamp: Utc::now(),
                    status: "error".to_string(),
                    error: Some(e.to_string()),
                    engine: engine_name.to_string(),
                })
                .await;

            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: e.to_string() }),
            ));
        }
    };

    finish_sql_response(
        &state, query_id, &req.sql, query_type, engine_name,
        batches, parse_ms, exec_ms, duration_ms,
    ).await
}

/// Determine which engine to use for this query.
/// Returns an engine name: "DataFusion", "DuckDB", or "Polars".
pub(crate) fn determine_engine(state: &AppState, engine_choice: &str, recommended: &EngineTarget) -> &'static str {
    match engine_choice.to_lowercase().as_str() {
        "duckdb" if state.duckdb_available() => "DuckDB",
        "polars" if state.polars_available() => "Polars",
        "datafusion" => "DataFusion",
        "auto" | _ => {
            if state.duckdb_available() && matches!(recommended, EngineTarget::DuckDb) {
                "DuckDB"
            } else {
                "DataFusion"
            }
        }
    }
}

/// Execute SQL via DuckDB engine, returning batches or error string.
pub(crate) async fn execute_via_duckdb(
    state: &AppState,
    sql: &str,
) -> std::result::Result<Vec<RecordBatch>, String> {
    #[cfg(feature = "duckdb")]
    {
        if let Some(ref engine) = state.duckdb_engine {
            return engine.sql(sql).await.map_err(|e| e.to_string());
        }
    }
    Err("DuckDB engine not available".to_string())
}

/// Execute SQL via Polars engine, returning batches or error string.
pub(crate) async fn execute_via_polars(
    state: &AppState,
    sql: &str,
) -> std::result::Result<Vec<RecordBatch>, String> {
    #[cfg(feature = "polars")]
    {
        if let Some(ref engine) = state.polars_engine {
            return engine.sql(sql).await.map_err(|e| e.to_string());
        }
    }
    Err("Polars engine not available".to_string())
}

/// Execute SQL on DataFusion with direct S3 access via `object_store`.
///
/// Creates a temporary `SessionContext`, registers an S3 object store with the given
/// credentials, and registers ListingTables at the S3 paths for each referenced table.
/// The SQL is then rewritten to reference these temporary table names.
async fn execute_df_s3_direct(
    rake_sql: &str,
    table_s3_locations: &std::collections::HashMap<String, String>,
    creds: &S3BucketCreds,
    _conn_id: &str,
) -> std::result::Result<(usize, String), String> {
    use datafusion::prelude::*;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use object_store::aws::AmazonS3Builder;

    let (access_key, secret_key, region) = (&creds.access_key, &creds.secret_key, &creds.region);

    // Create a temporary SessionContext for S3 direct queries
    let ctx = SessionContext::new();

    // Track which S3 buckets we've registered object stores for
    let mut registered_buckets: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Build the SQL with table names replaced by S3-backed listing table names
    let mut rewritten_sql = rake_sql.to_string();
    let mut s3_table_count = 0;

    for (fqn, s3_location) in table_s3_locations {
        // Parse s3://bucket/path from the location
        let s3_url = if s3_location.ends_with('/') {
            s3_location.clone()
        } else {
            // For Iceberg tables, data is usually under /data/ subdirectory
            format!("{}/data/", s3_location)
        };

        // Extract bucket from s3://bucket/...
        if let Some(bucket) = s3_location
            .strip_prefix("s3://")
            .and_then(|rest| rest.split('/').next())
        {
            if !registered_buckets.contains(bucket) {
                let s3_store = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_access_key_id(access_key)
                    .with_secret_access_key(secret_key)
                    .with_allow_http(true)
                    .build()
                    .map_err(|e| format!("S3 store build for bucket '{}': {}", bucket, e))?;

                let url = url::Url::parse(&format!("s3://{}", bucket))
                    .map_err(|e| format!("URL parse for bucket '{}': {}", bucket, e))?;
                ctx.runtime_env()
                    .register_object_store(&url, Arc::new(s3_store));

                registered_buckets.insert(bucket.to_string());
            }
        }

        // Derive a safe table name from the FQN for use in the temp context
        // e.g., "iceberg.sf1.orders" → "s3_iceberg_sf1_orders"
        let safe_name = format!("s3_{}", fqn.replace('.', "_"));

        // Register a ListingTable at the S3 path
        let table_url = ListingTableUrl::parse(&s3_url)
            .map_err(|e| format!("ListingTableUrl parse '{}': {}", s3_url, e))?;

        let parquet_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(parquet_format))
            .with_file_extension(".parquet");

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .map_err(|e| format!("Schema infer for '{}' at '{}': {}", fqn, s3_url, e))?;

        let listing_table = ListingTable::try_new(config)
            .map_err(|e| format!("ListingTable for '{}': {}", fqn, e))?;

        ctx.register_table(&safe_name, Arc::new(listing_table))
            .map_err(|e| format!("Register S3 table '{}': {}", safe_name, e))?;

        // Rewrite the rake_sql to use the safe_name instead of the Rake-registered name
        // The rake_sql already has trino_{catalog}.{schema}_{table} names — replace them
        // Try schema-qualified form: trino_{catalog}.{schema}_{table}
        let parts: Vec<&str> = fqn.splitn(3, '.').collect();
        if parts.len() == 3 {
            let schema_qualified = format!("trino_{}.{}_{}", parts[0], parts[1], parts[2]);
            rewritten_sql = rewritten_sql.replace(&schema_qualified, &safe_name);
            // Also try flat form
            let flat = format!("trino_{}_{}_{}", parts[0], parts[1], parts[2]);
            rewritten_sql = rewritten_sql.replace(&flat, &safe_name);
        }
        // Also replace the original FQN if it somehow remains
        rewritten_sql = rewritten_sql.replace(fqn, &safe_name);
        s3_table_count += 1;
    }

    if s3_table_count == 0 {
        return Err("No S3 table locations available for direct S3 query".to_string());
    }

    let engine_label = format!("Rake DataFusion (S3 direct, {} tables)", s3_table_count);

    // Execute the rewritten SQL on the S3-backed context
    let df = ctx.sql(&rewritten_sql).await
        .map_err(|e| format!("DataFusion S3 SQL: {}", e))?;
    let batches = df.collect().await
        .map_err(|e| format!("DataFusion S3 collect: {}", e))?;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    Ok((row_count, engine_label))
}

/// Execute SQL on DuckDB with direct S3 access via httpfs extension.
///
/// Configures DuckDB's built-in S3 support (httpfs), creates views that read from
/// `read_parquet('s3://...')`, and runs the rewritten SQL.
async fn execute_duckdb_s3_direct(
    state: &AppState,
    original_sql: &str,
    table_s3_locations: &std::collections::HashMap<String, String>,
    creds: &S3BucketCreds,
    _conn_id: &str,
) -> std::result::Result<Vec<RecordBatch>, String> {
    #[cfg(feature = "duckdb")]
    {
        if let Some(ref engine) = state.duckdb_engine {
            let (access_key, secret_key, region) = (&creds.access_key, &creds.secret_key, &creds.region);

            // Install and load httpfs, configure S3 credentials
            let setup_sql = format!(
                "INSTALL httpfs; LOAD httpfs; \
                 SET s3_region='{}'; \
                 SET s3_access_key_id='{}'; \
                 SET s3_secret_access_key='{}';",
                region, access_key, secret_key
            );

            engine.sql(&setup_sql).await
                .map_err(|e| format!("DuckDB S3 setup: {}", e))?;

            // Rewrite SQL: replace table references with read_parquet('s3://...')
            let mut rewritten_sql = original_sql.to_string();
            let mut view_setup = String::new();

            for (fqn, s3_location) in table_s3_locations {
                let parts: Vec<&str> = fqn.splitn(3, '.').collect();
                if parts.len() != 3 {
                    continue;
                }

                // Use the original Trino FQN table reference from the SQL
                // The original_sql has catalog.schema.table references
                let s3_glob = if s3_location.ends_with('/') {
                    format!("{}*.parquet", s3_location)
                } else {
                    format!("{}/data/*.parquet", s3_location)
                };

                // Create a safe view name and set up the view
                let safe_name = format!("s3_{}_{}_{}", parts[0], parts[1], parts[2]);
                view_setup.push_str(&format!(
                    "CREATE OR REPLACE VIEW \"{}\" AS SELECT * FROM read_parquet('{}'); ",
                    safe_name, s3_glob
                ));

                // Replace the FQN in the SQL with the view name
                rewritten_sql = rewritten_sql.replace(fqn, &safe_name);
                // Also try quoted form
                let quoted_fqn = format!("\"{}\".\"{}\".\"{}\""  , parts[0], parts[1], parts[2]);
                rewritten_sql = rewritten_sql.replace(&quoted_fqn, &safe_name);
            }

            if !view_setup.is_empty() {
                engine.sql(&view_setup).await
                    .map_err(|e| format!("DuckDB S3 view setup: {}", e))?;
            }

            return engine.sql(&rewritten_sql).await
                .map_err(|e| format!("DuckDB S3 query: {}", e));
        }
    }
    let _ = (state, original_sql, table_s3_locations, creds, _conn_id);
    Err("DuckDB engine not available".to_string())
}

/// Compare SQL execution across all available engines.
/// Returns per-engine timing, row counts, and the winner.
async fn compare_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let query_id = Uuid::new_v4();
    tracing::info!(%query_id, sql = %req.sql, "Comparing SQL across engines");

    // Run on DataFusion
    let df_start = Instant::now();
    let ctx = state.ctx.read().await;
    let df_result = match ctx.datafusion_ctx().sql(&req.sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                serde_json::json!({
                    "duration_ms": df_start.elapsed().as_millis() as u64,
                    "row_count": row_count,
                    "status": "success"
                })
            }
            Err(e) => serde_json::json!({
                "duration_ms": df_start.elapsed().as_millis() as u64,
                "row_count": 0,
                "status": "error",
                "error": e.to_string()
            }),
        },
        Err(e) => serde_json::json!({
            "duration_ms": df_start.elapsed().as_millis() as u64,
            "row_count": 0,
            "status": "error",
            "error": e.to_string()
        }),
    };
    drop(ctx);

    // Run on DuckDB
    let duck_result = if state.duckdb_available() {
        let duck_start = Instant::now();
        match execute_via_duckdb(&state, &req.sql).await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                serde_json::json!({
                    "duration_ms": duck_start.elapsed().as_millis() as u64,
                    "row_count": row_count,
                    "status": "success"
                })
            }
            Err(e) => serde_json::json!({
                "duration_ms": duck_start.elapsed().as_millis() as u64,
                "row_count": 0,
                "status": "error",
                "error": e.to_string()
            }),
        }
    } else {
        serde_json::json!({ "duration_ms": 0, "row_count": 0, "status": "unavailable" })
    };

    // Run on Polars
    let polars_result = if state.polars_available() {
        let polars_start = Instant::now();
        match execute_via_polars(&state, &req.sql).await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                serde_json::json!({
                    "duration_ms": polars_start.elapsed().as_millis() as u64,
                    "row_count": row_count,
                    "status": "success"
                })
            }
            Err(e) => serde_json::json!({
                "duration_ms": polars_start.elapsed().as_millis() as u64,
                "row_count": 0,
                "status": "error",
                "error": e.to_string()
            }),
        }
    } else {
        serde_json::json!({ "duration_ms": 0, "row_count": 0, "status": "unavailable" })
    };

    // Determine winner
    let df_ms = df_result["duration_ms"].as_f64().unwrap_or(f64::MAX);
    let df_ok = df_result["status"].as_str() == Some("success");
    let dk_ms = duck_result["duration_ms"].as_f64().unwrap_or(f64::MAX);
    let dk_ok = duck_result["status"].as_str() == Some("success");
    let pl_ms = polars_result["duration_ms"].as_f64().unwrap_or(f64::MAX);
    let pl_ok = polars_result["status"].as_str() == Some("success");

    let mut best_ms = f64::MAX;
    let mut winner = "N/A";
    if df_ok && df_ms < best_ms { best_ms = df_ms; winner = "DataFusion"; }
    if dk_ok && dk_ms < best_ms { best_ms = dk_ms; winner = "DuckDB"; }
    if pl_ok && pl_ms < best_ms { best_ms = pl_ms; winner = "Polars"; }

    let slowest = [df_ms, dk_ms, pl_ms].iter().copied()
        .filter(|&v| v < f64::MAX && v > 0.0)
        .fold(0.0_f64, f64::max);
    let speedup = if best_ms > 0.0 && slowest > 0.0 { slowest / best_ms } else { 1.0 };

    Ok(Json(serde_json::json!({
        "query_id": query_id.to_string(),
        "sql": req.sql,
        "datafusion": df_result,
        "duckdb": duck_result,
        "polars": polars_result,
        "speedup": (speedup * 100.0).round() / 100.0,
        "winner": winner,
    })))
}

/// Shared response builder for execute_sql (avoids duplication with fallback path).
async fn finish_sql_response(
    state: &AppState,
    query_id: Uuid,
    sql: &str,
    query_type: QueryType,
    engine_name: &str,
    batches: Vec<RecordBatch>,
    parse_ms: u128,
    exec_ms: u128,
    duration_ms: u128,
) -> std::result::Result<Json<SqlResponse>, (StatusCode, Json<ErrorResponse>)> {
    let columns = if let Some(batch) = batches.first() {
        batch.schema().fields().iter().map(|f| f.name().clone()).collect()
    } else {
        vec![]
    };

    let rows = batches_to_json(&batches).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to serialize results: {}", e),
            }),
        )
    })?;

    let row_count = rows.len();

    tracing::info!(
        %query_id,
        engine = engine_name,
        query_type = %query_type,
        row_count,
        exec_ms,
        duration_ms,
        "Query complete"
    );

    state
        .record_query(QueryHistoryEntry {
            query_id,
            sql: sql.to_string(),
            query_type: query_type.to_string(),
            row_count,
            duration_ms,
            timestamp: Utc::now(),
            status: "success".to_string(),
            error: None,
            engine: engine_name.to_string(),
        })
        .await;

    Ok(Json(SqlResponse {
        query_id: query_id.to_string(),
        columns,
        rows,
        row_count,
        query_type: query_type.to_string(),
        duration_ms,
        parse_ms: Some(parse_ms),
        exec_ms: Some(exec_ms),
        engine: engine_name.to_string(),
    }))
}

/// Handle CREATE TABLE ... AS SELECT by executing the SELECT, creating a MemTable, and registering it.
async fn handle_ctas(
    state: Arc<AppState>,
    sql: String,
    query_id: Uuid,
    query_type: QueryType,
    parse_ms: u128,
    start: Instant,
) -> std::result::Result<Json<SqlResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Parse: CREATE TABLE <name> AS <select_sql>
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    let as_pos = upper.find(" AS ").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: "Invalid CTAS syntax. Expected: CREATE TABLE <name> AS SELECT ...".into() }),
        )
    })?;

    let before_as = &trimmed[..as_pos];
    let select_sql = trimmed[as_pos + 4..].trim();
    let table_name = before_as
        .strip_prefix("CREATE TABLE")
        .or_else(|| before_as.strip_prefix("create table"))
        .unwrap_or(before_as)
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .to_string();

    if table_name.is_empty() || select_sql.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: "Invalid CTAS syntax. Expected: CREATE TABLE <name> AS SELECT ...".into() }),
        ));
    }

    // Execute the SELECT query
    let exec_start = Instant::now();
    let ctx = state.ctx.read().await;
    let batches = ctx.sql(select_sql).await.map_err(|e| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("CTAS SELECT failed: {}", e) }))
    })?;

    if batches.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: "CTAS SELECT returned no data".into() }),
        ));
    }

    // Register as MemTable
    let schema = batches[0].schema();
    let mem_table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches.clone()])
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Failed to create table: {}", e) }))
        })?;

    ctx.datafusion_ctx()
        .register_table(&table_name, std::sync::Arc::new(mem_table))
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Failed to register table '{}': {}", table_name, e) }))
        })?;

    let exec_ms = exec_start.elapsed().as_millis();
    let duration_ms = start.elapsed().as_millis();

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    state.query_count.fetch_add(1, Ordering::Relaxed);
    state
        .record_query(QueryHistoryEntry {
            query_id,
            sql,
            query_type: query_type.to_string(),
            row_count,
            duration_ms,
            timestamp: Utc::now(),
            status: "success".to_string(),
            error: None,
            engine: "DataFusion".to_string(),
        })
        .await;

    Ok(Json(SqlResponse {
        query_id: query_id.to_string(),
        columns: vec!["result".to_string()],
        rows: vec![serde_json::json!({"result": format!("Table '{}' created with {} rows", table_name, row_count)})],
        row_count: 1,
        query_type: "DDL".to_string(),
        duration_ms,
        parse_ms: Some(parse_ms),
        exec_ms: Some(exec_ms),
        engine: "DataFusion".to_string(),
    }))
}

async fn list_tables(
    State(state): State<Arc<AppState>>,
) -> std::result::Result<Json<TableListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;
    let tables = ctx.list_tables().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    Ok(Json(TableListResponse { tables }))
}

async fn register_table(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterTableRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;

    // Auto-detect format from file extension if format is "auto"
    let format = if req.format == "auto" {
        if req.path.ends_with(".csv") {
            "csv"
        } else if req.path.ends_with(".json") || req.path.ends_with(".ndjson") {
            "json"
        } else {
            "parquet"
        }
    } else {
        req.format.as_str()
    };

    let result = match format {
        "parquet" => ctx.register_parquet(&req.name, &req.path).await,
        "csv" => ctx.register_csv(&req.name, &req.path).await,
        "json" => ctx.register_json(&req.name, &req.path).await,
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!(
                        "Unsupported format: {}. Use 'parquet', 'csv', or 'json'.",
                        other
                    ),
                }),
            ));
        }
    };

    result.map_err(|e| {
        tracing::warn!(table = %req.name, path = %req.path, error = %e, "Table registration failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    tracing::info!(table = %req.name, path = %req.path, format, "Table registered");

    // Sync newly registered table to DuckDB and Polars engines
    let table_name = req.name.clone();
    let sync_sql = format!("SELECT * FROM \"{}\"", table_name);
    if let Ok(df) = ctx.datafusion_ctx().sql(&sync_sql).await {
        if let Ok(batches) = df.collect().await {
            if !batches.is_empty() {
                #[cfg(feature = "duckdb")]
                if let Some(ref duckdb) = state.duckdb_engine {
                    match duckdb.register_arrow_table(&table_name, &batches).await {
                        Ok(()) => tracing::debug!(table = %table_name, "Synced to DuckDB"),
                        Err(e) => tracing::debug!(table = %table_name, error = %e, "DuckDB sync skipped"),
                    }
                }
                #[cfg(feature = "polars")]
                if let Some(ref polars) = state.polars_engine {
                    match polars.register_arrow_table(&table_name, &batches).await {
                        Ok(()) => tracing::debug!(table = %table_name, "Synced to Polars"),
                        Err(e) => tracing::debug!(table = %table_name, error = %e, "Polars sync skipped"),
                    }
                }
            }
        }
    }

    Ok(Json(serde_json::json!({
        "status": "ok",
        "table": req.name,
        "path": req.path,
        "format": req.format,
    })))
}

// ── New Endpoints ─────────────────────────────────────────────────

/// GET /api/v1/query/history — returns the last N queries from in-memory history.
async fn query_history(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HistoryQuery>,
) -> Json<serde_json::Value> {
    let limit = params.limit.unwrap_or(50);
    let history = state.query_history.read().await;

    // Return the most recent entries first, up to `limit`
    let entries: Vec<&QueryHistoryEntry> = history.iter().rev().take(limit).collect();

    Json(serde_json::json!({
        "count": entries.len(),
        "history": entries,
    }))
}

/// GET /api/v1/tables/:name/schema — returns column names, data types, and nullability.
async fn table_schema(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> std::result::Result<Json<TableSchemaResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;
    let df_ctx = ctx.datafusion_ctx();

    // Look up the table provider — support schema-qualified names like "s3_sales.orders"
    let table_ref = parse_table_reference(&name);
    let provider = df_ctx.table_provider(table_ref).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found: {}", name, e),
            }),
        )
    })?;

    let schema = provider.schema();
    let columns: Vec<ColumnSchema> = schema
        .fields()
        .iter()
        .map(|field| ColumnSchema {
            name: field.name().clone(),
            data_type: format!("{}", field.data_type()),
            nullable: field.is_nullable(),
        })
        .collect();

    Ok(Json(TableSchemaResponse {
        table: name,
        columns,
    }))
}

/// GET /api/v1/tables/:name/preview — returns the first 100 rows of a table.
async fn table_preview(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> std::result::Result<Json<TablePreviewResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;

    // For schema-qualified names like "s3_sales.orders", use schema.table directly
    let sql_name = if name.contains('.') {
        name.clone()
    } else {
        format!("\"{}\"", name)
    };
    let sql = format!("SELECT * FROM {} LIMIT 100", sql_name);
    let batches = ctx.sql(&sql).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Failed to preview table '{}': {}", name, e),
            }),
        )
    })?;

    let columns = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    } else {
        vec![]
    };

    let rows = batches_to_json(&batches).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to serialize preview: {}", e),
            }),
        )
    })?;

    let row_count = rows.len();

    Ok(Json(TablePreviewResponse {
        table: name,
        columns,
        rows,
        row_count,
    }))
}

/// GET /api/v1/tables/:name/stats — returns row count, column count, and per-column statistics.
async fn table_stats(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> std::result::Result<Json<TableStatsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;

    // Get the schema first — support schema-qualified names
    let df_ctx = ctx.datafusion_ctx();
    let table_ref = parse_table_reference(&name);
    let provider = df_ctx.table_provider(table_ref).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found: {}", name, e),
            }),
        )
    })?;

    let schema = provider.schema();
    let column_count = schema.fields().len();

    // Get row count
    let count_sql = format!("SELECT COUNT(*) AS cnt FROM \"{}\"", name);
    let count_batches = ctx.sql(&count_sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to count rows for '{}': {}", name, e),
            }),
        )
    })?;

    let row_count = if let Some(batch) = count_batches.first() {
        if batch.num_rows() > 0 {
            let col = batch.column(0);
            // The count result could be Int64 or UInt64
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                arr.value(0) as usize
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                arr.value(0) as usize
            } else {
                0
            }
        } else {
            0
        }
    } else {
        0
    };

    // Gather per-column stats: min, max, null_count
    let mut column_stats = Vec::with_capacity(column_count);
    for field in schema.fields() {
        let col_name = field.name();
        let data_type = format!("{}", field.data_type());

        // Build a stats query for this column.
        // MIN/MAX work on numeric, string, date, timestamp types in DataFusion.
        // We wrap in a TRY and fall back to None if the aggregate fails for a given type.
        let stats_sql = format!(
            "SELECT MIN(\"{col}\") AS min_val, MAX(\"{col}\") AS max_val, \
             COUNT(*) - COUNT(\"{col}\") AS null_count FROM \"{table}\"",
            col = col_name,
            table = name,
        );

        match ctx.sql(&stats_sql).await {
            Ok(stat_batches) => {
                let stat_rows = batches_to_json(&stat_batches).unwrap_or_default();
                if let Some(row) = stat_rows.first() {
                    let min_val = row.get("min_val").cloned().filter(|v| !v.is_null());
                    let max_val = row.get("max_val").cloned().filter(|v| !v.is_null());
                    let null_count = row.get("null_count").and_then(|v| v.as_u64()).unwrap_or(0);

                    column_stats.push(ColumnStats {
                        name: col_name.clone(),
                        data_type,
                        min: min_val,
                        max: max_val,
                        null_count,
                    });
                } else {
                    column_stats.push(ColumnStats {
                        name: col_name.clone(),
                        data_type,
                        min: None,
                        max: None,
                        null_count: 0,
                    });
                }
            }
            Err(_) => {
                // If aggregation fails for this column type, still report the column
                column_stats.push(ColumnStats {
                    name: col_name.clone(),
                    data_type,
                    min: None,
                    max: None,
                    null_count: 0,
                });
            }
        }
    }

    Ok(Json(TableStatsResponse {
        table: name,
        row_count,
        column_count,
        columns: column_stats,
    }))
}

/// GET /api/v1/system/info — returns platform metadata, uptime, and query counts.
async fn system_info(State(state): State<Arc<AppState>>) -> Json<SystemInfoResponse> {
    let uptime = state.start_time.elapsed();
    let total_queries = state.query_count.load(Ordering::Relaxed);

    Json(SystemInfoResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        engine: "DataFusion".to_string(),
        uptime_seconds: uptime.as_secs(),
        total_queries,
        arrow_version: "57".to_string(),
        datafusion_version: "51".to_string(),
    })
}

/// GET /api/v1/system/resources — returns machine CPU/memory and engine configuration.
async fn system_resources(State(state): State<Arc<AppState>>) -> Json<SystemResourcesResponse> {
    let cpu_cores = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);

    // Platform-specific total memory detection
    let total_memory_bytes: u64 = {
        #[cfg(target_os = "macos")]
        {
            std::process::Command::new("sysctl")
                .args(["-n", "hw.memsize"])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0)
        }
        #[cfg(target_os = "linux")]
        {
            std::fs::read_to_string("/proc/meminfo")
                .ok()
                .and_then(|s| {
                    s.lines()
                        .find(|l| l.starts_with("MemTotal:"))
                        .and_then(|l| {
                            l.split_whitespace()
                                .nth(1)
                                .and_then(|v| v.parse::<u64>().ok())
                                .map(|kb| kb * 1024)
                        })
                })
                .unwrap_or(0)
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            0u64
        }
    };

    let flight_status = match &state.flight_metrics {
        Some(fm) if fm.running.load(std::sync::atomic::Ordering::Relaxed) => "running".to_string(),
        Some(_) => "stopped".to_string(),
        None => "disabled".to_string(),
    };

    let ctx = state.ctx.read().await;
    let node_role_str = match ctx.config().cluster.node_role {
        rustlake_core::config::NodeRole::Standalone => "standalone",
        rustlake_core::config::NodeRole::Coordinator => "coordinator",
        rustlake_core::config::NodeRole::Worker => "worker",
    };
    let distributed = ctx.config().cluster.node_role != rustlake_core::config::NodeRole::Standalone;
    drop(ctx);

    Json(SystemResourcesResponse {
        cpu_cores,
        total_memory_bytes,
        engine_memory_limit: None,
        batch_size: 8192,
        target_partitions: cpu_cores,
        tokio_workers: cpu_cores,
        distributed_mode: distributed,
        flight_status,
        node_role: node_role_str.to_string(),
    })
}

// ── Flight Info ───────────────────────────────────────────────────

/// GET /api/v1/flight/info — returns Arrow Flight server capabilities and status.
async fn flight_info(State(state): State<Arc<AppState>>) -> Json<FlightInfoResponse> {
    let (status, active_clients, queries_served) = match &state.flight_metrics {
        Some(fm) => {
            let running = fm.running.load(std::sync::atomic::Ordering::Relaxed);
            let status = if running { "running" } else { "stopped" };
            let active = fm.active_connections.load(std::sync::atomic::Ordering::Relaxed);
            let served = fm.queries_served.load(std::sync::atomic::Ordering::Relaxed);
            (status.to_string(), active, served)
        }
        None => ("disabled".to_string(), 0, 0),
    };

    // Read flight config from the engine context.
    let ctx = state.ctx.read().await;
    let flight_cfg = &ctx.config().flight;

    Json(FlightInfoResponse {
        protocol: "Arrow Flight RPC".to_string(),
        host: flight_cfg.host.clone(),
        port: flight_cfg.port,
        status,
        max_message_size: flight_cfg.max_message_size,
        capabilities: vec![
            "SQL queries (do_get)".to_string(),
            "Schema inspection (get_flight_info)".to_string(),
            "Health check action".to_string(),
            "Bulk data transfer".to_string(),
        ],
        arrow_version: "57".to_string(),
        active_clients,
        queries_served,
        supported_clients: vec![
            "DBeaver".to_string(),
            "Tableau".to_string(),
            "Superset".to_string(),
            "Python (pyarrow.flight)".to_string(),
            "Rust (arrow-flight)".to_string(),
            "Go (apache-arrow-go)".to_string(),
        ],
    })
}

/// GET /api/v1/flight/status — returns detailed Flight server status.
async fn flight_status(State(state): State<Arc<AppState>>) -> Json<FlightStatusResponse> {
    let ctx = state.ctx.read().await;
    let flight_cfg = &ctx.config().flight;

    let (running, active_connections, queries_served) = match &state.flight_metrics {
        Some(fm) => (
            fm.running.load(std::sync::atomic::Ordering::Relaxed),
            fm.active_connections.load(std::sync::atomic::Ordering::Relaxed),
            fm.queries_served.load(std::sync::atomic::Ordering::Relaxed),
        ),
        None => (false, 0, 0),
    };

    Json(FlightStatusResponse {
        enabled: flight_cfg.enabled,
        running,
        host: flight_cfg.host.clone(),
        port: flight_cfg.port,
        active_connections,
        queries_served,
    })
}

// ── Transform / Lineage ──────────────────────────────────────────

/// Build the hardcoded list of transform entries used by the transforms and lineage endpoints.
fn build_transform_entries() -> Vec<TransformEntry> {
    vec![
        TransformEntry {
            name: "stg_orders".to_string(),
            sql: "SELECT order_id, customer_id, total_amount, status, CAST(order_date AS DATE) as order_date FROM {{ source('raw', 'orders') }}".to_string(),
            depends_on: vec!["raw.orders".to_string()],
            materialization: "view".to_string(),
            description: "Staged orders with clean types".to_string(),
        },
        TransformEntry {
            name: "stg_customers".to_string(),
            sql: "SELECT customer_id, name, email, city, tier FROM {{ source('raw', 'customers') }}".to_string(),
            depends_on: vec!["raw.customers".to_string()],
            materialization: "view".to_string(),
            description: "Staged customer profiles".to_string(),
        },
        TransformEntry {
            name: "fct_revenue".to_string(),
            sql: "SELECT o.customer_id, c.name, c.tier, ROUND(SUM(o.total_amount), 2) as total_revenue, COUNT(*) as order_count FROM {{ ref('stg_orders') }} o JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id WHERE o.status = 'completed' GROUP BY o.customer_id, c.name, c.tier".to_string(),
            depends_on: vec!["stg_orders".to_string(), "stg_customers".to_string()],
            materialization: "table".to_string(),
            description: "Revenue fact table by customer".to_string(),
        },
        TransformEntry {
            name: "dim_product_category".to_string(),
            sql: "SELECT category, COUNT(*) as product_count, ROUND(AVG(price), 2) as avg_price, SUM(stock_qty) as total_stock FROM {{ source('raw', 'products') }} GROUP BY category".to_string(),
            depends_on: vec!["raw.products".to_string()],
            materialization: "table".to_string(),
            description: "Product category dimension".to_string(),
        },
        TransformEntry {
            name: "rpt_customer_ltv".to_string(),
            sql: "SELECT r.name, r.tier, r.total_revenue, r.order_count, ROUND(r.total_revenue / r.order_count, 2) as avg_order_value, CASE WHEN r.total_revenue > 500 THEN 'high' WHEN r.total_revenue > 200 THEN 'medium' ELSE 'low' END as ltv_segment FROM {{ ref('fct_revenue') }} r ORDER BY r.total_revenue DESC".to_string(),
            depends_on: vec!["fct_revenue".to_string()],
            materialization: "view".to_string(),
            description: "Customer lifetime value report".to_string(),
        },
        TransformEntry {
            name: "tpch_revenue_by_nation".to_string(),
            sql: "SELECT c.city as nation, ROUND(SUM(o.total_amount), 2) as revenue, COUNT(*) as order_count FROM {{ ref('stg_orders') }} o JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id WHERE o.status = 'completed' GROUP BY c.city ORDER BY revenue DESC".to_string(),
            depends_on: vec!["stg_orders".to_string(), "stg_customers".to_string()],
            materialization: "table".to_string(),
            description: "Revenue aggregation by customer city/nation".to_string(),
        },
    ]
}

/// Build the `SqlCompiler` from the transform entries, using `rustlake_transform::Model`.
fn build_compiler_from_entries(entries: &[TransformEntry]) -> SqlCompiler {
    let models: Vec<Model> = entries
        .iter()
        .map(|e| Model {
            name: e.name.clone(),
            sql: e.sql.clone(),
            config: ModelConfig::default(),
            description: e.description.clone(),
            columns: vec![],
        })
        .collect();
    SqlCompiler::new(models)
}

/// Map a `source('raw', 'table')` reference to a file path for query execution.
/// Returns a single-quoted path that the engine's auto-register will pick up.
fn source_to_file_path(source_name: &str, table_name: &str) -> Option<String> {
    if source_name == "raw" {
        Some(format!("'sample-data/{}.csv'", table_name))
    } else {
        None
    }
}

/// GET /api/v1/transforms — returns the list of available SQL transforms.
async fn list_transforms(
    State(state): State<Arc<AppState>>,
) -> Json<TransformsResponse> {
    let mut transforms = build_transform_entries();
    // Append user-created transforms
    let user = state.user_transforms.read().await;
    for ut in user.iter() {
        transforms.push(TransformEntry {
            name: ut.name.clone(),
            sql: ut.sql.clone(),
            depends_on: ut.depends_on.clone(),
            materialization: ut.materialization.clone(),
            description: ut.description.clone(),
        });
    }
    Json(TransformsResponse { transforms })
}

/// GET /api/v1/lineage — returns the DAG lineage graph.
async fn lineage() -> Json<LineageResponse> {
    Json(LineageResponse {
        nodes: vec![
            LineageNode {
                id: "raw.orders".to_string(),
                node_type: "source".to_string(),
                format: Some("csv".to_string()),
                materialization: None,
            },
            LineageNode {
                id: "raw.customers".to_string(),
                node_type: "source".to_string(),
                format: Some("csv".to_string()),
                materialization: None,
            },
            LineageNode {
                id: "raw.products".to_string(),
                node_type: "source".to_string(),
                format: Some("csv".to_string()),
                materialization: None,
            },
            LineageNode {
                id: "stg_orders".to_string(),
                node_type: "staging".to_string(),
                format: None,
                materialization: Some("view".to_string()),
            },
            LineageNode {
                id: "stg_customers".to_string(),
                node_type: "staging".to_string(),
                format: None,
                materialization: Some("view".to_string()),
            },
            LineageNode {
                id: "fct_revenue".to_string(),
                node_type: "fact".to_string(),
                format: None,
                materialization: Some("table".to_string()),
            },
            LineageNode {
                id: "dim_product_category".to_string(),
                node_type: "dimension".to_string(),
                format: None,
                materialization: Some("table".to_string()),
            },
            LineageNode {
                id: "rpt_customer_ltv".to_string(),
                node_type: "report".to_string(),
                format: None,
                materialization: Some("view".to_string()),
            },
        ],
        edges: vec![
            LineageEdge {
                from: "raw.orders".to_string(),
                to: "stg_orders".to_string(),
            },
            LineageEdge {
                from: "raw.customers".to_string(),
                to: "stg_customers".to_string(),
            },
            LineageEdge {
                from: "stg_orders".to_string(),
                to: "fct_revenue".to_string(),
            },
            LineageEdge {
                from: "stg_customers".to_string(),
                to: "fct_revenue".to_string(),
            },
            LineageEdge {
                from: "fct_revenue".to_string(),
                to: "rpt_customer_ltv".to_string(),
            },
            LineageEdge {
                from: "raw.products".to_string(),
                to: "dim_product_category".to_string(),
            },
        ],
    })
}

/// POST /api/v1/transforms/:name/run — compiles and executes a named transform.
///
/// Uses the `SqlCompiler` from `rustlake-transform` to resolve `ref()` and `source()` macros,
/// then runs the compiled SQL against the engine. Source macros are mapped to `sample-data/` CSV files.
async fn run_transform(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> std::result::Result<Json<TransformRunResponse>, (StatusCode, Json<ErrorResponse>)> {
    let start = Instant::now();
    let mut entries = build_transform_entries();
    // Merge user-created transforms so they're runnable
    let user = state.user_transforms.read().await;
    for ut in user.iter() {
        entries.push(TransformEntry {
            name: ut.name.clone(),
            sql: ut.sql.clone(),
            depends_on: ut.depends_on.clone(),
            materialization: ut.materialization.clone(),
            description: ut.description.clone(),
        });
    }
    drop(user);
    let compiler = build_compiler_from_entries(&entries);

    // Verify the requested transform exists
    if !entries.iter().any(|e| e.name == name) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!(
                    "Transform '{}' not found. Available: {}",
                    name,
                    entries
                        .iter()
                        .map(|e| e.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }),
        ));
    }

    // Compile the model with source mapping to file paths (for the response).
    let compiled_sql = compiler
        .compile_with_source_map(&name, source_to_file_path)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to compile transform '{}': {}", name, e),
                }),
            )
        })?;

    // For models that depend on other models (ref), we need to compile those too
    // and wrap them as CTEs. Build the full executable SQL.
    let executable_sql = build_executable_sql(&name, &entries, &compiler)?;

    tracing::info!(
        transform = %name,
        compiled_sql = %executable_sql,
        "Executing transform"
    );

    // Execute the compiled SQL
    let ctx = state.ctx.read().await;
    let batches = ctx.sql(&executable_sql).await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Transform execution failed: {}", e),
            }),
        )
    })?;

    let duration_ms = start.elapsed().as_millis();

    let columns = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    } else {
        vec![]
    };

    let rows = batches_to_json(&batches).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to serialize transform results: {}", e),
            }),
        )
    })?;

    let row_count = rows.len();

    Ok(Json(TransformRunResponse {
        transform: name,
        compiled_sql,
        columns,
        rows,
        row_count,
        duration_ms,
    }))
}

/// Build executable SQL for a transform, recursively resolving ref() dependencies as CTEs.
fn build_executable_sql(
    model_name: &str,
    entries: &[TransformEntry],
    compiler: &SqlCompiler,
) -> std::result::Result<String, (StatusCode, Json<ErrorResponse>)> {
    let mut cte_parts: Vec<String> = Vec::new();
    let mut resolved: Vec<String> = Vec::new();

    resolve_deps(model_name, entries, compiler, &mut cte_parts, &mut resolved)?;

    if cte_parts.len() <= 1 {
        let target_sql = compiler
            .compile_with_source_map(model_name, source_to_file_path)
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Failed to compile transform '{}': {}", model_name, e),
                    }),
                )
            })?;
        Ok(target_sql)
    } else {
        let target_cte = cte_parts.pop().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Transform produced no SQL output".into(),
                }),
            )
        })?;
        let target_sql = target_cte
            .find(" AS (")
            .map(|pos| &target_cte[pos + 5..target_cte.len() - 1])
            .unwrap_or(&target_cte);
        Ok(format!("WITH {} {}", cte_parts.join(", "), target_sql))
    }
}

/// Recursively resolve model dependencies, adding each as a CTE in topological order.
fn resolve_deps(
    name: &str,
    entries: &[TransformEntry],
    compiler: &SqlCompiler,
    cte_parts: &mut Vec<String>,
    resolved: &mut Vec<String>,
) -> std::result::Result<(), (StatusCode, Json<ErrorResponse>)> {
    if resolved.contains(&name.to_string()) {
        return Ok(());
    }

    let entry = match entries.iter().find(|e| e.name == name) {
        Some(e) => e,
        None => return Ok(()),
    };

    for dep in &entry.depends_on {
        if !dep.contains('.') {
            resolve_deps(dep, entries, compiler, cte_parts, resolved)?;
        }
    }

    let compiled = compiler
        .compile_with_source_map(name, source_to_file_path)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to compile dependency '{}': {}", name, e),
                }),
            )
        })?;

    cte_parts.push(format!("{} AS ({})", name, compiled));
    resolved.push(name.to_string());
    Ok(())
}

// ── Vector Search Endpoints ──────────────────────────────────────

/// Request body for indexing documents into the vector index.
#[derive(Deserialize)]
pub struct VectorIndexRequest {
    /// Documents to add to the vector index.
    pub documents: Vec<VectorDocument>,
}

/// A single document to be indexed.
#[derive(Deserialize)]
pub struct VectorDocument {
    /// Unique document identifier.
    pub id: String,
    /// Text content to generate an embedding from.
    pub text: String,
    /// Optional metadata to store alongside the embedding.
    #[serde(default = "default_metadata")]
    pub metadata: serde_json::Value,
}

fn default_metadata() -> serde_json::Value {
    serde_json::json!({})
}

/// Request body for vector similarity search.
#[derive(Deserialize)]
pub struct VectorSearchRequest {
    /// Natural language query text to search for.
    pub query: String,
    /// Number of nearest neighbors to return. Defaults to 10.
    #[serde(default = "default_k")]
    pub k: usize,
}

fn default_k() -> usize {
    10
}

/// Response for vector search.
#[derive(Serialize)]
pub struct VectorSearchResponse {
    /// The original query text.
    pub query: String,
    /// Matching documents sorted by descending similarity.
    pub results: Vec<rustlake_vector::search::IndexSearchResult>,
    /// Number of results returned.
    pub result_count: usize,
    /// Search execution time in milliseconds.
    pub duration_ms: u128,
}

/// Response for vector index status.
#[derive(Serialize)]
pub struct VectorStatusResponse {
    /// Number of documents currently indexed.
    pub document_count: usize,
    /// Embedding vector dimensionality.
    pub dimensions: usize,
    /// Index algorithm type (e.g., "brute_force_cosine").
    pub index_type: String,
    /// Name of the embedding model in use.
    pub embedding_model: String,
}

/// POST /api/v1/vector/index — index a set of documents with embeddings.
async fn vector_index_documents(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VectorIndexRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let doc_count = req.documents.len();
    tracing::info!(
        doc_count = doc_count,
        "Indexing documents into vector store"
    );

    let mut index = state.vector_index.write().await;

    for doc in req.documents {
        let embedding = state.embedding_generator.generate_embedding(&doc.text);
        index.add(doc.id, doc.text, embedding, doc.metadata);
    }

    let total = index.len();

    Ok(Json(serde_json::json!({
        "status": "ok",
        "indexed": doc_count,
        "total_documents": total,
    })))
}

/// POST /api/v1/vector/search — perform semantic similarity search.
async fn vector_search(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VectorSearchRequest>,
) -> Json<VectorSearchResponse> {
    let start = Instant::now();

    tracing::info!(query = %req.query, k = req.k, "Vector similarity search");

    let query_embedding = state.embedding_generator.generate_embedding(&req.query);
    let index = state.vector_index.read().await;
    let results = index.search(&query_embedding, req.k);

    let result_count = results.len();
    let duration_ms = start.elapsed().as_millis();

    Json(VectorSearchResponse {
        query: req.query,
        results,
        result_count,
        duration_ms,
    })
}

/// GET /api/v1/vector/status — returns vector index statistics.
async fn vector_status(State(state): State<Arc<AppState>>) -> Json<VectorStatusResponse> {
    let index = state.vector_index.read().await;

    Json(VectorStatusResponse {
        document_count: index.len(),
        dimensions: index.dimensions(),
        index_type: "brute_force_cosine".to_string(),
        embedding_model: "simple-hash-v1".to_string(),
    })
}

// ── Upload Response ──────────────────────────────────────────────────

/// Response for the file upload endpoint.
#[derive(Serialize)]
pub struct UploadResponse {
    /// Request status.
    pub status: String,
    /// Registered table name.
    pub table: String,
    /// File path on disk.
    pub path: String,
    /// Detected file format.
    pub format: String,
    /// File size in bytes.
    pub file_size: u64,
}

// ── Connection Request / Response Types ─────────────────────────────

/// Request body for adding a new database connection.
#[derive(Clone, Deserialize)]
pub struct AddConnectionRequest {
    /// User-assigned connection name.
    pub name: String,
    /// Connection type (e.g., "postgres").
    #[serde(default = "default_conn_type")]
    pub conn_type: String,
    /// Database host.
    pub host: String,
    /// Database port.
    #[serde(default = "default_pg_port")]
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Database username.
    pub username: String,
    /// Database password.
    #[serde(default)]
    pub password: String,
    /// Authentication method: "scram" (default), "aws_iam", "x509", "connection_string".
    #[serde(default = "default_auth_method")]
    pub auth_method: String,
    /// Raw connection string (for auth_method = "connection_string", e.g., Atlas mongodb+srv:// URI).
    #[serde(default)]
    pub connection_string: String,
    /// AWS access key ID (for auth_method = "aws_iam").
    #[serde(default)]
    pub aws_access_key: String,
    /// AWS secret access key (for auth_method = "aws_iam").
    #[serde(default)]
    pub aws_secret_key: String,
    /// AWS session token (for temporary credentials with auth_method = "aws_iam").
    #[serde(default)]
    pub aws_session_token: String,
    /// AWS region (for auth_method = "aws_iam"). Reserved for future use.
    #[serde(default)]
    #[allow(dead_code)]
    pub aws_region: String,
}

fn default_auth_method() -> String {
    "scram".to_string()
}

/// Build `MongoConnParams` from an `AddConnectionRequest`.
fn build_mongo_params(req: &AddConnectionRequest) -> crate::mongodb_conn::MongoConnParams {
    let auth_method = match req.auth_method.as_str() {
        "aws_iam" => crate::mongodb_conn::MongoAuthMethod::AwsIam,
        "x509" => crate::mongodb_conn::MongoAuthMethod::X509,
        "connection_string" if !req.connection_string.is_empty() => {
            crate::mongodb_conn::MongoAuthMethod::ConnectionString(req.connection_string.clone())
        }
        _ => crate::mongodb_conn::MongoAuthMethod::Scram,
    };
    crate::mongodb_conn::MongoConnParams {
        host: req.host.clone(),
        port: req.port,
        database: req.database.clone(),
        username: req.username.clone(),
        password: req.password.clone(),
        auth_method,
        auth_source: None,
        aws_access_key: if req.aws_access_key.is_empty() { None } else { Some(req.aws_access_key.clone()) },
        aws_secret_key: if req.aws_secret_key.is_empty() { None } else { Some(req.aws_secret_key.clone()) },
        aws_session_token: if req.aws_session_token.is_empty() { None } else { Some(req.aws_session_token.clone()) },
        tls: req.auth_method == "aws_iam" || req.auth_method == "x509",
        replica_set: None,
    }
}

/// Build `MongoConnParams` from a saved `ConnectionEntry` and password.
fn build_mongo_params_from_entry(
    conn: &ConnectionEntry,
    password: &str,
) -> crate::mongodb_conn::MongoConnParams {
    let auth_method = match conn.auth_method.as_str() {
        "aws_iam" => crate::mongodb_conn::MongoAuthMethod::AwsIam,
        "x509" => crate::mongodb_conn::MongoAuthMethod::X509,
        "connection_string" => {
            let uri = conn.connection_string.clone().unwrap_or_default();
            crate::mongodb_conn::MongoAuthMethod::ConnectionString(uri)
        }
        _ => crate::mongodb_conn::MongoAuthMethod::Scram,
    };
    crate::mongodb_conn::MongoConnParams {
        host: conn.host.clone(),
        port: conn.port,
        database: conn.database.clone(),
        username: conn.username.clone(),
        password: password.to_string(),
        auth_method,
        auth_source: None,
        aws_access_key: conn.aws_access_key.clone(),
        aws_secret_key: conn.aws_secret_key.clone(),
        aws_session_token: conn.aws_session_token.clone(),
        tls: conn.auth_method == "aws_iam" || conn.auth_method == "x509",
        replica_set: None,
    }
}

fn default_conn_type() -> String {
    "postgres".to_string()
}

fn default_pg_port() -> u16 {
    5432
}

// ── Streaming Request / Response Types ───────────────────────────────

/// Request body for the stream ingest endpoint.
#[derive(Deserialize)]
pub struct StreamIngestRequest {
    /// Number of events to generate. Defaults to 100.
    #[serde(default = "default_event_count")]
    pub count: usize,
}

fn default_event_count() -> usize {
    100
}

/// Response for the stream ingest endpoint.
#[derive(Serialize)]
pub struct StreamIngestResponse {
    /// Request status ("ok" on success).
    pub status: String,
    /// Number of events generated in this request.
    pub events_generated: usize,
    /// File path where events were written as CSV.
    pub csv_path: String,
    /// Current global streaming metrics after ingestion.
    pub metrics: StreamingMetricsSnapshot,
}

/// Response for the stream status endpoint.
#[derive(Serialize)]
pub struct StreamStatusResponse {
    /// Service status.
    pub status: String,
    /// Current global streaming metrics.
    pub metrics: StreamingMetricsSnapshot,
    /// Number of events in the in-memory buffer.
    pub buffer_size: usize,
}

/// Query parameters for the stream events endpoint.
#[derive(Deserialize)]
pub struct StreamEventsQuery {
    /// Maximum number of events to return. Defaults to 50.
    pub limit: Option<usize>,
}

/// Response for the stream events endpoint.
#[derive(Serialize)]
pub struct StreamEventsResponse {
    /// Number of events returned.
    pub count: usize,
    /// Recent stream events (most recent first).
    pub events: Vec<rustlake_stream::StreamEvent>,
}

// ── Chat / Feedback Request / Response Types ────────────────────────

/// Request body for submitting a chat message (user feedback or developer response).
#[derive(Deserialize)]
pub struct ChatMessageRequest {
    /// The message text.
    pub message: String,
    /// Who is sending: "user" or "developer". Defaults to "user".
    pub sender: Option<String>,
    /// Category: "bug", "feature", "general", "completed", "in_progress", "info".
    pub category: Option<String>,
}

/// Response body after submitting a chat message.
#[derive(Serialize)]
pub struct ChatMessageResponse {
    /// Request status ("ok" on success).
    pub status: String,
    /// Unique ID assigned to this message.
    pub id: String,
    /// Resolved sender.
    pub sender: String,
    /// Resolved category.
    pub category: String,
}

/// Response body for listing chat messages.
#[derive(Serialize)]
pub struct ChatMessagesListResponse {
    /// Total number of messages.
    pub count: usize,
    /// All messages in chronological order.
    pub messages: Vec<ChatMessage>,
}

/// Query params for polling new messages.
#[derive(Deserialize)]
pub struct ChatPollQuery {
    /// Only return messages after this timestamp (ISO 8601).
    pub after: Option<String>,
}

// ── Streaming Handlers ──────────────────────────────────────────────

/// POST /api/v1/stream/ingest -- trigger simulated event ingestion.
///
/// Generates N e-commerce events, appends them to the in-memory buffer,
/// writes them to `sample-data/stream_events.csv`, and returns metrics.
async fn stream_ingest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StreamIngestRequest>,
) -> std::result::Result<Json<StreamIngestResponse>, (StatusCode, Json<ErrorResponse>)> {
    let count = req.count.min(10000); // cap at 10k per request
    let start = Instant::now();

    tracing::info!(count = count, "Triggering simulated stream ingestion");

    // Generate realistic e-commerce events
    let events = SimulatedSource::generate_events(count);
    let generated = events.len();

    // Compute byte size estimate (serialized JSON size)
    let byte_estimate: u64 = events
        .iter()
        .map(|e| {
            serde_json::to_string(e)
                .map(|s| s.len() as u64)
                .unwrap_or(0)
        })
        .sum();

    let elapsed = start.elapsed();
    let eps = if elapsed.as_secs_f64() > 0.0 {
        generated as f64 / elapsed.as_secs_f64()
    } else {
        generated as f64
    };

    // Update global streaming metrics
    state
        .stream_metrics
        .record_ingestion(generated as u64, byte_estimate, eps);

    // Append to the in-memory circular buffer
    state.append_stream_events(events.clone()).await;

    // Write to CSV file
    let csv_path = "sample-data/stream_events.csv".to_string();
    if let Err(e) = write_events_csv(&csv_path, &events) {
        tracing::error!(error = %e, "Failed to write stream events CSV");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to write CSV: {}", e),
            }),
        ));
    }

    tracing::info!(
        events = generated,
        bytes = byte_estimate,
        duration_ms = elapsed.as_millis(),
        "Stream ingestion complete"
    );

    Ok(Json(StreamIngestResponse {
        status: "ok".to_string(),
        events_generated: generated,
        csv_path,
        metrics: state.stream_metrics.snapshot(),
    }))
}

/// GET /api/v1/stream/status -- returns current streaming metrics.
async fn stream_status(State(state): State<Arc<AppState>>) -> Json<StreamStatusResponse> {
    let buffer_size = state.stream_events.read().await.len();

    Json(StreamStatusResponse {
        status: "ok".to_string(),
        metrics: state.stream_metrics.snapshot(),
        buffer_size,
    })
}

/// GET /api/v1/stream/events -- returns last N events from the stream buffer.
async fn stream_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<StreamEventsQuery>,
) -> Json<StreamEventsResponse> {
    let limit = params.limit.unwrap_or(50);
    let buffer = state.stream_events.read().await;

    // Return the most recent events, up to `limit`
    let events: Vec<rustlake_stream::StreamEvent> =
        buffer.iter().rev().take(limit).cloned().collect();
    let count = events.len();

    Json(StreamEventsResponse { count, events })
}

// ── Chat / Feedback Handlers ────────────────────────────────────────

/// POST /api/v1/feedback or /api/v1/feedback/respond — submit a chat message.
async fn submit_chat_message(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatMessageRequest>,
) -> std::result::Result<Json<ChatMessageResponse>, (StatusCode, Json<ErrorResponse>)> {
    let message = req.message.trim().to_string();
    if message.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Message cannot be empty".to_string(),
            }),
        ));
    }

    let sender = match req.sender.as_deref() {
        Some("developer") => "developer".to_string(),
        Some("system") => "system".to_string(),
        _ => "user".to_string(),
    };

    let category = req
        .category
        .unwrap_or_else(|| "general".to_string());

    let id = Uuid::new_v4();
    let msg = ChatMessage {
        id,
        message,
        sender: sender.clone(),
        category: category.clone(),
        timestamp: Utc::now(),
    };

    state.record_chat_message(msg).await;

    tracing::info!(%id, %sender, %category, "Chat message recorded");

    Ok(Json(ChatMessageResponse {
        status: "ok".to_string(),
        id: id.to_string(),
        sender,
        category,
    }))
}

/// GET /api/v1/feedback or /api/v1/feedback/messages — list chat messages.
///
/// Supports `?after=<ISO timestamp>` to poll only new messages.
async fn list_chat_messages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ChatPollQuery>,
) -> Json<ChatMessagesListResponse> {
    let messages = state.chat_messages.read().await;

    let filtered: Vec<ChatMessage> = if let Some(ref after_str) = params.after {
        if let Ok(after_ts) = after_str.parse::<DateTime<Utc>>() {
            messages
                .iter()
                .filter(|m| m.timestamp > after_ts)
                .cloned()
                .collect()
        } else {
            messages.clone()
        }
    } else {
        messages.clone()
    };

    let count = filtered.len();
    Json(ChatMessagesListResponse {
        count,
        messages: filtered,
    })
}

// ── File Upload Handler ─────────────────────────────────────────────

/// POST /api/v1/upload — upload a file (CSV/Parquet/JSON) and auto-register as a table.
async fn upload_file(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> std::result::Result<Json<UploadResponse>, (StatusCode, Json<ErrorResponse>)> {
    const MAX_SIZE: u64 = 100 * 1024 * 1024; // 100MB

    // Read the first file field from the multipart form
    let field = multipart
        .next_field()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Failed to read multipart field: {}", e),
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "No file provided".to_string(),
                }),
            )
        })?;

    let file_name = field
        .file_name()
        .unwrap_or("upload.csv")
        .to_string();

    let mut bytes = Vec::new();
    let mut stream = field;
    while let Ok(Some(chunk)) = stream.chunk().await {
        bytes.extend_from_slice(&chunk);
        if bytes.len() as u64 > MAX_SIZE {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: "File exceeds 100MB limit".to_string(),
                }),
            ));
        }
    }

    let file_size = bytes.len() as u64;

    // Detect format
    let format = if file_name.ends_with(".csv") {
        "csv"
    } else if file_name.ends_with(".parquet") || file_name.ends_with(".parq") {
        "parquet"
    } else if file_name.ends_with(".json") || file_name.ends_with(".ndjson") {
        "json"
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!(
                    "Unsupported file type: {}. Supported: .csv, .parquet, .json, .ndjson",
                    file_name
                ),
            }),
        ));
    };

    // Derive table name from filename
    let table_name = file_name
        .replace(['/', '\\', '.', '-', ' '], "_")
        .trim_end_matches("_csv")
        .trim_end_matches("_parquet")
        .trim_end_matches("_parq")
        .trim_end_matches("_json")
        .trim_end_matches("_ndjson")
        .trim_start_matches('_')
        .to_lowercase();

    // Save to uploads/ directory
    let file_path = format!("uploads/{}", file_name);
    std::fs::write(&file_path, &bytes).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to save file: {}", e),
            }),
        )
    })?;

    // Register with DataFusion
    let ctx = state.ctx.read().await;
    ctx.register_table(&table_name, &file_path).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to register table: {}", e),
            }),
        )
    })?;

    tracing::info!(
        table = %table_name,
        path = %file_path,
        format = %format,
        size = file_size,
        "File uploaded and registered"
    );

    Ok(Json(UploadResponse {
        status: "ok".to_string(),
        table: table_name,
        path: file_path,
        format: format.to_string(),
        file_size,
    }))
}

// ── Connection Handlers ─────────────────────────────────────────────

/// POST /api/v1/connections — verify connectivity, return immediately, discover tables in background.
async fn add_connection(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddConnectionRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let id = Uuid::new_v4().to_string();
    let conn_type = req.conn_type.clone();

    // Quick connectivity check only — validate we can reach the server
    match conn_type.as_str() {
        "trino" | "presto" => {
            let user = if req.username.is_empty() { "rustlake".to_string() } else { req.username.clone() };
            let base_url = trino_base_url(&req.host, req.port);
            let rest = crate::trino_client::TrinoRestClient::new(base_url, user, req.password.clone());
            rest.server_info().await
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach Trino: {}", e) })))?;
        }
        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => {
            // Quick TCP check
            let addr = format!("{}:{}", req.host, req.port);
            tokio::net::TcpStream::connect(&addr).await
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach Postgres: {}", e) })))?;
        }
        #[cfg(feature = "mysql")]
        "mysql" | "mariadb" => {
            let addr = format!("{}:{}", req.host, req.port);
            tokio::net::TcpStream::connect(&addr).await
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach MySQL: {}", e) })))?;
        }
        "mongodb" => {
            // For connection_string or Atlas (aws_iam), try parsing the URI instead of TCP check
            if req.auth_method == "connection_string" || req.auth_method == "aws_iam" {
                let params = build_mongo_params(&req);
                params.build_client().await
                    .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot connect to MongoDB: {}", e) })))?;
            } else {
                let addr = format!("{}:{}", req.host, req.port);
                tokio::net::TcpStream::connect(&addr).await
                    .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach MongoDB: {}", e) })))?;
            }
        }
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            // SQLite: just check file exists
            if !std::path::Path::new(&req.host).exists() {
                return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("SQLite file not found: {}", req.host) })));
            }
        }
        other => {
            return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Unsupported connection type: {}", other) })));
        }
    }

    // Create connection entry immediately with "syncing" status
    let entry = ConnectionEntry {
        id: id.clone(),
        name: req.name.clone(),
        conn_type: req.conn_type.clone(),
        host: req.host.clone(),
        port: req.port,
        database: req.database.clone(),
        username: req.username.clone(),
        status: "connected".to_string(),
        tables: vec![],
        created_at: Utc::now(),
        source: "user".to_string(),
        sync_status: "syncing".to_string(),
        sync_error: None,
        sync_progress: None,
        auth_method: req.auth_method.clone(),
        connection_string: if req.connection_string.is_empty() { None } else { Some(req.connection_string.clone()) },
        aws_access_key: if req.aws_access_key.is_empty() { None } else { Some(req.aws_access_key.clone()) },
        aws_secret_key: if req.aws_secret_key.is_empty() { None } else { Some(req.aws_secret_key.clone()) },
        aws_session_token: if req.aws_session_token.is_empty() { None } else { Some(req.aws_session_token.clone()) },
    };

    state.add_connection_entry(entry).await;
    state.store_password(id.clone(), req.password.clone()).await;

    tracing::info!(
        id = %id,
        name = %req.name,
        conn_type = %req.conn_type,
        "Connection added — starting background table discovery"
    );

    // Return immediately
    let response = serde_json::json!({
        "status": "connected",
        "sync_status": "syncing",
        "id": id,
        "name": req.name,
        "tables": serde_json::Value::Array(vec![]),
    });

    // Spawn background task for table discovery and registration
    let bg_state = state.clone();
    let bg_id = id.clone();
    let bg_req = req.clone();
    tokio::spawn(async move {
        let result = discover_and_register_tables(&bg_state, &bg_id, &bg_req).await;
        match result {
            Ok(tables) => {
                bg_state.update_connection_entry(&bg_id, |entry| {
                    entry.tables = tables.clone();
                    entry.sync_status = "ready".to_string();
                    entry.sync_error = None;
                }).await;
                tracing::info!(
                    id = %bg_id,
                    name = %bg_req.name,
                    tables = tables.len(),
                    "Background sync complete: {} tables registered",
                    tables.len()
                );
            }
            Err(e) => {
                bg_state.update_connection_entry(&bg_id, |entry| {
                    entry.sync_status = "error".to_string();
                    entry.sync_error = Some(e.clone());
                }).await;
                tracing::error!(
                    id = %bg_id,
                    name = %bg_req.name,
                    error = %e,
                    "Background sync failed"
                );
            }
        }
    });

    Ok(Json(response))
}

/// Background task: discover tables and register them as DataFusion providers.
async fn discover_and_register_tables(
    state: &Arc<AppState>,
    id: &str,
    req: &AddConnectionRequest,
) -> std::result::Result<Vec<String>, String> {
    let prefix = match req.conn_type.as_str() {
        "postgres" | "postgresql" => "pg",
        "mysql" | "mariadb" => "mysql",
        "sqlite" => "sqlite",
        "mongodb" => "mongo",
        "trino" | "presto" => "trino",
        _ => &req.conn_type,
    };

    match req.conn_type.as_str() {
        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => {
            let ctx = state.ctx.read().await;
            let df_ctx = ctx.datafusion_ctx();
            state.provider_registry.register_postgres(
                id, &req.host, req.port, &req.database, &req.username, &req.password,
                prefix, df_ctx,
            ).await
        }
        #[cfg(feature = "mysql")]
        "mysql" | "mariadb" => {
            let ctx = state.ctx.read().await;
            let df_ctx = ctx.datafusion_ctx();
            state.provider_registry.register_mysql(
                id, &req.host, req.port, &req.database, &req.username, &req.password,
                prefix, df_ctx,
            ).await
        }
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let ctx = state.ctx.read().await;
            state.provider_registry.register_sqlite(
                id, &req.host, prefix, ctx.datafusion_ctx(),
            ).await
        }
        "mongodb" => {
            use futures::stream::{self, StreamExt};
            let mongo_params = build_mongo_params(req);
            tracing::info!(
                host = %mongo_params.host, database = %mongo_params.database,
                auth_method = ?mongo_params.auth_method,
                "MongoDB discovery: connecting and listing collections"
            );
            let collections = crate::mongodb_conn::connect_and_discover(&mongo_params)
                .await?;
            tracing::info!(
                collections = collections.len(),
                names = ?collections.iter().take(10).collect::<Vec<_>>(),
                "MongoDB discovery: {} collections found",
                collections.len()
            );

            let ctx = state.ctx.read().await;
            let mongo_schema = crate::providers::ensure_schema(ctx.datafusion_ctx(), "mongo")
                .map_err(|e| e.to_string())?;

            // Fetch collections in parallel (up to 4 concurrent to avoid overwhelming large DBs)
            let total = collections.len();
            let fetch_start = Instant::now();
            let results: Vec<Option<(String, arrow::record_batch::RecordBatch)>> = stream::iter(collections.iter().cloned().enumerate())
                .map(|(i, coll_name)| {
                    let params = mongo_params.clone();
                    let total = total;
                    async move {
                        tracing::info!(
                            collection = %coll_name,
                            progress = format!("{}/{}", i + 1, total),
                            "MongoDB discovery: fetching collection"
                        );
                        let start = Instant::now();
                        match crate::mongodb_conn::fetch_collection_as_arrow(&params, &coll_name).await {
                            Ok(batch) => {
                                tracing::info!(
                                    collection = %coll_name,
                                    rows = batch.num_rows(),
                                    columns = batch.num_columns(),
                                    elapsed_ms = start.elapsed().as_millis(),
                                    "MongoDB discovery: collection fetched OK"
                                );
                                Some((coll_name, batch))
                            }
                            Err(e) => {
                                tracing::warn!(
                                    collection = %coll_name,
                                    error = %e,
                                    elapsed_ms = start.elapsed().as_millis(),
                                    "MongoDB discovery: collection fetch FAILED — skipping"
                                );
                                None
                            }
                        }
                    }
                })
                .buffer_unordered(4)
                .collect()
                .await;

            let fetch_ms = fetch_start.elapsed().as_millis();
            let fetched_count = results.iter().filter(|r| r.is_some()).count();
            tracing::info!(
                fetched = fetched_count, skipped = total - fetched_count,
                elapsed_ms = fetch_ms,
                "MongoDB discovery: fetch phase complete"
            );

            let mut mongo_registered = Vec::new();
            for (coll_name, batch) in results.into_iter().flatten() {
                let schema = batch.schema();
                if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                    let df_name = format!("mongo.{}", coll_name);
                    if mongo_schema.register_table(coll_name.clone(), std::sync::Arc::new(mem_table)).is_ok() {
                        mongo_registered.push(df_name);
                    } else {
                        tracing::warn!(collection = %coll_name, "MongoDB discovery: failed to register table in DataFusion");
                    }
                }
            }
            tracing::info!(
                registered = mongo_registered.len(),
                tables = ?mongo_registered.iter().take(10).collect::<Vec<_>>(),
                "MongoDB discovery: registration complete"
            );
            Ok(mongo_registered)
        }
        "trino" | "presto" => {
            let user = if req.username.is_empty() { "rustlake".to_string() } else { req.username.clone() };
            let pass = req.password.clone();
            let catalog = if req.database.is_empty() { "postgresql".to_string() } else { req.database.clone() };
            let base_url = trino_base_url(&req.host, req.port);

            #[cfg(feature = "duckdb")]
            {
                let rest = crate::trino_client::TrinoRestClient::new(base_url.clone(), user.clone(), pass.clone());
                let cache = state.trino_cache.clone()
                    .ok_or_else(|| "Trino cache not initialized".to_string())?;

                // Quick validation: just check Trino is reachable with a lightweight query
                rest.execute_query("SELECT 1", &catalog).await
                    .map_err(|e| format!("Cannot reach Trino: {}", e))?;

                let conn = crate::trino_client::TrinoConnection {
                    id: id.to_string(),
                    name: req.name.clone(),
                    rest,
                    default_catalog: catalog.clone(),
                    cache,
                };
                let conn_arc = std::sync::Arc::new(conn);
                state.trino_connections.write().await.insert(id.to_string(), conn_arc.clone());

                // Update sync_progress to indicate scan is starting
                state.update_connection_entry(id, |entry| {
                    entry.sync_progress = Some("Connecting to Trino...".to_string());
                }).await;

                // Spawn background scan task — returns immediately to the caller
                let state_bg = state.clone();
                let conn_id_bg = id.to_string();
                let conn_arc_bg = conn_arc.clone();
                let base_url_bg = base_url.clone();
                let user_bg = user.clone();
                let pass_bg = pass.clone();

                tokio::spawn(async move {
                    tracing::info!(conn_id = %conn_id_bg, "Starting background Trino catalog scan");

                    // Phase 1: Discover catalogs and cache metadata
                    state_bg.update_connection_entry(&conn_id_bg, |entry| {
                        entry.sync_progress = Some("Discovering catalogs...".to_string());
                    }).await;

                    match conn_arc_bg.refresh_cache().await {
                        Ok(table_count) => {
                            tracing::info!(conn_id = %conn_id_bg, tables = table_count, "Trino cache populated");
                            state_bg.update_connection_entry(&conn_id_bg, |entry| {
                                entry.sync_progress = Some(format!("Found {} tables, registering providers...", table_count));
                            }).await;
                        }
                        Err(e) => {
                            tracing::warn!(conn_id = %conn_id_bg, error = %e, "Trino cache refresh failed");
                            state_bg.update_connection_entry(&conn_id_bg, |entry| {
                                entry.sync_status = "error".to_string();
                                entry.sync_error = Some(format!("Cache refresh failed: {}", e));
                                entry.sync_progress = None;
                            }).await;
                            return;
                        }
                    }

                    // Phase 2: Register Trino table providers in DataFusion
                    state_bg.update_connection_entry(&conn_id_bg, |entry| {
                        entry.sync_progress = Some("Registering table providers...".to_string());
                    }).await;

                    let rest_arc = std::sync::Arc::new(
                        crate::trino_client::TrinoRestClient::new(base_url_bg, user_bg, pass_bg)
                    );
                    let ctx = state_bg.ctx.read().await;
                    let df_ctx = ctx.datafusion_ctx();
                    let trino_registered = state_bg.provider_registry
                        .register_trino(&conn_id_bg, &conn_arc_bg, rest_arc, df_ctx)
                        .await
                        .unwrap_or_else(|e| {
                            tracing::warn!(error = %e, "Failed to register Trino table providers");
                            vec![]
                        });
                    drop(ctx);

                    if !trino_registered.is_empty() {
                        tracing::info!(count = trino_registered.len(), "Trino tables registered as DataFusion providers");
                    }

                    // Phase 3: Build catalog tree
                    state_bg.update_connection_entry(&conn_id_bg, |entry| {
                        entry.sync_progress = Some("Building catalog tree...".to_string());
                    }).await;

                    let tree = conn_arc_bg.browse().await.unwrap_or_else(|_| crate::trino_client::TrinoCatalogTree {
                        catalogs: vec![], cached_at: None, total_tables: 0,
                    });
                    let tables: Vec<String> = tree.catalogs.iter()
                        .flat_map(|c| c.schemas.iter().flat_map(move |s| {
                            s.tables.iter().map(move |t| format!("trino.{}_{}", s.name, t))
                        }))
                        .collect();

                    // Done — mark as ready with final table list
                    let table_count = tables.len();
                    state_bg.update_connection_entry(&conn_id_bg, |entry| {
                        entry.sync_status = "ready".to_string();
                        entry.sync_error = None;
                        entry.sync_progress = Some(format!("Scan complete: {} tables", table_count));
                        entry.tables = tables;
                    }).await;
                    tracing::info!(conn_id = %conn_id_bg, tables = table_count, "Trino background scan complete");
                });

                // Return empty tables — the background task will populate them via update_connection_entry
                Ok(vec![])
            }

            #[cfg(not(feature = "duckdb"))]
            {
                let _ = (user, pass, catalog, base_url);
                Ok(vec![])
            }
        }
        _ => Ok(vec![]),
    }
}

/// Reconnect all previously saved connections on server restart.
///
/// For each connection with a persisted password, performs a health check and
/// re-registers tables via the appropriate provider (Postgres, MySQL, MongoDB, Trino).
/// Runs as a background task so it does not block server startup.
pub(crate) async fn reconnect_saved_connections(state: Arc<AppState>) {
    let connections = state.connections.read().await.clone();
    let passwords = state.connection_passwords.read().await.clone();

    if connections.is_empty() || passwords.is_empty() {
        return;
    }

    let mut reconnected = 0u32;
    let mut failed = 0u32;

    for conn in &connections {
        // Skip bootstrap connections — they are handled by bootstrap_demo_connections
        if conn.source == "bootstrap" {
            continue;
        }

        let password = match passwords.get(&conn.id) {
            Some(p) => p.clone(),
            None => {
                tracing::debug!(conn_id = %conn.id, name = %conn.name, "No saved password, skipping reconnect");
                continue;
            }
        };

        tracing::info!(
            conn_id = %conn.id,
            name = %conn.name,
            conn_type = %conn.conn_type,
            "Reconnecting saved connection"
        );

        // Update status to syncing
        state.update_connection_entry(&conn.id, |c| {
            c.sync_status = "syncing".to_string();
            c.sync_progress = Some("Reconnecting...".to_string());
            c.status = "connecting".to_string();
        }).await;

        match conn.conn_type.as_str() {
            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" => {
                let ctx = state.ctx.read().await;
                let df_ctx = ctx.datafusion_ctx();
                match state.provider_registry.register_postgres(
                    &conn.id, &conn.host, conn.port, &conn.database, &conn.username, &password,
                    "pg", df_ctx,
                ).await {
                    Ok(tables) => {
                        drop(ctx);
                        state.update_connection_entry(&conn.id, |c| {
                            c.status = "connected".to_string();
                            c.sync_status = "ready".to_string();
                            c.sync_progress = Some(format!("Reconnected: {} tables", tables.len()));
                            c.tables = tables;
                            c.sync_error = None;
                        }).await;
                        reconnected += 1;
                        tracing::info!(conn_id = %conn.id, "Postgres reconnected");
                    }
                    Err(e) => {
                        drop(ctx);
                        state.update_connection_entry(&conn.id, |c| {
                            c.status = "error".to_string();
                            c.sync_status = "error".to_string();
                            c.sync_error = Some(format!("Reconnect failed: {}", e));
                            c.sync_progress = None;
                        }).await;
                        failed += 1;
                        tracing::warn!(conn_id = %conn.id, error = %e, "Postgres reconnect failed");
                    }
                }
            }
            #[cfg(feature = "mysql")]
            "mysql" | "mariadb" => {
                let ctx = state.ctx.read().await;
                let df_ctx = ctx.datafusion_ctx();
                match state.provider_registry.register_mysql(
                    &conn.id, &conn.host, conn.port, &conn.database, &conn.username, &password,
                    "mysql", df_ctx,
                ).await {
                    Ok(tables) => {
                        drop(ctx);
                        state.update_connection_entry(&conn.id, |c| {
                            c.status = "connected".to_string();
                            c.sync_status = "ready".to_string();
                            c.sync_progress = Some(format!("Reconnected: {} tables", tables.len()));
                            c.tables = tables;
                            c.sync_error = None;
                        }).await;
                        reconnected += 1;
                        tracing::info!(conn_id = %conn.id, "MySQL reconnected");
                    }
                    Err(e) => {
                        drop(ctx);
                        state.update_connection_entry(&conn.id, |c| {
                            c.status = "error".to_string();
                            c.sync_status = "error".to_string();
                            c.sync_error = Some(format!("Reconnect failed: {}", e));
                            c.sync_progress = None;
                        }).await;
                        failed += 1;
                        tracing::warn!(conn_id = %conn.id, error = %e, "MySQL reconnect failed");
                    }
                }
            }
            "mongodb" => {
                let mongo_params = build_mongo_params_from_entry(&conn, &password);
                tracing::info!(
                    conn_id = %conn.id, name = %conn.name,
                    host = %mongo_params.host, database = %mongo_params.database,
                    has_aws_key = mongo_params.aws_access_key.is_some(),
                    "MongoDB reconnect: starting"
                );
                match crate::mongodb_conn::connect_and_discover(&mongo_params).await {
                    Ok(collections) => {
                        tracing::info!(
                            conn_id = %conn.id, collections = collections.len(),
                            "MongoDB reconnect: {} collections found, starting fetch",
                            collections.len()
                        );
                        let ctx = state.ctx.read().await;
                        let df_ctx = ctx.datafusion_ctx();
                        let mongo_schema = match crate::providers::ensure_schema(df_ctx, "mongo") {
                            Ok(s) => s,
                            Err(e) => {
                                drop(ctx);
                                tracing::warn!(error = %e, "Failed to ensure mongo schema");
                                failed += 1;
                                continue;
                            }
                        };

                        let mut mongo_registered = Vec::new();
                        let mut mongo_errors = 0u32;
                        for (i, coll_name) in collections.iter().enumerate() {
                            tracing::debug!(
                                collection = %coll_name,
                                progress = format!("{}/{}", i + 1, collections.len()),
                                "MongoDB reconnect: fetching collection"
                            );
                            match crate::mongodb_conn::fetch_collection_as_arrow(&mongo_params, coll_name).await {
                                Ok(batch) => {
                                    let schema = batch.schema();
                                    if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                                        let df_name = format!("mongo.{}", coll_name);
                                        if mongo_schema.register_table(coll_name.clone(), std::sync::Arc::new(mem_table)).is_ok() {
                                            mongo_registered.push(df_name);
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        collection = %coll_name, error = %e,
                                        "MongoDB reconnect: collection fetch failed, skipping"
                                    );
                                    mongo_errors += 1;
                                }
                            }
                        }
                        drop(ctx);

                        tracing::info!(
                            conn_id = %conn.id,
                            registered = mongo_registered.len(),
                            errors = mongo_errors,
                            "MongoDB reconnect: complete"
                        );

                        state.update_connection_entry(&conn.id, |c| {
                            c.status = "connected".to_string();
                            c.sync_status = "ready".to_string();
                            c.sync_progress = Some(format!("Reconnected: {} collections", mongo_registered.len()));
                            c.tables = mongo_registered;
                            c.sync_error = None;
                        }).await;
                        reconnected += 1;
                    }
                    Err(e) => {
                        state.update_connection_entry(&conn.id, |c| {
                            c.status = "error".to_string();
                            c.sync_status = "error".to_string();
                            c.sync_error = Some(format!("Reconnect failed: {}", e));
                            c.sync_progress = None;
                        }).await;
                        failed += 1;
                        tracing::warn!(conn_id = %conn.id, error = %e, "MongoDB reconnect failed");
                    }
                }
            }
            "trino" | "presto" => {
                #[cfg(feature = "duckdb")]
                {
                    let user = if conn.username.is_empty() { "rustlake".to_string() } else { conn.username.clone() };
                    let catalog = if conn.database.is_empty() { "postgresql".to_string() } else { conn.database.clone() };
                    let base_url = trino_base_url(&conn.host, conn.port);
                    let rest = crate::trino_client::TrinoRestClient::new(base_url.clone(), user.clone(), password.clone());

                    // Quick health check
                    match rest.execute_query("SELECT 1", &catalog).await {
                        Ok(_) => {
                            let cache = match state.trino_cache.clone() {
                                Some(c) => c,
                                None => {
                                    tracing::warn!(conn_id = %conn.id, "Trino cache not available, skipping");
                                    failed += 1;
                                    continue;
                                }
                            };
                            let trino_conn = crate::trino_client::TrinoConnection {
                                id: conn.id.clone(),
                                name: conn.name.clone(),
                                rest,
                                default_catalog: catalog,
                                cache,
                            };
                            let conn_arc = std::sync::Arc::new(trino_conn);
                            state.trino_connections.write().await.insert(conn.id.clone(), conn_arc.clone());

                            // Spawn background scan (same pattern as add_connection)
                            let state_bg = state.clone();
                            let conn_id = conn.id.clone();
                            let conn_arc_bg = conn_arc.clone();
                            let base_url_bg = base_url;
                            let user_bg = user;
                            let pass_bg = password;

                            tokio::spawn(async move {
                                state_bg.update_connection_entry(&conn_id, |entry| {
                                    entry.sync_progress = Some("Discovering catalogs...".to_string());
                                }).await;

                                match conn_arc_bg.refresh_cache().await {
                                    Ok(table_count) => {
                                        state_bg.update_connection_entry(&conn_id, |entry| {
                                            entry.sync_progress = Some(format!("Found {} tables, registering...", table_count));
                                        }).await;
                                    }
                                    Err(e) => {
                                        state_bg.update_connection_entry(&conn_id, |entry| {
                                            entry.sync_status = "error".to_string();
                                            entry.sync_error = Some(format!("Cache refresh failed: {}", e));
                                            entry.sync_progress = None;
                                        }).await;
                                        return;
                                    }
                                }

                                let rest_arc = std::sync::Arc::new(
                                    crate::trino_client::TrinoRestClient::new(base_url_bg, user_bg, pass_bg)
                                );
                                let ctx = state_bg.ctx.read().await;
                                let df_ctx = ctx.datafusion_ctx();
                                let _registered = state_bg.provider_registry
                                    .register_trino(&conn_id, &conn_arc_bg, rest_arc, df_ctx)
                                    .await
                                    .unwrap_or_default();
                                drop(ctx);

                                let tree = conn_arc_bg.browse().await.unwrap_or_else(|_| crate::trino_client::TrinoCatalogTree {
                                    catalogs: vec![], cached_at: None, total_tables: 0,
                                });
                                let tables: Vec<String> = tree.catalogs.iter()
                                    .flat_map(|c| c.schemas.iter().flat_map(move |s| {
                                        s.tables.iter().map(move |t| format!("trino.{}_{}", s.name, t))
                                    }))
                                    .collect();

                                let table_count = tables.len();
                                state_bg.update_connection_entry(&conn_id, |entry| {
                                    entry.status = "connected".to_string();
                                    entry.sync_status = "ready".to_string();
                                    entry.sync_progress = Some(format!("Reconnected: {} tables", table_count));
                                    entry.tables = tables;
                                    entry.sync_error = None;
                                }).await;
                                tracing::info!(conn_id = %conn_id, tables = table_count, "Trino reconnected");
                            });
                            reconnected += 1;
                        }
                        Err(e) => {
                            state.update_connection_entry(&conn.id, |c| {
                                c.status = "error".to_string();
                                c.sync_status = "error".to_string();
                                c.sync_error = Some(format!("Trino unreachable: {}", e));
                                c.sync_progress = None;
                            }).await;
                            failed += 1;
                            tracing::warn!(conn_id = %conn.id, error = %e, "Trino reconnect failed");
                        }
                    }
                }
                #[cfg(not(feature = "duckdb"))]
                {
                    let _ = password;
                    tracing::debug!(conn_id = %conn.id, "Trino reconnect requires DuckDB feature");
                }
            }
            _ => {
                tracing::debug!(conn_id = %conn.id, conn_type = %conn.conn_type, "Skipping reconnect for unsupported type");
            }
        }
    }

    if reconnected > 0 || failed > 0 {
        tracing::info!(reconnected, failed, "Saved connection reconnection complete");
    }
}

/// GET /api/v1/connections — list all connections.
async fn list_connections(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let connections = state.connections.read().await;
    Json(serde_json::json!({
        "connections": connections.clone(),
    }))
}

/// DELETE /api/v1/connections/:id — remove a connection.
async fn delete_connection(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Find the connection before removing so we can log details
    let removed = state.connections.read().await.iter().find(|c| c.id == id).cloned();

    if !state.remove_connection_entry(&id).await {
        tracing::warn!(id = %id, "Data source removal failed: not found");
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Connection '{}' not found", id),
            }),
        ));
    }

    state.connection_passwords.write().await.remove(&id);
    if let Err(e) = state.credential_store.remove_password(&id) {
        tracing::warn!(error = %e, conn_id = %id, "Failed to remove encrypted password");
    }

    if let Some(conn) = &removed {
        tracing::info!(
            source = %conn.source,
            id = %id,
            name = %conn.name,
            conn_type = %conn.conn_type,
            host = %conn.host,
            port = conn.port,
            database = %conn.database,
            tables = conn.tables.len(),
            "Data source removed: {}",
            conn.name
        );
    }

    Ok(Json(serde_json::json!({
        "status": "ok",
        "deleted": id,
    })))
}

/// PUT /api/v1/connections/{id} — update an existing connection.
async fn update_connection(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<AddConnectionRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Check the connection exists
    {
        let connections = state.connections.read().await;
        if !connections.iter().any(|c| c.id == id) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Connection '{}' not found", id),
                }),
            ));
        }
    }

    // Quick connectivity check (same as add_connection)
    let conn_type = req.conn_type.clone();
    match conn_type.as_str() {
        "trino" | "presto" => {
            let user = if req.username.is_empty() { "rustlake".to_string() } else { req.username.clone() };
            let base_url = trino_base_url(&req.host, req.port);
            let rest = crate::trino_client::TrinoRestClient::new(base_url, user, req.password.clone());
            rest.server_info().await
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach Trino: {}", e) })))?;
        }
        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => {
            let addr = format!("{}:{}", req.host, req.port);
            tokio::net::TcpStream::connect(&addr).await
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach Postgres: {}", e) })))?;
        }
        #[cfg(feature = "mysql")]
        "mysql" | "mariadb" => {
            let addr = format!("{}:{}", req.host, req.port);
            tokio::net::TcpStream::connect(&addr).await
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach MySQL: {}", e) })))?;
        }
        "mongodb" => {
            if req.auth_method == "connection_string" || req.auth_method == "aws_iam" {
                let params = build_mongo_params(&req);
                params.build_client().await
                    .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot connect to MongoDB: {}", e) })))?;
            } else {
                let addr = format!("{}:{}", req.host, req.port);
                tokio::net::TcpStream::connect(&addr).await
                    .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Cannot reach MongoDB: {}", e) })))?;
            }
        }
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            if !std::path::Path::new(&req.host).exists() {
                return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("SQLite file not found: {}", req.host) })));
            }
        }
        other => {
            return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("Unsupported connection type: {}", other) })));
        }
    }

    // Update the connection entry fields and set sync_status back to "syncing"
    state.update_connection_entry(&id, |entry| {
        entry.name = req.name.clone();
        entry.conn_type = req.conn_type.clone();
        entry.host = req.host.clone();
        entry.port = req.port;
        entry.database = req.database.clone();
        entry.username = req.username.clone();
        entry.status = "connected".to_string();
        entry.tables = vec![];
        entry.sync_status = "syncing".to_string();
        entry.sync_error = None;
        entry.sync_progress = None;
        entry.auth_method = req.auth_method.clone();
        entry.connection_string = if req.connection_string.is_empty() { None } else { Some(req.connection_string.clone()) };
        entry.aws_access_key = if req.aws_access_key.is_empty() { None } else { Some(req.aws_access_key.clone()) };
        entry.aws_secret_key = if req.aws_secret_key.is_empty() { None } else { Some(req.aws_secret_key.clone()) };
        entry.aws_session_token = if req.aws_session_token.is_empty() { None } else { Some(req.aws_session_token.clone()) };
    }).await;

    // Update stored password
    state.store_password(id.clone(), req.password.clone()).await;

    tracing::info!(
        id = %id,
        name = %req.name,
        conn_type = %req.conn_type,
        "Connection updated — starting background table re-discovery"
    );

    let response = serde_json::json!({
        "status": "connected",
        "sync_status": "syncing",
        "id": id,
        "name": req.name,
        "tables": serde_json::Value::Array(vec![]),
    });

    // Spawn background task for table re-discovery (same logic as add_connection)
    let bg_state = state.clone();
    let bg_id = id.clone();
    let bg_req = req.clone();
    tokio::spawn(async move {
        let result = discover_and_register_tables(&bg_state, &bg_id, &bg_req).await;
        match result {
            Ok(tables) => {
                bg_state.update_connection_entry(&bg_id, |entry| {
                    entry.tables = tables.clone();
                    entry.sync_status = "ready".to_string();
                    entry.sync_error = None;
                }).await;
                tracing::info!(
                    id = %bg_id,
                    name = %bg_req.name,
                    tables = tables.len(),
                    "Background re-sync complete: {} tables registered",
                    tables.len()
                );
            }
            Err(e) => {
                bg_state.update_connection_entry(&bg_id, |entry| {
                    entry.sync_status = "error".to_string();
                    entry.sync_error = Some(e.clone());
                }).await;
                tracing::error!(
                    id = %bg_id,
                    name = %bg_req.name,
                    error = %e,
                    "Background re-sync failed"
                );
            }
        }
    });

    Ok(Json(response))
}

/// GET /api/v1/connections/:id/status — poll sync status for a connection.
async fn connection_sync_status(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let connections = state.connections.read().await;
    let conn = connections.iter().find(|c| c.id == id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse { error: format!("Connection '{}' not found", id) }))
    })?;
    Ok(Json(serde_json::json!({
        "id": conn.id,
        "sync_status": conn.sync_status,
        "sync_error": conn.sync_error,
        "tables": conn.tables,
        "table_count": conn.tables.len(),
    })))
}

/// POST /api/v1/connections/:id/register/:table — register a table from an external database.
///
/// For federated providers (Postgres, MySQL, SQLite), creates a live `TableProvider`
/// with predicate/projection pushdown. For MongoDB, falls back to a MemTable snapshot.
async fn register_external_table(
    State(state): State<Arc<AppState>>,
    Path((id, table_name)): Path<(String, String)>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let connections = state.connections.read().await;
    let conn = connections.iter().find(|c| c.id == id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Connection '{}' not found", id),
            }),
        )
    })?;
    let conn_type = conn.conn_type.clone();
    let conn_host = conn.host.clone();
    let conn_port = conn.port;
    let conn_database = conn.database.clone();
    let conn_username = conn.username.clone();
    drop(connections);

    let passwords = state.connection_passwords.read().await;
    let password = passwords.get(&id).cloned().unwrap_or_default();
    drop(passwords);

    let prefix = match conn_type.as_str() {
        "postgres" | "postgresql" => "pg",
        "mysql" | "mariadb" => "mysql",
        "sqlite" => "sqlite",
        "mongodb" => "mongo",
        _ => &conn_type,
    };
    let df_table_name = format!("{}.{}", prefix, table_name);

    match conn_type.as_str() {
        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => {
            use datafusion::common::TableReference;
            use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
            use datafusion_table_providers::postgres::PostgresTableFactory;
            use datafusion_table_providers::util::secrets::to_secret_map;

            let mut opts = std::collections::HashMap::new();
            opts.insert("host".to_string(), conn_host);
            opts.insert("port".to_string(), conn_port.to_string());
            opts.insert("db".to_string(), conn_database);
            opts.insert("user".to_string(), conn_username);
            opts.insert("pass".to_string(), password);
            opts.insert("sslmode".to_string(), "disable".to_string());

            let pool = Arc::new(
                PostgresConnectionPool::new(to_secret_map(opts)).await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Pool error: {}", e) })))?,
            );
            let factory = PostgresTableFactory::new(pool);
            let provider = factory.table_provider(TableReference::partial("public", table_name.as_str())).await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Provider error: {}", e) })))?;

            let ctx = state.ctx.read().await;
            let schema_prov = crate::providers::ensure_schema(ctx.datafusion_ctx(), prefix)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;
            schema_prov.register_table(table_name.clone(), provider)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Register error: {}", e) })))?;
        }
        #[cfg(feature = "mysql")]
        "mysql" | "mariadb" => {
            use datafusion::common::TableReference;
            use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
            use datafusion_table_providers::mysql::MySQLTableFactory;
            use datafusion_table_providers::util::secrets::to_secret_map;

            let mut opts = std::collections::HashMap::new();
            opts.insert("host".to_string(), conn_host);
            opts.insert("tcp_port".to_string(), conn_port.to_string());
            opts.insert("db".to_string(), conn_database.clone());
            opts.insert("user".to_string(), conn_username);
            opts.insert("pass".to_string(), password);
            opts.insert("sslmode".to_string(), "disabled".to_string());

            let pool = Arc::new(
                MySQLConnectionPool::new(to_secret_map(opts)).await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Pool error: {}", e) })))?,
            );
            let factory = MySQLTableFactory::new(pool);
            let provider = factory.table_provider(TableReference::partial(&*conn_database, table_name.as_str())).await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Provider error: {}", e) })))?;

            let ctx = state.ctx.read().await;
            let schema_prov = crate::providers::ensure_schema(ctx.datafusion_ctx(), prefix)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;
            schema_prov.register_table(table_name.clone(), provider)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Register error: {}", e) })))?;
        }
        "mongodb" => {
            // MongoDB: snapshot approach in "mongo" schema (no provider available)
            let mongo_params = crate::mongodb_conn::MongoConnParams {
                host: conn_host, port: conn_port, database: conn_database,
                username: conn_username, password,
                ..Default::default()
            };
            let batch = crate::mongodb_conn::fetch_collection_as_arrow(&mongo_params, &table_name).await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;
            let schema = batch.schema();
            let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("MemTable error: {}", e) })))?;
            let ctx = state.ctx.read().await;
            let schema_prov = crate::providers::ensure_schema(ctx.datafusion_ctx(), prefix)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;
            schema_prov.register_table(table_name.clone(), std::sync::Arc::new(mem_table))
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Register error: {}", e) })))?;
        }
        other => {
            return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse {
                error: format!("Unsupported connection type for table registration: {}", other),
            })));
        }
    }

    tracing::info!(table = %df_table_name, "External table registered");

    Ok(Json(serde_json::json!({
        "status": "ok",
        "table": df_table_name,
        "source_table": table_name,
    })))
}

// ── Benchmark endpoints ────────────────────────────────────────────

/// A TPC-H benchmark query definition.
#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkQuery {
    pub id: String,
    pub name: String,
    pub description: String,
    pub sql: String,
    pub category: String,
}

/// A stored benchmark run result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub query_id: String,
    pub query_name: String,
    pub duration_ms: u128,
    pub row_count: usize,
    pub status: String,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub engine: String,
}

/// TPC-H queries adapted for the `pg_tpch_*` table names registered via bootstrap.
fn tpch_queries() -> Vec<BenchmarkQuery> {
    vec![
        BenchmarkQuery {
            id: "tpch-q1".into(),
            name: "Q1: Pricing Summary".into(),
            description: "Aggregates lineitem data by return flag and line status with sum/avg/count".into(),
            category: "Aggregation".into(),
            sql: r#"SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price, SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, AVG(l_quantity) as avg_qty, AVG(l_extendedprice) as avg_price, AVG(l_discount) as avg_disc, COUNT(*) as count_order FROM pg_tpch_lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q3".into(),
            name: "Q3: Shipping Priority".into(),
            description: "Top 10 unshipped orders by revenue for BUILDING market segment before 1995-03-15".into(),
            category: "Join + Filter".into(),
            sql: r#"SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority FROM pg_tpch_customer c JOIN pg_tpch_orders o ON c.c_custkey = o.o_custkey JOIN pg_tpch_lineitem l ON l.l_orderkey = o.o_orderkey WHERE c.c_mktsegment = 'BUILDING' AND o.o_orderdate < DATE '1995-03-15' AND l.l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q4".into(),
            name: "Q4: Order Priority Checking".into(),
            description: "Count orders by priority where at least one lineitem was received after commit date".into(),
            category: "Subquery".into(),
            sql: r#"SELECT o_orderpriority, COUNT(*) as order_count FROM pg_tpch_orders WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01' AND EXISTS (SELECT * FROM pg_tpch_lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q5".into(),
            name: "Q5: Local Supplier Volume".into(),
            description: "Revenue by nation for suppliers in ASIA region in 1994".into(),
            category: "Multi-Join".into(),
            sql: r#"SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue FROM pg_tpch_customer c JOIN pg_tpch_orders o ON c.c_custkey = o.o_custkey JOIN pg_tpch_lineitem l ON l.l_orderkey = o.o_orderkey JOIN pg_tpch_supplier s ON l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey JOIN pg_tpch_nation n ON s.s_nationkey = n.n_nationkey JOIN pg_tpch_region r ON n.n_regionkey = r.r_regionkey WHERE r.r_name = 'ASIA' AND o.o_orderdate >= DATE '1994-01-01' AND o.o_orderdate < DATE '1995-01-01' GROUP BY n_name ORDER BY revenue DESC"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q6".into(),
            name: "Q6: Forecasting Revenue Change".into(),
            description: "Revenue from lineitems with discount between 5-7% and quantity < 24 in 1994".into(),
            category: "Scan + Filter".into(),
            sql: r#"SELECT SUM(l_extendedprice * l_discount) as revenue FROM pg_tpch_lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q9".into(),
            name: "Q9: Product Type Profit".into(),
            description: "Profit by nation and year for parts containing 'steel' in their name".into(),
            category: "Complex Join".into(),
            sql: r#"SELECT nation, o_year, SUM(amount) as sum_profit FROM (SELECT n.n_name as nation, EXTRACT(YEAR FROM o.o_orderdate) as o_year, l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount FROM pg_tpch_part p JOIN pg_tpch_lineitem l ON p.p_partkey = l.l_partkey JOIN pg_tpch_supplier s ON l.l_suppkey = s.s_suppkey JOIN pg_tpch_partsupp ps ON l.l_suppkey = ps.ps_suppkey AND l.l_partkey = ps.ps_partkey JOIN pg_tpch_orders o ON o.o_orderkey = l.l_orderkey JOIN pg_tpch_nation n ON s.s_nationkey = n.n_nationkey WHERE p.p_name LIKE '%steel%') as profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q10".into(),
            name: "Q10: Returned Item Reporting".into(),
            description: "Top 20 customers by lost revenue from returned items in Q4 1993".into(),
            category: "Join + Aggregation".into(),
            sql: r#"SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue, c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment FROM pg_tpch_customer c JOIN pg_tpch_orders o ON c.c_custkey = o.o_custkey JOIN pg_tpch_lineitem l ON l.l_orderkey = o.o_orderkey JOIN pg_tpch_nation n ON c.c_nationkey = n.n_nationkey WHERE o.o_orderdate >= DATE '1993-10-01' AND o.o_orderdate < DATE '1994-01-01' AND l.l_returnflag = 'R' GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment ORDER BY revenue DESC LIMIT 20"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q12".into(),
            name: "Q12: Shipping Modes".into(),
            description: "High-priority and low-priority order counts by shipping mode for 1994".into(),
            category: "Case + Aggregation".into(),
            sql: r#"SELECT l.l_shipmode, SUM(CASE WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count, SUM(CASE WHEN o.o_orderpriority <> '1-URGENT' AND o.o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) as low_line_count FROM pg_tpch_orders o JOIN pg_tpch_lineitem l ON o.o_orderkey = l.l_orderkey WHERE l.l_shipmode IN ('MAIL', 'SHIP') AND l.l_commitdate < l.l_receiptdate AND l.l_shipdate < l.l_commitdate AND l.l_receiptdate >= DATE '1994-01-01' AND l.l_receiptdate < DATE '1995-01-01' GROUP BY l.l_shipmode ORDER BY l.l_shipmode"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q13".into(),
            name: "Q13: Customer Distribution".into(),
            description: "Distribution of customers by their order count (excluding special requests)".into(),
            category: "Left Join + Aggregation".into(),
            sql: r#"SELECT c_count, COUNT(*) as custdist FROM (SELECT c.c_custkey, COUNT(o.o_orderkey) as c_count FROM pg_tpch_customer c LEFT OUTER JOIN pg_tpch_orders o ON c.c_custkey = o.o_custkey AND o.o_comment NOT LIKE '%special%requests%' GROUP BY c.c_custkey) as c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC"#.into(),
        },
        BenchmarkQuery {
            id: "tpch-q14".into(),
            name: "Q14: Promotion Effect".into(),
            description: "Percentage of revenue from promotional parts in December 1995".into(),
            category: "Join + Conditional".into(),
            sql: r#"SELECT 100.00 * SUM(CASE WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount) ELSE 0 END) / SUM(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue FROM pg_tpch_lineitem l JOIN pg_tpch_part p ON l.l_partkey = p.p_partkey WHERE l.l_shipdate >= DATE '1995-09-01' AND l.l_shipdate < DATE '1995-10-01'"#.into(),
        },
    ]
}

/// GET /api/v1/benchmarks/queries — list available TPC-H benchmark queries.
async fn list_benchmark_queries() -> Json<serde_json::Value> {
    let queries = tpch_queries();
    Json(serde_json::json!({
        "queries": queries,
        "scale_factor": "SF0.01",
        "tables": {
            "lineitem": 60000,
            "orders": 15000,
            "customer": 1500,
            "part": 2000,
            "partsupp": 8000,
            "supplier": 100,
            "nation": 25,
            "region": 5,
        },
    }))
}

/// Request to run a benchmark query.
#[derive(Deserialize)]
struct RunBenchmarkRequest {
    /// Query ID (e.g., "tpch-q1").
    query_id: String,
}

/// POST /api/v1/benchmarks/run — execute a TPC-H benchmark query.
async fn run_benchmark_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RunBenchmarkRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let queries = tpch_queries();
    let query = queries.iter().find(|q| q.id == req.query_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Benchmark query '{}' not found", req.query_id),
            }),
        )
    })?;

    let start = Instant::now();
    let ctx = state.ctx.read().await;
    let df = ctx
        .datafusion_ctx()
        .sql(&query.sql)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Query planning failed: {}", e),
                }),
            )
        })?;

    let batches = df.collect().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Query execution failed: {}", e),
            }),
        )
    })?;
    let duration_ms = start.elapsed().as_millis();

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    let columns: Vec<String> = if let Some(batch) = batches.first() {
        batch.schema().fields().iter().map(|f| f.name().clone()).collect()
    } else {
        vec![]
    };

    // Convert rows to JSON (limited to 100 for display)
    let mut rows = Vec::new();
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        let limited: Vec<_> = batches.iter().take(5).cloned().collect();
        for batch in &limited {
            writer.write(batch).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: format!("JSON serialization failed: {}", e) }),
                )
            })?;
        }
        writer.finish().map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("JSON finish failed: {}", e) }),
            )
        })?;
    }
    if !buf.is_empty() {
        if let Ok(parsed) = serde_json::from_slice::<Vec<serde_json::Value>>(&buf) {
            rows = parsed.into_iter().take(100).collect();
        }
    }

    // Store result
    let result = BenchmarkResult {
        query_id: query.id.clone(),
        query_name: query.name.clone(),
        duration_ms,
        row_count,
        status: "success".to_string(),
        error: None,
        timestamp: Utc::now(),
        engine: "DataFusion".to_string(),
    };
    state.benchmark_results.write().await.push(result);

    Ok(Json(serde_json::json!({
        "query_id": query.id,
        "query_name": query.name,
        "sql": query.sql,
        "duration_ms": duration_ms,
        "row_count": row_count,
        "columns": columns,
        "rows": rows,
        "status": "success",
        "engine": "DataFusion",
    })))
}

/// GET /api/v1/benchmarks/results — list stored benchmark results.
async fn list_benchmark_results(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let results = state.benchmark_results.read().await;
    Json(serde_json::json!({
        "results": results.clone(),
    }))
}

// ── Bootstrap endpoints ────────────────────────────────────────────

/// Response for bootstrap status.
#[derive(Serialize)]
struct ServiceStatus {
    available: bool,
    tables: Vec<String>,
    error: Option<String>,
}

/// Response for GET /api/v1/bootstrap/status.
#[derive(Serialize)]
struct BootstrapStatusResponse {
    postgres: ServiceStatus,
    mysql: ServiceStatus,
    mongodb: ServiceStatus,
    minio: ServiceStatus,
    demo_jobs: usize,
    demo_pipelines: usize,
    demo_transforms: usize,
    registered_tables: Vec<String>,
}

/// GET /api/v1/bootstrap/status — check what demo data is available.
async fn bootstrap_status(
    State(state): State<Arc<AppState>>,
) -> Json<BootstrapStatusResponse> {
    // Check Postgres connection
    let connections = state.connections.read().await;
    let pg_conn = connections.iter().find(|c| c.conn_type == "postgres");
    let postgres = match pg_conn {
        Some(conn) => ServiceStatus {
            available: conn.status == "connected",
            tables: conn.tables.clone(),
            error: None,
        },
        None => ServiceStatus {
            available: false,
            tables: vec![],
            error: Some("No Postgres connection".to_string()),
        },
    };

    // Check MySQL connection
    let mysql_conn = connections.iter().find(|c| c.conn_type == "mysql");
    let mysql = match mysql_conn {
        Some(conn) => ServiceStatus {
            available: conn.status == "connected",
            tables: conn.tables.clone(),
            error: None,
        },
        None => ServiceStatus {
            available: false,
            tables: vec![],
            error: Some("No MySQL connection".to_string()),
        },
    };

    // Check MongoDB connection
    let mongo_conn = connections.iter().find(|c| c.conn_type == "mongodb");
    let mongodb = match mongo_conn {
        Some(conn) => ServiceStatus {
            available: conn.status == "connected",
            tables: conn.tables.clone(),
            error: None,
        },
        None => ServiceStatus {
            available: false,
            tables: vec![],
            error: Some("No MongoDB connection".to_string()),
        },
    };
    drop(connections);

    // Check MinIO S3 config
    let s3_configs = state.s3_configs.read().await;
    let minio = if s3_configs.iter().any(|c| c.name == "Local MinIO") {
        ServiceStatus {
            available: true,
            tables: vec![],
            error: None,
        }
    } else {
        ServiceStatus {
            available: false,
            tables: vec![],
            error: Some("No MinIO config".to_string()),
        }
    };
    drop(s3_configs);

    // Count demo items
    let jobs = state.scheduled_jobs.read().await;
    let demo_jobs = jobs.iter().filter(|j| j.tags.contains(&"demo".to_string())).count();
    drop(jobs);

    let pipelines = state.streaming_pipelines.read().await;
    let demo_pipelines = pipelines.len();
    drop(pipelines);

    let transforms = state.user_transforms.read().await;
    let demo_transforms = transforms.len();
    drop(transforms);

    // Get registered tables from DataFusion
    let ctx = state.ctx.read().await;
    let catalog = ctx.datafusion_ctx().catalog("datafusion");
    let registered_tables = match catalog {
        Some(cat) => {
            match cat.schema("public") {
                Some(schema) => schema.table_names(),
                None => vec![],
            }
        }
        None => vec![],
    };

    Json(BootstrapStatusResponse {
        postgres,
        mysql,
        mongodb,
        minio,
        demo_jobs,
        demo_pipelines,
        demo_transforms,
        registered_tables,
    })
}

/// POST /api/v1/bootstrap — re-run bootstrap on demand.
async fn run_bootstrap(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let mut results = serde_json::Map::new();

    // Try Postgres (federated provider)
    #[cfg(feature = "postgres")]
    {
        let pg_host = std::env::var("RUSTLAKE_PG_HOST").unwrap_or_else(|_| "localhost".to_string());
        let pg_port: u16 = std::env::var("RUSTLAKE_PG_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(5433);
        let pg_db = std::env::var("RUSTLAKE_PG_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
        let pg_user = std::env::var("RUSTLAKE_PG_USER").unwrap_or_else(|_| "rustlake".to_string());
        let pg_pass = std::env::var("RUSTLAKE_PG_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

        let ctx = state.ctx.read().await;
        match state.provider_registry.register_postgres(
            "bootstrap-postgres", &pg_host, pg_port, &pg_db, &pg_user, &pg_pass,
            "pg", ctx.datafusion_ctx(),
        ).await {
            Ok(tables) => {
                drop(ctx);
                let entry = ConnectionEntry {
                    id: "bootstrap-postgres".to_string(),
                    name: "Docker Postgres".to_string(),
                    conn_type: "postgres".to_string(),
                    host: pg_host, port: pg_port, database: pg_db, username: pg_user,
                    status: "connected".to_string(),
                    tables: tables.clone(),
                    created_at: Utc::now(),
                    source: "bootstrap".to_string(),
                    sync_status: "ready".to_string(),
                    sync_error: None,
                    sync_progress: None,
                    auth_method: "scram".to_string(),
                    connection_string: None,
                    aws_access_key: None,
                    aws_secret_key: None,
                    aws_session_token: None,
                };
                state.seed_connection(entry, pg_pass).await;
                results.insert("postgres".to_string(), serde_json::json!({
                    "status": "connected",
                    "mode": "federated",
                    "tables_registered": tables,
                }));
            }
            Err(e) => {
                drop(ctx);
                results.insert("postgres".to_string(), serde_json::json!({
                    "status": "unavailable",
                    "error": e,
                }));
            }
        }
    }

    // Seed MinIO (config via env vars)
    {
        let minio_endpoint = std::env::var("RUSTLAKE_MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
        let minio_access = std::env::var("RUSTLAKE_MINIO_ACCESS_KEY").unwrap_or_else(|_| "rustlake".to_string());
        let minio_secret = std::env::var("RUSTLAKE_MINIO_SECRET_KEY").unwrap_or_else(|_| "rustlake123".to_string());
        let minio_bucket = std::env::var("RUSTLAKE_MINIO_BUCKET").unwrap_or_else(|_| "rustlake-warehouse".to_string());
        let minio_region = std::env::var("RUSTLAKE_MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let mut configs = state.s3_configs.write().await;
        if !configs.iter().any(|c| c.name == "Local MinIO") {
            configs.push(S3Config {
                name: "Local MinIO".to_string(),
                endpoint: minio_endpoint,
                access_key: minio_access,
                secret_key: minio_secret,
                bucket: minio_bucket,
                region: minio_region,
                status: "configured".to_string(),
                created_at: Utc::now(),
                tables: vec![],
                table_types: std::collections::HashMap::new(),
                table_formats: std::collections::HashMap::new(),
                sync_status: "ready".to_string(),
                sync_error: None,
                scan_progress: None,
                scan_detail: None,
                scan_scanned: 0,
                scan_total: 0,
                scan_found: 0,
                scan_elapsed_ms: 0,
                format_counts: std::collections::HashMap::new(),
            });
            results.insert("minio".to_string(), serde_json::json!({ "status": "configured" }));
        } else {
            results.insert("minio".to_string(), serde_json::json!({ "status": "already_configured" }));
        }
    }

    // Try MySQL (federated provider)
    #[cfg(feature = "mysql")]
    {
        let mysql_host = std::env::var("RUSTLAKE_MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string());
        let mysql_port: u16 = std::env::var("RUSTLAKE_MYSQL_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(3307);
        let mysql_db = std::env::var("RUSTLAKE_MYSQL_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
        let mysql_user = std::env::var("RUSTLAKE_MYSQL_USER").unwrap_or_else(|_| "rustlake".to_string());
        let mysql_pass = std::env::var("RUSTLAKE_MYSQL_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

        let ctx = state.ctx.read().await;
        match state.provider_registry.register_mysql(
            "bootstrap-mysql", &mysql_host, mysql_port, &mysql_db, &mysql_user, &mysql_pass,
            "mysql", ctx.datafusion_ctx(),
        ).await {
            Ok(tables) => {
                drop(ctx);
                let entry = ConnectionEntry {
                    id: "bootstrap-mysql".to_string(),
                    name: "Docker MySQL".to_string(),
                    conn_type: "mysql".to_string(),
                    host: mysql_host, port: mysql_port, database: mysql_db, username: mysql_user,
                    status: "connected".to_string(),
                    tables: tables.clone(),
                    created_at: Utc::now(),
                    source: "bootstrap".to_string(),
                    sync_status: "ready".to_string(),
                    sync_error: None,
                    sync_progress: None,
                    auth_method: "scram".to_string(),
                    connection_string: None,
                    aws_access_key: None,
                    aws_secret_key: None,
                    aws_session_token: None,
                };
                state.seed_connection(entry, mysql_pass).await;
                results.insert("mysql".to_string(), serde_json::json!({
                    "status": "connected",
                    "mode": "federated",
                    "tables_registered": tables,
                }));
            }
            Err(e) => {
                drop(ctx);
                results.insert("mysql".to_string(), serde_json::json!({
                    "status": "unavailable",
                    "error": e,
                }));
            }
        }
    }

    // Try MongoDB (config via env vars, fallback to Docker defaults)
    let mongo_host = std::env::var("RUSTLAKE_MONGO_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mongo_port: u16 = std::env::var("RUSTLAKE_MONGO_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(27018);
    let mongo_db = std::env::var("RUSTLAKE_MONGO_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
    let mongo_user = std::env::var("RUSTLAKE_MONGO_USER").unwrap_or_else(|_| "rustlake".to_string());
    let mongo_pass = std::env::var("RUSTLAKE_MONGO_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

    let mongo_params = crate::mongodb_conn::MongoConnParams {
        host: mongo_host.clone(),
        port: mongo_port,
        database: mongo_db.clone(),
        username: mongo_user.clone(),
        password: mongo_pass,
        ..Default::default()
    };

    match crate::mongodb_conn::connect_and_discover(&mongo_params).await {
        Ok(collections) => {
            let entry = ConnectionEntry {
                id: "bootstrap-mongodb".to_string(),
                name: "Docker MongoDB".to_string(),
                conn_type: "mongodb".to_string(),
                host: mongo_host,
                port: mongo_port,
                database: mongo_db,
                username: mongo_user,
                status: "connected".to_string(),
                tables: collections.clone(),
                created_at: Utc::now(),
                source: "bootstrap".to_string(),
                sync_status: "ready".to_string(),
                sync_error: None,
                sync_progress: None,
                auth_method: "scram".to_string(),
                connection_string: None,
                aws_access_key: None,
                aws_secret_key: None,
                aws_session_token: None,
            };
            state.seed_connection(entry, "rustlake".to_string()).await;

            let mut mongo_tables_registered = Vec::new();
            {
                let ctx = state.ctx.read().await;
                let _ = crate::providers::ensure_schema(ctx.datafusion_ctx(), "mongo");
            }
            for coll_name in &collections {
                match crate::mongodb_conn::fetch_collection_as_arrow(&mongo_params, coll_name).await {
                    Ok(batch) => {
                        let schema = batch.schema();
                        if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                            let df_name = format!("mongo.{}", coll_name);
                            let ctx = state.ctx.read().await;
                            if let Some(catalog) = ctx.datafusion_ctx().catalog("datafusion") {
                                if let Some(schema_prov) = catalog.schema("mongo") {
                                    if schema_prov.register_table(coll_name.clone(), std::sync::Arc::new(mem_table)).is_ok() {
                                        mongo_tables_registered.push(df_name);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(collection = coll_name, error = %e, "Bootstrap: failed to fetch MongoDB collection");
                    }
                }
            }
            results.insert("mongodb".to_string(), serde_json::json!({
                "status": "connected",
                "collections_discovered": collections.len(),
                "tables_registered": mongo_tables_registered,
            }));
        }
        Err(e) => {
            results.insert("mongodb".to_string(), serde_json::json!({
                "status": "unavailable",
                "error": e,
            }));
        }
    }

    // Seed demo jobs
    let demo_jobs = vec![
        ScheduledJob {
            id: "demo-pg-snapshot".to_string(),
            name: "Postgres Snapshot".to_string(),
            job_type: "sql_query".to_string(),
            cron: "*/5 * * * *".to_string(),
            target: "SELECT count(*) FROM pg_customers".to_string(),
            enabled: true,
            last_run: None, next_run: None, last_status: None,
            created_at: Utc::now(),
            engine: "auto".to_string(),
            trigger_type: "time".to_string(),
            event_config: None,
            cluster: Some("default".to_string()),
            timeout_seconds: Some(60),
            retries: 1,
            tags: vec!["demo".to_string(), "postgres".to_string()],
        },
        ScheduledJob {
            id: "demo-quality-check".to_string(),
            name: "Data Quality Check".to_string(),
            job_type: "quality_check".to_string(),
            cron: "*/15 * * * *".to_string(),
            target: "*".to_string(),
            enabled: true,
            last_run: None, next_run: None, last_status: None,
            created_at: Utc::now(),
            engine: "auto".to_string(),
            trigger_type: "time".to_string(),
            event_config: None,
            cluster: Some("default".to_string()),
            timeout_seconds: Some(120),
            retries: 0,
            tags: vec!["demo".to_string(), "quality".to_string()],
        },
        ScheduledJob {
            id: "demo-mv-sales".to_string(),
            name: "MV: Sales Summary".to_string(),
            job_type: "materialized_view".to_string(),
            cron: "*/30 * * * *".to_string(),
            target: "SELECT product_id, SUM(amount) as total_amount FROM pg_sales GROUP BY product_id".to_string(),
            enabled: true,
            last_run: None, next_run: None, last_status: None,
            created_at: Utc::now(),
            engine: "auto".to_string(),
            trigger_type: "time".to_string(),
            event_config: None,
            cluster: Some("default".to_string()),
            timeout_seconds: Some(300),
            retries: 2,
            tags: vec!["demo".to_string(), "materialized_view".to_string()],
        },
    ];
    let mut jobs_seeded = 0usize;
    for job in demo_jobs {
        if state.seed_scheduled_job(job).await { jobs_seeded += 1; }
    }
    results.insert("demo_jobs_seeded".to_string(), serde_json::json!(jobs_seeded));

    // Seed demo pipeline
    let pipeline_seeded = state.seed_pipeline(StreamingPipeline {
        id: "demo-events-ingestion".to_string(),
        name: "Events Ingestion".to_string(),
        source_type: "simulated".to_string(),
        source_config: serde_json::json!({
            "event_types": ["click", "purchase", "page_view", "signup"],
            "rate_per_second": 10
        }),
        transform_sql: Some("SELECT * WHERE event_type IN ('purchase', 'signup')".to_string()),
        sink_table: "events_stream".to_string(),
        status: "active".to_string(),
        events_processed: 0,
        created_at: Utc::now(),
    }).await;
    results.insert("demo_pipeline_seeded".to_string(), serde_json::json!(pipeline_seeded));

    // Seed demo transforms
    let demo_transforms = vec![
        UserTransform {
            name: "customer_orders".to_string(),
            sql: "SELECT c.name, COUNT(o.id) as order_count, SUM(o.amount) as total_spent\nFROM pg_customers c\nJOIN pg_orders o ON c.id = o.customer_id\nGROUP BY c.name\nORDER BY total_spent DESC".to_string(),
            depends_on: vec!["pg_customers".to_string(), "pg_orders".to_string()],
            materialization: "view".to_string(),
            description: "Customer order summary with count and total spend".to_string(),
            created_at: Utc::now(),
        },
        UserTransform {
            name: "sales_by_product".to_string(),
            sql: "SELECT p.name as product_name, p.category,\n  COUNT(s.id) as sale_count, SUM(s.amount) as revenue\nFROM pg_products p\nJOIN pg_sales s ON p.id = s.product_id\nGROUP BY p.name, p.category\nORDER BY revenue DESC".to_string(),
            depends_on: vec!["pg_products".to_string(), "pg_sales".to_string()],
            materialization: "table".to_string(),
            description: "Product sales aggregation with revenue breakdown".to_string(),
            created_at: Utc::now(),
        },
    ];
    let mut transforms_seeded = 0usize;
    for ut in demo_transforms {
        if state.seed_user_transform(ut).await { transforms_seeded += 1; }
    }
    results.insert("demo_transforms_seeded".to_string(), serde_json::json!(transforms_seeded));

    Json(serde_json::Value::Object(results))
}

// ── Helpers ────────────────────────────────────────────────────────

/// Write a slice of `StreamEvent`s to a CSV file.
///
/// Appends to the file if it already exists; creates it with headers otherwise.
fn write_events_csv(path: &str, events: &[rustlake_stream::StreamEvent]) -> std::io::Result<()> {
    use std::io::Write;

    // Ensure parent directory exists
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file_exists = std::path::Path::new(path).exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    // Write header if creating new file
    if !file_exists {
        writeln!(
            file,
            "event_id,event_type,customer_id,session_id,product_id,page,timestamp,properties"
        )?;
    }

    for event in events {
        let product_id_str = event
            .product_id
            .map(|id| id.to_string())
            .unwrap_or_default();
        let props = serde_json::to_string(&event.properties).unwrap_or_default();
        // Escape properties for CSV (wrap in quotes, escape inner quotes)
        let props_escaped = props.replace('"', "\"\"");
        writeln!(
            file,
            "{},{},{},{},{},{},{},\"{}\"",
            event.event_id,
            event.event_type,
            event.customer_id,
            event.session_id,
            product_id_str,
            event.page,
            event.timestamp.to_rfc3339(),
            props_escaped,
        )?;
    }

    Ok(())
}

pub(crate) fn batches_to_json(
    batches: &[RecordBatch],
) -> std::result::Result<Vec<serde_json::Value>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(&mut buf);
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&buf)
        .map_err(|e| arrow::error::ArrowError::JsonError(e.to_string()))?;
    Ok(rows)
}

// ── Transform CRUD ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateTransformRequest {
    name: String,
    sql: String,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default = "default_materialization")]
    materialization: String,
    #[serde(default)]
    description: String,
}

fn default_materialization() -> String {
    "view".to_string()
}

async fn create_transform(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateTransformRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if req.name.is_empty() || req.sql.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "name and sql are required".to_string(),
            }),
        ));
    }

    let ut = UserTransform {
        name: req.name.clone(),
        sql: req.sql,
        depends_on: req.depends_on,
        materialization: req.materialization,
        description: req.description,
        created_at: Utc::now(),
    };

    state.add_user_transform(ut).await;

    Ok(Json(serde_json::json!({
        "status": "created",
        "name": req.name,
    })))
}

async fn delete_transform(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if !state.remove_user_transform(&name).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Transform '{}' not found", name),
            }),
        ));
    }
    Ok(Json(serde_json::json!({
        "status": "deleted",
        "name": name,
    })))
}

// ── Scheduling CRUD ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateScheduleRequest {
    name: String,
    job_type: String,
    cron: String,
    target: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default = "default_auto_engine")]
    engine: String,
    #[serde(default = "default_trigger_type")]
    trigger_type: String,
    event_config: Option<EventConfig>,
    cluster: Option<String>,
    timeout_seconds: Option<u64>,
    #[serde(default)]
    retries: Option<u32>,
    #[serde(default)]
    tags: Option<Vec<String>>,
}

/// Request body for updating a scheduled job.
#[derive(Deserialize)]
struct UpdateScheduleRequest {
    name: Option<String>,
    job_type: Option<String>,
    cron: Option<String>,
    target: Option<String>,
    enabled: Option<bool>,
    engine: Option<String>,
    trigger_type: Option<String>,
    event_config: Option<EventConfig>,
    cluster: Option<String>,
    timeout_seconds: Option<u64>,
    retries: Option<u32>,
    tags: Option<Vec<String>>,
}

fn default_enabled() -> bool {
    true
}

fn default_auto_engine() -> String {
    "auto".to_string()
}

fn default_trigger_type() -> String {
    "time".to_string()
}

/// Compute the next run time from a cron expression.
///
/// Supports:
/// - Simple intervals: `every 5m`, `every 1h`, `every 30s`, `every 7d`
/// - Special expressions: `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`
/// - Standard 5-field CRON: `min hour dom month dow` (e.g., `0 9 * * 1-5`)
/// - Quartz 6-field CRON (with seconds): `sec min hour dom month dow`
/// - Quartz 7-field CRON (with year): `sec min hour dom month dow year`
fn next_cron_run(cron: &str) -> Option<DateTime<Utc>> {
    let now = Utc::now();
    let trimmed = cron.trim();

    // Simple interval: "every 5m", "every 1h", etc.
    if let Some(rest) = trimmed.strip_prefix("every ") {
        let rest = rest.trim();
        let (num_str, unit) = rest.split_at(rest.len().saturating_sub(1));
        let num: i64 = num_str.parse().unwrap_or(1);
        let duration = match unit {
            "s" => chrono::Duration::seconds(num),
            "m" => chrono::Duration::minutes(num),
            "h" => chrono::Duration::hours(num),
            "d" => chrono::Duration::days(num),
            _ => return None,
        };
        return Some(now + duration);
    }

    // Special expressions
    match trimmed {
        "@hourly" => return Some(now + chrono::Duration::hours(1)),
        "@daily" | "@midnight" => return Some(now + chrono::Duration::days(1)),
        "@weekly" => return Some(now + chrono::Duration::weeks(1)),
        "@monthly" => return Some(now + chrono::Duration::days(30)),
        "@yearly" | "@annually" => return Some(now + chrono::Duration::days(365)),
        _ => {}
    }

    // CRON field parsing (5, 6, or 7 fields)
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    match parts.len() {
        5 => {
            // Standard CRON: min hour dom month dow
            parse_cron_fields(parts[0], parts[1], parts[2], parts[3], parts[4])
        }
        6 => {
            // Quartz 6-field: sec min hour dom month dow (ignore seconds for next_run)
            parse_cron_fields(parts[1], parts[2], parts[3], parts[4], parts[5])
        }
        7 => {
            // Quartz 7-field: sec min hour dom month dow year (ignore seconds+year)
            parse_cron_fields(parts[1], parts[2], parts[3], parts[4], parts[5])
        }
        _ => Some(now + chrono::Duration::hours(1)),
    }
}

/// Parse basic CRON fields (min, hour, dom, month, dow) to compute next run.
fn parse_cron_fields(
    min_f: &str,
    hour_f: &str,
    _dom_f: &str,
    _month_f: &str,
    _dow_f: &str,
) -> Option<DateTime<Utc>> {
    use chrono::Timelike;

    let now = Utc::now();

    // Parse minute and hour — support specific values, wildcards, ranges
    let target_min: Option<u32> = if min_f == "*" { None } else { parse_cron_value(min_f) };
    let target_hour: Option<u32> = if hour_f == "*" { None } else { parse_cron_value(hour_f) };

    match (target_hour, target_min) {
        (Some(h), Some(m)) => {
            // Specific time — find next occurrence
            let today = now.date_naive().and_hms_opt(h, m, 0)?;
            let today_utc = today.and_utc();
            if today_utc > now {
                Some(today_utc)
            } else {
                Some(today_utc + chrono::Duration::days(1))
            }
        }
        (Some(h), None) => {
            // Specific hour, any minute — next occurrence at h:00
            let today = now.date_naive().and_hms_opt(h, 0, 0)?;
            let today_utc = today.and_utc();
            if today_utc > now {
                Some(today_utc)
            } else {
                Some(today_utc + chrono::Duration::days(1))
            }
        }
        (None, Some(m)) => {
            // Any hour, specific minute — next occurrence at current_hour:m or next hour
            let this_hour = now.date_naive().and_hms_opt(now.hour(), m, 0)?;
            let this_hour_utc = this_hour.and_utc();
            if this_hour_utc > now {
                Some(this_hour_utc)
            } else {
                Some(this_hour_utc + chrono::Duration::hours(1))
            }
        }
        (None, None) => {
            // Wildcards — runs every minute
            Some(now + chrono::Duration::minutes(1))
        }
    }
}

/// Parse a single CRON field value, returning the first numeric value.
/// Handles: "5", "09", "MON-FRI" (returns None for named), "1-5" (returns first).
fn parse_cron_value(field: &str) -> Option<u32> {
    // Handle "?" (any, used in Quartz for dom/dow)
    if field == "?" {
        return None;
    }
    // Try direct parse
    if let Ok(v) = field.parse::<u32>() {
        return Some(v);
    }
    // Handle ranges like "1-5" — return the first value
    if let Some((first, _)) = field.split_once('-') {
        return first.parse::<u32>().ok();
    }
    // Handle lists like "1,3,5" — return the first value
    if let Some((first, _)) = field.split_once(',') {
        return first.parse::<u32>().ok();
    }
    None
}

async fn list_schedules(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let jobs = state.scheduled_jobs.read().await;
    Json(serde_json::json!({
        "count": jobs.len(),
        "schedules": *jobs,
    }))
}

/// GET /api/v1/schedules/{id} — Get a single scheduled job by ID.
async fn get_schedule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let jobs = state.scheduled_jobs.read().await;
    match jobs.iter().find(|j| j.id == id) {
        Some(job) => Ok(Json(serde_json::to_value(job).unwrap_or_default())),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Schedule '{}' not found", id),
            }),
        )),
    }
}

/// PUT /api/v1/schedules/{id} — Update a scheduled job's fields.
async fn update_schedule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateScheduleRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut jobs = state.scheduled_jobs.write().await;
    match jobs.iter_mut().find(|j| j.id == id) {
        Some(job) => {
            if let Some(name) = req.name { job.name = name; }
            if let Some(job_type) = req.job_type { job.job_type = job_type; }
            if let Some(cron) = &req.cron { job.cron = cron.clone(); }
            if let Some(target) = req.target { job.target = target; }
            if let Some(enabled) = req.enabled { job.enabled = enabled; }
            if let Some(engine) = req.engine { job.engine = engine; }
            if let Some(trigger_type) = req.trigger_type { job.trigger_type = trigger_type; }
            if req.event_config.is_some() { job.event_config = req.event_config; }
            if req.cluster.is_some() { job.cluster = req.cluster; }
            if req.timeout_seconds.is_some() { job.timeout_seconds = req.timeout_seconds; }
            if let Some(retries) = req.retries { job.retries = retries; }
            if let Some(tags) = req.tags { job.tags = tags; }
            // Recalculate next_run if cron or enabled changed
            if job.enabled && job.trigger_type != "continuous" && job.trigger_type != "event" {
                job.next_run = next_cron_run(&job.cron);
            } else if !job.enabled {
                job.next_run = None;
            }
            let updated_name = job.name.clone();
            drop(jobs);
            state.persist_jobs().await;
            Ok(Json(serde_json::json!({
                "status": "updated",
                "id": id,
                "name": updated_name,
            })))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Schedule '{}' not found", id),
            }),
        )),
    }
}

async fn create_schedule(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateScheduleRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if req.name.is_empty() || req.target.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "name and target are required".to_string(),
            }),
        ));
    }

    let id = Uuid::new_v4().to_string();
    let next_run = if req.enabled && req.trigger_type != "continuous" && req.trigger_type != "event" {
        next_cron_run(&req.cron)
    } else {
        None
    };

    let job = ScheduledJob {
        id: id.clone(),
        name: req.name.clone(),
        job_type: req.job_type,
        cron: req.cron,
        target: req.target,
        enabled: req.enabled,
        last_run: None,
        next_run,
        last_status: None,
        created_at: Utc::now(),
        engine: req.engine,
        trigger_type: req.trigger_type,
        event_config: req.event_config,
        cluster: req.cluster,
        timeout_seconds: req.timeout_seconds,
        retries: req.retries.unwrap_or(0),
        tags: req.tags.unwrap_or_default(),
    };

    state.add_scheduled_job(job).await;

    Ok(Json(serde_json::json!({
        "status": "created",
        "id": id,
        "name": req.name,
    })))
}

async fn delete_schedule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if !state.remove_scheduled_job(&id).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Schedule '{}' not found", id),
            }),
        ));
    }
    Ok(Json(serde_json::json!({
        "status": "deleted",
        "id": id,
    })))
}

/// GET /api/v1/schedules/runs — List job execution history.
async fn list_job_runs(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let runs = state.job_runs.read().await;
    Json(serde_json::json!({
        "count": runs.len(),
        "runs": *runs,
    }))
}

/// GET /api/v1/clusters — List available job clusters.
async fn list_clusters(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let clusters = state.job_clusters.read().await;
    Json(serde_json::json!({
        "clusters": *clusters,
    }))
}

/// POST /api/v1/schedules/{id}/run — Manually trigger a scheduled job.
async fn run_schedule(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let run_start = Instant::now();
    let mut jobs = state.scheduled_jobs.write().await;
    let job = jobs.iter_mut().find(|j| j.id == id);
    match job {
        Some(job) => {
            let now = Utc::now();
            let target = job.target.clone();
            let job_type = job.job_type.clone();
            let job_name = job.name.clone();
            let job_id = job.id.clone();
            let job_engine = job.engine.clone();
            job.last_run = Some(now);
            drop(jobs);

            let result = match job_type.as_str() {
                "transform" => run_transform_job(&state, &target, &run_start).await,
                "sql" => run_sql_job(&state, &target, &job_engine, &run_start).await,
                "notebook" => run_notebook_job(&state, &target, &run_start).await,
                "pipeline" => run_pipeline_job(&state, &target, &run_start).await,
                "dashboard_refresh" => run_dashboard_refresh_job(&target, &run_start).await,
                _ => run_simulated_job(&job_type, &target, &run_start),
            };

            let duration_ms = run_start.elapsed().as_millis();
            let (status, message, error) = match result {
                Ok(msg) => ("success".to_string(), msg, None),
                Err(err) => ("error".to_string(), err.clone(), Some(err)),
            };

            let mut jobs = state.scheduled_jobs.write().await;
            if let Some(j) = jobs.iter_mut().find(|j| j.id == id) {
                j.last_status = Some(status.clone());
            }
            drop(jobs);
            state.persist_jobs().await;
            state.record_job_run(JobRunEntry {
                job_id: job_id.clone(),
                job_name,
                target: target.clone(),
                status: status.clone(),
                duration_ms,
                error,
                timestamp: Utc::now(),
            }).await;

            Ok(Json(serde_json::json!({
                "status": status, "job_id": job_id, "target": target,
                "duration_ms": duration_ms, "message": message,
            })))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Schedule '{}' not found", id),
            }),
        )),
    }
}

/// Execute a transform job — compile and run SQL via the transform compiler.
async fn run_transform_job(
    state: &Arc<AppState>,
    target: &str,
    _run_start: &Instant,
) -> Result<String, String> {
    let mut entries = build_transform_entries();
    let user = state.user_transforms.read().await;
    for ut in user.iter() {
        entries.push(TransformEntry {
            name: ut.name.clone(),
            sql: ut.sql.clone(),
            depends_on: ut.depends_on.clone(),
            materialization: ut.materialization.clone(),
            description: ut.description.clone(),
        });
    }
    drop(user);
    let compiler = build_compiler_from_entries(&entries);
    let executable_sql = build_executable_sql(target, &entries, &compiler)
        .map_err(|_| format!("Transform '{}' not found", target))?;
    let ctx = state.ctx.read().await;
    ctx.sql(&executable_sql)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("Transform '{}' executed successfully", target))
}

/// Execute a raw SQL job via the specified engine (auto/datafusion/duckdb/polars).
async fn run_sql_job(
    state: &Arc<AppState>,
    sql: &str,
    engine: &str,
    _run_start: &Instant,
) -> Result<String, String> {
    let batches = match engine.to_lowercase().as_str() {
        "duckdb" if state.duckdb_available() => execute_via_duckdb(state, sql).await?,
        "polars" if state.polars_available() => execute_via_polars(state, sql).await?,
        _ => {
            let ctx = state.ctx.read().await;
            ctx.sql(sql).await.map_err(|e| e.to_string())?
        }
    };
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok(format!("SQL executed — {} rows returned", row_count))
}

/// Execute a notebook job — run semicolon-separated SQL statements sequentially.
async fn run_notebook_job(
    state: &Arc<AppState>,
    target: &str,
    _run_start: &Instant,
) -> Result<String, String> {
    let statements: Vec<&str> = target
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if statements.is_empty() {
        return Err("Notebook has no SQL statements".to_string());
    }
    let ctx = state.ctx.read().await;
    let mut total_rows = 0usize;
    for (i, stmt) in statements.iter().enumerate() {
        let batches = ctx.sql(stmt).await.map_err(|e| {
            format!("Statement {} failed: {}", i + 1, e)
        })?;
        total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    Ok(format!(
        "Notebook executed — {} statements, {} total rows",
        statements.len(),
        total_rows
    ))
}

/// Execute a pipeline job — simulates starting a streaming pipeline.
async fn run_pipeline_job(
    state: &Arc<AppState>,
    target: &str,
    _run_start: &Instant,
) -> Result<String, String> {
    let pipelines = state.streaming_pipelines.read().await;
    let found = pipelines.iter().any(|p| p.name == target || p.id == target);
    drop(pipelines);
    if found {
        Ok(format!("Pipeline '{}' triggered", target))
    } else {
        Ok(format!("Pipeline '{}' scheduled (not yet registered)", target))
    }
}

/// Execute a dashboard refresh job — simulates cache refresh.
async fn run_dashboard_refresh_job(
    target: &str,
    _run_start: &Instant,
) -> Result<String, String> {
    Ok(format!("Dashboard '{}' cache refreshed", target))
}

/// Execute a simulated job for compaction, snapshot, ingest, vacuum, etc.
fn run_simulated_job(
    job_type: &str,
    target: &str,
    _run_start: &Instant,
) -> Result<String, String> {
    Ok(format!("{} job '{}' completed (simulated)", job_type, target))
}

/// DELETE /api/v1/tables/{name} — Deregister a table from the DataFusion context.
async fn deregister_table(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;
    match ctx.deregister_table(&name).await {
        Ok(()) => Ok(Json(serde_json::json!({
            "status": "deregistered",
            "table": name,
        }))),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Failed to deregister '{}': {}", name, e),
            }),
        )),
    }
}

// ── Table Description Editing ────────────────────────────────────────

#[derive(Deserialize)]
struct UpdateDescriptionRequest {
    #[serde(default)]
    table_description: Option<String>,
    #[serde(default)]
    column_descriptions: Option<std::collections::HashMap<String, String>>,
}

/// PUT /api/v1/tables/{name}/description — Update table and column descriptions.
async fn update_table_description(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<UpdateDescriptionRequest>,
) -> Json<serde_json::Value> {
    if let Some(desc) = req.table_description {
        let mut descs = state.table_descriptions.write().await;
        if desc.is_empty() {
            descs.remove(&name);
        } else {
            descs.insert(name.clone(), desc);
        }
    }
    if let Some(col_descs) = req.column_descriptions {
        let mut descs = state.column_descriptions.write().await;
        for (col, desc) in col_descs {
            let key = format!("{}.{}", name, col);
            if desc.is_empty() {
                descs.remove(&key);
            } else {
                descs.insert(key, desc);
            }
        }
    }
    Json(serde_json::json!({ "status": "updated", "table": name }))
}

/// GET /api/v1/tables/{name}/description — Get table and column descriptions.
async fn get_table_description(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    let table_desc = state.table_descriptions.read().await.get(&name).cloned();
    let col_descs = state.column_descriptions.read().await;
    let prefix = format!("{}.", name);
    let columns: std::collections::HashMap<String, String> = col_descs
        .iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .map(|(k, v)| (k[prefix.len()..].to_string(), v.clone()))
        .collect();
    Json(serde_json::json!({
        "table": name,
        "description": table_desc,
        "columns": columns,
    }))
}

// ── Streaming Pipeline CRUD ─────────────────────────────────────────

#[derive(Deserialize)]
struct CreatePipelineRequest {
    name: String,
    source_type: String,
    source_config: serde_json::Value,
    transform_sql: Option<String>,
    sink_table: String,
}

async fn list_pipelines(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let pipelines = state.streaming_pipelines.read().await;
    Json(serde_json::json!({
        "count": pipelines.len(),
        "pipelines": *pipelines,
    }))
}

async fn create_pipeline(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreatePipelineRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if req.name.is_empty() || req.sink_table.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "name and sink_table are required".to_string(),
            }),
        ));
    }

    let id = Uuid::new_v4().to_string();
    let pipeline = StreamingPipeline {
        id: id.clone(),
        name: req.name.clone(),
        source_type: req.source_type,
        source_config: req.source_config,
        transform_sql: req.transform_sql,
        sink_table: req.sink_table,
        status: "created".to_string(),
        events_processed: 0,
        created_at: Utc::now(),
    };

    #[cfg(feature = "duckdb")]
    if let Some(ref db) = state.state_db {
        let _ = db.upsert_pipeline(&pipeline);
    }
    state.streaming_pipelines.write().await.push(pipeline);

    Ok(Json(serde_json::json!({
        "status": "created",
        "id": id,
        "name": req.name,
    })))
}

async fn delete_pipeline(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut pipelines = state.streaming_pipelines.write().await;
    let before = pipelines.len();
    pipelines.retain(|p| p.id != id);
    if pipelines.len() == before {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Pipeline '{}' not found", id),
            }),
        ));
    }
    #[cfg(feature = "duckdb")]
    if let Some(ref db) = state.state_db {
        let _ = db.delete_pipeline(&id);
    }
    Ok(Json(serde_json::json!({
        "status": "deleted",
        "id": id,
    })))
}

/// POST /api/v1/streaming/pipelines/{id}/start — start a CDC pipeline.
///
/// For `mongodb-cdc` source type, creates a `MongoDbCdcSource` and spawns a
/// background task that reads change events and logs them. The task handle is
/// stored so it can be stopped later.
async fn start_pipeline(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Check if already running
    {
        let cdc_sources = state.cdc_sources.read().await;
        if let Some(src) = cdc_sources.get(&id) {
            if src.is_running() {
                return Err((
                    StatusCode::CONFLICT,
                    Json(ErrorResponse {
                        error: format!("Pipeline '{}' is already running", id),
                    }),
                ));
            }
        }
    }

    // Look up the pipeline
    let pipeline = {
        let pipelines = state.streaming_pipelines.read().await;
        pipelines.iter().find(|p| p.id == id).cloned()
    };
    let pipeline = pipeline.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Pipeline '{}' not found", id),
            }),
        )
    })?;

    if pipeline.source_type == "mongodb-cdc" {
        // Parse CDC config from source_config
        let cdc_config: crate::mongodb_cdc::CdcSourceConfig =
            serde_json::from_value(pipeline.source_config.clone()).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("Invalid mongodb-cdc source_config: {}", e),
                    }),
                )
            })?;

        // Find the MongoDB connection to get params
        let mongo_params = if !cdc_config.connection_id.is_empty() {
            let connections = state.connections.read().await;
            let conn = connections
                .iter()
                .find(|c| c.id == cdc_config.connection_id)
                .cloned();
            drop(connections);
            match conn {
                Some(c) => {
                    let passwords = state.connection_passwords.read().await;
                    let password = passwords.get(&c.id).cloned().unwrap_or_default();
                    drop(passwords);
                    build_mongo_params_from_entry(&c, &password)
                }
                None => {
                    return Err((
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!(
                                "MongoDB connection '{}' not found",
                                cdc_config.connection_id
                            ),
                        }),
                    ));
                }
            }
        } else {
            // Use database field directly with default local connection
            crate::mongodb_conn::MongoConnParams {
                database: cdc_config.database.clone(),
                ..Default::default()
            }
        };

        // Start the CDC source
        match crate::mongodb_cdc::MongoDbCdcSource::start(&mongo_params, &cdc_config, None).await {
            Ok((source, mut rx)) => {
                let source = std::sync::Arc::new(source);

                // Store the source handle
                state
                    .cdc_sources
                    .write()
                    .await
                    .insert(id.clone(), source.clone());

                // Update pipeline status
                {
                    let mut pipelines = state.streaming_pipelines.write().await;
                    if let Some(p) = pipelines.iter_mut().find(|p| p.id == id) {
                        p.status = "running".to_string();
                    }
                }

                // Spawn background task to consume CDC events
                let state_clone = state.clone();
                let pipeline_id = id.clone();
                let pipeline_name = pipeline.name.clone();
                let pipeline_sink = pipeline.sink_table.clone();
                let pipeline_source = pipeline.source_type.clone();
                tokio::spawn(async move {
                    let mut total_events: u64 = 0;
                    while let Some(result) = rx.recv().await {
                        match result {
                            Ok(batch) => {
                                let rows = batch.num_rows() as u64;
                                total_events += rows;
                                tracing::info!(
                                    pipeline_id = %pipeline_id,
                                    batch_rows = rows,
                                    total_events = total_events,
                                    "CDC batch received"
                                );
                                // Update events_processed counter
                                let mut pipelines =
                                    state_clone.streaming_pipelines.write().await;
                                if let Some(p) =
                                    pipelines.iter_mut().find(|p| p.id == pipeline_id)
                                {
                                    p.events_processed = total_events;
                                }
                                drop(pipelines);

                                // Broadcast real-time event to SSE listeners
                                let _ = state_clone.pipeline_events_tx.send(
                                    crate::state::PipelineEvent {
                                        pipeline_id: pipeline_id.clone(),
                                        pipeline_name: pipeline_name.clone(),
                                        status: "running".to_string(),
                                        events_processed: total_events,
                                        batch_rows: rows,
                                        source_type: pipeline_source.clone(),
                                        sink_table: pipeline_sink.clone(),
                                    }
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    pipeline_id = %pipeline_id,
                                    error = %e,
                                    "CDC source error"
                                );
                                break;
                            }
                        }
                    }
                    // Pipeline ended — update status
                    let mut pipelines = state_clone.streaming_pipelines.write().await;
                    if let Some(p) = pipelines.iter_mut().find(|p| p.id == pipeline_id) {
                        p.status = "stopped".to_string();
                    }
                    state_clone.cdc_sources.write().await.remove(&pipeline_id);
                    tracing::info!(pipeline_id = %pipeline_id, total_events = total_events, "CDC pipeline ended");
                });

                Ok(Json(serde_json::json!({
                    "status": "started",
                    "id": id,
                    "source_type": "mongodb-cdc",
                })))
            }
            Err(e) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to start CDC source: {}", e),
                }),
            )),
        }
    } else {
        // For non-CDC pipelines, just update status
        let mut pipelines = state.streaming_pipelines.write().await;
        if let Some(p) = pipelines.iter_mut().find(|p| p.id == id) {
            p.status = "running".to_string();
        }
        Ok(Json(serde_json::json!({
            "status": "started",
            "id": id,
            "source_type": pipeline.source_type,
        })))
    }
}

/// POST /api/v1/streaming/pipelines/{id}/stop — stop a running pipeline.
async fn stop_pipeline(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Stop CDC source if running
    {
        let cdc_sources = state.cdc_sources.read().await;
        if let Some(source) = cdc_sources.get(&id) {
            source.stop();
        }
    }
    // Remove from active sources
    state.cdc_sources.write().await.remove(&id);

    // Update pipeline status
    let mut pipelines = state.streaming_pipelines.write().await;
    if let Some(p) = pipelines.iter_mut().find(|p| p.id == id) {
        p.status = "stopped".to_string();
        Ok(Json(serde_json::json!({
            "status": "stopped",
            "id": id,
        })))
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Pipeline '{}' not found", id),
            }),
        ))
    }
}

/// POST /api/v1/streaming/pipelines/import — bulk import pipelines from JSON.
async fn import_pipelines(
    State(state): State<Arc<AppState>>,
    Json(req): Json<Vec<CreatePipelineRequest>>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut created = Vec::new();
    for p_req in req {
        if p_req.name.is_empty() || p_req.sink_table.is_empty() {
            continue;
        }
        let id = Uuid::new_v4().to_string();
        let pipeline = crate::state::StreamingPipeline {
            id: id.clone(),
            name: p_req.name.clone(),
            source_type: p_req.source_type,
            source_config: p_req.source_config,
            transform_sql: p_req.transform_sql,
            sink_table: p_req.sink_table,
            status: "created".to_string(),
            events_processed: 0,
            created_at: Utc::now(),
        };
        #[cfg(feature = "duckdb")]
        if let Some(ref db) = state.state_db {
            let _ = db.upsert_pipeline(&pipeline);
        }
        state.streaming_pipelines.write().await.push(pipeline);
        created.push(serde_json::json!({ "id": id, "name": p_req.name }));
    }
    tracing::info!(count = created.len(), "Imported {} pipelines", created.len());
    Ok(Json(serde_json::json!({
        "status": "imported",
        "count": created.len(),
        "pipelines": created,
    })))
}

// ── S3 / Object Storage Config Handlers ─────────────────────────────

#[derive(Clone, Deserialize)]
pub(crate) struct AddS3ConfigRequest {
    name: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    #[serde(default = "default_s3_region")]
    region: String,
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

async fn add_s3_config(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddS3ConfigRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let config = S3Config {
        name: req.name.clone(),
        endpoint: req.endpoint.clone(),
        access_key: req.access_key.clone(),
        secret_key: req.secret_key.clone(),
        bucket: req.bucket.clone(),
        region: req.region.clone(),
        status: "configured".to_string(),
        created_at: Utc::now(),
        tables: vec![],
        table_types: std::collections::HashMap::new(),
        table_formats: std::collections::HashMap::new(),
        sync_status: "syncing".to_string(),
        sync_error: None,
        scan_progress: None,
        scan_detail: None,
        scan_scanned: 0,
        scan_total: 0,
        scan_found: 0,
        scan_elapsed_ms: 0,
        format_counts: std::collections::HashMap::new(),
    };

    #[cfg(feature = "duckdb")]
    if let Some(ref db) = state.state_db {
        let _ = db.upsert_s3_config(&req.name, &req.endpoint, &req.bucket, &req.region);
    }
    let mut configs = state.s3_configs.write().await;
    configs.push(config);
    drop(configs);

    tracing::info!(
        name = %req.name,
        bucket = %req.bucket,
        "S3 config saved — starting background Iceberg table discovery"
    );

    // Spawn background task to discover Iceberg tables on S3
    let bg_state = state.clone();
    let bg_name = req.name.clone();
    let bg_endpoint = req.endpoint.clone();
    let bg_access_key = req.access_key.clone();
    let bg_secret_key = req.secret_key;
    let bg_bucket = req.bucket.clone();
    let bg_region = req.region.clone();
    tokio::spawn(async move {
        let result = discover_s3_tables(
            &bg_state, &bg_name, &bg_endpoint, &bg_access_key, &bg_secret_key, &bg_bucket, &bg_region,
        ).await;
        match result {
            Ok(result) => {
                let count = result.tables.len();
                let mut configs = bg_state.s3_configs.write().await;
                if let Some(cfg) = configs.iter_mut().find(|c| c.name == bg_name) {
                    cfg.tables = result.tables;
                    cfg.table_types = result.table_types;
                    cfg.table_formats = result.table_formats;
                    cfg.format_counts = result.format_counts;
                    cfg.status = "ready".to_string();
                    cfg.sync_status = "ready".to_string();
                    cfg.sync_error = None;
                    cfg.scan_progress = Some("complete".to_string());
                    cfg.scan_detail = None;
                }
                tracing::info!(
                    name = %bg_name,
                    tables = count,
                    "S3 Iceberg discovery complete: {} tables found",
                    count
                );
            }
            Err(e) => {
                let mut configs = bg_state.s3_configs.write().await;
                if let Some(cfg) = configs.iter_mut().find(|c| c.name == bg_name) {
                    cfg.status = "error".to_string();
                    cfg.sync_status = "error".to_string();
                    cfg.sync_error = Some(e.clone());
                }
                tracing::error!(
                    name = %bg_name,
                    error = %e,
                    "S3 Iceberg discovery failed"
                );
            }
        }
    });

    Ok(Json(serde_json::json!({
        "status": "ok",
        "sync_status": "syncing",
        "name": req.name,
        "endpoint": req.endpoint,
        "bucket": req.bucket,
        "message": "S3 configuration saved. Iceberg table discovery running in background."
    })))
}

// ── Bulk Import / Export Handlers ─────────────────────────────────────

/// Request body for bulk-importing multiple connections and S3 configs at once.
#[derive(Deserialize)]
struct BulkImportRequest {
    /// Database connections to add.
    #[serde(default)]
    connections: Vec<AddConnectionRequest>,
    /// S3 storage configs to add.
    #[serde(default)]
    s3_configs: Vec<AddS3ConfigRequest>,
}

/// POST /api/v1/connections/import — bulk-import connections and S3 configs.
///
/// Skips connectivity checks for speed; background sync will detect failures.
async fn import_connections(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BulkImportRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut conn_results = Vec::new();
    let mut s3_results = Vec::new();
    let errors: Vec<serde_json::Value> = Vec::new();

    // ── Process database connections ────────────────────────────────
    for conn_req in req.connections {
        let id = Uuid::new_v4().to_string();
        let entry = ConnectionEntry {
            id: id.clone(),
            name: conn_req.name.clone(),
            conn_type: conn_req.conn_type.clone(),
            host: conn_req.host.clone(),
            port: conn_req.port,
            database: conn_req.database.clone(),
            username: conn_req.username.clone(),
            status: "connected".to_string(),
            tables: vec![],
            created_at: Utc::now(),
            source: "import".to_string(),
            sync_status: "syncing".to_string(),
            sync_error: None,
            sync_progress: None,
            auth_method: conn_req.auth_method.clone(),
            connection_string: if conn_req.connection_string.is_empty() {
                None
            } else {
                Some(conn_req.connection_string.clone())
            },
            aws_access_key: if conn_req.aws_access_key.is_empty() { None } else { Some(conn_req.aws_access_key.clone()) },
            aws_secret_key: if conn_req.aws_secret_key.is_empty() { None } else { Some(conn_req.aws_secret_key.clone()) },
            aws_session_token: if conn_req.aws_session_token.is_empty() { None } else { Some(conn_req.aws_session_token.clone()) },
        };

        state.add_connection_entry(entry).await;
        state.store_password(id.clone(), conn_req.password.clone()).await;

        tracing::info!(
            id = %id,
            name = %conn_req.name,
            conn_type = %conn_req.conn_type,
            "Bulk import: connection added — starting background table discovery"
        );

        // Spawn background table discovery (same as add_connection)
        let bg_state = state.clone();
        let bg_id = id.clone();
        let bg_req = conn_req.clone();
        tokio::spawn(async move {
            let result = discover_and_register_tables(&bg_state, &bg_id, &bg_req).await;
            match result {
                Ok(tables) => {
                    bg_state
                        .update_connection_entry(&bg_id, |entry| {
                            entry.tables = tables.clone();
                            entry.sync_status = "ready".to_string();
                            entry.sync_error = None;
                        })
                        .await;
                    tracing::info!(
                        id = %bg_id,
                        tables = tables.len(),
                        "Bulk import: background sync complete — {} tables registered",
                        tables.len()
                    );
                }
                Err(e) => {
                    bg_state
                        .update_connection_entry(&bg_id, |entry| {
                            entry.sync_status = "error".to_string();
                            entry.sync_error = Some(e.clone());
                        })
                        .await;
                    tracing::error!(
                        id = %bg_id,
                        error = %e,
                        "Bulk import: background sync failed"
                    );
                }
            }
        });

        conn_results.push(serde_json::json!({
            "name": conn_req.name,
            "id": id,
            "status": "connected",
            "sync_status": "syncing",
        }));
    }

    // ── Process S3 configs ──────────────────────────────────────────
    for s3_req in req.s3_configs {
        let config = S3Config {
            name: s3_req.name.clone(),
            endpoint: s3_req.endpoint.clone(),
            access_key: s3_req.access_key.clone(),
            secret_key: s3_req.secret_key.clone(),
            bucket: s3_req.bucket.clone(),
            region: s3_req.region.clone(),
            status: "configured".to_string(),
            created_at: Utc::now(),
            tables: vec![],
            table_types: std::collections::HashMap::new(),
            table_formats: std::collections::HashMap::new(),
            sync_status: "syncing".to_string(),
            sync_error: None,
            scan_progress: None,
            scan_detail: None,
            scan_scanned: 0,
            scan_total: 0,
            scan_found: 0,
            scan_elapsed_ms: 0,
            format_counts: std::collections::HashMap::new(),
        };

        let mut configs = state.s3_configs.write().await;
        configs.push(config);
        drop(configs);

        tracing::info!(
            name = %s3_req.name,
            bucket = %s3_req.bucket,
            "Bulk import: S3 config saved — starting background Iceberg table discovery"
        );

        // Spawn background Iceberg discovery (same as add_s3_config)
        let bg_state = state.clone();
        let bg_name = s3_req.name.clone();
        let bg_endpoint = s3_req.endpoint.clone();
        let bg_access_key = s3_req.access_key.clone();
        let bg_secret_key = s3_req.secret_key.clone();
        let bg_bucket = s3_req.bucket.clone();
        let bg_region = s3_req.region.clone();
        tokio::spawn(async move {
            let result = discover_s3_tables(
                &bg_state,
                &bg_name,
                &bg_endpoint,
                &bg_access_key,
                &bg_secret_key,
                &bg_bucket,
                &bg_region,
            )
            .await;
            match result {
                Ok(result) => {
                    let count = result.tables.len();
                    let mut configs = bg_state.s3_configs.write().await;
                    if let Some(cfg) = configs.iter_mut().find(|c| c.name == bg_name) {
                        cfg.tables = result.tables;
                        cfg.table_types = result.table_types;
                        cfg.table_formats = result.table_formats;
                        cfg.format_counts = result.format_counts;
                        cfg.status = "ready".to_string();
                        cfg.sync_status = "ready".to_string();
                        cfg.sync_error = None;
                        cfg.scan_progress = Some("complete".to_string());
                        cfg.scan_detail = None;
                    }
                    tracing::info!(
                        name = %bg_name,
                        tables = count,
                        "Bulk import: S3 Iceberg discovery complete — {} tables found",
                        count
                    );
                }
                Err(e) => {
                    let mut configs = bg_state.s3_configs.write().await;
                    if let Some(cfg) = configs.iter_mut().find(|c| c.name == bg_name) {
                        cfg.status = "error".to_string();
                        cfg.sync_status = "error".to_string();
                        cfg.sync_error = Some(e.clone());
                    }
                    tracing::error!(
                        name = %bg_name,
                        error = %e,
                        "Bulk import: S3 Iceberg discovery failed"
                    );
                }
            }
        });

        s3_results.push(serde_json::json!({
            "name": s3_req.name,
            "bucket": s3_req.bucket,
            "status": "configured",
            "sync_status": "syncing",
        }));
    }

    let total = conn_results.len() + s3_results.len();

    Ok(Json(serde_json::json!({
        "imported": {
            "connections": conn_results,
            "s3_configs": s3_results,
        },
        "total": total,
        "errors": errors,
    })))
}

/// GET /api/v1/connections/export — export all connections and S3 configs (passwords redacted).
async fn export_connections(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let connections = state.connections.read().await;
    let exported_connections: Vec<serde_json::Value> = connections
        .iter()
        .map(|c| {
            serde_json::json!({
                "name": c.name,
                "conn_type": c.conn_type,
                "host": c.host,
                "port": c.port,
                "database": c.database,
                "username": c.username,
                "password": "",
            })
        })
        .collect();
    drop(connections);

    let s3_configs = state.s3_configs.read().await;
    let exported_s3: Vec<serde_json::Value> = s3_configs
        .iter()
        .map(|c| {
            serde_json::json!({
                "name": c.name,
                "endpoint": c.endpoint,
                "access_key": c.access_key,
                "secret_key": "",
                "bucket": c.bucket,
                "region": c.region,
            })
        })
        .collect();
    drop(s3_configs);

    Json(serde_json::json!({
        "connections": exported_connections,
        "s3_configs": exported_s3,
    }))
}

/// Parse a table name into a DataFusion `TableReference`.
/// Supports bare names ("my_table"), schema-qualified ("pg.orders"), and
/// catalog-qualified ("catalog.schema.table").
fn parse_table_reference(name: &str) -> datafusion::common::TableReference {
    let parts: Vec<&str> = name.splitn(3, '.').collect();
    match parts.len() {
        3 => datafusion::common::TableReference::full(parts[0], parts[1], parts[2]),
        2 => datafusion::common::TableReference::partial(parts[0], parts[1]),
        _ => datafusion::common::TableReference::bare(name),
    }
}

/// Map Iceberg type strings to Arrow DataType.
fn iceberg_type_to_arrow(iceberg_type: &str) -> arrow::datatypes::DataType {
    // Iceberg types can be simple ("long") or parameterized ("decimal(10,2)", "fixed[16]")
    let lower = iceberg_type.to_lowercase();
    let base = lower.split('(').next().unwrap_or(&lower).trim();
    match base {
        "boolean" => arrow::datatypes::DataType::Boolean,
        "int" | "integer" => arrow::datatypes::DataType::Int32,
        "long" | "bigint" => arrow::datatypes::DataType::Int64,
        "float" => arrow::datatypes::DataType::Float32,
        "double" => arrow::datatypes::DataType::Float64,
        "string" => arrow::datatypes::DataType::Utf8,
        "binary" | "fixed" => arrow::datatypes::DataType::Binary,
        "date" => arrow::datatypes::DataType::Date32,
        "time" => arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        "timestamp" => arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond, None,
        ),
        "timestamptz" | "timestamp_ltz" => arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()),
        ),
        "decimal" => {
            // Parse decimal(precision, scale) — default to (38, 10)
            if let Some(params) = iceberg_type.split('(').nth(1) {
                let parts: Vec<&str> = params.trim_end_matches(')').split(',').collect();
                let precision = parts.first().and_then(|p| p.trim().parse::<u8>().ok()).unwrap_or(38);
                let scale = parts.get(1).and_then(|s| s.trim().parse::<i8>().ok()).unwrap_or(10);
                arrow::datatypes::DataType::Decimal128(precision, scale)
            } else {
                arrow::datatypes::DataType::Decimal128(38, 10)
            }
        }
        "uuid" => arrow::datatypes::DataType::Utf8, // UUID stored as string
        "list" | "array" => arrow::datatypes::DataType::Utf8, // Nested types → JSON string
        "map" | "struct" => arrow::datatypes::DataType::Utf8, // Nested types → JSON string
        _ => arrow::datatypes::DataType::Utf8, // Unknown → string fallback
    }
}

/// Try to register an S3-backed ListingTable so queries return real data.
///
/// Creates a DataFusion ListingTable that reads .parquet files from the S3 path.
/// Schema is inferred from the Parquet file headers (fast — only reads footer metadata).
async fn try_register_listing_table(
    df_ctx: &datafusion::prelude::SessionContext,
    table_name: &str,
    s3_data_path: &str,
    schema_provider: &Arc<dyn datafusion::catalog::SchemaProvider>,
) -> Result<(), String> {
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
    use datafusion::datasource::file_format::parquet::ParquetFormat;

    tracing::debug!(table_name = %table_name, path = %s3_data_path, "Parsing ListingTableUrl");
    let table_url = ListingTableUrl::parse(s3_data_path)
        .map_err(|e| format!("ListingTableUrl '{}': {}", s3_data_path, e))?;

    let parquet_format = ParquetFormat::default();
    let listing_options = ListingOptions::new(Arc::new(parquet_format))
        .with_file_extension(".parquet");

    tracing::debug!(table_name = %table_name, "Inferring schema from parquet files");
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .infer_schema(&df_ctx.state())
        .await
        .map_err(|e| format!("Schema infer at '{}': {}", s3_data_path, e))?;

    tracing::debug!(table_name = %table_name, schema_fields = config.file_schema.as_ref().map(|s| s.fields().len()).unwrap_or(0), "Schema inferred");
    let listing_table = ListingTable::try_new(config)
        .map_err(|e| format!("ListingTable '{}': {}", table_name, e))?;

    let provider: Arc<dyn datafusion::datasource::TableProvider> = Arc::new(listing_table);
    schema_provider.register_table(table_name.to_string(), provider)
        .map_err(|e| format!("Register '{}': {}", table_name, e))?;

    tracing::debug!(table_name = %table_name, "Table registered in schema provider");
    Ok(())
}

/// Result of S3 discovery: registered table names, table types, table formats, and format counts.
struct S3DiscoveryResult {
    tables: Vec<String>,
    table_types: std::collections::HashMap<String, String>,
    table_formats: std::collections::HashMap<String, String>,
    format_counts: std::collections::HashMap<String, usize>,
}

/// Background task: scan S3 warehouse for tables (all formats) and register in DataFusion.
///
/// Sends real-time progress updates via the S3Config's scan_* fields, picked up by SSE.
async fn discover_s3_tables(
    state: &Arc<AppState>,
    name: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    bucket: &str,
    region: &str,
) -> std::result::Result<S3DiscoveryResult, String> {
    let ep = if endpoint.is_empty() { None } else { Some(endpoint) };
    let store = crate::iceberg_s3::build_s3_store(bucket, access_key, secret_key, region, ep)?;

    // Set up progress channel — updates S3Config scan fields for SSE pickup
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<crate::iceberg_s3::ScanProgress>();

    // Spawn progress consumer — writes updates to state
    let progress_state = state.clone();
    let progress_name = name.to_string();
    let progress_handle = tokio::spawn(async move {
        while let Some(progress) = progress_rx.recv().await {
            let mut configs = progress_state.s3_configs.write().await;
            if let Some(cfg) = configs.iter_mut().find(|c| c.name == progress_name) {
                cfg.scan_progress = Some(progress.phase);
                cfg.scan_detail = Some(progress.detail);
                cfg.scan_scanned = progress.tables_scanned;
                cfg.scan_total = progress.total_to_scan;
                cfg.scan_found = progress.tables_found;
                cfg.scan_elapsed_ms = progress.elapsed_ms;
                cfg.format_counts = progress.formats;
            }
        }
    });

    let scan_result = crate::iceberg_s3::scan_warehouse_with_progress(&store, "", Some(progress_tx)).await?;

    // Wait for progress consumer to finish
    let _ = progress_handle.await;

    tracing::info!(
        name = %name,
        bucket = %bucket,
        databases = scan_result.databases.len(),
        tables = scan_result.total_tables,
        elapsed_ms = scan_result.scan_duration_ms,
        formats = ?scan_result.format_counts,
        "S3 multi-format scan complete"
    );

    let mut registered_tables = Vec::new();
    let mut table_types: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut table_formats: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let ctx = state.ctx.read().await;
    let df_ctx = ctx.datafusion_ctx();

    // ── Register S3 object store with DataFusion's runtime so ListingTable can read files ──
    {
        let s3_store = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(true);
        let s3_store = if let Some(ep) = ep {
            s3_store.with_endpoint(ep)
        } else {
            s3_store
        };
        let s3_store = s3_store.build()
            .map_err(|e| format!("S3 ObjectStore for DataFusion: {}", e))?;

        let url = url::Url::parse(&format!("s3://{}", bucket))
            .map_err(|e| format!("URL parse for bucket '{}': {}", bucket, e))?;
        df_ctx.runtime_env()
            .register_object_store(&url, Arc::new(s3_store));

        tracing::info!(bucket = %bucket, "Registered S3 object store with DataFusion runtime");
    }

    // Group tables by database → register as schema-qualified names (s3_{db}.{table})
    let mut db_tables: std::collections::HashMap<String, Vec<&crate::iceberg_s3::DiscoveredTable>> =
        std::collections::HashMap::new();
    for table_info in &scan_result.tables {
        db_tables.entry(table_info.database.clone()).or_default().push(table_info);
    }

    let mut errors = 0u32;
    for (db_name, tables) in &db_tables {
        let clean_db = db_name.trim_end_matches(".db");
        let schema_name = format!("s3_{}", clean_db);
        let schema_provider = match crate::providers::ensure_schema(df_ctx, &schema_name) {
            Ok(sp) => sp,
            Err(e) => {
                tracing::warn!(schema = %schema_name, error = %e, "Failed to create schema for S3 database");
                errors += tables.len() as u32;
                continue;
            }
        };

        for table_info in tables {
            let full_name = format!("{}.{}", schema_name, table_info.table_name);

            // Try to register as a ListingTable backed by real S3 data files.
            // For Iceberg/Delta: data lives under {table_path}/data/*.parquet
            // For raw Parquet: data is at {table_path}/*.parquet
            let data_path = {
                let base = &table_info.s3_location;
                match table_info.format {
                    crate::iceberg_s3::TableFormat::Iceberg | crate::iceberg_s3::TableFormat::Delta => {
                        format!("s3://{}/{}/data/", bucket, base)
                    }
                    _ => {
                        format!("s3://{}/{}/", bucket, base)
                    }
                }
            };

            // Attempt ListingTable registration (reads actual parquet files from S3)
            tracing::info!(
                schema = %schema_name, table = %table_info.table_name,
                full_name = %full_name, format = %table_info.format,
                data_path = %data_path, s3_location = %table_info.s3_location,
                "Attempting S3 table registration"
            );
            let registered = match try_register_listing_table(df_ctx, &table_info.table_name, &data_path, &schema_provider).await {
                Ok(()) => {
                    tracing::info!(
                        table = %full_name, format = %table_info.format, path = %data_path,
                        "✓ Registered S3 ListingTable (queryable)"
                    );
                    true
                }
                Err(e) => {
                    // Fallback: register MemTable with schema only (browseable but not queryable for data)
                    tracing::warn!(
                        table = %full_name, format = %table_info.format, path = %data_path,
                        error = %e, "✗ ListingTable failed, falling back to schema-only MemTable"
                    );
                    let arrow_fields: Vec<arrow::datatypes::Field> = table_info.columns.iter().map(|col| {
                        let dt = iceberg_type_to_arrow(&col.data_type);
                        arrow::datatypes::Field::new(&col.name, dt, col.nullable)
                    }).collect();
                    let arrow_fields = if arrow_fields.is_empty() {
                        vec![arrow::datatypes::Field::new("_placeholder", arrow::datatypes::DataType::Utf8, true)]
                    } else {
                        arrow_fields
                    };
                    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(arrow_fields));
                    match datafusion::datasource::MemTable::try_new(schema, vec![vec![]]) {
                        Ok(mem_table) => {
                            let provider: std::sync::Arc<dyn datafusion::datasource::TableProvider> =
                                std::sync::Arc::new(mem_table);
                            schema_provider.register_table(table_info.table_name.clone(), provider).is_ok()
                        }
                        Err(_) => false,
                    }
                }
            };

            if registered {
                if !table_info.table_type.is_empty() {
                    table_types.insert(full_name.clone(), table_info.table_type.clone());
                }
                table_formats.insert(full_name.clone(), table_info.format.to_string());
                registered_tables.push(full_name);
            } else {
                errors += 1;
            }
        }
    }

    // ── Post-registration summary ──
    {
        // Log every schema and its tables for diagnostics
        let catalog = df_ctx.catalog("datafusion").unwrap();
        let schema_names = catalog.schema_names();
        let s3_schemas: Vec<_> = schema_names.iter().filter(|s| s.starts_with("s3_")).collect();
        tracing::info!(
            total_registered = registered_tables.len(),
            total_errors = errors,
            s3_schemas = ?s3_schemas,
            "S3 registration summary"
        );
        for sn in &s3_schemas {
            if let Some(sp) = catalog.schema(sn) {
                let tables = sp.table_names();
                tracing::info!(
                    schema = %sn,
                    table_count = tables.len(),
                    tables = ?tables,
                    "Schema contents after registration"
                );
            }
        }
    }

    if errors > 0 {
        tracing::warn!(
            registered = registered_tables.len(), errors = errors,
            "S3 table registration: some tables had issues"
        );
    }

    Ok(S3DiscoveryResult {
        tables: registered_tables,
        table_types,
        table_formats,
        format_counts: scan_result.format_counts,
    })
}

async fn list_s3_configs(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let configs = state.s3_configs.read().await;
    let list: Vec<serde_json::Value> = configs
        .iter()
        .map(|c| {
            serde_json::json!({
                "name": c.name,
                "endpoint": c.endpoint,
                "access_key": c.access_key,
                "bucket": c.bucket,
                "region": c.region,
                "status": c.status,
                "created_at": c.created_at,
                "tables": c.tables,
                "table_types": c.table_types,
                "table_formats": c.table_formats,
                "sync_status": c.sync_status,
                "sync_error": c.sync_error,
                "scan_progress": c.scan_progress,
                "scan_detail": c.scan_detail,
                "scan_scanned": c.scan_scanned,
                "scan_total": c.scan_total,
                "scan_found": c.scan_found,
                "scan_elapsed_ms": c.scan_elapsed_ms,
                "format_counts": c.format_counts,
            })
        })
        .collect();
    Json(serde_json::json!({ "configs": list }))
}

async fn delete_s3_config(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut configs = state.s3_configs.write().await;
    let before = configs.len();
    configs.retain(|c| c.name != name);
    if configs.len() == before {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("S3 config '{}' not found", name),
            }),
        ));
    }
    Ok(Json(serde_json::json!({
        "status": "deleted",
        "name": name,
    })))
}

/// PUT /api/v1/storage/s3/{name} — update S3 config (e.g. rotated credentials) and re-discover.
async fn update_s3_config(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<AddS3ConfigRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut configs = state.s3_configs.write().await;
    let found = configs.iter_mut().find(|c| c.name == name);
    let cfg = match found {
        Some(c) => c,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("S3 config '{}' not found", name),
                }),
            ));
        }
    };

    // Update fields
    cfg.name = req.name.clone();
    cfg.endpoint = req.endpoint.clone();
    cfg.access_key = req.access_key.clone();
    cfg.secret_key = req.secret_key.clone();
    cfg.bucket = req.bucket.clone();
    cfg.region = req.region.clone();
    cfg.sync_status = "syncing".to_string();
    cfg.sync_error = None;
    cfg.status = "configured".to_string();
    drop(configs);

    tracing::info!(
        name = %req.name,
        bucket = %req.bucket,
        "S3 config updated — re-running Iceberg table discovery"
    );

    // Deregister old tables from DataFusion
    {
        let ctx = state.ctx.read().await;
        let df_ctx = ctx.datafusion_ctx();
        let old_configs = state.s3_configs.read().await;
        if let Some(cfg) = old_configs.iter().find(|c| c.name == name) {
            for table_name in &cfg.tables {
                if let Some(dot) = table_name.find('.') {
                    let schema = &table_name[..dot];
                    let tbl = &table_name[dot + 1..];
                    if let Ok(sp) = df_ctx.catalog("datafusion")
                        .ok_or("no catalog")
                        .and_then(|cat| cat.schema(schema).ok_or("no schema"))
                    {
                        let _ = sp.deregister_table(tbl);
                    }
                }
            }
        }
    }

    // Re-discover in background
    let bg_state = state.clone();
    let bg_name = req.name.clone();
    let bg_endpoint = req.endpoint.clone();
    let bg_access_key = req.access_key.clone();
    let bg_secret_key = req.secret_key;
    let bg_bucket = req.bucket.clone();
    let bg_region = req.region.clone();
    tokio::spawn(async move {
        let result = discover_s3_tables(
            &bg_state, &bg_name, &bg_endpoint, &bg_access_key, &bg_secret_key, &bg_bucket, &bg_region,
        ).await;
        match result {
            Ok(result) => {
                let count = result.tables.len();
                let mut configs = bg_state.s3_configs.write().await;
                if let Some(cfg) = configs.iter_mut().find(|c| c.name == bg_name) {
                    cfg.tables = result.tables;
                    cfg.table_types = result.table_types;
                    cfg.table_formats = result.table_formats;
                    cfg.format_counts = result.format_counts;
                    cfg.status = "ready".to_string();
                    cfg.sync_status = "ready".to_string();
                    cfg.sync_error = None;
                    cfg.scan_progress = Some("complete".to_string());
                    cfg.scan_detail = None;
                }
                tracing::info!(
                    name = %bg_name,
                    tables = count,
                    "S3 config update: re-discovery complete — {} tables found",
                    count
                );
            }
            Err(e) => {
                let mut configs = bg_state.s3_configs.write().await;
                if let Some(cfg) = configs.iter_mut().find(|c| c.name == bg_name) {
                    cfg.status = "error".to_string();
                    cfg.sync_status = "error".to_string();
                    cfg.sync_error = Some(e.clone());
                }
                tracing::error!(
                    name = %bg_name,
                    error = %e,
                    "S3 config update: re-discovery failed"
                );
            }
        }
    });

    Ok(Json(serde_json::json!({
        "status": "updated",
        "sync_status": "syncing",
        "name": req.name,
        "message": "S3 configuration updated. Re-discovering Iceberg tables."
    })))
}

// ── EXPLAIN Plan ──────────────────────────────────────────────────

/// POST /api/v1/sql/explain — returns logical and physical query plans.
async fn explain_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<ExplainResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;
    let df_ctx = ctx.datafusion_ctx();

    let logical_plan = match df_ctx.sql(&format!("EXPLAIN {}", req.sql)).await {
        Ok(df) => {
            let batches = df.collect().await.unwrap_or_default();
            batches_to_text(&batches)
        }
        Err(e) => e.to_string(),
    };

    let physical_plan = match df_ctx.sql(&format!("EXPLAIN VERBOSE {}", req.sql)).await {
        Ok(df) => {
            let batches = df.collect().await.unwrap_or_default();
            batches_to_text(&batches)
        }
        Err(e) => e.to_string(),
    };

    let nodes = parse_plan_nodes(&physical_plan);

    Ok(Json(ExplainResponse {
        sql: req.sql,
        logical_plan,
        physical_plan,
        nodes,
    }))
}

fn batches_to_text(batches: &[RecordBatch]) -> String {
    let mut lines = Vec::new();
    for batch in batches {
        for col_idx in 0..batch.num_columns() {
            let arr = batch.column(col_idx);
            if let Some(str_arr) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..str_arr.len() {
                    if !str_arr.is_null(i) { lines.push(str_arr.value(i).to_string()); }
                }
            } else if let Some(str_arr) = arr.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
                for i in 0..str_arr.len() {
                    if !str_arr.is_null(i) { lines.push(str_arr.value(i).to_string()); }
                }
            }
        }
    }
    lines.join("\n")
}

fn parse_plan_nodes(plan_text: &str) -> Vec<PlanNode> {
    let mut nodes = Vec::new();
    for (i, line) in plan_text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        let depth = line.len() - line.trim_start().len();
        let depth_level = depth / 2;
        let operator = trimmed.split(&[' ', ':', '(', ','][..]).next().unwrap_or(trimmed).to_string();
        let parent = if depth_level == 0 { None } else {
            nodes.iter().enumerate().rev().find_map(|(idx, n): (usize, &PlanNode)| {
                if n.depth < depth_level { Some(idx) } else { None }
            })
        };
        nodes.push(PlanNode { id: i, operator, detail: trimmed.to_string(), estimated_rows: None, parent, depth: depth_level });
    }
    nodes
}

// ── Quality Checks ────────────────────────────────────────────────

async fn quality_checks(
    State(state): State<Arc<AppState>>,
) -> Result<Json<QualityChecksResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;
    let tables = ctx.list_tables().await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;
    let rules = state.quality_rules.read().await;
    let mut checks = Vec::new();

    for table_name in &tables {
        let row_count = match ctx.sql(&format!("SELECT COUNT(*) AS cnt FROM \"{}\"", table_name)).await {
            Ok(batches) => batches.first().and_then(|b| b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().map(|a| a.value(0) as usize)).unwrap_or(0),
            Err(_) => 0,
        };

        let df_ctx = ctx.datafusion_ctx();
        let schema_info = df_ctx.table(table_name).await.ok().map(|t| t.schema().clone());
        let column_count = schema_info.as_ref().map(|s| s.fields().len()).unwrap_or(0);
        let mut null_percentages = Vec::new();
        let mut issues = Vec::new();

        if let Some(schema) = &schema_info {
            for field in schema.fields() {
                let col_name = field.name();
                let null_count = match ctx.sql(&format!(
                    "SELECT COUNT(*) AS cnt FROM \"{}\" WHERE \"{}\" IS NULL", table_name, col_name
                )).await {
                    Ok(batches) => batches.first().and_then(|b| b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().map(|a| a.value(0) as u64)).unwrap_or(0),
                    Err(_) => 0,
                };
                let null_pct = if row_count > 0 { (null_count as f64 / row_count as f64) * 100.0 } else { 0.0 };

                for rule in rules.iter().filter(|r| r.enabled && r.table_name == *table_name && r.rule_type == "null_threshold") {
                    if null_pct > rule.threshold {
                        issues.push(format!("Column '{}' has {:.1}% nulls (threshold: {:.1}%)", col_name, null_pct, rule.threshold));
                    }
                }
                if null_pct > 50.0 { issues.push(format!("Column '{}' has {:.1}% nulls", col_name, null_pct)); }

                null_percentages.push(ColumnNullInfo { name: col_name.clone(), data_type: format!("{}", field.data_type()), null_count, total_rows: row_count, null_pct });
            }
        }

        for rule in rules.iter().filter(|r| r.enabled && r.table_name == *table_name && r.rule_type == "min_row_count") {
            if (row_count as f64) < rule.threshold { issues.push(format!("Row count {} below threshold {}", row_count, rule.threshold as usize)); }
        }

        let health = if issues.is_empty() { "healthy" } else if issues.len() <= 2 { "warning" } else { "critical" }.to_string();
        checks.push(TableQualityCheck { table: table_name.clone(), row_count, column_count, null_percentages, health, issues, checked_at: Utc::now().to_rfc3339() });
    }

    let healthy_count = checks.iter().filter(|c| c.health == "healthy").count();
    let warning_count = checks.iter().filter(|c| c.health == "warning").count();
    let critical_count = checks.iter().filter(|c| c.health == "critical").count();
    let total_tables = checks.len();
    Ok(Json(QualityChecksResponse { checks, healthy_count, warning_count, critical_count, total_tables }))
}

async fn list_quality_rules(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let rules = state.quality_rules.read().await;
    Json(serde_json::json!({ "rules": *rules }))
}

async fn create_quality_rule(State(state): State<Arc<AppState>>, Json(mut rule): Json<QualityRule>) -> Json<serde_json::Value> {
    if rule.id.is_empty() { rule.id = Uuid::new_v4().to_string(); }
    if rule.created_at.is_empty() { rule.created_at = Utc::now().to_rfc3339(); }
    let mut rules = state.quality_rules.write().await;
    rules.push(rule.clone());
    Json(serde_json::json!({ "status": "created", "rule": rule }))
}

async fn delete_quality_rule(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut rules = state.quality_rules.write().await;
    let before = rules.len();
    rules.retain(|r| r.id != id);
    if rules.len() == before { return Err((StatusCode::NOT_FOUND, Json(ErrorResponse { error: format!("Rule '{}' not found", id) }))); }
    Ok(Json(serde_json::json!({ "status": "deleted" })))
}

// ── Scheduler DAG ─────────────────────────────────────────────────

async fn scheduler_dag(State(state): State<Arc<AppState>>) -> Json<SchedulerDagResponse> {
    let jobs = state.scheduled_jobs.read().await;
    let runs = state.job_runs.read().await;
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for job in jobs.iter() {
        let last_run_status = runs.iter()
            .filter(|r| r.job_id == job.id)
            .max_by_key(|r| &r.timestamp)
            .map(|r| r.status.clone())
            .unwrap_or_else(|| "pending".to_string());

        nodes.push(DagNode { id: job.id.clone(), name: job.name.clone(), job_type: job.job_type.clone(), status: last_run_status, cron: job.cron.clone(), enabled: job.enabled, last_run: job.last_run.map(|d| d.to_rfc3339()) });

        for tag in &job.tags {
            if let Some(dep_name) = tag.strip_prefix("after:") {
                if let Some(dep_job) = jobs.iter().find(|j| j.name == dep_name) {
                    edges.push(DagEdge { from: dep_job.id.clone(), to: job.id.clone(), label: Some("depends on".to_string()) });
                }
            }
        }
    }

    Json(SchedulerDagResponse { nodes, edges })
}

// ── dbt Integration ───────────────────────────────────────────────

async fn dbt_upload(State(state): State<Arc<AppState>>, Json(project): Json<DbtProject>) -> Json<serde_json::Value> {
    let model_count = project.models.len();
    let source_count = project.sources.len();
    let mut stored = state.dbt_project.write().await;
    *stored = Some(project);
    Json(serde_json::json!({ "status": "uploaded", "models": model_count, "sources": source_count }))
}

async fn dbt_project_info(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let project = state.dbt_project.read().await;
    match &*project {
        Some(p) => Ok(Json(serde_json::json!({ "name": p.name, "version": p.version, "model_count": p.models.len(), "source_count": p.sources.len(), "uploaded_at": p.uploaded_at }))),
        None => Err((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "No dbt project uploaded".into() }))),
    }
}

async fn list_dbt_models(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let project = state.dbt_project.read().await;
    match &*project {
        Some(p) => Ok(Json(serde_json::json!({ "models": p.models, "sources": p.sources }))),
        None => Err((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "No dbt project uploaded".into() }))),
    }
}

async fn run_dbt_model(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Result<Json<DbtRunResponse>, (StatusCode, Json<ErrorResponse>)> {
    let project = state.dbt_project.read().await;
    let project = project.as_ref().ok_or_else(|| (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "No dbt project uploaded".into() })))?;
    let model = project.models.iter().find(|m| m.name == name).ok_or_else(|| (StatusCode::NOT_FOUND, Json(ErrorResponse { error: format!("Model '{}' not found", name) })))?;

    let mut compiled = model.sql.clone();
    for dep in &model.depends_on {
        compiled = compiled.replace(&format!("ref('{}')", dep), &format!("\"{}\"", dep));
        compiled = compiled.replace(&format!("ref(\"{}\")", dep), &format!("\"{}\"", dep));
    }

    let start = Instant::now();
    let ctx = state.ctx.read().await;
    match ctx.sql(&compiled).await {
        Ok(batches) => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            if model.materialization == "table" || model.materialization == "incremental" {
                if let Some(batch) = batches.first() {
                    let schema = batch.schema();
                    if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(schema, vec![batches]) {
                        let _ = ctx.datafusion_ctx().register_table(&name, std::sync::Arc::new(mem_table));
                    }
                }
            }
            Ok(Json(DbtRunResponse { model: name, status: "success".into(), compiled_sql: compiled, row_count, duration_ms: start.elapsed().as_millis(), error: None }))
        }
        Err(e) => Ok(Json(DbtRunResponse { model: name, status: "error".into(), compiled_sql: compiled, row_count: 0, duration_ms: start.elapsed().as_millis(), error: Some(e.to_string()) })),
    }
}

async fn run_all_dbt_models(State(state): State<Arc<AppState>>) -> Result<Json<DbtRunAllResponse>, (StatusCode, Json<ErrorResponse>)> {
    let project = state.dbt_project.read().await;
    let project_ref = project.as_ref().ok_or_else(|| (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "No dbt project uploaded".into() })))?;
    let mut remaining: Vec<DbtModel> = project_ref.models.clone();
    drop(project);

    let mut results = Vec::new();
    let mut executed: std::collections::HashSet<String> = std::collections::HashSet::new();
    let all_start = Instant::now();

    for _ in 0..20 {
        if remaining.is_empty() { break; }
        let mut still_remaining = Vec::new();
        for model in remaining {
            if !model.depends_on.iter().all(|d| executed.contains(d)) { still_remaining.push(model); continue; }
            let mut compiled = model.sql.clone();
            for dep in &model.depends_on {
                compiled = compiled.replace(&format!("ref('{}')", dep), &format!("\"{}\"", dep));
                compiled = compiled.replace(&format!("ref(\"{}\")", dep), &format!("\"{}\"", dep));
            }
            let start = Instant::now();
            let ctx = state.ctx.read().await;
            match ctx.sql(&compiled).await {
                Ok(batches) => {
                    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                    if model.materialization == "table" || model.materialization == "incremental" {
                        if let Some(batch) = batches.first() {
                            let schema = batch.schema();
                            if let Ok(mt) = datafusion::datasource::MemTable::try_new(schema, vec![batches]) {
                                let _ = ctx.datafusion_ctx().register_table(&model.name, std::sync::Arc::new(mt));
                            }
                        }
                    }
                    executed.insert(model.name.clone());
                    results.push(DbtRunResponse { model: model.name, status: "success".into(), compiled_sql: compiled, row_count, duration_ms: start.elapsed().as_millis(), error: None });
                }
                Err(e) => {
                    executed.insert(model.name.clone());
                    results.push(DbtRunResponse { model: model.name, status: "error".into(), compiled_sql: compiled, row_count: 0, duration_ms: start.elapsed().as_millis(), error: Some(e.to_string()) });
                }
            }
        }
        remaining = still_remaining;
    }

    let success_count = results.iter().filter(|r| r.status == "success").count();
    let failure_count = results.iter().filter(|r| r.status == "error").count();
    Ok(Json(DbtRunAllResponse { results, total_duration_ms: all_start.elapsed().as_millis(), success_count, failure_count }))
}

// ── Cluster Topology ────────────────────────────────────────────────

/// GET /api/v1/cluster/topology — Get the full cluster topology overview.
async fn cluster_topology(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let ctx = state.ctx.read().await;
    let config = ctx.config();
    let node_role = match config.cluster.node_role {
        rustlake_core::config::NodeRole::Standalone => "standalone",
        rustlake_core::config::NodeRole::Coordinator => "coordinator",
        rustlake_core::config::NodeRole::Worker => "worker",
    };
    let discovery = match config.cluster.discovery {
        rustlake_core::config::DiscoveryMethod::Static => "static",
        rustlake_core::config::DiscoveryMethod::Register => "register",
        rustlake_core::config::DiscoveryMethod::Kubernetes => "kubernetes",
    };
    let flight_enabled = config.flight.enabled;
    let flight_host = config.flight.host.clone();
    let flight_port = config.flight.port;
    let heartbeat_interval = config.cluster.heartbeat_interval_secs;
    let heartbeat_timeout = config.cluster.heartbeat_timeout_secs;
    let max_partitions = config.cluster.max_partitions_per_worker;
    let shuffle_buffer = config.cluster.shuffle_buffer_size;
    let coordinator_addr = config.cluster.coordinator_addr.clone();
    drop(ctx);

    let worker_count = if let Some(ref coord) = state.coordinator {
        coord.active_worker_count().await
    } else {
        0
    };

    let workers = if let Some(ref coord) = state.coordinator {
        let handles = coord.list_workers().await;
        handles
            .into_iter()
            .map(|w| serde_json::json!({
                "id": w.id,
                "endpoint": w.endpoint,
                "label": w.label,
                "cpu_cores": w.cpu_cores,
                "memory_bytes": w.memory_bytes,
                "status": format!("{:?}", w.status).to_lowercase(),
                "active_partitions": w.active_partitions,
                "queries_executed": w.queries_executed,
            }))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let flight_running = state.flight_metrics.as_ref()
        .map(|fm| fm.running.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(false);

    Json(serde_json::json!({
        "node_role": node_role,
        "discovery_method": discovery,
        "flight_enabled": flight_enabled,
        "flight_running": flight_running,
        "flight_host": flight_host,
        "flight_port": flight_port,
        "coordinator_addr": coordinator_addr,
        "heartbeat_interval_secs": heartbeat_interval,
        "heartbeat_timeout_secs": heartbeat_timeout,
        "max_partitions_per_worker": max_partitions,
        "shuffle_buffer_size": shuffle_buffer,
        "active_workers": worker_count,
        "workers": workers,
    }))
}

/// GET /api/v1/cluster/workers — List registered workers (coordinator only).
async fn list_workers(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let workers = if let Some(ref coord) = state.coordinator {
        let handles = coord.list_workers().await;
        handles
            .into_iter()
            .map(|w| serde_json::json!({
                "id": w.id,
                "endpoint": w.endpoint,
                "label": w.label,
                "cpu_cores": w.cpu_cores,
                "memory_bytes": w.memory_bytes,
                "status": format!("{:?}", w.status).to_lowercase(),
                "active_partitions": w.active_partitions,
                "queries_executed": w.queries_executed,
            }))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    Json(serde_json::json!({
        "workers": workers,
        "count": workers.len(),
    }))
}

// ── System Metrics (real-time OS metrics) ────────────────────────

/// GET /api/v1/system/metrics — returns real-time CPU, memory, disk usage.
async fn system_metrics(State(state): State<Arc<AppState>>) -> Json<SystemMetricsResponse> {
    let total_queries = state.query_count.load(Ordering::Relaxed);
    let uptime = state.start_time.elapsed().as_secs();

    // CPU usage (approximate via load average on macOS/Linux)
    let (load_1m, load_5m, cpu_percent) = get_load_average();

    // Memory usage
    let (mem_used, mem_total) = get_memory_usage();
    let mem_percent = if mem_total > 0 {
        (mem_used as f64 / mem_total as f64) * 100.0
    } else {
        0.0
    };

    // Disk usage
    let (disk_used, disk_total) = get_disk_usage();
    let disk_percent = if disk_total > 0 {
        (disk_used as f64 / disk_total as f64) * 100.0
    } else {
        0.0
    };

    // Approximate QPS (total / uptime)
    let qps = if uptime > 0 {
        total_queries as f64 / uptime as f64
    } else {
        0.0
    };

    Json(SystemMetricsResponse {
        cpu_usage_percent: cpu_percent,
        memory_used_bytes: mem_used,
        memory_total_bytes: mem_total,
        memory_usage_percent: (mem_percent * 10.0).round() / 10.0,
        disk_used_bytes: disk_used,
        disk_total_bytes: disk_total,
        disk_usage_percent: (disk_percent * 10.0).round() / 10.0,
        load_avg_1m: load_1m,
        load_avg_5m: load_5m,
        active_queries: 0, // No in-flight tracking yet
        total_queries,
        queries_per_second: (qps * 100.0).round() / 100.0,
        uptime_seconds: uptime,
    })
}

fn get_load_average() -> (f64, f64, f64) {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        let output = std::process::Command::new("uptime")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .unwrap_or_default();
        // Parse "load averages: 2.50, 2.30, 2.10" or "load average: 2.50, 2.30, 2.10"
        if let Some(idx) = output.find("load average") {
            let rest = &output[idx..];
            let nums: Vec<f64> = rest
                .split(|c: char| !c.is_ascii_digit() && c != '.')
                .filter_map(|s| s.parse::<f64>().ok())
                .collect();
            let load_1 = nums.first().copied().unwrap_or(0.0);
            let load_5 = nums.get(1).copied().unwrap_or(0.0);
            let cores = std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1);
            let cpu_pct = (load_1 / cores as f64 * 100.0).min(100.0);
            return (load_1, load_5, (cpu_pct * 10.0).round() / 10.0);
        }
        (0.0, 0.0, 0.0)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        (0.0, 0.0, 0.0)
    }
}

fn get_memory_usage() -> (u64, u64) {
    #[cfg(target_os = "macos")]
    {
        let total = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);

        // vm_stat gives page-level memory info
        let vm_out = std::process::Command::new("vm_stat")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .unwrap_or_default();

        let page_size: u64 = 16384; // macOS Apple Silicon default
        let mut active: u64 = 0;
        let mut wired: u64 = 0;
        let mut compressed: u64 = 0;

        for line in vm_out.lines() {
            let val = line
                .split(':')
                .nth(1)
                .map(|s| s.trim().trim_end_matches('.'))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            if line.contains("Pages active") {
                active = val;
            } else if line.contains("Pages wired") {
                wired = val;
            } else if line.contains("Pages occupied by compressor") {
                compressed = val;
            }
        }

        let used = (active + wired + compressed) * page_size;
        (used, total)
    }
    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mut total: u64 = 0;
        let mut available: u64 = 0;
        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                total = line.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) * 1024;
            } else if line.starts_with("MemAvailable:") {
                available = line.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) * 1024;
            }
        }
        (total.saturating_sub(available), total)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        (0, 0)
    }
}

fn get_disk_usage() -> (u64, u64) {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        let output = std::process::Command::new("df")
            .args(["-k", "/"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .unwrap_or_default();

        // Skip header line, parse: Filesystem 1K-blocks Used Available ...
        if let Some(line) = output.lines().nth(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let total = parts[1].parse::<u64>().unwrap_or(0) * 1024;
                let used = parts[2].parse::<u64>().unwrap_or(0) * 1024;
                return (used, total);
            }
        }
        (0, 0)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        (0, 0)
    }
}

// ── Query Cost Estimation ────────────────────────────────────────

/// POST /api/v1/sql/estimate — estimate query cost before execution.
async fn estimate_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<QueryEstimateResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;
    let df_ctx = ctx.datafusion_ctx();

    // Parse the query to extract table references
    let tables_referenced = extract_table_references(&req.sql);

    // Get table sizes for estimation
    let mut total_estimated_rows: u64 = 0;
    let mut total_estimated_bytes: u64 = 0;
    let mut notes = Vec::new();

    for table_name in &tables_referenced {
        // Try to get row count via COUNT(*)
        match df_ctx.sql(&format!("SELECT COUNT(*) as cnt FROM {}", table_name)).await {
            Ok(df) => {
                if let Ok(batches) = df.collect().await {
                    for batch in &batches {
                        if batch.num_rows() > 0 {
                            if let Some(arr) = batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>() {
                                let rows = arr.value(0) as u64;
                                total_estimated_rows += rows;
                                // Rough estimate: ~100 bytes per row average
                                total_estimated_bytes += rows * 100;
                                notes.push(format!("{}: ~{} rows", table_name, rows));
                            }
                        }
                    }
                }
            }
            Err(_) => {
                notes.push(format!("{}: unable to estimate", table_name));
            }
        }
    }

    // Get partitions from EXPLAIN
    let partitions = match df_ctx.sql(&format!("EXPLAIN {}", req.sql)).await {
        Ok(df) => {
            let text = df.collect().await
                .map(|batches| batches_to_text(&batches))
                .unwrap_or_default();
            // Count RepartitionExec or similar partition nodes
            text.matches("partition").count().max(1)
        }
        Err(_) => 1,
    };

    let cost_rating = if total_estimated_rows < 10_000 {
        "low"
    } else if total_estimated_rows < 1_000_000 {
        "medium"
    } else {
        "high"
    };

    let scan_size = format_bytes(total_estimated_bytes);

    Ok(Json(QueryEstimateResponse {
        sql: req.sql,
        estimated_rows: total_estimated_rows,
        estimated_bytes: total_estimated_bytes,
        estimated_scan_size: scan_size,
        partitions,
        cost_rating: cost_rating.to_string(),
        tables_referenced,
        notes,
    }))
}

fn extract_table_references(sql: &str) -> Vec<String> {
    let _upper = sql.to_uppercase();
    let mut tables = Vec::new();
    let words: Vec<&str> = sql.split_whitespace().collect();

    for (i, word) in words.iter().enumerate() {
        let upper_word = word.to_uppercase();
        if (upper_word == "FROM" || upper_word == "JOIN") && i + 1 < words.len() {
            let table = words[i + 1]
                .trim_matches(|c: char| c == '(' || c == ')' || c == ',' || c == ';')
                .to_string();
            if !table.is_empty()
                && !table.starts_with('(')
                && table.to_uppercase() != "SELECT"
                && table.to_uppercase() != "WHERE"
                && !table.starts_with('\'')
            {
                tables.push(table);
            }
        }
    }

    tables.sort();
    tables.dedup();
    tables
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

// ── Connection Test ──────────────────────────────────────────────

/// POST /api/v1/connections/test — test database connectivity.
async fn test_connection(
    Json(req): Json<ConnectionTestRequest>,
) -> Json<ConnectionTestResponse> {
    let start = Instant::now();
    let mut checks: Vec<ConnectionCheck> = Vec::new();

    // ── Check 1: Config validation ──
    let default_port = default_port_for(&req.conn_type);
    let port = req.port.unwrap_or(default_port);
    let required_fields = required_fields_for(&req.conn_type);

    let mut config_ok = true;
    if req.host.is_empty() {
        checks.push(ConnectionCheck { name: "host".into(), passed: false, detail: "Host is required".into() });
        config_ok = false;
    } else {
        checks.push(ConnectionCheck { name: "host".into(), passed: true, detail: format!("{}", req.host) });
    }
    if port == 0 {
        checks.push(ConnectionCheck { name: "port".into(), passed: false, detail: "Invalid port".into() });
        config_ok = false;
    } else {
        checks.push(ConnectionCheck { name: "port".into(), passed: true, detail: format!("{}", port) });
    }
    for field in &required_fields {
        let present = match field.as_str() {
            "database" => req.database.as_ref().map_or(false, |s| !s.is_empty()),
            "username" => req.username.as_ref().map_or(false, |s| !s.is_empty()),
            _ => true,
        };
        checks.push(ConnectionCheck {
            name: field.clone(),
            passed: present,
            detail: if present { "provided".into() } else { format!("{} is required for {}", field, req.conn_type) },
        });
        if !present { config_ok = false; }
    }

    if !config_ok {
        return Json(ConnectionTestResponse {
            success: false,
            message: "Configuration validation failed".into(),
            latency_ms: Some(start.elapsed().as_millis()),
            server_version: None,
            tables_found: None,
            validation_level: "config".into(),
            checks,
        });
    }

    // ── Check 2: DNS resolution ──
    // Strip scheme from host if user passed a full URL (e.g. https://trino.example.com)
    let clean_host = req.host
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    let addr_str = format!("{}:{}", clean_host, port);
    let dns_ok = match tokio::net::lookup_host(&addr_str).await {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                checks.push(ConnectionCheck { name: "dns".into(), passed: true, detail: format!("Resolved to {}", addr.ip()) });
                true
            } else {
                checks.push(ConnectionCheck { name: "dns".into(), passed: false, detail: "No addresses returned".into() });
                false
            }
        }
        Err(e) => {
            checks.push(ConnectionCheck { name: "dns".into(), passed: false, detail: format!("DNS lookup failed: {}", e) });
            false
        }
    };

    if !dns_ok {
        return Json(ConnectionTestResponse {
            success: false,
            message: format!("Cannot resolve host '{}'", clean_host),
            latency_ms: Some(start.elapsed().as_millis()),
            server_version: None,
            tables_found: None,
            validation_level: "dns".into(),
            checks,
        });
    }

    // ── Check 3: TCP reachability ──
    let tcp_ok = match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(&addr_str),
    ).await {
        Ok(Ok(_)) => {
            checks.push(ConnectionCheck { name: "tcp".into(), passed: true, detail: format!("Port {} is open", port) });
            true
        }
        Ok(Err(e)) => {
            checks.push(ConnectionCheck { name: "tcp".into(), passed: false, detail: format!("Port {} refused: {}", port, e) });
            false
        }
        Err(_) => {
            checks.push(ConnectionCheck { name: "tcp".into(), passed: false, detail: format!("Port {} timed out (3s)", port) });
            false
        }
    };

    if !tcp_ok {
        return Json(ConnectionTestResponse {
            success: false,
            message: format!("Cannot reach {}:{}", clean_host, port),
            latency_ms: Some(start.elapsed().as_millis()),
            server_version: None,
            tables_found: None,
            validation_level: "tcp".into(),
            checks,
        });
    }

    // ── Check 4: Protocol handshake (only for types we have client libraries) ──
    match req.conn_type.as_str() {
        // All Postgres wire protocol compatible databases
        "postgres" | "postgresql" | "cockroachdb" | "yugabytedb" | "timescaledb"
        | "greenplum" | "cdc_postgres" | "redshift" => {
            let db = req.database.as_deref().unwrap_or("postgres");
            let user = req.username.as_deref().unwrap_or("postgres");
            let pass = req.password.as_deref().unwrap_or("");
            let conn_str = format!("host={} port={} dbname={} user={} password={}", req.host, port, db, user, pass);

            match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move { let _ = connection.await; });

                    let version = client.simple_query("SELECT version()").await.ok().and_then(|rows| {
                        rows.into_iter().find_map(|msg| {
                            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg { row.get(0).map(|v| v.to_string()) } else { None }
                        })
                    });
                    let table_count = client.simple_query("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'").await.ok().and_then(|rows| {
                        rows.into_iter().find_map(|msg| {
                            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg { row.get(0).and_then(|v| v.parse::<usize>().ok()) } else { None }
                        })
                    });

                    checks.push(ConnectionCheck { name: "protocol".into(), passed: true, detail: format!("{} (Postgres wire protocol) handshake OK", req.conn_type) });
                    checks.push(ConnectionCheck { name: "auth".into(), passed: true, detail: format!("Authenticated as {}", user) });

                    Json(ConnectionTestResponse {
                        success: true,
                        message: "Full connection verified".into(),
                        latency_ms: Some(start.elapsed().as_millis()),
                        server_version: version,
                        tables_found: table_count,
                        validation_level: "full".into(),
                        checks,
                    })
                }
                Err(e) => {
                    checks.push(ConnectionCheck { name: "protocol".into(), passed: false, detail: format!("Handshake failed: {}", e) });
                    Json(ConnectionTestResponse {
                        success: false,
                        message: format!("{} auth failed: {}", req.conn_type, e),
                        latency_ms: Some(start.elapsed().as_millis()),
                        server_version: None,
                        tables_found: None,
                        validation_level: "tcp".into(),
                        checks,
                    })
                }
            }
        }
        // Trino / Presto — HTTP REST API protocol test
        "trino" | "presto" => {
            let user = req.username.as_deref().unwrap_or("rustlake");
            let pass = req.password.as_deref().unwrap_or("");
            let base_url = trino_base_url(&req.host, port);

            // Check 4a: HTTP /v1/info — server status
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap_or_default();

            let info_url = format!("{}/v1/info", base_url);
            match client.get(&info_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let body: serde_json::Value = resp.json().await.unwrap_or_default();
                    let version = body.get("nodeVersion")
                        .and_then(|v| v.get("version"))
                        .and_then(|v| v.as_str())
                        .map(|s| format!("Trino {}", s));
                    let state_str = body.get("state").and_then(|v| v.as_str()).unwrap_or("unknown");

                    checks.push(ConnectionCheck {
                        name: "protocol".into(),
                        passed: true,
                        detail: format!("HTTP REST API OK — server state: {}", state_str),
                    });

                    // Check 4b: Run a test query with auth to verify credentials
                    let catalog = req.database.as_deref().unwrap_or("system");
                    let mut query_req = client.post(&format!("{}/v1/statement", base_url))
                        .header("X-Trino-User", user)
                        .header("X-Trino-Catalog", catalog)
                        .header("X-Trino-Schema", "information_schema")
                        .body("SELECT count(*) FROM information_schema.tables");
                    if !pass.is_empty() {
                        query_req = query_req.basic_auth(user, Some(pass));
                    }

                    match query_req.send().await {
                        Ok(resp) if resp.status().is_success() => {
                            let query_body: serde_json::Value = resp.json().await.unwrap_or_default();

                            // Follow nextUri to get actual results
                            let mut next_uri = query_body.get("nextUri").and_then(|v| v.as_str()).map(|s| s.to_string());
                            let mut table_count: Option<usize> = None;
                            for _ in 0..20 {
                                let Some(uri) = next_uri.take() else { break };
                                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                                let mut poll_req = client.get(&uri).header("X-Trino-User", user);
                                if !pass.is_empty() {
                                    poll_req = poll_req.basic_auth(user, Some(pass));
                                }
                                if let Ok(poll_resp) = poll_req.send().await {
                                    if let Ok(poll_body) = poll_resp.json::<serde_json::Value>().await {
                                        if let Some(data) = poll_body.get("data").and_then(|d| d.as_array()) {
                                            table_count = data.first()
                                                .and_then(|row| row.as_array())
                                                .and_then(|cols| cols.first())
                                                .and_then(|v| v.as_i64())
                                                .map(|n| n as usize);
                                        }
                                        next_uri = poll_body.get("nextUri").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        let state_val = poll_body.get("stats").and_then(|s| s.get("state")).and_then(|v| v.as_str()).unwrap_or("");
                                        if state_val == "FINISHED" || state_val == "FAILED" { break; }
                                    }
                                }
                            }

                            checks.push(ConnectionCheck { name: "auth".into(), passed: true, detail: format!("Authenticated as {}", user) });

                            Json(ConnectionTestResponse {
                                success: true,
                                message: "Full connection verified".into(),
                                latency_ms: Some(start.elapsed().as_millis()),
                                server_version: version,
                                tables_found: table_count,
                                validation_level: "full".into(),
                                checks,
                            })
                        }
                        Ok(resp) => {
                            let status = resp.status();
                            let body_text = resp.text().await.unwrap_or_default();
                            checks.push(ConnectionCheck {
                                name: "auth".into(),
                                passed: false,
                                detail: format!("Query failed (HTTP {}): {}", status, &body_text[..body_text.len().min(200)]),
                            });
                            Json(ConnectionTestResponse {
                                success: false,
                                message: format!("Trino authentication failed (HTTP {})", status),
                                latency_ms: Some(start.elapsed().as_millis()),
                                server_version: version,
                                tables_found: None,
                                validation_level: "tcp".into(),
                                checks,
                            })
                        }
                        Err(e) => {
                            checks.push(ConnectionCheck {
                                name: "auth".into(),
                                passed: false,
                                detail: format!("Query request failed: {}", e),
                            });
                            Json(ConnectionTestResponse {
                                success: false,
                                message: format!("Trino query test failed: {}", e),
                                latency_ms: Some(start.elapsed().as_millis()),
                                server_version: version,
                                tables_found: None,
                                validation_level: "tcp".into(),
                                checks,
                            })
                        }
                    }
                }
                Ok(resp) => {
                    checks.push(ConnectionCheck {
                        name: "protocol".into(),
                        passed: false,
                        detail: format!("HTTP {} from /v1/info", resp.status()),
                    });
                    Json(ConnectionTestResponse {
                        success: false,
                        message: format!("Trino REST API returned HTTP {}", resp.status()),
                        latency_ms: Some(start.elapsed().as_millis()),
                        server_version: None,
                        tables_found: None,
                        validation_level: "tcp".into(),
                        checks,
                    })
                }
                Err(e) => {
                    checks.push(ConnectionCheck {
                        name: "protocol".into(),
                        passed: false,
                        detail: format!("HTTP request failed: {}", e),
                    });
                    Json(ConnectionTestResponse {
                        success: false,
                        message: format!("Cannot reach Trino REST API: {}", e),
                        latency_ms: Some(start.elapsed().as_millis()),
                        server_version: None,
                        tables_found: None,
                        validation_level: "tcp".into(),
                        checks,
                    })
                }
            }
        }
        _ => {
            // No client library — TCP reachability is the best we can do
            checks.push(ConnectionCheck {
                name: "protocol".into(),
                passed: false,
                detail: format!("No {} client library — TCP reachability confirmed only", req.conn_type),
            });
            Json(ConnectionTestResponse {
                success: true,
                message: format!("Host reachable (TCP). Full {} protocol test not yet available.", req.conn_type),
                latency_ms: Some(start.elapsed().as_millis()),
                server_version: None,
                tables_found: None,
                validation_level: "tcp".into(),
                checks,
            })
        }
    }
}

/// Default port for common connector types.
fn default_port_for(conn_type: &str) -> u16 {
    match conn_type {
        "postgres" | "postgresql" | "cdc_postgres" => 5432,
        "mysql" | "mariadb" => 3306,
        "mongodb" | "cdc_mongodb" => 27017,
        "redis" => 6379,
        "clickhouse" => 8123,
        "trino" | "presto" => 8080,
        "kafka" => 9092,
        "elasticsearch" | "opensearch" => 9200,
        "cassandra" | "scylladb" => 9042,
        "minio" => 9000,
        "neo4j" => 7687,
        "cockroachdb" => 26257,
        "mssql" | "sqlserver" => 1433,
        "oracle" => 1521,
        "snowflake" => 443,
        "bigquery" | "redshift" | "databricks" | "s3" | "gcs" | "azure_blob"
        | "salesforce" | "hubspot" | "stripe" | "shopify" | "github"
        | "rest_api" | "graphql" => 443,
        _ => 443,
    }
}

/// Required fields beyond host/port for a given connector type.
fn required_fields_for(conn_type: &str) -> Vec<String> {
    match conn_type {
        "postgres" | "postgresql" | "mysql" | "mariadb" | "mssql" | "sqlserver"
        | "oracle" | "cockroachdb" | "cdc_postgres" => vec!["database".into(), "username".into()],
        "mongodb" | "cdc_mongodb" => vec!["database".into()],
        "trino" | "presto" => vec!["username".into()],
        "clickhouse" | "neo4j" | "cassandra" | "scylladb" => vec!["username".into()],
        _ => vec![],
    }
}

// ── Trino helpers ─────────────────────────────────────────────────

/// Build the base URL for a Trino connection.
///
/// Handles:
/// - Full URLs: `https://trino.example.com` → used as-is
/// - Port 443 → defaults to HTTPS
/// - Everything else → HTTP with explicit port
fn trino_base_url(host: &str, port: u16) -> String {
    // If the host already includes a scheme, use it directly
    if host.starts_with("http://") || host.starts_with("https://") {
        host.trim_end_matches('/').to_string()
    } else if port == 443 {
        format!("https://{}", host)
    } else {
        format!("http://{}:{}", host, port)
    }
}

/// Map Trino SQL types to Arrow DataType.
#[allow(dead_code)]
fn trino_type_to_arrow(trino_type: &str) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    let t = trino_type.to_lowercase();
    let t = t.trim();
    if t.starts_with("varchar") || t.starts_with("char") || t == "json" || t == "uuid" || t == "ipaddress" {
        DataType::Utf8
    } else if t == "boolean" {
        DataType::Boolean
    } else if t == "tinyint" {
        DataType::Int8
    } else if t == "smallint" {
        DataType::Int16
    } else if t == "integer" || t == "int" {
        DataType::Int32
    } else if t == "bigint" {
        DataType::Int64
    } else if t == "real" || t == "float" {
        DataType::Float32
    } else if t == "double" {
        DataType::Float64
    } else if t.starts_with("decimal") || t.starts_with("numeric") {
        DataType::Utf8 // store as string to avoid precision loss
    } else if t == "date" {
        DataType::Utf8
    } else if t.starts_with("timestamp") {
        DataType::Utf8
    } else if t.starts_with("time") {
        DataType::Utf8
    } else if t == "varbinary" {
        DataType::Binary
    } else {
        DataType::Utf8 // fallback
    }
}

/// Build an Arrow RecordBatch from Trino JSON row data.
#[allow(dead_code)]
fn build_arrow_batch_from_trino_rows(
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
    rows: &[Vec<serde_json::Value>],
) -> Option<arrow::record_batch::RecordBatch> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if rows.is_empty() { return None; }

    let mut columns: Vec<std::sync::Arc<dyn arrow::array::Array>> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let arr: std::sync::Arc<dyn arrow::array::Array> = match field.data_type() {
            DataType::Boolean => {
                let vals: Vec<Option<bool>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| v.as_bool())
                }).collect();
                std::sync::Arc::new(BooleanArray::from(vals))
            }
            DataType::Int8 => {
                let vals: Vec<Option<i8>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| v.as_i64()).map(|n| n as i8)
                }).collect();
                std::sync::Arc::new(Int8Array::from(vals))
            }
            DataType::Int16 => {
                let vals: Vec<Option<i16>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| v.as_i64()).map(|n| n as i16)
                }).collect();
                std::sync::Arc::new(Int16Array::from(vals))
            }
            DataType::Int32 => {
                let vals: Vec<Option<i32>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| v.as_i64()).map(|n| n as i32)
                }).collect();
                std::sync::Arc::new(Int32Array::from(vals))
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| v.as_i64())
                }).collect();
                std::sync::Arc::new(Int64Array::from(vals))
            }
            DataType::Float32 => {
                let vals: Vec<Option<f32>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| v.as_f64()).map(|n| n as f32)
                }).collect();
                std::sync::Arc::new(Float32Array::from(vals))
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| v.as_f64())
                }).collect();
                std::sync::Arc::new(Float64Array::from(vals))
            }
            _ => {
                // Utf8 fallback — stringify everything
                let vals: Vec<Option<String>> = rows.iter().map(|row| {
                    row.get(i).and_then(|v| {
                        if v.is_null() { None }
                        else if let Some(s) = v.as_str() { Some(s.to_string()) }
                        else { Some(v.to_string()) }
                    })
                }).collect();
                std::sync::Arc::new(StringArray::from(vals))
            }
        };
        columns.push(arr);
    }

    arrow::record_batch::RecordBatch::try_new(schema.clone(), columns).ok()
}

/// Sync specific tables from DataFusion into DuckDB and Polars engines.
/// Used after Trino table registration to make them available in all engines.
async fn sync_trino_tables_to_engines(state: &Arc<AppState>, table_names: &[String]) {
    let ctx = state.ctx.read().await;
    let df_ctx = ctx.datafusion_ctx();
    let mut sync_data = Vec::new();

    for table_name in table_names {
        // Schema-qualified names like trino_postgresql.public_customers must NOT be quoted
        let sql = format!("SELECT * FROM {} LIMIT 10000", table_name);
        match df_ctx.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) if !batches.is_empty() => {
                    // DuckDB needs flat table names (no dots) — use underscore-joined
                    let flat_name = table_name.replace('.', "_");
                    sync_data.push((flat_name, batches));
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(table = %table_name, error = %e, "Engine sync: skip table");
                }
            },
            Err(e) => {
                tracing::debug!(table = %table_name, error = %e, "Engine sync: skip table");
            }
        }
    }
    drop(ctx);

    if sync_data.is_empty() {
        return;
    }

    let count = sync_data.len();

    #[cfg(feature = "duckdb")]
    if let Some(ref duckdb_engine) = state.duckdb_engine {
        // Clone the data for DuckDB
        let dk_data: Vec<(String, Vec<RecordBatch>)> = sync_data.iter()
            .map(|(name, batches)| (name.clone(), batches.clone()))
            .collect();
        match duckdb_engine.sync_tables(dk_data).await {
            Ok(synced) => tracing::info!(synced, total = count, "DuckDB: Trino tables synced"),
            Err(e) => tracing::warn!(error = %e, "DuckDB: Trino table sync failed"),
        }
    }

    #[cfg(feature = "polars")]
    if let Some(ref polars_engine) = state.polars_engine {
        match polars_engine.sync_tables(sync_data).await {
            Ok(synced) => tracing::info!(synced, total = count, "Polars: Trino tables synced"),
            Err(e) => tracing::warn!(error = %e, "Polars: Trino table sync failed"),
        }
    }
}

/// POST /api/v1/engines/sync — re-sync all DataFusion tables to DuckDB + Polars.
async fn engines_sync(
    State(state): State<Arc<AppState>>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let ctx = state.ctx.read().await;
    let tables = ctx.list_tables().await.unwrap_or_default();
    drop(ctx);
    sync_trino_tables_to_engines(&state, &tables).await;
    Ok(Json(serde_json::json!({ "status": "ok", "tables_synced": tables.len() })))
}

// ── Trino cached browse endpoints ─────────────────────────────────

/// Lazy-initialize a TrinoConnection from the connections list if not already in trino_connections.
/// This handles the case where the server restarted and the connection was loaded from disk.
async fn get_or_init_trino(
    state: &Arc<AppState>,
    conn_id: &str,
) -> std::result::Result<std::sync::Arc<crate::trino_client::TrinoConnection>, (StatusCode, Json<ErrorResponse>)> {
    // Check if already initialized
    {
        let conns = state.trino_connections.read().await;
        if let Some(c) = conns.get(conn_id) {
            return Ok(c.clone());
        }
    }
    // Not initialized — find in connections list and create
    let connections = state.connections.read().await;
    let entry = connections.iter().find(|c| c.id == conn_id && c.conn_type == "trino")
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, Json(ErrorResponse { error: format!("Trino connection '{}' not found", conn_id) })))?;
    drop(connections);

    // Retrieve stored password
    let password = state.connection_passwords.read().await.get(conn_id).cloned();

    #[cfg(feature = "duckdb")]
    {
        let cache = state.trino_cache.clone()
            .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "Trino cache not initialized".into() })))?;
        let base_url = trino_base_url(&entry.host, entry.port);
        let user = if entry.username.is_empty() { "rustlake".to_string() } else { entry.username.clone() };
        let pass = password.unwrap_or_default();
        let rest = crate::trino_client::TrinoRestClient::new(base_url.clone(), user.clone(), pass.clone());
        let conn = crate::trino_client::TrinoConnection {
            id: conn_id.to_string(),
            name: entry.name.clone(),
            rest,
            default_catalog: if entry.database.is_empty() { "postgresql".to_string() } else { entry.database.clone() },
            cache,
        };
        // Try to refresh cache if empty
        let _ = conn.refresh_cache().await;
        let rest_arc = std::sync::Arc::new(
            crate::trino_client::TrinoRestClient::new(base_url, user, pass)
        );
        let arc = std::sync::Arc::new(conn);
        state.trino_connections.write().await.insert(conn_id.to_string(), arc.clone());

        // Register Trino tables as DataFusion providers (lazy re-registration on restart)
        let ctx = state.ctx.read().await;
        let df_ctx = ctx.datafusion_ctx();
        let trino_registered = state.provider_registry
            .register_trino(conn_id, &arc, rest_arc, df_ctx)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to register Trino table providers on lazy init");
                vec![]
            });
        drop(ctx);
        if !trino_registered.is_empty() {
            tracing::info!(count = trino_registered.len(), "Trino tables lazily registered as DataFusion providers");
            // Sync newly registered Trino tables to DuckDB and Polars engines
            sync_trino_tables_to_engines(state, &trino_registered).await;
        }

        Ok(arc)
    }
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = entry;
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "DuckDB feature required for Trino".into() })))
    }
}

/// GET /api/v1/trino/:conn_id/browse — return cached catalog tree (instant, from DuckDB).
async fn trino_browse(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let conn = get_or_init_trino(&state, &conn_id).await?;

    #[cfg(feature = "duckdb")]
    {
        let tree = conn.browse().await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;
        Ok(Json(serde_json::json!(tree)))
    }
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = conn;
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "DuckDB cache required for Trino browse".into() })))
    }
}

/// Query params for Trino column/preview lookups.
#[derive(Deserialize)]
struct TrinoTableQuery {
    catalog: String,
    schema: String,
    table: String,
}

/// GET /api/v1/trino/:conn_id/columns?catalog=X&schema=Y&table=Z — cached column info.
async fn trino_columns(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
    Query(q): Query<TrinoTableQuery>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let conn = get_or_init_trino(&state, &conn_id).await?;

    #[cfg(feature = "duckdb")]
    {
        let cols = conn.columns(&q.catalog, &q.schema, &q.table).await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;
        Ok(Json(serde_json::json!({ "columns": cols, "table": format!("{}.{}.{}", q.catalog, q.schema, q.table) })))
    }
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = (conn, q);
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "DuckDB required".into() })))
    }
}

/// GET /api/v1/trino/:conn_id/preview?catalog=X&schema=Y&table=Z — live preview (LIMIT 100 via Trino).
async fn trino_preview(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
    Query(q): Query<TrinoTableQuery>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let conn = get_or_init_trino(&state, &conn_id).await?;

    let result = conn.preview(&q.catalog, &q.schema, &q.table, 100).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;

    // Convert to row-of-objects format for UI
    let rows: Vec<serde_json::Value> = result.rows.iter().map(|row| {
        let mut obj = serde_json::Map::new();
        for (i, col) in result.columns.iter().enumerate() {
            obj.insert(col.clone(), row.get(i).cloned().unwrap_or(serde_json::Value::Null));
        }
        serde_json::Value::Object(obj)
    }).collect();

    Ok(Json(serde_json::json!({
        "columns": result.columns,
        "column_types": result.column_types,
        "rows": rows,
        "row_count": result.row_count,
        "duration_ms": result.duration_ms,
        "engine": "Trino",
        "table": format!("{}.{}.{}", q.catalog, q.schema, q.table),
    })))
}

/// POST /api/v1/trino/:conn_id/query — execute SQL through Trino REST API.
#[derive(Deserialize)]
struct TrinoQueryRequest {
    sql: String,
    #[serde(default = "default_trino_catalog")]
    catalog: String,
}
fn default_trino_catalog() -> String { "system".into() }

async fn trino_query(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
    Json(req): Json<TrinoQueryRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let conn = get_or_init_trino(&state, &conn_id).await?;

    let result = conn.query(&req.sql, &req.catalog).await
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })))?;

    let rows: Vec<serde_json::Value> = result.rows.iter().map(|row| {
        let mut obj = serde_json::Map::new();
        for (i, col) in result.columns.iter().enumerate() {
            obj.insert(col.clone(), row.get(i).cloned().unwrap_or(serde_json::Value::Null));
        }
        serde_json::Value::Object(obj)
    }).collect();

    Ok(Json(serde_json::json!({
        "columns": result.columns,
        "column_types": result.column_types,
        "rows": rows,
        "row_count": result.row_count,
        "duration_ms": result.duration_ms,
        "engine": "Trino",
    })))
}

/// POST /api/v1/trino/:conn_id/refresh — re-fetch catalog metadata from Trino, update DuckDB cache.
async fn trino_refresh(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let conn = get_or_init_trino(&state, &conn_id).await?;

    #[cfg(feature = "duckdb")]
    {
        let table_count = conn.refresh_cache().await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;
        Ok(Json(serde_json::json!({ "status": "refreshed", "tables_cached": table_count })))
    }
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = conn;
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "DuckDB required".into() })))
    }
}

/// GET /api/v1/trino/:conn_id/stats — cache statistics.
async fn trino_stats(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let conn = get_or_init_trino(&state, &conn_id).await?;

    #[cfg(feature = "duckdb")]
    {
        let stats = conn.stats().await;
        Ok(Json(stats))
    }
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = conn;
        Ok(Json(serde_json::json!({})))
    }
}

// ── Engine info endpoints ──────────────────────────────────────────

/// GET /api/v1/engines — list available query engines with status.
async fn list_engines(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let mut engines = vec![
        serde_json::json!({
            "name": "DataFusion",
            "version": "51",
            "status": "running",
            "default": true,
            "description": "Primary SQL engine — planning, optimization, catalog management"
        }),
    ];

    #[cfg(feature = "duckdb")]
    {
        if let Some(ref engine) = state.duckdb_engine {
            let version = engine.version();
            engines.push(serde_json::json!({
                "name": "DuckDB",
                "version": version,
                "status": "running",
                "default": false,
                "description": "OLAP accelerator — heavy scans, aggregations, joins"
            }));
        } else {
            engines.push(serde_json::json!({
                "name": "DuckDB",
                "version": "1.2",
                "status": "disabled",
                "default": false,
                "description": "OLAP accelerator — enable with RUSTLAKE_DUCKDB__ENABLED=true"
            }));
        }
    }

    #[cfg(not(feature = "duckdb"))]
    {
        engines.push(serde_json::json!({
            "name": "DuckDB",
            "version": "N/A",
            "status": "not_compiled",
            "default": false,
            "description": "Compile with --features duckdb to enable"
        }));
    }

    #[cfg(feature = "polars")]
    {
        if let Some(ref engine) = state.polars_engine {
            engines.push(serde_json::json!({
                "name": "Polars",
                "version": engine.version(),
                "status": "running",
                "default": false,
                "description": "DataFrame engine — lazy evaluation, memory-efficient transforms"
            }));
        } else {
            engines.push(serde_json::json!({
                "name": "Polars",
                "version": "0.53",
                "status": "disabled",
                "default": false,
                "description": "DataFrame engine — enable with RUSTLAKE_POLARS__ENABLED=true"
            }));
        }
    }

    #[cfg(not(feature = "polars"))]
    {
        engines.push(serde_json::json!({
            "name": "Polars",
            "version": "N/A",
            "status": "not_compiled",
            "default": false,
            "description": "Compile with --features polars to enable"
        }));
    }

    Json(serde_json::json!({ "engines": engines }))
}

/// Request for benchmark comparison.
#[derive(Deserialize)]
struct CompareBenchmarkRequest {
    query_id: String,
}

/// POST /api/v1/benchmarks/compare — run a benchmark query on BOTH engines.
async fn compare_benchmark(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CompareBenchmarkRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let queries = tpch_queries();
    let query = queries.iter().find(|q| q.id == req.query_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Benchmark query '{}' not found", req.query_id),
            }),
        )
    })?;

    // Run on DataFusion
    let df_start = Instant::now();
    let ctx = state.ctx.read().await;
    let df_result = match ctx.datafusion_ctx().sql(&query.sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                let duration_ms = df_start.elapsed().as_millis();
                serde_json::json!({
                    "duration_ms": duration_ms,
                    "row_count": row_count,
                    "status": "success"
                })
            }
            Err(e) => serde_json::json!({
                "duration_ms": df_start.elapsed().as_millis(),
                "row_count": 0,
                "status": "error",
                "error": e.to_string()
            }),
        },
        Err(e) => serde_json::json!({
            "duration_ms": df_start.elapsed().as_millis(),
            "row_count": 0,
            "status": "error",
            "error": e.to_string()
        }),
    };
    drop(ctx);

    // Run on DuckDB
    let duck_result = if state.duckdb_available() {
        let duck_start = Instant::now();
        match execute_via_duckdb(&state, &query.sql).await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                let duration_ms = duck_start.elapsed().as_millis();
                serde_json::json!({
                    "duration_ms": duration_ms,
                    "row_count": row_count,
                    "status": "success"
                })
            }
            Err(e) => serde_json::json!({
                "duration_ms": duck_start.elapsed().as_millis(),
                "row_count": 0,
                "status": "error",
                "error": e.to_string()
            }),
        }
    } else {
        serde_json::json!({
            "duration_ms": 0,
            "row_count": 0,
            "status": "unavailable"
        })
    };

    // Run on Polars
    let polars_result = if state.polars_available() {
        let polars_start = Instant::now();
        match execute_via_polars(&state, &query.sql).await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                let duration_ms = polars_start.elapsed().as_millis();
                serde_json::json!({
                    "duration_ms": duration_ms,
                    "row_count": row_count,
                    "status": "success"
                })
            }
            Err(e) => serde_json::json!({
                "duration_ms": polars_start.elapsed().as_millis(),
                "row_count": 0,
                "status": "error",
                "error": e.to_string()
            }),
        }
    } else {
        serde_json::json!({
            "duration_ms": 0,
            "row_count": 0,
            "status": "unavailable"
        })
    };

    // Calculate winner across all engines
    let df_ms = df_result["duration_ms"].as_f64().unwrap_or(f64::MAX);
    let dk_ms = duck_result["duration_ms"].as_f64().unwrap_or(f64::MAX);
    let pl_ms = polars_result["duration_ms"].as_f64().unwrap_or(f64::MAX);
    let dk_ok = duck_result["status"].as_str() == Some("success");
    let pl_ok = polars_result["status"].as_str() == Some("success");

    let mut best_ms = df_ms;
    let mut winner = "DataFusion";
    if dk_ok && dk_ms < best_ms {
        best_ms = dk_ms;
        winner = "DuckDB";
    }
    if pl_ok && pl_ms < best_ms {
        winner = "Polars";
    }
    let speedup = if dk_ms > 0.0 { df_ms / dk_ms } else { 1.0 };

    Ok(Json(serde_json::json!({
        "query_id": query.id,
        "query_name": query.name,
        "datafusion": df_result,
        "duckdb": duck_result,
        "polars": polars_result,
        "speedup": speedup,
        "winner": winner,
    })))
}

// ── Provider endpoints ─────────────────────────────────────────────

/// GET /api/v1/providers — list enabled federated data providers and their status.
async fn list_providers(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let entries = state.provider_registry.list_entries().await;

    let mut providers = Vec::new();

    // Report compiled-in providers
    #[cfg(feature = "postgres")]
    providers.push(serde_json::json!({
        "name": "PostgreSQL",
        "id": "postgres",
        "enabled": true,
        "mode": "federated",
        "description": "Live queries with predicate/projection pushdown via bb8 pool",
    }));
    #[cfg(not(feature = "postgres"))]
    providers.push(serde_json::json!({
        "name": "PostgreSQL",
        "id": "postgres",
        "enabled": false,
        "mode": "disabled",
        "description": "Enable with --features postgres",
    }));

    #[cfg(feature = "mysql")]
    providers.push(serde_json::json!({
        "name": "MySQL",
        "id": "mysql",
        "enabled": true,
        "mode": "federated",
        "description": "Live queries with predicate/projection pushdown via mysql_async pool",
    }));
    #[cfg(not(feature = "mysql"))]
    providers.push(serde_json::json!({
        "name": "MySQL",
        "id": "mysql",
        "enabled": false,
        "mode": "disabled",
        "description": "Enable with --features mysql",
    }));

    #[cfg(feature = "sqlite")]
    providers.push(serde_json::json!({
        "name": "SQLite",
        "id": "sqlite",
        "enabled": true,
        "mode": "federated",
        "description": "Bundled SQLite engine with pushdown support",
    }));
    #[cfg(not(feature = "sqlite"))]
    providers.push(serde_json::json!({
        "name": "SQLite",
        "id": "sqlite",
        "enabled": false,
        "mode": "disabled",
        "description": "Enable with --features sqlite",
    }));

    // MongoDB always available (snapshot mode)
    providers.push(serde_json::json!({
        "name": "MongoDB",
        "id": "mongodb",
        "enabled": true,
        "mode": "snapshot",
        "description": "MemTable snapshot — no provider available for federated mode",
    }));

    #[cfg(feature = "clickhouse")]
    providers.push(serde_json::json!({
        "name": "ClickHouse",
        "id": "clickhouse",
        "enabled": true,
        "mode": "federated",
        "description": "HTTP-based ClickHouse federation",
    }));

    // Active connections using providers
    let active_connections: Vec<serde_json::Value> = entries
        .iter()
        .map(|(id, entry)| {
            serde_json::json!({
                "connection_id": id,
                "conn_type": entry.conn_type,
                "tables": entry.tables,
            })
        })
        .collect();

    Json(serde_json::json!({
        "providers": providers,
        "active_connections": active_connections,
    }))
}

// ── Migration: Iceberg Catalog Migration (Trino → Rake) ───────────

/// Request body for storing S3 credentials for migration.
#[derive(Deserialize)]
struct MigrationCredentialsRequest {
    /// Identifier for these credentials (e.g., bucket name or "default").
    key: String,
    /// AWS access key ID.
    access_key: String,
    /// AWS secret access key.
    secret_key: String,
    /// AWS region (e.g., "us-east-1").
    #[serde(default = "default_migration_s3_region")]
    region: String,
}

fn default_migration_s3_region() -> String {
    "us-east-1".to_string()
}

/// Request body for registering discovered Iceberg tables into Rake.
#[derive(Deserialize)]
struct MigrationRegisterRequest {
    /// Optional list of fully-qualified table names to register.
    /// If omitted, registers all discovered Iceberg tables with S3 locations.
    tables: Option<Vec<String>>,
}

/// Request body for migration compare: run SQL on Trino and Rake engines.
#[derive(Deserialize)]
struct MigrationCompareRequest {
    /// Trino connection ID for executing the query on Trino.
    conn_id: String,
    /// SQL to run on Trino.
    sql: String,
    /// Optional SQL to run on Rake engines (auto-derived if omitted by rewriting
    /// Trino catalog references to Rake-registered table names).
    rake_sql: Option<String>,
    /// If true, use native S3 connections for each engine instead of going through
    /// Trino-registered tables. Requires S3 credentials stored via /api/v1/migration/credentials.
    #[serde(default)]
    use_native_s3: bool,
}

/// POST /api/v1/migration/credentials — store S3 credentials for migration (in-memory only).
async fn migration_credentials(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MigrationCredentialsRequest>,
) -> Json<serde_json::Value> {
    let key = req.key.clone();
    state.migration_s3_creds.write().await.insert(
        key.clone(),
        S3BucketCreds {
            account_id: key.clone(),
            access_key: req.access_key,
            secret_key: req.secret_key,
            session_token: None,
            region: req.region,
        },
    );
    // Persist encrypted to disk
    {
        let creds = state.migration_s3_creds.read().await;
        if let Some(creds_val) = creds.get(&key) {
            if let Err(e) = state.credential_store.store_s3_creds(&key, creds_val) {
                tracing::warn!(error = %e, "Failed to persist encrypted S3 credentials");
            }
        }
    }
    tracing::info!(key = %key, "Stored S3 credentials for migration");
    Json(serde_json::json!({
        "status": "stored",
        "key": key,
        "message": "S3 credentials stored (encrypted on disk)",
    }))
}

/// POST /api/v1/migration/:conn_id/discover — discover Iceberg tables.
///
/// Strategy (fast path first):
/// Minimal-stress Trino migration discovery. Two phases:
///
/// **Phase 1 (no S3 creds)**: Query Trino for Iceberg catalog names and warehouse
/// locations only. Return the required S3 buckets so the UI can prompt for credentials.
///
/// **Phase 2 (S3 creds available)**: Re-discover catalogs + warehouses (same light
/// Trino queries), then scan S3 directly via `scan_warehouse()`. If S3 scan fails
/// for a catalog, log a warning and skip it — never fall back to Trino.
async fn migration_discover(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let conn = get_or_init_trino(&state, &conn_id).await?;
    let start = std::time::Instant::now();

    #[cfg(feature = "duckdb")]
    {
        // ── Phase 1: Discover Iceberg catalogs and warehouse locations (light Trino queries) ──
        tracing::info!("Phase 1: Querying Trino for Iceberg catalogs");

        let iceberg_catalogs = conn.rest.list_iceberg_catalogs().await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e })))?;

        tracing::info!(count = iceberg_catalogs.len(), catalogs = ?iceberg_catalogs, "Found Iceberg catalogs");

        // For each Iceberg catalog, get the warehouse location (1 Trino query per catalog)
        let mut warehouse_locations: Vec<(String, String)> = Vec::new(); // (catalog, warehouse_path)
        let mut required_buckets: Vec<String> = Vec::new();

        for catalog in &iceberg_catalogs {
            tracing::info!(catalog = %catalog, "Getting warehouse location for catalog");
            let warehouse = crate::iceberg_s3::get_warehouse_location_from_trino(&conn.rest, catalog).await;

            if let Some(warehouse_path) = warehouse {
                tracing::info!(catalog = %catalog, warehouse = %warehouse_path, "Catalog warehouse location");
                // Extract bucket name from s3://bucket/path
                if let Some(bucket) = warehouse_path.strip_prefix("s3://")
                    .or_else(|| warehouse_path.strip_prefix("s3a://"))
                    .and_then(|rest| rest.split('/').next())
                {
                    if !required_buckets.contains(&bucket.to_string()) {
                        required_buckets.push(bucket.to_string());
                    }
                }
                warehouse_locations.push((catalog.clone(), warehouse_path));
            } else {
                tracing::warn!(catalog = %catalog, "No warehouse location found, skipping catalog");
            }
        }

        tracing::info!(buckets = ?required_buckets, "Required S3 buckets");

        // ── Check for S3 credentials ──
        let s3_creds = state.migration_s3_creds.read().await;
        let has_s3_creds = !s3_creds.is_empty();
        let default_creds = s3_creds.values().next().cloned();
        drop(s3_creds);

        if !has_s3_creds {
            // Phase 1 response: return catalog info so UI can prompt for S3 keys
            let elapsed_ms = start.elapsed().as_millis();
            tracing::info!(elapsed_ms = elapsed_ms, "Phase 1 complete — awaiting S3 credentials");

            return Ok(Json(serde_json::json!({
                "status": "awaiting_credentials",
                "conn_id": conn_id,
                "discovery_method": "trino_metadata_only",
                "phase": 1,
                "iceberg_catalogs": iceberg_catalogs,
                "warehouse_locations": warehouse_locations.iter()
                    .map(|(cat, wh)| serde_json::json!({"catalog": cat, "warehouse": wh}))
                    .collect::<Vec<_>>(),
                "required_s3_buckets": required_buckets,
                "table_count": 0,
                "tables": [],
                "elapsed_ms": elapsed_ms,
            })));
        }

        // ── Phase 2: Scan S3 directly for each Iceberg catalog ──
        let default_bucket_creds = default_creds.unwrap(); // safe: has_s3_creds is true
        let access_key = default_bucket_creds.access_key.clone();
        let secret_key = default_bucket_creds.secret_key.clone();
        let region = default_bucket_creds.region.clone();

        let mut tables: Vec<MigrationTable> = Vec::new();

        for (catalog, warehouse_path) in &warehouse_locations {
            tracing::info!(catalog = %catalog, warehouse = %warehouse_path, "Phase 2: Scanning S3 directly for catalog");

            let bucket = match warehouse_path.strip_prefix("s3://")
                .or_else(|| warehouse_path.strip_prefix("s3a://"))
                .and_then(|rest| rest.split('/').next())
            {
                Some(b) => b,
                None => {
                    tracing::warn!(catalog = %catalog, warehouse = %warehouse_path, "Could not extract bucket from warehouse path, skipping");
                    continue;
                }
            };

            // Use bucket-specific creds if available, otherwise default
            let s3_creds = state.migration_s3_creds.read().await;
            let bucket_creds = s3_creds.get(bucket).cloned();
            drop(s3_creds);
            let ak = bucket_creds.as_ref().map(|c| c.access_key.as_str()).unwrap_or(&access_key);
            let sk = bucket_creds.as_ref().map(|c| c.secret_key.as_str()).unwrap_or(&secret_key);
            let reg = bucket_creds.as_ref().map(|c| c.region.as_str()).unwrap_or(&region);

            let store = match crate::iceberg_s3::build_s3_store(bucket, ak, sk, reg, None) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(catalog = %catalog, bucket = %bucket, error = %e, "S3 store build failed, skipping catalog");
                    continue;
                }
            };

            match crate::iceberg_s3::scan_warehouse(&store, warehouse_path).await {
                Ok(scan_result) => {
                    tracing::info!(
                        catalog = %catalog,
                        tables = scan_result.total_tables,
                        databases = scan_result.databases.len(),
                        elapsed_ms = scan_result.scan_duration_ms,
                        "S3 direct scan complete for catalog"
                    );

                    for table_info in &scan_result.tables {
                        tables.push(MigrationTable {
                            conn_id: conn_id.clone(),
                            catalog: catalog.clone(),
                            schema_name: table_info.database.clone(),
                            table_name: table_info.table_name.clone(),
                            format: "iceberg".to_string(),
                            location: Some(table_info.s3_location.clone()),
                            metastore_uri: None,
                            column_count: table_info.column_count,
                            row_count: None,
                            registered_in_rake: false,
                            rake_table_name: None,
                            status: "discovered".to_string(),
                            error: None,
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(catalog = %catalog, error = %e, "S3 scan failed, skipping catalog (no Trino fallback)");
                }
            }
        }

        let total_count = tables.len();
        let with_location = tables.iter().filter(|t| t.location.is_some()).count();
        let elapsed_ms = start.elapsed().as_millis();

        tracing::info!(
            total_tables = total_count,
            with_location = with_location,
            elapsed_ms = elapsed_ms,
            "Phase 2 complete"
        );

        *state.migration_tables.write().await = tables.clone();

        Ok(Json(serde_json::json!({
            "status": "discovered",
            "conn_id": conn_id,
            "discovery_method": "s3_direct",
            "phase": 2,
            "table_count": total_count,
            "iceberg_table_count": total_count,
            "tables_with_s3_location": with_location,
            "iceberg_catalogs": iceberg_catalogs,
            "warehouse_locations": warehouse_locations.iter()
                .map(|(cat, wh)| serde_json::json!({"catalog": cat, "warehouse": wh}))
                .collect::<Vec<_>>(),
            "required_s3_buckets": required_buckets,
            "tables": tables,
            "elapsed_ms": elapsed_ms,
        })))
    }
    #[cfg(not(feature = "duckdb"))]
    {
        let _ = (conn, start);
        Err((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "DuckDB feature required for migration discover".into() })))
    }
}

/// POST /api/v1/migration/:conn_id/register — register discovered Iceberg tables into Rake.
///
/// For tables with S3 locations, registers them in DataFusion as Parquet-backed tables
/// (future: register as Iceberg tables via iceberg-rust catalog integration).
/// Also syncs registered tables to DuckDB/Polars engines if available.
async fn migration_register(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
    Json(req): Json<MigrationRegisterRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let mut registered_count = 0usize;
    let mut skipped_count = 0usize;
    let mut errors: Vec<serde_json::Value> = Vec::new();

    // Select tables to register
    let tables_to_register: Vec<MigrationTable> = {
        let all_tables = state.migration_tables.read().await;
        all_tables.iter()
            .filter(|t| t.conn_id == conn_id)
            .filter(|t| !t.registered_in_rake)
            .filter(|t| {
                if let Some(ref filter) = req.tables {
                    let fqn = format!("{}.{}.{}", t.catalog, t.schema_name, t.table_name);
                    filter.iter().any(|f| f == &fqn || f == &t.table_name)
                } else {
                    // Default: only register Iceberg tables with known S3 locations
                    t.format == "iceberg" && t.location.is_some()
                }
            })
            .cloned()
            .collect()
    };

    let total = tables_to_register.len();
    let mut registered_names: Vec<String> = Vec::new();

    for mt in &tables_to_register {
        let fqn = format!("{}.{}.{}", mt.catalog, mt.schema_name, mt.table_name);

        // Tables must have an S3 location to register
        let s3_location = match &mt.location {
            Some(loc) => loc.clone(),
            None => {
                skipped_count += 1;
                errors.push(serde_json::json!({
                    "table": fqn,
                    "error": "No S3 location available — cannot register without data location",
                }));
                // Update status
                let mut all = state.migration_tables.write().await;
                if let Some(entry) = all.iter_mut().find(|t|
                    t.catalog == mt.catalog && t.schema_name == mt.schema_name && t.table_name == mt.table_name
                ) {
                    entry.status = "error".to_string();
                    entry.error = Some("No S3 location".to_string());
                }
                continue;
            }
        };

        // Build a Rake table name: iceberg_{catalog}_{schema}_{table}
        let rake_name = format!(
            "iceberg_{}_{}_{}",
            mt.catalog.replace('.', "_").replace('-', "_"),
            mt.schema_name.replace('.', "_").replace('-', "_"),
            mt.table_name.replace('.', "_").replace('-', "_"),
        );

        // Register the S3 path as a Parquet table in DataFusion.
        // NOTE: This is a temporary approach. When iceberg-rust catalog integration
        // is available, this will register as a proper Iceberg table with full
        // metadata, time travel, and snapshot support.
        let register_result = {
            let ctx = state.ctx.read().await;
            let df_ctx = ctx.datafusion_ctx();
            df_ctx.register_parquet(&rake_name, &s3_location, Default::default()).await
        };

        match register_result {
            Ok(_) => {
                tracing::info!(
                    table = %fqn,
                    rake_name = %rake_name,
                    location = %s3_location,
                    "Registered Iceberg table in Rake"
                );
                registered_names.push(rake_name.clone());

                // Update migration table entry
                let mut all = state.migration_tables.write().await;
                if let Some(entry) = all.iter_mut().find(|t|
                    t.catalog == mt.catalog && t.schema_name == mt.schema_name && t.table_name == mt.table_name
                ) {
                    entry.registered_in_rake = true;
                    entry.rake_table_name = Some(rake_name.clone());
                    entry.status = "registered".to_string();
                    entry.error = None;
                }
                drop(all);

                // Mark as read-only — migrated tables are for comparison only
                state.read_only_tables.write().await.insert(rake_name.clone());
                tracing::info!(table = %rake_name, "Marked migration table as read-only");

                registered_count += 1;
            }
            Err(e) => {
                let err_msg = format!("DataFusion registration failed: {}", e);
                tracing::warn!(
                    table = %fqn,
                    error = %e,
                    location = %s3_location,
                    "Failed to register Iceberg table in Rake"
                );
                errors.push(serde_json::json!({
                    "table": fqn,
                    "location": s3_location,
                    "error": err_msg,
                }));
                let mut all = state.migration_tables.write().await;
                if let Some(entry) = all.iter_mut().find(|t|
                    t.catalog == mt.catalog && t.schema_name == mt.schema_name && t.table_name == mt.table_name
                ) {
                    entry.status = "error".to_string();
                    entry.error = Some(err_msg);
                }
            }
        }
    }

    // Sync newly registered tables to DuckDB/Polars engines
    if !registered_names.is_empty() {
        sync_trino_tables_to_engines(&state, &registered_names).await;
    }

    Ok(Json(serde_json::json!({
        "status": "complete",
        "conn_id": conn_id,
        "registered": registered_count,
        "skipped": skipped_count,
        "total": total,
        "registered_tables": registered_names,
        "errors": errors,
    })))
}

/// POST /api/v1/migration/compare — run SQL on Trino and on Rake's engines, compare performance.
///
/// Executes the query via the Trino REST API for timing, then executes the equivalent
/// SQL on DataFusion, DuckDB, and Polars (if available) on the same Iceberg data
/// registered in Rake. Returns timing comparison with winner and speedup factor.
///
/// When `use_native_s3` is true and S3 credentials are available, each Rake engine
/// connects to S3 directly using its native connector instead of reading from
/// Trino-registered in-memory copies:
/// - **DataFusion**: `object_store` AmazonS3Builder + ListingTable at S3 path
/// - **DuckDB**: `INSTALL httpfs; SET s3_*; SELECT * FROM read_parquet('s3://...')`
/// - **Polars**: Uses DataFusion S3 backend (labeled "via DataFusion S3")
async fn migration_compare(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MigrationCompareRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let comparison_id = Uuid::new_v4().to_string();
    let use_native_s3 = req.use_native_s3;

    // Collect S3 credentials and table locations when native S3 is requested
    let s3_creds: Option<S3BucketCreds> = if use_native_s3 {
        let creds_map = state.migration_s3_creds.read().await;
        // Try conn_id-specific creds first, then "default"
        creds_map.get(&req.conn_id)
            .or_else(|| creds_map.get("default"))
            .cloned()
    } else {
        None
    };

    // Build a mapping of table FQN → S3 location for native S3 mode
    let table_s3_locations: std::collections::HashMap<String, String> = if use_native_s3 {
        let tables = state.migration_tables.try_read()
            .map(|t| t.clone())
            .unwrap_or_default();
        tables.iter()
            .filter(|t| t.conn_id == req.conn_id && t.location.is_some())
            .map(|t| {
                let fqn = format!("{}.{}.{}", t.catalog, t.schema_name, t.table_name);
                (fqn, t.location.clone().unwrap())
            })
            .collect()
    } else {
        std::collections::HashMap::new()
    };

    // Build Rake SQL by rewriting Trino catalog.schema.table references to Rake names
    // Trino FQN: catalog.schema.table → Rake: trino_{catalog}.{schema}_{table}
    let rake_sql = req.rake_sql.unwrap_or_else(|| {
        let mut sql = req.sql.clone();

        // First, try explicit mappings from registered migration tables
        let re_pattern: Vec<(String, String)> = {
            let tables = state.migration_tables.try_read()
                .map(|t| t.clone())
                .unwrap_or_default();
            tables.iter()
                .filter(|t| t.conn_id == req.conn_id && t.rake_table_name.is_some())
                .flat_map(|t| {
                    let rake_name = t.rake_table_name.clone().unwrap();
                    let fqn_unquoted = format!(
                        "{}.{}.{}",
                        t.catalog, t.schema_name, t.table_name
                    );
                    vec![(fqn_unquoted, rake_name)]
                })
                .collect()
        };
        for (trino_ref, rake_name) in &re_pattern {
            sql = sql.replace(trino_ref, rake_name);
        }

        // Then, auto-rewrite any remaining catalog.schema.table patterns
        // Pattern: word.word.word that looks like a Trino FQN
        // Rewrite: catalog.schema.table → trino_{catalog}.{schema}_{table}
        let fqn_re = regex::Regex::new(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b").unwrap();
        // Don't rewrite SQL keywords that happen to match (e.g. GROUP.BY.something)
        let sql_keywords: std::collections::HashSet<&str> = [
            "GROUP", "ORDER", "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT",
            "INNER", "OUTER", "CROSS", "ON", "AND", "OR", "NOT", "AS", "LIMIT",
            "OFFSET", "HAVING", "UNION", "INSERT", "INTO", "VALUES", "UPDATE",
            "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW",
        ].iter().copied().collect();

        let result = fqn_re.replace_all(&sql, |caps: &regex::Captures| {
            let catalog = &caps[1];
            let schema = &caps[2];
            let table = &caps[3];
            // Skip if any part is a SQL keyword
            if sql_keywords.contains(catalog.to_uppercase().as_str())
                || sql_keywords.contains(schema.to_uppercase().as_str())
                || sql_keywords.contains(table.to_uppercase().as_str())
            {
                return caps[0].to_string();
            }
            format!("trino_{}.{}_{}", catalog, schema, table)
        });
        result.to_string()
    });

    let mut results: Vec<EngineResult> = Vec::new();

    // 1. Trino — execute via Trino REST API directly for accurate Trino timing
    {
        let start = Instant::now();
        let conn_result = get_or_init_trino(&state, &req.conn_id).await;
        match conn_result {
            Ok(conn) => {
                let default_catalog = conn.default_catalog.clone();
                match conn.rest.execute_query(&req.sql, &default_catalog).await {
                    Ok(query_result) => {
                        results.push(EngineResult {
                            engine: "Trino".to_string(),
                            duration_ms: start.elapsed().as_millis() as u64,
                            row_count: query_result.row_count,
                            status: "success".to_string(),
                            error: None,
                            path: Some("trino_native".to_string()),
                        });
                    }
                    Err(e) => {
                        results.push(EngineResult {
                            engine: "Trino".to_string(),
                            duration_ms: start.elapsed().as_millis() as u64,
                            row_count: 0,
                            status: "error".to_string(),
                            error: Some(e),
                            path: Some("trino_native".to_string()),
                        });
                    }
                }
            }
            Err(_) => {
                results.push(EngineResult {
                    engine: "Trino".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count: 0,
                    status: "error".to_string(),
                    error: Some(format!("Trino connection '{}' not available", req.conn_id)),
                    path: Some("trino_native".to_string()),
                });
            }
        }
    }

    // 2. Rake DataFusion — native S3 or via Trino-registered tables
    if use_native_s3 && s3_creds.is_some() {
        // DataFusion S3 direct: create a temporary SessionContext with S3 object_store,
        // register ListingTables at S3 paths, and run the rewritten SQL.
        let start = Instant::now();
        let creds = s3_creds.as_ref().unwrap();
        match execute_df_s3_direct(&rake_sql, &table_s3_locations, creds, &req.conn_id).await {
            Ok((row_count, engine_label)) => {
                results.push(EngineResult {
                    engine: engine_label,
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count,
                    status: "success".to_string(),
                    error: None,
                    path: Some("s3_direct".to_string()),
                });
            }
            Err(e) => {
                // Fallback to via-Trino path on S3 error
                tracing::warn!(error = %e, "DataFusion S3 direct failed, falling back to via-Trino");
                let start2 = Instant::now();
                let ctx = state.ctx.read().await;
                match ctx.datafusion_ctx().sql(&rake_sql).await {
                    Ok(df) => match df.collect().await {
                        Ok(batches) => {
                            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                            results.push(EngineResult {
                                engine: "Rake DataFusion".to_string(),
                                duration_ms: start2.elapsed().as_millis() as u64,
                                row_count,
                                status: "success".to_string(),
                                error: None,
                                path: Some("via_trino".to_string()),
                            });
                        }
                        Err(e2) => {
                            results.push(EngineResult {
                                engine: "Rake DataFusion".to_string(),
                                duration_ms: start2.elapsed().as_millis() as u64,
                                row_count: 0,
                                status: "error".to_string(),
                                error: Some(format!("S3 direct: {}; via-Trino: {}", e, e2)),
                                path: Some("via_trino".to_string()),
                            });
                        }
                    },
                    Err(e2) => {
                        results.push(EngineResult {
                            engine: "Rake DataFusion".to_string(),
                            duration_ms: start2.elapsed().as_millis() as u64,
                            row_count: 0,
                            status: "error".to_string(),
                            error: Some(format!("S3 direct: {}; via-Trino: {}", e, e2)),
                            path: Some("via_trino".to_string()),
                        });
                    }
                }
                drop(ctx);
            }
        }
    } else {
        // Standard path: execute on Trino-registered tables in DataFusion
        let start = Instant::now();
        let ctx = state.ctx.read().await;
        match ctx.datafusion_ctx().sql(&rake_sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                    results.push(EngineResult {
                        engine: "Rake DataFusion".to_string(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        row_count,
                        status: "success".to_string(),
                        error: None,
                        path: Some("via_trino".to_string()),
                    });
                }
                Err(e) => {
                    results.push(EngineResult {
                        engine: "Rake DataFusion".to_string(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        row_count: 0,
                        status: "error".to_string(),
                        error: Some(e.to_string()),
                        path: Some("via_trino".to_string()),
                    });
                }
            },
            Err(e) => {
                results.push(EngineResult {
                    engine: "Rake DataFusion".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count: 0,
                    status: "error".to_string(),
                    error: Some(e.to_string()),
                    path: Some("via_trino".to_string()),
                });
            }
        }
        drop(ctx);
    }

    // For DuckDB/Polars, flatten schema-qualified trino_ table names to flat names
    // trino_tpch.sf1_orders → trino_tpch_sf1_orders (only trino_ prefixed names)
    let trino_dot_re = regex::Regex::new(r"\btrino_(\w+)\.(\w+)\b").unwrap();
    let flat_sql = trino_dot_re.replace_all(&rake_sql, "trino_${1}_${2}").to_string();

    // 3. Rake DuckDB — native S3 via httpfs or in-memory synced copy
    if use_native_s3 && s3_creds.is_some() && !table_s3_locations.is_empty() {
        let start = Instant::now();
        let creds = s3_creds.as_ref().unwrap();
        match execute_duckdb_s3_direct(&state, &req.sql, &table_s3_locations, creds, &req.conn_id).await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                results.push(EngineResult {
                    engine: "Rake DuckDB (S3 direct)".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count,
                    status: "success".to_string(),
                    error: None,
                    path: Some("s3_direct".to_string()),
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "DuckDB S3 direct failed, falling back to in-memory");
                // Fallback to in-memory synced copy
                let start2 = Instant::now();
                match execute_via_duckdb(&state, &flat_sql).await {
                    Ok(batches) => {
                        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                        results.push(EngineResult {
                            engine: "Rake DuckDB".to_string(),
                            duration_ms: start2.elapsed().as_millis() as u64,
                            row_count,
                            status: "success".to_string(),
                            error: None,
                            path: Some("in_memory".to_string()),
                        });
                    }
                    Err(e2) => {
                        results.push(EngineResult {
                            engine: "Rake DuckDB".to_string(),
                            duration_ms: start2.elapsed().as_millis() as u64,
                            row_count: 0,
                            status: if e2.contains("not available") { "unavailable" } else { "error" }.to_string(),
                            error: Some(format!("S3 direct: {}; in-memory: {}", e, e2)),
                            path: Some("in_memory".to_string()),
                        });
                    }
                }
            }
        }
    } else {
        let start = Instant::now();
        match execute_via_duckdb(&state, &flat_sql).await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                results.push(EngineResult {
                    engine: "Rake DuckDB".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count,
                    status: "success".to_string(),
                    error: None,
                    path: Some("in_memory".to_string()),
                });
            }
            Err(e) => {
                results.push(EngineResult {
                    engine: "Rake DuckDB".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count: 0,
                    status: if e.contains("not available") { "unavailable" } else { "error" }.to_string(),
                    error: Some(e),
                    path: Some("in_memory".to_string()),
                });
            }
        }
    }

    // 4. Rake Polars — native S3 via DataFusion S3 backend or in-memory synced copy
    if use_native_s3 && s3_creds.is_some() {
        // Polars uses the DataFusion S3 backend for native S3 access.
        // This gives real S3 reads but routed through the Polars execution engine label.
        let start = Instant::now();
        let creds = s3_creds.as_ref().unwrap();
        match execute_df_s3_direct(&rake_sql, &table_s3_locations, creds, &req.conn_id).await {
            Ok((row_count, _)) => {
                results.push(EngineResult {
                    engine: "Rake Polars (via DataFusion S3)".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count,
                    status: "success".to_string(),
                    error: None,
                    path: Some("s3_direct".to_string()),
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "Polars S3 direct failed, falling back to in-memory");
                let start2 = Instant::now();
                match execute_via_polars(&state, &flat_sql).await {
                    Ok(batches) => {
                        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                        results.push(EngineResult {
                            engine: "Rake Polars".to_string(),
                            duration_ms: start2.elapsed().as_millis() as u64,
                            row_count,
                            status: "success".to_string(),
                            error: None,
                            path: Some("in_memory".to_string()),
                        });
                    }
                    Err(e2) => {
                        results.push(EngineResult {
                            engine: "Rake Polars".to_string(),
                            duration_ms: start2.elapsed().as_millis() as u64,
                            row_count: 0,
                            status: if e2.contains("not available") { "unavailable" } else { "error" }.to_string(),
                            error: Some(format!("S3 direct: {}; in-memory: {}", e, e2)),
                            path: Some("in_memory".to_string()),
                        });
                    }
                }
            }
        }
    } else {
        let start = Instant::now();
        match execute_via_polars(&state, &flat_sql).await {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                results.push(EngineResult {
                    engine: "Rake Polars".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count,
                    status: "success".to_string(),
                    error: None,
                    path: Some("in_memory".to_string()),
                });
            }
            Err(e) => {
                results.push(EngineResult {
                    engine: "Rake Polars".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    row_count: 0,
                    status: if e.contains("not available") { "unavailable" } else { "error" }.to_string(),
                    error: Some(e),
                    path: Some("in_memory".to_string()),
                });
            }
        }
    }

    // Determine winner (fastest successful Rake engine) and speedup vs Trino
    let trino_ms = results.iter()
        .find(|r| r.engine == "Trino")
        .filter(|r| r.status == "success")
        .map(|r| r.duration_ms)
        .unwrap_or(0);

    let best_rake = results.iter()
        .filter(|r| r.engine.starts_with("Rake") && r.status == "success")
        .min_by_key(|r| r.duration_ms);

    let (winner, speedup) = match best_rake {
        Some(best) => {
            let sp = if best.duration_ms > 0 && trino_ms > 0 {
                trino_ms as f64 / best.duration_ms as f64
            } else {
                1.0
            };
            (best.engine.clone(), (sp * 100.0).round() / 100.0)
        }
        None => ("N/A".to_string(), 1.0),
    };

    // Check data match: all successful engines return same row count
    let successful_counts: Vec<usize> = results.iter()
        .filter(|r| r.status == "success")
        .map(|r| r.row_count)
        .collect();
    let data_match = if successful_counts.len() >= 2 {
        successful_counts.iter().all(|&c| c == successful_counts[0])
    } else {
        true
    };

    let comparison = MigrationComparison {
        id: comparison_id.clone(),
        sql: req.sql.clone(),
        results: results.clone(),
        winner: winner.clone(),
        speedup,
        data_match,
        timestamp: Utc::now(),
    };

    state.migration_comparisons.write().await.push(comparison.clone());

    // Record all engine results in the performance tracker for adaptive routing
    let query_type = classify_query_for_engine(&req.sql);
    {
        let mut tracker = state.engine_tracker.write().await;
        let now = Utc::now().to_rfc3339();
        for result in &results {
            if result.status == "success" {
                tracker.record(EngineLatencyRecord {
                    engine: result.engine.clone(),
                    query_type: query_type.clone(),
                    duration_ms: result.duration_ms,
                    row_count: result.row_count,
                    data_size_bytes: (result.row_count as u64) * 64, // approximate: 64 bytes per row
                    path: result.path.clone().unwrap_or_else(|| "unknown".to_string()),
                    timestamp: now.clone(),
                });
            }
        }
    }

    // Build an adaptive recommendation based on this comparison and all history
    let (recommendation, alternatives) = {
        let tracker = state.engine_tracker.read().await;
        build_recommendation(
            &query_type,
            &tracker,
            state.duckdb_available(),
            state.polars_available(),
        )
    };

    // Override estimated_speedup with actual speedup from this comparison
    let recommendation = EngineRecommendation {
        estimated_speedup: speedup,
        ..recommendation
    };

    Ok(Json(serde_json::json!({
        "id": comparison_id,
        "sql": req.sql,
        "trino_sql": req.sql,
        "rake_sql": rake_sql,
        "native_s3": use_native_s3,
        "s3_available": s3_creds.is_some(),
        "results": results,
        "winner": winner,
        "speedup": speedup,
        "data_match": data_match,
        "timestamp": comparison.timestamp,
        "query_type": query_type,
        "recommendation": {
            "strategy": recommendation.strategy,
            "primary_engine": recommendation.primary_engine,
            "reason": recommendation.reason,
            "estimated_speedup": recommendation.estimated_speedup,
            "scan_engine": recommendation.scan_engine,
            "process_engine": recommendation.process_engine,
            "alternatives": alternatives,
        },
    })))
}

/// GET /api/v1/migration/:conn_id/tables — list discovered Iceberg tables for a connection.
async fn migration_tables(
    State(state): State<Arc<AppState>>,
    Path(conn_id): Path<String>,
) -> Json<serde_json::Value> {
    let tables = state.migration_tables.read().await;
    let filtered: Vec<&MigrationTable> = tables.iter()
        .filter(|t| t.conn_id == conn_id)
        .collect();
    let iceberg_count = filtered.iter().filter(|t| t.format == "iceberg").count();
    let registered_count = filtered.iter().filter(|t| t.registered_in_rake).count();
    let with_location = filtered.iter().filter(|t| t.location.is_some()).count();
    Json(serde_json::json!({
        "conn_id": conn_id,
        "tables": filtered,
        "count": filtered.len(),
        "iceberg_count": iceberg_count,
        "registered_count": registered_count,
        "with_s3_location": with_location,
    }))
}

/// GET /api/v1/migration/comparisons — list all migration comparison results.
async fn migration_comparisons(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let comparisons = state.migration_comparisons.read().await;
    Json(serde_json::json!({
        "comparisons": *comparisons,
        "count": comparisons.len(),
    }))
}

/// Extract the target table name from DDL/DML SQL (uppercased input).
///
/// Handles: INSERT INTO table, UPDATE table, DELETE FROM table,
/// DROP TABLE table, ALTER TABLE table, TRUNCATE TABLE table.
pub(crate) fn extract_target_table(sql_upper: &str) -> Option<String> {
    let patterns = [
        "INSERT INTO ",
        "UPDATE ",
        "DELETE FROM ",
        "DROP TABLE IF EXISTS ",
        "DROP TABLE ",
        "ALTER TABLE ",
        "TRUNCATE TABLE ",
        "TRUNCATE ",
    ];
    for pat in &patterns {
        if let Some(rest) = sql_upper.strip_prefix(pat) {
            // Take the first word (table name), stop at space/paren/semicolon
            let table: String = rest.chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
                .collect();
            if !table.is_empty() {
                return Some(table);
            }
        }
    }
    None
}

/// Classify a SQL query into a workload type for engine recommendation.
///
/// Returns one of: "complex_join", "join", "scan_aggregate", "point_lookup",
/// "ordered_scan", "full_scan".
fn classify_query_for_engine(sql: &str) -> String {
    let sql_upper = sql.to_uppercase();
    let has_join = sql_upper.contains("JOIN");
    let has_agg = sql_upper.contains("COUNT(")
        || sql_upper.contains("SUM(")
        || sql_upper.contains("AVG(")
        || sql_upper.contains("MIN(")
        || sql_upper.contains("MAX(")
        || sql_upper.contains("GROUP BY");
    let has_where_eq = sql_upper.contains("WHERE") && sql_upper.contains("=");
    let has_order = sql_upper.contains("ORDER BY");
    let has_subquery = sql_upper.matches("SELECT").count() > 1;

    if has_join && has_subquery {
        "complex_join".to_string()
    } else if has_join {
        "join".to_string()
    } else if has_agg {
        "scan_aggregate".to_string()
    } else if has_where_eq && !has_agg {
        "point_lookup".to_string()
    } else if has_order {
        "ordered_scan".to_string()
    } else {
        "full_scan".to_string()
    }
}

/// Build an engine recommendation based on query type and performance history.
fn build_recommendation(
    query_type: &str,
    tracker: &crate::state::EnginePerformanceTracker,
    duckdb_available: bool,
    polars_available: bool,
) -> (EngineRecommendation, Vec<AlternativeStrategy>) {
    // Check if we have performance history for this query type
    let has_history = tracker.has_history_for(query_type);

    let (primary_engine, reason, strategy, estimated_speedup, scan_engine, process_engine) =
        if has_history {
            // Use historical data to pick the best engine
            let df_avg = tracker.avg_latency("Rake DataFusion", query_type);
            let dk_avg = tracker.avg_latency("Rake DuckDB", query_type)
                .or_else(|| tracker.avg_latency("Rake DuckDB (S3 direct)", query_type));
            let pl_avg = tracker.avg_latency("Rake Polars", query_type)
                .or_else(|| tracker.avg_latency("Rake Polars (via DataFusion S3)", query_type));
            let trino_avg = tracker.avg_latency("Trino", query_type);

            // Find the fastest Rake engine
            let mut candidates: Vec<(&str, f64)> = Vec::new();
            if let Some(avg) = df_avg {
                candidates.push(("Rake DataFusion", avg));
            }
            if let Some(avg) = dk_avg {
                if duckdb_available {
                    candidates.push(("Rake DuckDB", avg));
                }
            }
            if let Some(avg) = pl_avg {
                if polars_available {
                    candidates.push(("Rake Polars", avg));
                }
            }

            if candidates.is_empty() {
                // No successful history, fall through to defaults
                default_recommendation(query_type, duckdb_available)
            } else {
                candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                let (best_engine, best_avg) = candidates[0];
                let speedup = trino_avg
                    .map(|t| if best_avg > 0.0 { (t / best_avg * 100.0).round() / 100.0 } else { 1.0 })
                    .unwrap_or(1.0);

                // Check if scan_handoff would be better:
                // If DuckDB scans faster but DataFusion joins better
                let should_scan_handoff = query_type == "join" || query_type == "complex_join";
                if should_scan_handoff && dk_avg.is_some() && df_avg.is_some() {
                    let dk = dk_avg.unwrap();
                    let df = df_avg.unwrap();
                    // If DuckDB is at least 2x faster for scan-like queries
                    // but DataFusion is better at joins overall
                    let dk_scan_avg = tracker.avg_latency("Rake DuckDB", "scan_aggregate")
                        .or_else(|| tracker.avg_latency("Rake DuckDB", "full_scan"));
                    let df_scan_avg = tracker.avg_latency("Rake DataFusion", "scan_aggregate")
                        .or_else(|| tracker.avg_latency("Rake DataFusion", "full_scan"));

                    if let (Some(dk_s), Some(df_s)) = (dk_scan_avg, df_scan_avg) {
                        if dk_s < df_s * 0.5 && df < dk {
                            // DuckDB scans 2x+ faster, DataFusion joins faster
                            let reason_str = format!(
                                "Hybrid strategy: DuckDB scans {:.1}x faster, DataFusion joins {:.1}x faster. \
                                 DuckDB handles table scans, hands off Arrow batches to DataFusion for join processing.",
                                df_s / dk_s, dk / df
                            );
                            return (
                                EngineRecommendation {
                                    strategy: "scan_handoff".to_string(),
                                    primary_engine: "Rake DataFusion".to_string(),
                                    reason: reason_str,
                                    estimated_speedup: speedup,
                                    scan_engine: Some("Rake DuckDB".to_string()),
                                    process_engine: Some("Rake DataFusion".to_string()),
                                },
                                build_alternatives(query_type),
                            );
                        }
                    }
                }

                let reason_str = format!(
                    "{} has the lowest average latency ({:.1}ms) for {} queries based on {} previous executions.{}",
                    best_engine,
                    best_avg,
                    query_type,
                    candidates.len(),
                    if speedup > 1.0 { format!(" {:.0}x faster than Trino.", speedup) } else { String::new() }
                );

                (
                    best_engine.to_string(),
                    reason_str,
                    "single_engine".to_string(),
                    speedup,
                    None,
                    None,
                )
            }
        } else {
            // No history: use defaults
            default_recommendation(query_type, duckdb_available)
        };

    let recommendation = EngineRecommendation {
        strategy,
        primary_engine,
        reason,
        estimated_speedup,
        scan_engine,
        process_engine,
    };

    (recommendation, build_alternatives(query_type))
}

/// Default engine recommendation when no performance history exists.
fn default_recommendation(
    query_type: &str,
    duckdb_available: bool,
) -> (String, String, String, f64, Option<String>, Option<String>) {
    match query_type {
        "scan_aggregate" => {
            let engine = if duckdb_available { "Rake DuckDB" } else { "Rake DataFusion" };
            (
                engine.to_string(),
                format!(
                    "{} is the default choice for scan+aggregate queries. {} excels at columnar scans with aggregations.",
                    engine,
                    if duckdb_available { "DuckDB" } else { "DataFusion" },
                ),
                "single_engine".to_string(),
                1.0,
                None,
                None,
            )
        }
        "complex_join" | "join" => (
            "Rake DataFusion".to_string(),
            "DataFusion is the default choice for join queries. Its optimizer handles multi-table joins \
             with predicate pushdown and join reordering."
                .to_string(),
            "single_engine".to_string(),
            1.0,
            None,
            None,
        ),
        "point_lookup" => {
            let engine = if duckdb_available { "Rake DuckDB" } else { "Rake DataFusion" };
            (
                engine.to_string(),
                format!(
                    "{} is the default for point lookups. Single-row fetches are fastest on {}.",
                    engine,
                    if duckdb_available { "DuckDB's in-memory store" } else { "DataFusion" },
                ),
                "single_engine".to_string(),
                1.0,
                None,
                None,
            )
        }
        "full_scan" | "ordered_scan" => {
            let engine = if duckdb_available { "Rake DuckDB" } else { "Rake DataFusion" };
            (
                engine.to_string(),
                format!(
                    "{} is the default for full/ordered scans. {} provides fast sequential scan throughput.",
                    engine,
                    if duckdb_available { "DuckDB's httpfs" } else { "DataFusion" },
                ),
                "single_engine".to_string(),
                1.0,
                None,
                None,
            )
        }
        _ => (
            "Rake DataFusion".to_string(),
            "DataFusion is the default engine for unclassified query types.".to_string(),
            "single_engine".to_string(),
            1.0,
            None,
            None,
        ),
    }
}

/// Build alternative strategy suggestions based on query type.
fn build_alternatives(query_type: &str) -> Vec<AlternativeStrategy> {
    let mut alts = Vec::new();

    match query_type {
        "scan_aggregate" | "full_scan" | "ordered_scan" => {
            alts.push(AlternativeStrategy {
                strategy: "parallel_fanout".to_string(),
                description: "Split partitions across DuckDB + Polars, DataFusion merges".to_string(),
                when: "Use for very large scans (>1GB) with simple aggregations".to_string(),
            });
            alts.push(AlternativeStrategy {
                strategy: "scan_handoff".to_string(),
                description: "DuckDB scans S3 -> Arrow handoff -> DataFusion joins".to_string(),
                when: "Use when query involves joins across multiple tables".to_string(),
            });
        }
        "join" | "complex_join" => {
            alts.push(AlternativeStrategy {
                strategy: "scan_handoff".to_string(),
                description: "DuckDB scans S3 -> Arrow handoff -> DataFusion joins".to_string(),
                when: "Use when scan component is the bottleneck (large tables with selective joins)".to_string(),
            });
            alts.push(AlternativeStrategy {
                strategy: "parallel_fanout".to_string(),
                description: "Split partitions across DuckDB + Polars, DataFusion merges".to_string(),
                when: "Use for hash-partitioned tables where each partition can be joined independently".to_string(),
            });
        }
        "point_lookup" => {
            alts.push(AlternativeStrategy {
                strategy: "single_engine".to_string(),
                description: "Route directly to DuckDB for sub-millisecond point lookups".to_string(),
                when: "Default for point lookups — no orchestration overhead needed".to_string(),
            });
        }
        _ => {}
    }

    alts
}

