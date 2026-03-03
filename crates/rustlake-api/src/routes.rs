use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, RecordBatch};
use axum::extract::{Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use rustlake_router::{EngineTarget, QueryClassifier, QueryType};
use rustlake_stream::connector::SimulatedSource;
use rustlake_stream::StreamingMetricsSnapshot;
use rustlake_transform::{Model, ModelConfig, SqlCompiler};

use crate::postgres::{self, PgConnParams};
use crate::state::{
    AppState, ChatMessage, ConnectionEntry, EventConfig, JobRunEntry, QueryHistoryEntry, S3Config,
    ScheduledJob, StreamingPipeline, UserTransform,
};

// ── Request / Response types ───────────────────────────────────────

/// Request body for the SQL execution endpoint.
#[derive(Deserialize)]
pub struct SqlRequest {
    /// The SQL query string to execute.
    pub sql: String,
    /// Target engine: "auto" (default), "datafusion", or "duckdb".
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
        .route("/api/v1/connections/{id}", delete(delete_connection))
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
        // S3/Object storage config
        .route(
            "/api/v1/storage/s3",
            get(list_s3_configs).post(add_s3_config),
        )
        .route("/api/v1/storage/s3/{id}", delete(delete_s3_config))
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

async fn execute_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> std::result::Result<Json<SqlResponse>, (StatusCode, Json<ErrorResponse>)> {
    let query_id = Uuid::new_v4();
    let start = Instant::now();

    tracing::info!(sql = %req.sql, %query_id, engine_choice = %req.engine, "Received SQL request");

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

    // Handle CTAS: CREATE TABLE <name> AS SELECT ...
    let sql_upper = req.sql.trim().to_uppercase();
    if sql_upper.starts_with("CREATE TABLE") && sql_upper.contains(" AS ") {
        return handle_ctas(state, req.sql, query_id, query_type, parse_ms, start).await;
    }

    // Determine target engine: explicit override > classifier recommendation
    let use_duckdb = determine_use_duckdb(&state, &req.engine, &classification.engine);
    let engine_name = if use_duckdb { "DuckDB" } else { "DataFusion" };

    // Execute via the selected engine
    let exec_start = Instant::now();
    let result = if use_duckdb {
        execute_via_duckdb(&state, &req.sql).await
    } else {
        let ctx = state.ctx.read().await;
        ctx.sql(&req.sql).await.map_err(|e| e.to_string())
    };
    let exec_ms = exec_start.elapsed().as_millis();
    let duration_ms = start.elapsed().as_millis();

    // Increment query counter
    state.query_count.fetch_add(1, Ordering::Relaxed);

    let batches = match result {
        Ok(batches) => batches,
        Err(e) => {
            // If DuckDB failed, try fallback to DataFusion
            if use_duckdb {
                tracing::warn!(error = %e, "DuckDB execution failed, falling back to DataFusion");
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

/// Determine whether to use DuckDB for this query.
fn determine_use_duckdb(state: &AppState, engine_choice: &str, recommended: &EngineTarget) -> bool {
    if !state.duckdb_available() {
        return false;
    }
    match engine_choice.to_lowercase().as_str() {
        "duckdb" => true,
        "datafusion" => false,
        _ => matches!(recommended, EngineTarget::DuckDb),
    }
}

/// Execute SQL via DuckDB engine, returning batches or error string.
async fn execute_via_duckdb(
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
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

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

    // Look up the table provider to get its Arrow schema directly
    let table_ref = datafusion::common::TableReference::bare(name.clone());
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

    let sql = format!("SELECT * FROM \"{}\" LIMIT 100", name);
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

    // Get the schema first
    let df_ctx = ctx.datafusion_ctx();
    let table_ref = datafusion::common::TableReference::bare(name.clone());
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
#[derive(Deserialize)]
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

/// POST /api/v1/connections — test connection, discover tables, store config.
async fn add_connection(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddConnectionRequest>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let params = PgConnParams {
        host: req.host.clone(),
        port: req.port,
        database: req.database.clone(),
        username: req.username.clone(),
        password: req.password.clone(),
    };

    // Test connection and discover tables
    let tables = postgres::connect_and_discover(&params).await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: e }),
        )
    })?;

    let id = Uuid::new_v4().to_string();
    let entry = ConnectionEntry {
        id: id.clone(),
        name: req.name.clone(),
        conn_type: req.conn_type.clone(),
        host: req.host.clone(),
        port: req.port,
        database: req.database.clone(),
        username: req.username.clone(),
        status: "connected".to_string(),
        tables: tables.clone(),
        created_at: Utc::now(),
        source: "user".to_string(),
    };

    // Store connection and password
    let password_clone = req.password.clone();
    state.connections.write().await.push(entry.clone());
    state
        .connection_passwords
        .write()
        .await
        .insert(id.clone(), req.password);

    tracing::info!(
        source = "user",
        id = %id,
        name = %req.name,
        conn_type = %req.conn_type,
        host = %req.host,
        port = req.port,
        database = %req.database,
        tables = tables.len(),
        "Data source added: {} ({} tables discovered)",
        req.name,
        tables.len()
    );

    // Auto-register all discovered tables into DataFusion as MemTables
    let mut registered = Vec::new();
    let params_for_reg = PgConnParams {
        host: req.host.clone(),
        port: req.port,
        database: req.database.clone(),
        username: req.username.clone(),
        password: password_clone,
    };
    for table_name in &tables {
        match postgres::fetch_table_as_arrow(&params_for_reg, table_name).await {
            Ok(batch) => {
                let schema = batch.schema();
                let row_count = batch.num_rows();
                match datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                    Ok(mem_table) => {
                        let df_name = format!("pg_{}", table_name);
                        let ctx = state.ctx.read().await;
                        match ctx.datafusion_ctx().register_table(
                            &df_name,
                            std::sync::Arc::new(mem_table),
                        ) {
                            Ok(_) => {
                                tracing::info!(table = %df_name, rows = row_count, "Registered table into DataFusion");
                                registered.push(df_name);
                            }
                            Err(e) => tracing::warn!(table = %df_name, error = %e, "Failed to register table"),
                        }
                    }
                    Err(e) => tracing::warn!(table = %table_name, error = %e, "Failed to create MemTable"),
                }
            }
            Err(e) => tracing::warn!(table = %table_name, error = %e, "Failed to fetch table"),
        }
    }

    Ok(Json(serde_json::json!({
        "status": "connected",
        "id": id,
        "name": req.name,
        "tables": tables,
        "registered": registered,
    })))
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
    let mut connections = state.connections.write().await;

    // Find the connection before removing so we can log details
    let removed = connections.iter().find(|c| c.id == id).cloned();

    let initial_len = connections.len();
    connections.retain(|c| c.id != id);

    if connections.len() == initial_len {
        tracing::warn!(id = %id, "Data source removal failed: not found");
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Connection '{}' not found", id),
            }),
        ));
    }

    drop(connections);
    state.connection_passwords.write().await.remove(&id);

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

/// POST /api/v1/connections/:id/register/:table — snapshot a PG table into DataFusion as a MemTable.
async fn register_external_table(
    State(state): State<Arc<AppState>>,
    Path((id, table_name)): Path<(String, String)>,
) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Look up connection
    let connections = state.connections.read().await;
    let conn = connections.iter().find(|c| c.id == id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Connection '{}' not found", id),
            }),
        )
    })?;

    let passwords = state.connection_passwords.read().await;
    let password = passwords.get(&id).cloned().unwrap_or_default();

    let params = PgConnParams {
        host: conn.host.clone(),
        port: conn.port,
        database: conn.database.clone(),
        username: conn.username.clone(),
        password,
    };

    drop(connections);
    drop(passwords);

    // Fetch the table as an Arrow RecordBatch
    let batch = postgres::fetch_table_as_arrow(&params, &table_name)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: e }),
            )
        })?;

    let row_count = batch.num_rows();
    let schema = batch.schema();

    // Register as a MemTable in DataFusion
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).map_err(
        |e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to create MemTable: {}", e),
                }),
            )
        },
    )?;

    // Prefix with "pg_" to distinguish from local tables
    let df_table_name = format!("pg_{}", table_name);

    let ctx = state.ctx.read().await;
    ctx.datafusion_ctx()
        .register_table(&df_table_name, std::sync::Arc::new(mem_table))
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to register table: {}", e),
                }),
            )
        })?;

    tracing::info!(
        table = %df_table_name,
        rows = row_count,
        "External Postgres table registered in DataFusion"
    );

    Ok(Json(serde_json::json!({
        "status": "ok",
        "table": df_table_name,
        "source_table": table_name,
        "row_count": row_count,
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
    let mut pg_tables_registered = Vec::new();

    // Try Postgres (config via env vars, fallback to Docker defaults)
    let pg_host = std::env::var("RUSTLAKE_PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let pg_port: u16 = std::env::var("RUSTLAKE_PG_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(5433);
    let pg_db = std::env::var("RUSTLAKE_PG_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
    let pg_user = std::env::var("RUSTLAKE_PG_USER").unwrap_or_else(|_| "rustlake".to_string());
    let pg_pass = std::env::var("RUSTLAKE_PG_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

    let pg_params = crate::postgres::PgConnParams {
        host: pg_host.clone(),
        port: pg_port,
        database: pg_db.clone(),
        username: pg_user.clone(),
        password: pg_pass,
    };

    match crate::postgres::connect_and_discover(&pg_params).await {
        Ok(tables) => {
            let entry = ConnectionEntry {
                id: "bootstrap-postgres".to_string(),
                name: "Docker Postgres".to_string(),
                conn_type: "postgres".to_string(),
                host: pg_host,
                port: pg_port,
                database: pg_db,
                username: pg_user,
                status: "connected".to_string(),
                tables: tables.clone(),
                created_at: Utc::now(),
                source: "bootstrap".to_string(),
            };
            let is_new = state.seed_connection(entry, "rustlake".to_string()).await;
            if is_new {
                tracing::info!(
                    source = "bootstrap",
                    conn_type = "postgres",
                    host = "localhost",
                    port = 5433,
                    database = "rustlake_demo",
                    tables = tables.len(),
                    "Data source added: Docker Postgres ({} tables discovered)",
                    tables.len()
                );
            }

            // Register tables into DataFusion
            for table_name in &tables {
                match crate::postgres::fetch_table_as_arrow(&pg_params, table_name).await {
                    Ok(batch) => {
                        let schema = batch.schema();
                        if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                            let df_name = format!("pg_{}", table_name);
                            let ctx = state.ctx.read().await;
                            if ctx.datafusion_ctx().register_table(&df_name, std::sync::Arc::new(mem_table)).is_ok() {
                                pg_tables_registered.push(df_name);
                            }
                        }
                    }
                    Err(_) => {}
                }
            }
            results.insert("postgres".to_string(), serde_json::json!({
                "status": "connected",
                "tables_discovered": tables.len(),
                "tables_registered": pg_tables_registered,
            }));
        }
        Err(e) => {
            results.insert("postgres".to_string(), serde_json::json!({
                "status": "unavailable",
                "error": e,
            }));
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
            });
            results.insert("minio".to_string(), serde_json::json!({ "status": "configured" }));
        } else {
            results.insert("minio".to_string(), serde_json::json!({ "status": "already_configured" }));
        }
    }

    // Try MySQL (config via env vars, fallback to Docker defaults)
    let mysql_host = std::env::var("RUSTLAKE_MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mysql_port: u16 = std::env::var("RUSTLAKE_MYSQL_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(3307);
    let mysql_db = std::env::var("RUSTLAKE_MYSQL_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
    let mysql_user = std::env::var("RUSTLAKE_MYSQL_USER").unwrap_or_else(|_| "rustlake".to_string());
    let mysql_pass = std::env::var("RUSTLAKE_MYSQL_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

    let mysql_params = crate::mysql_conn::MysqlConnParams {
        host: mysql_host.clone(),
        port: mysql_port,
        database: mysql_db.clone(),
        username: mysql_user.clone(),
        password: mysql_pass,
    };

    match crate::mysql_conn::connect_and_discover(&mysql_params).await {
        Ok(tables) => {
            let entry = ConnectionEntry {
                id: "bootstrap-mysql".to_string(),
                name: "Docker MySQL".to_string(),
                conn_type: "mysql".to_string(),
                host: mysql_host,
                port: mysql_port,
                database: mysql_db,
                username: mysql_user,
                status: "connected".to_string(),
                tables: tables.clone(),
                created_at: Utc::now(),
                source: "bootstrap".to_string(),
            };
            state.seed_connection(entry, "rustlake".to_string()).await;

            let mut mysql_tables_registered = Vec::new();
            for table_name in &tables {
                match crate::mysql_conn::fetch_table_as_arrow(&mysql_params, table_name).await {
                    Ok(batch) => {
                        let schema = batch.schema();
                        if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                            let df_name = format!("mysql_{}", table_name);
                            let ctx = state.ctx.read().await;
                            if ctx.datafusion_ctx().register_table(&df_name, std::sync::Arc::new(mem_table)).is_ok() {
                                mysql_tables_registered.push(df_name);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(table = table_name, error = %e, "Bootstrap: failed to fetch MySQL table");
                    }
                }
            }
            results.insert("mysql".to_string(), serde_json::json!({
                "status": "connected",
                "tables_discovered": tables.len(),
                "tables_registered": mysql_tables_registered,
            }));
        }
        Err(e) => {
            results.insert("mysql".to_string(), serde_json::json!({
                "status": "unavailable",
                "error": e,
            }));
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
            };
            state.seed_connection(entry, "rustlake".to_string()).await;

            let mut mongo_tables_registered = Vec::new();
            for coll_name in &collections {
                match crate::mongodb_conn::fetch_collection_as_arrow(&mongo_params, coll_name).await {
                    Ok(batch) => {
                        let schema = batch.schema();
                        if let Ok(mem_table) = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                            let df_name = format!("mongo_{}", coll_name);
                            let ctx = state.ctx.read().await;
                            if ctx.datafusion_ctx().register_table(&df_name, std::sync::Arc::new(mem_table)).is_ok() {
                                mongo_tables_registered.push(df_name);
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

fn batches_to_json(
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
            job.last_run = Some(now);
            drop(jobs);

            let result = match job_type.as_str() {
                "transform" => run_transform_job(&state, &target, &run_start).await,
                "sql" => run_sql_job(&state, &target, &run_start).await,
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

/// Execute a raw SQL job directly via DataFusion.
async fn run_sql_job(
    state: &Arc<AppState>,
    sql: &str,
    _run_start: &Instant,
) -> Result<String, String> {
    let ctx = state.ctx.read().await;
    let batches = ctx.sql(sql).await.map_err(|e| e.to_string())?;
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
    Ok(Json(serde_json::json!({
        "status": "deleted",
        "id": id,
    })))
}

// ── S3 / Object Storage Config Handlers ─────────────────────────────

#[derive(Deserialize)]
struct AddS3ConfigRequest {
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
        secret_key: req.secret_key,
        bucket: req.bucket.clone(),
        region: req.region.clone(),
        status: "configured".to_string(),
        created_at: Utc::now(),
    };

    let mut configs = state.s3_configs.write().await;
    configs.push(config);

    Ok(Json(serde_json::json!({
        "status": "ok",
        "name": req.name,
        "endpoint": req.endpoint,
        "bucket": req.bucket,
        "message": "S3 configuration saved. Use s3://<bucket>/<path> in Register Table to access objects."
    })))
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
    let addr_str = format!("{}:{}", req.host, port);
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
            message: format!("Cannot resolve host '{}'", req.host),
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
            message: format!("Cannot reach {}:{}", req.host, port),
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
        "clickhouse" | "neo4j" | "cassandra" | "scylladb" => vec!["username".into()],
        _ => vec![],
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
        let _ = &state; // suppress unused warning
        engines.push(serde_json::json!({
            "name": "DuckDB",
            "version": "N/A",
            "status": "not_compiled",
            "default": false,
            "description": "Compile with --features duckdb to enable"
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

    // Calculate speedup
    let df_ms = df_result["duration_ms"].as_f64().unwrap_or(1.0);
    let dk_ms = duck_result["duration_ms"].as_f64().unwrap_or(1.0);
    let speedup = if dk_ms > 0.0 { df_ms / dk_ms } else { 1.0 };
    let winner = if df_ms <= dk_ms { "DataFusion" } else { "DuckDB" };

    Ok(Json(serde_json::json!({
        "query_id": query.id,
        "query_name": query.name,
        "datafusion": df_result,
        "duckdb": duck_result,
        "speedup": speedup,
        "winner": winner,
    })))
}
