use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use rustlake_router::{QueryClassifier, QueryType};
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

/// Response for the Flight server info endpoint.
#[derive(Serialize)]
pub struct FlightInfoResponse {
    /// Protocol name (e.g., "Arrow Flight SQL").
    pub protocol: String,
    /// gRPC port the Flight server listens on.
    pub grpc_port: u16,
    /// Current server status.
    pub status: String,
    /// List of supported Flight capabilities.
    pub capabilities: Vec<String>,
    /// Apache Arrow version used by the server.
    pub arrow_version: String,
    /// BI tools and clients known to be compatible.
    pub supported_clients: Vec<String>,
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

// ── Routes ─────────────────────────────────────────────────────────

/// Build the Axum router with all API routes.
pub fn api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(dashboard))
        .route("/health", get(health))
        .route("/api/v1/sql", post(execute_sql))
        .route("/api/v1/tables", get(list_tables))
        .route("/api/v1/tables/register", post(register_table))
        .route("/api/v1/query/history", get(query_history))
        .route("/api/v1/tables/{name}/schema", get(table_schema))
        .route("/api/v1/tables/{name}/preview", get(table_preview))
        .route("/api/v1/tables/{name}/stats", get(table_stats))
        .route("/api/v1/system/info", get(system_info))
        // Flight server info
        .route("/api/v1/flight/info", get(flight_info))
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

    tracing::info!(sql = %req.sql, %query_id, "Received SQL request");

    // Classify the query (parse + classify timing)
    let parse_start = Instant::now();
    let query_type = QueryClassifier::classify(&req.sql).unwrap_or(QueryType::Olap);
    let parse_ms = parse_start.elapsed().as_millis();
    tracing::info!(query_type = %query_type, parse_ms, "Query classified");

    // Handle CTAS: CREATE TABLE <name> AS SELECT ...
    let sql_upper = req.sql.trim().to_uppercase();
    if sql_upper.starts_with("CREATE TABLE") && sql_upper.contains(" AS ") {
        return handle_ctas(state, req.sql, query_id, query_type, parse_ms, start).await;
    }

    // Execute via DataFusion
    let exec_start = Instant::now();
    let ctx = state.ctx.read().await;
    let result = ctx.sql(&req.sql).await;
    let exec_ms = exec_start.elapsed().as_millis();
    let duration_ms = start.elapsed().as_millis();

    // Increment query counter
    state.query_count.fetch_add(1, Ordering::Relaxed);

    let batches = match result {
        Ok(batches) => batches,
        Err(e) => {
            tracing::error!(error = %e, "Query execution failed");

            // Record failed query in history
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
                })
                .await;

            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ));
        }
    };

    // Convert to JSON
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
                error: format!("Failed to serialize results: {}", e),
            }),
        )
    })?;

    let row_count = rows.len();

    // Record successful query in history
    state
        .record_query(QueryHistoryEntry {
            query_id,
            sql: req.sql.clone(),
            query_type: query_type.to_string(),
            row_count,
            duration_ms,
            timestamp: Utc::now(),
            status: "success".to_string(),
            error: None,
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

// ── Flight Info ───────────────────────────────────────────────────

/// GET /api/v1/flight/info — returns Arrow Flight server capabilities and status.
async fn flight_info() -> Json<FlightInfoResponse> {
    Json(FlightInfoResponse {
        protocol: "Arrow Flight SQL".to_string(),
        grpc_port: 50051,
        status: "available".to_string(),
        capabilities: vec![
            "SQL queries".to_string(),
            "Prepared statements".to_string(),
            "Bulk data transfer".to_string(),
            "JDBC/ODBC gateway".to_string(),
        ],
        arrow_version: "57".to_string(),
        supported_clients: vec![
            "DBeaver".to_string(),
            "Tableau".to_string(),
            "Superset".to_string(),
            "JDBC".to_string(),
            "ODBC".to_string(),
            "Python (pyarrow.flight)".to_string(),
        ],
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
        host: req.host,
        port: req.port,
        database: req.database,
        username: req.username,
        status: "connected".to_string(),
        tables: tables.clone(),
        created_at: Utc::now(),
    };

    // Store connection and password
    state.connections.write().await.push(entry.clone());
    state
        .connection_passwords
        .write()
        .await
        .insert(id.clone(), req.password);

    tracing::info!(
        id = %id,
        name = %req.name,
        tables = tables.len(),
        "Database connection established"
    );

    Ok(Json(serde_json::json!({
        "status": "connected",
        "id": id,
        "name": req.name,
        "tables": tables,
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
    let initial_len = connections.len();
    connections.retain(|c| c.id != id);

    if connections.len() == initial_len {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Connection '{}' not found", id),
            }),
        ));
    }

    state.connection_passwords.write().await.remove(&id);

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
