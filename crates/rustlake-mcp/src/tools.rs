use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars, tool, tool_router,
    ErrorData as McpError,
};

use crate::server::RustLakeMcp;

// ── Parameter Structs ──────────────────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SqlQueryParams {
    /// SQL query to execute
    pub sql: String,
    /// Query engine: "auto" (default), "datafusion", "duckdb", or "polars"
    pub engine: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SqlExplainParams {
    /// SQL query to get the execution plan for
    pub sql: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct QueryHistoryParams {
    /// Maximum number of history entries to return (default 20)
    pub limit: Option<usize>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TableNameParams {
    /// Table name (e.g. "pg.orders", "mongo.users", "s3_warehouse_data")
    pub name: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ConnectionIdParams {
    /// Connection UUID
    pub id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct PipelineIdParams {
    /// Pipeline UUID
    pub id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct S3BrowseParams {
    /// S3 config UUID
    pub id: String,
    /// S3 prefix path to browse (e.g. "warehouse/cdc/"). Defaults to root.
    pub prefix: Option<String>,
}

// ── Action Parameter Structs ────────────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CreatePipelineParams {
    /// Pipeline name (e.g. "mongo-cdc-orders")
    pub name: String,
    /// Source type: "mongodb-cdc", "kafka", "postgres-cdc"
    pub source_type: String,
    /// Source config JSON: { "connection_id": "...", "database": "...", "collection": "..." } for CDC, or { "brokers": "...", "topic": "..." } for Kafka
    pub source_config: serde_json::Value,
    /// Sink table/path (e.g. "s3://bucket/path" or table name)
    pub sink_table: String,
    /// Optional SQL transform to apply to incoming events
    pub transform_sql: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CreateGlacierParams {
    /// Glacier table name (e.g. "cdc_events_glacier")
    pub table_name: String,
    /// Transform type: "sql" or "rust"
    pub transform_type: String,
    /// SQL query or Rust code that produces the table
    pub source_code: String,
    /// Upstream table dependencies (e.g. ["mongo.orders", "mongo.customers"])
    pub input_tables: Vec<String>,
    /// Optional cron schedule (e.g. "0 * * * *" for hourly)
    pub schedule: Option<String>,
    /// Optional quality gates as JSON array: [{"gate_type":"not_null","column":"id","description":"..."}]
    pub quality_gates: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CreateGlacierFromPipelineParams {
    /// Pipeline UUID to promote to a Glacier table
    pub pipeline_id: String,
    /// Optional glacier name (auto-derived from pipeline if omitted)
    pub name: Option<String>,
    /// Optional SQL transform on the pipeline's sink data
    pub transform_sql: Option<String>,
    /// Optional quality gates
    pub quality_gates: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CreateConnectionParams {
    /// Connection name (e.g. "my-postgres")
    pub name: String,
    /// Type: "postgres", "mysql", "mongodb", "trino"
    pub conn_type: String,
    /// Host address
    pub host: String,
    /// Port number
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// Password (optional)
    pub password: Option<String>,
}

// ── Helper ─────────────────────────────────────────────────────────

fn ok(json: serde_json::Value) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string());
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

fn err(msg: String) -> Result<CallToolResult, McpError> {
    Ok(CallToolResult::error(vec![Content::text(msg)]))
}

// ── Tool Implementations ───────────────────────────────────────────

#[tool_router]
impl RustLakeMcp {
    pub fn new(api_url: String) -> Self {
        Self {
            client: crate::client::RustLakeClient::new(api_url),
            tool_router: Self::tool_router(),
        }
    }

    // ── Query & Debug ──────────────────────────────────────────

    #[tool(description = "Execute a SQL query against RustLake. Returns columns, rows, timing, and engine used. Use engine param to target a specific engine.")]
    async fn sql_query(
        &self,
        Parameters(p): Parameters<SqlQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let engine = p.engine.unwrap_or_else(|| "auto".to_string());
        match self.client.sql_query(&p.sql, &engine).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get the execution plan for a SQL query without running it. Shows how DataFusion will optimize and execute the query.")]
    async fn sql_explain(
        &self,
        Parameters(p): Parameters<SqlExplainParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.sql_explain(&p.sql).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get recent query execution history with SQL text, timing, engine used, row counts, and errors.")]
    async fn query_history(
        &self,
        Parameters(p): Parameters<QueryHistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = p.limit.unwrap_or(20);
        match self.client.query_history(limit).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Schema & Discovery ─────────────────────────────────────

    #[tool(description = "List all registered tables across all schemas (pg, mysql, mongo, s3, etc). Shows table names grouped by source.")]
    async fn list_tables(&self) -> Result<CallToolResult, McpError> {
        match self.client.list_tables().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get the schema (column names, data types, nullability) for a specific table.")]
    async fn table_schema(
        &self,
        Parameters(p): Parameters<TableNameParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.table_schema(&p.name).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get sample rows from a table (up to 100 rows). Useful for understanding data shape and content.")]
    async fn table_preview(
        &self,
        Parameters(p): Parameters<TableNameParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.table_preview(&p.name).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get statistics for a table: row count, column count, and per-column min/max/null counts.")]
    async fn table_stats(
        &self,
        Parameters(p): Parameters<TableNameParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.table_stats(&p.name).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Connections ────────────────────────────────────────────

    #[tool(description = "List all data source connections (PostgreSQL, MySQL, MongoDB, Trino, S3) with their status, table counts, and sync state.")]
    async fn list_connections(&self) -> Result<CallToolResult, McpError> {
        match self.client.list_connections().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get detailed status for a specific data source connection by its UUID.")]
    async fn connection_status(
        &self,
        Parameters(p): Parameters<ConnectionIdParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.connection_status(&p.id).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Streaming & CDC ────────────────────────────────────────

    #[tool(description = "List all streaming/CDC pipelines with status, phase (snapshotting/streaming), event counts, and sink info.")]
    async fn list_pipelines(&self) -> Result<CallToolResult, McpError> {
        match self.client.list_pipelines().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get detailed status for a specific streaming pipeline by UUID. Shows events processed, files written, phase, and source config.")]
    async fn pipeline_status(
        &self,
        Parameters(p): Parameters<PipelineIdParams>,
    ) -> Result<CallToolResult, McpError> {
        // Filter from the full list since there's no single-pipeline GET endpoint
        match self.client.list_pipelines().await {
            Ok(json) => {
                if let Some(pipelines) = json.get("pipelines").and_then(|v| v.as_array()) {
                    if let Some(pipeline) = pipelines.iter().find(|pl| {
                        pl.get("id").and_then(|v| v.as_str()) == Some(&p.id)
                    }) {
                        ok(pipeline.clone())
                    } else {
                        err(format!("Pipeline {} not found", p.id))
                    }
                } else {
                    ok(json)
                }
            }
            Err(e) => err(e),
        }
    }

    // ── Glaciers (Executable Tables) ───────────────────────────

    #[tool(description = "List all Glacier (executable) tables with health status, freshness, transform type, and version count.")]
    async fn list_glaciers(&self) -> Result<CallToolResult, McpError> {
        match self.client.list_executable_tables().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get full detail for a Glacier table: transform code, versions, quality gates, execution history, cost tracking, and properties.")]
    async fn glacier_detail(
        &self,
        Parameters(p): Parameters<TableNameParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.executable_table_properties(&p.name).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get column-level data lineage for a Glacier table. Shows which source columns feed into each output column.")]
    async fn glacier_lineage(
        &self,
        Parameters(p): Parameters<TableNameParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.column_lineage(&p.name).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── System ─────────────────────────────────────────────────

    #[tool(description = "Get RustLake server info: version, uptime, total queries executed, engine versions, and node role.")]
    async fn system_info(&self) -> Result<CallToolResult, McpError> {
        match self.client.system_info().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get system resource usage: CPU cores, memory, disk, and runtime metrics.")]
    async fn system_resources(&self) -> Result<CallToolResult, McpError> {
        match self.client.system_resources().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "List available query engines (DataFusion, DuckDB, Polars) with version, status, and capabilities.")]
    async fn list_engines(&self) -> Result<CallToolResult, McpError> {
        match self.client.list_engines().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Scheduling ─────────────────────────────────────────────

    #[tool(description = "List all scheduled jobs with cron expressions, engine assignment, and last run status.")]
    async fn list_schedules(&self) -> Result<CallToolResult, McpError> {
        match self.client.list_schedules().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Get recent scheduled job execution history showing run times, durations, and success/failure status.")]
    async fn schedule_runs(&self) -> Result<CallToolResult, McpError> {
        match self.client.schedule_runs().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── S3 Storage ─────────────────────────────────────────────

    #[tool(description = "Browse S3 bucket contents at a given prefix. Returns files and directories with sizes and timestamps.")]
    async fn s3_browse(
        &self,
        Parameters(p): Parameters<S3BrowseParams>,
    ) -> Result<CallToolResult, McpError> {
        let prefix = p.prefix.unwrap_or_default();
        match self.client.s3_browse(&p.id, &prefix).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Pipeline Actions ───────────────────────────────────────

    #[tool(description = "Create a new streaming/CDC pipeline. Specify source_type ('mongodb-cdc', 'kafka'), source_config JSON, and sink_table.")]
    async fn create_pipeline(
        &self,
        Parameters(p): Parameters<CreatePipelineParams>,
    ) -> Result<CallToolResult, McpError> {
        let body = serde_json::json!({
            "name": p.name,
            "source_type": p.source_type,
            "source_config": p.source_config,
            "sink_table": p.sink_table,
            "transform_sql": p.transform_sql,
        });
        match self.client.create_pipeline(&body).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Start a streaming/CDC pipeline by its UUID. Begins consuming events from the source.")]
    async fn start_pipeline(
        &self,
        Parameters(p): Parameters<PipelineIdParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.start_pipeline(&p.id).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Stop a running streaming/CDC pipeline by its UUID.")]
    async fn stop_pipeline(
        &self,
        Parameters(p): Parameters<PipelineIdParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.stop_pipeline(&p.id).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Delete a streaming/CDC pipeline by its UUID.")]
    async fn delete_pipeline(
        &self,
        Parameters(p): Parameters<PipelineIdParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.delete_pipeline(&p.id).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Glacier Actions ────────────────────────────────────────

    #[tool(description = "Create a new Glacier (executable table) with a SQL or Rust transform, quality gates, and optional schedule. The transform runs against registered tables to produce a versioned, quality-gated output.")]
    async fn create_glacier(
        &self,
        Parameters(p): Parameters<CreateGlacierParams>,
    ) -> Result<CallToolResult, McpError> {
        let now = chrono_now();
        let hash = simple_hash(&p.source_code);
        let gates: Vec<serde_json::Value> = p.quality_gates.unwrap_or_default();
        let body = serde_json::json!({
            "table_name": p.table_name,
            "table_location": format!("s3://warehouse/{}", p.table_name),
            "transform": {
                "transform_type": p.transform_type,
                "source_code": p.source_code,
                "source_hash": hash,
                "binary_path": null,
                "binary_size": null,
                "binary_cached": false,
                "compiler_version": null,
                "target_arch": null,
            },
            "schedule": p.schedule,
            "quality_gates": gates,
            "input_tables": p.input_tables,
            "status": {
                "state": "active",
                "health": "healthy",
                "last_error": null,
                "staleness_hours": 0.0,
                "data_freshness": "unknown",
            },
            "history": [],
            "versions": [{
                "version": 1,
                "source_code": p.source_code,
                "source_hash": hash,
                "created_at": now,
                "created_by": "mcp",
                "change_description": "Initial version created via MCP",
                "binary_size_bytes": null,
                "snapshot_ids": [],
            }],
            "created_at": now,
            "last_refresh": null,
            "next_refresh": null,
            "estimated_cost_usd": 0.0,
            "total_executions": 0,
            "total_cost_usd": 0.0,
            "incremental": false,
            "watermark_column": null,
            "last_watermark": null,
            "executions_skipped": 0,
            "cost_saved_usd": 0.0,
            "auto_refresh": false,
            "refresh_interval_seconds": 0,
        });
        match self.client.create_executable_table(&body).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Promote a streaming pipeline to a Glacier table. Creates a versioned executable table backed by the pipeline's sink data.")]
    async fn create_glacier_from_pipeline(
        &self,
        Parameters(p): Parameters<CreateGlacierFromPipelineParams>,
    ) -> Result<CallToolResult, McpError> {
        let body = serde_json::json!({
            "pipeline_id": p.pipeline_id,
            "name": p.name,
            "transform_sql": p.transform_sql,
            "quality_gates": p.quality_gates.unwrap_or_default(),
        });
        match self.client.create_glacier_from_pipeline(&body).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Execute a Glacier table's transform. Runs the SQL/Rust code, validates quality gates, and records a new execution in history.")]
    async fn execute_glacier(
        &self,
        Parameters(p): Parameters<TableNameParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.execute_executable_table(&p.name).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    #[tool(description = "Cascade replay: topologically sort and re-execute all upstream Glacier tables, then the target. Validates quality gates and contracts at each step.")]
    async fn cascade_replay(
        &self,
        Parameters(p): Parameters<TableNameParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.client.cascade_replay(&p.name).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Connection Actions ─────────────────────────────────────

    #[tool(description = "Create a new data source connection. Supports postgres, mysql, mongodb, trino types.")]
    async fn create_connection(
        &self,
        Parameters(p): Parameters<CreateConnectionParams>,
    ) -> Result<CallToolResult, McpError> {
        let body = serde_json::json!({
            "name": p.name,
            "conn_type": p.conn_type,
            "host": p.host,
            "port": p.port,
            "database": p.database,
            "username": p.username,
            "password": p.password.unwrap_or_default(),
            "auth_method": "scram",
            "connection_string": "",
        });
        match self.client.create_connection(&body).await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }

    // ── Health ─────────────────────────────────────────────────

    #[tool(description = "Quick health check — returns ok if the RustLake API server is running.")]
    async fn health_check(&self) -> Result<CallToolResult, McpError> {
        match self.client.health().await {
            Ok(json) => ok(json),
            Err(e) => err(e),
        }
    }
}

// ── Utility Functions ──────────────────────────────────────────────

fn chrono_now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Simple ISO-ish timestamp without chrono dependency
    format!("2026-03-24T{:02}:{:02}:{:02}Z", (now / 3600) % 24, (now / 60) % 60, now % 60)
}

fn simple_hash(s: &str) -> String {
    // Simple non-crypto hash for source_hash field
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", h)
}
