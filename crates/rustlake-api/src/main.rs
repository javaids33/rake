use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::Level;
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::EnvFilter;

use rustlake_core::config::NodeRole;
use rustlake_core::RustLakeConfig;
use rustlake_engine::RustLakeContext;
use rustlake_flight::coordinator::Coordinator;
use rustlake_flight::discovery;
use rustlake_flight::server::{FlightMetrics, RustLakeFlightService};
use rustlake_flight::worker::WorkerNode;
use rustlake_vector::embedding::SimpleEmbeddingGenerator;
use rustlake_vector::search::VectorIndex;

mod iceberg_s3;
mod mongodb_cdc;
mod mongodb_conn;
mod neo4j_conn;
mod notebook_runner;
mod providers;
mod routes;
mod state;
mod trino_client;
mod trino_provider;
mod credential_store;
mod auth;
mod iceberg_metadata;
mod iceberg_maintenance;
mod executable_table;
mod iceberg_rest_catalog;
mod iceberg_writer;
mod parquet_sink;
mod quality_gates;
mod rust_executor;
mod spark_compat;
mod state_db;
mod ws;

use state::{
    load_chat_messages_from_file, load_scheduled_jobs_from_file, load_user_transforms_from_file,
    AppState, ConnectionEntry, S3Config, ScheduledJob, StreamingPipeline, UserTransform,
};

/// Pre-populate the vector index with product data from sample-data/products.csv.
fn load_product_vectors() -> VectorIndex {
    const DIMENSIONS: usize = 128;
    let generator = SimpleEmbeddingGenerator::new(DIMENSIONS);
    let mut index = VectorIndex::new(DIMENSIONS);

    let candidates = ["sample-data/products.csv", "../../sample-data/products.csv"];

    let csv_content = candidates
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok());

    let csv_content = match csv_content {
        Some(content) => content,
        None => {
            tracing::warn!("sample-data/products.csv not found — vector index will start empty");
            return index;
        }
    };

    let mut lines = csv_content.lines();
    let _header = lines.next();

    for line in lines {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 4 {
            continue;
        }

        let product_id = fields[0].trim();
        let name = fields[1].trim();
        let category = fields[2].trim();
        let price = fields[3].trim();

        let text = format!("{} {}", name, category);
        let embedding = generator.generate_embedding(&text);

        let metadata = serde_json::json!({
            "product_id": product_id,
            "category": category,
            "price": price,
        });

        index.add(
            product_id.to_string(),
            name.to_string(),
            embedding,
            metadata,
        );
    }

    tracing::info!(
        documents = index.len(),
        dimensions = DIMENSIONS,
        "Vector index pre-populated from products.csv"
    );

    index
}

/// Auto-bootstrap demo connections and seed data on startup.
///
/// Runs in a background task so it doesn't block server startup.
/// Idempotent — skips items that already exist.
async fn bootstrap_demo_connections(state: Arc<AppState>) {
    // ── 1. Try connecting to Postgres (federated provider) ──────
    #[cfg(feature = "postgres")]
    {
        let pg_host = std::env::var("RUSTLAKE_PG_HOST").unwrap_or_else(|_| "localhost".to_string());
        let pg_port: u16 = std::env::var("RUSTLAKE_PG_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5433);
        let pg_db = std::env::var("RUSTLAKE_PG_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
        let pg_user = std::env::var("RUSTLAKE_PG_USER").unwrap_or_else(|_| "rustlake".to_string());
        let pg_pass = std::env::var("RUSTLAKE_PG_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

        let ctx = state.ctx.read().await;
        match state
            .provider_registry
            .register_postgres(
                "bootstrap-postgres",
                &pg_host,
                pg_port,
                &pg_db,
                &pg_user,
                &pg_pass,
                "pg",
                ctx.datafusion_ctx(),
            )
            .await
        {
            Ok(tables) => {
                drop(ctx);
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
                    created_at: chrono::Utc::now(),
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
                tracing::info!(count = tables.len(), "Bootstrap: Postgres tables registered (federated)");
            }
            Err(e) => {
                drop(ctx);
                tracing::info!(error = %e, "Bootstrap: Postgres not available (start with docker compose up -d)");
            }
        }
    }

    // ── 2. Try connecting to MySQL (federated provider) ─────────
    #[cfg(feature = "mysql")]
    {
        let mysql_host = std::env::var("RUSTLAKE_MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string());
        let mysql_port: u16 = std::env::var("RUSTLAKE_MYSQL_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3307);
        let mysql_db = std::env::var("RUSTLAKE_MYSQL_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
        let mysql_user = std::env::var("RUSTLAKE_MYSQL_USER").unwrap_or_else(|_| "rustlake".to_string());
        let mysql_pass = std::env::var("RUSTLAKE_MYSQL_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

        let ctx = state.ctx.read().await;
        match state
            .provider_registry
            .register_mysql(
                "bootstrap-mysql",
                &mysql_host,
                mysql_port,
                &mysql_db,
                &mysql_user,
                &mysql_pass,
                "mysql",
                ctx.datafusion_ctx(),
            )
            .await
        {
            Ok(tables) => {
                drop(ctx);
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
                    created_at: chrono::Utc::now(),
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
                tracing::info!(count = tables.len(), "Bootstrap: MySQL tables registered (federated)");
            }
            Err(e) => {
                drop(ctx);
                tracing::info!(error = %e, "Bootstrap: MySQL not available (start with docker compose up -d)");
            }
        }
    }

    // ── 3. Try connecting to MongoDB (snapshot — no provider available) ──
    let mongo_host = std::env::var("RUSTLAKE_MONGO_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mongo_port: u16 = std::env::var("RUSTLAKE_MONGO_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(27018);
    let mongo_db = std::env::var("RUSTLAKE_MONGO_DB").unwrap_or_else(|_| "rustlake_demo".to_string());
    let mongo_user = std::env::var("RUSTLAKE_MONGO_USER").unwrap_or_else(|_| "rustlake".to_string());
    let mongo_pass = std::env::var("RUSTLAKE_MONGO_PASSWORD").unwrap_or_else(|_| "rustlake".to_string());

    let mongo_params = mongodb_conn::MongoConnParams {
        host: mongo_host.clone(),
        port: mongo_port,
        database: mongo_db.clone(),
        username: mongo_user.clone(),
        password: mongo_pass.clone(),
        ..Default::default()
    };

    match mongodb_conn::connect_and_discover(&mongo_params).await {
        Ok(collections) => {
            let entry = ConnectionEntry {
                id: "bootstrap-mongodb".to_string(),
                name: "Docker MongoDB".to_string(),
                conn_type: "mongodb".to_string(),
                host: mongo_host.clone(),
                port: mongo_port,
                database: mongo_db.clone(),
                username: mongo_user.clone(),
                status: "connected".to_string(),
                tables: collections.clone(),
                created_at: chrono::Utc::now(),
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

            if state.seed_connection(entry, mongo_pass.clone()).await {
                tracing::info!(
                    source = "bootstrap",
                    conn_type = "mongodb",
                    collections = collections.len(),
                    "Data source added: Docker MongoDB ({} collections discovered)",
                    collections.len()
                );
            }

            // Register each collection into a "mongo" DataFusion schema
            {
                let ctx = state.ctx.read().await;
                let df_ctx = ctx.datafusion_ctx();
                // Ensure "mongo" schema exists
                if let Some(catalog) = df_ctx.catalog("datafusion") {
                    if catalog.schema("mongo").is_none() {
                        let mongo_schema: std::sync::Arc<dyn datafusion::catalog::SchemaProvider> =
                            std::sync::Arc::new(datafusion::catalog::MemorySchemaProvider::new());
                        let _ = catalog.register_schema("mongo", mongo_schema);
                    }
                }
            }
            for coll_name in &collections {
                match mongodb_conn::fetch_collection_as_arrow(&mongo_params, coll_name).await {
                    Ok(batch) => {
                        let row_count = batch.num_rows();
                        let schema = batch.schema();
                        match datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]) {
                            Ok(mem_table) => {
                                let df_name = format!("mongo.{}", coll_name);
                                let ctx = state.ctx.read().await;
                                let df_ctx = ctx.datafusion_ctx();
                                if let Some(catalog) = df_ctx.catalog("datafusion") {
                                    if let Some(schema_prov) = catalog.schema("mongo") {
                                        match schema_prov.register_table(coll_name.clone(), std::sync::Arc::new(mem_table)) {
                                            Ok(_) => {
                                                tracing::info!(table = %df_name, rows = row_count, "Bootstrap: registered MongoDB collection");
                                            }
                                            Err(e) => tracing::warn!(table = %df_name, error = %e, "Bootstrap: failed to register MongoDB collection"),
                                        }
                                    }
                                }
                            }
                            Err(e) => tracing::warn!(collection = %coll_name, error = %e, "Bootstrap: failed to create MongoDB MemTable"),
                        }
                    }
                    Err(e) => tracing::warn!(collection = %coll_name, error = %e, "Bootstrap: failed to fetch MongoDB collection"),
                }
            }
        }
        Err(e) => {
            tracing::info!(error = %e, "Bootstrap: MongoDB not available (start with docker compose up -d)");
        }
    }

    // ── 4. Seed MinIO S3 config (via env vars) ────────────────────
    let minio_endpoint = std::env::var("RUSTLAKE_MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let minio_access = std::env::var("RUSTLAKE_MINIO_ACCESS_KEY").unwrap_or_else(|_| "rustlake".to_string());
    let minio_secret = std::env::var("RUSTLAKE_MINIO_SECRET_KEY").unwrap_or_else(|_| "rustlake123".to_string());
    let minio_bucket = std::env::var("RUSTLAKE_MINIO_BUCKET").unwrap_or_else(|_| "rustlake-warehouse".to_string());
    let minio_region = std::env::var("RUSTLAKE_MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    {
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
                created_at: chrono::Utc::now(),
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
            tracing::info!("Bootstrap: added Local MinIO S3 config");
        }
    }

    // ── 5. Seed demo scheduled jobs ──────────────────────────────
    let demo_jobs = vec![
        ScheduledJob {
            id: "demo-pg-snapshot".to_string(),
            name: "Postgres Snapshot".to_string(),
            job_type: "sql_query".to_string(),
            cron: "*/5 * * * *".to_string(),
            target: "SELECT count(*) FROM pg.customers".to_string(),
            enabled: true,
            last_run: None,
            next_run: None,
            last_status: None,
            created_at: chrono::Utc::now(),
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
            last_run: None,
            next_run: None,
            last_status: None,
            created_at: chrono::Utc::now(),
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
            target: "SELECT product_id, SUM(amount) as total_amount FROM pg.sales GROUP BY product_id".to_string(),
            enabled: true,
            last_run: None,
            next_run: None,
            last_status: None,
            created_at: chrono::Utc::now(),
            engine: "auto".to_string(),
            trigger_type: "time".to_string(),
            event_config: None,
            cluster: Some("default".to_string()),
            timeout_seconds: Some(300),
            retries: 2,
            tags: vec!["demo".to_string(), "materialized_view".to_string()],
        },
        ScheduledJob {
            id: "demo-tpch-pricing".to_string(),
            name: "TPC-H Q1: Pricing Summary".to_string(),
            job_type: "sql_query".to_string(),
            cron: "0 * * * *".to_string(),
            target: "SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price, COUNT(*) as count_order FROM pg.tpch_lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus".to_string(),
            enabled: true,
            last_run: None,
            next_run: None,
            last_status: None,
            created_at: chrono::Utc::now(),
            engine: "auto".to_string(),
            trigger_type: "time".to_string(),
            event_config: None,
            cluster: Some("default".to_string()),
            timeout_seconds: Some(120),
            retries: 1,
            tags: vec!["demo".to_string(), "tpch".to_string(), "benchmark".to_string()],
        },
        ScheduledJob {
            id: "demo-tpch-etl-revenue".to_string(),
            name: "ETL: Revenue by Nation".to_string(),
            job_type: "etl_pipeline".to_string(),
            cron: "0 */6 * * *".to_string(),
            target: "SELECT n.n_name as nation, EXTRACT(YEAR FROM o.o_orderdate) as year, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue FROM pg.tpch_lineitem l JOIN pg.tpch_orders o ON l.l_orderkey = o.o_orderkey JOIN pg.tpch_customer c ON o.o_custkey = c.c_custkey JOIN pg.tpch_nation n ON c.c_nationkey = n.n_nationkey GROUP BY n.n_name, EXTRACT(YEAR FROM o.o_orderdate) ORDER BY nation, year".to_string(),
            enabled: true,
            last_run: None,
            next_run: None,
            last_status: None,
            created_at: chrono::Utc::now(),
            engine: "auto".to_string(),
            trigger_type: "time".to_string(),
            event_config: None,
            cluster: Some("default".to_string()),
            timeout_seconds: Some(300),
            retries: 2,
            tags: vec!["demo".to_string(), "tpch".to_string(), "etl".to_string()],
        },
    ];

    let mut jobs_seeded = 0u32;
    for job in demo_jobs {
        if state.seed_scheduled_job(job).await {
            jobs_seeded += 1;
        }
    }
    if jobs_seeded > 0 {
        tracing::info!(count = jobs_seeded, "Bootstrap: seeded demo scheduled jobs");
    }

    // ── 6. Seed demo streaming pipeline ──────────────────────────
    let demo_pipeline = StreamingPipeline {
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
        created_at: chrono::Utc::now(),
        snapshot_docs: None,
        snapshot_completed_at: None,
        files_written: 0,
        phase: String::new(),
    };

    if state.seed_pipeline(demo_pipeline).await {
        tracing::info!("Bootstrap: seeded demo streaming pipeline");
    }

    // ── 7. Seed demo transforms ──────────────────────────────────
    let demo_transforms = vec![
        UserTransform {
            name: "customer_orders".to_string(),
            sql: "SELECT c.name, COUNT(o.id) as order_count, SUM(o.amount) as total_spent\nFROM pg.customers c\nJOIN pg.orders o ON c.id = o.customer_id\nGROUP BY c.name\nORDER BY total_spent DESC".to_string(),
            depends_on: vec!["pg.customers".to_string(), "pg.orders".to_string()],
            materialization: "view".to_string(),
            description: "Customer order summary with count and total spend".to_string(),
            created_at: chrono::Utc::now(),
        },
        UserTransform {
            name: "sales_by_product".to_string(),
            sql: "SELECT p.name as product_name, p.category,\n  COUNT(s.id) as sale_count, SUM(s.amount) as revenue\nFROM pg.products p\nJOIN pg.sales s ON p.id = s.product_id\nGROUP BY p.name, p.category\nORDER BY revenue DESC".to_string(),
            depends_on: vec!["pg.products".to_string(), "pg.sales".to_string()],
            materialization: "table".to_string(),
            description: "Product sales aggregation with revenue breakdown".to_string(),
            created_at: chrono::Utc::now(),
        },
        UserTransform {
            name: "tpch_revenue_by_nation".to_string(),
            sql: "SELECT n.n_name as nation,\n  SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue\nFROM pg.tpch_customer c\nJOIN pg.tpch_orders o ON c.c_custkey = o.o_custkey\nJOIN pg.tpch_lineitem l ON l.l_orderkey = o.o_orderkey\nJOIN pg.tpch_supplier s ON l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey\nJOIN pg.tpch_nation n ON s.s_nationkey = n.n_nationkey\nJOIN pg.tpch_region r ON n.n_regionkey = r.r_regionkey\nWHERE r.r_name = 'ASIA'\nGROUP BY n.n_name\nORDER BY revenue DESC".to_string(),
            depends_on: vec!["pg.tpch_customer".to_string(), "pg.tpch_orders".to_string(), "pg.tpch_lineitem".to_string(), "pg.tpch_supplier".to_string(), "pg.tpch_nation".to_string(), "pg.tpch_region".to_string()],
            materialization: "table".to_string(),
            description: "TPC-H Q5: Revenue by nation for Asia-Pacific suppliers".to_string(),
            created_at: chrono::Utc::now(),
        },
        UserTransform {
            name: "tpch_customer_segments".to_string(),
            sql: "SELECT c.c_mktsegment as segment,\n  COUNT(DISTINCT c.c_custkey) as customer_count,\n  COUNT(o.o_orderkey) as order_count,\n  SUM(o.o_totalprice) as total_revenue\nFROM pg.tpch_customer c\nLEFT JOIN pg.tpch_orders o ON c.c_custkey = o.o_custkey\nGROUP BY c.c_mktsegment\nORDER BY total_revenue DESC".to_string(),
            depends_on: vec!["pg.tpch_customer".to_string(), "pg.tpch_orders".to_string()],
            materialization: "view".to_string(),
            description: "Customer segmentation with order counts and revenue by market segment".to_string(),
            created_at: chrono::Utc::now(),
        },
    ];

    let mut transforms_seeded = 0u32;
    for ut in demo_transforms {
        if state.seed_user_transform(ut).await {
            transforms_seeded += 1;
        }
    }
    if transforms_seeded > 0 {
        tracing::info!(count = transforms_seeded, "Bootstrap: seeded demo transforms");
    }

    tracing::info!("Bootstrap complete");
}

/// Seed demo executable tables — always runs (not gated by RUSTLAKE_AUTO_BOOTSTRAP).
async fn seed_demo_executable_tables(state: &crate::state::AppState) {
    use crate::executable_table::*;

    let now = chrono::Utc::now();

    // daily_revenue — SQL transform with 3 versions and execution history
    let daily_revenue = ExecutableTable {
            table_name: "daily_revenue".to_string(),
            table_location: "s3://warehouse/daily_revenue".to_string(),
            transform: TableTransform {
                transform_type: "sql".to_string(),
                source_code: "SELECT date, SUM(amount) as revenue, COUNT(*) as orders, AVG(amount) as avg_order\nFROM orders\nGROUP BY date\nORDER BY date DESC".to_string(),
                source_hash: hash_source("SELECT date, SUM(amount) as revenue, COUNT(*) as orders, AVG(amount) as avg_order\nFROM orders\nGROUP BY date\nORDER BY date DESC"),
                binary_path: None,
                binary_size: None,
                binary_cached: false,
                compiler_version: None,
                target_arch: None,
            },
            schedule: Some("0 * * * *".to_string()),
            quality_gates: vec![
                QualityGateRef {
                    gate_type: "not_null".to_string(),
                    column: Some("date".to_string()),
                    threshold: None,
                    description: "date must not be null".to_string(),
                },
                QualityGateRef {
                    gate_type: "row_count".to_string(),
                    column: None,
                    threshold: Some(1.0),
                    description: "Must produce at least 1 row".to_string(),
                },
            ],
            input_tables: vec!["orders".to_string()],
            status: ExecutableTableStatus {
                state: "active".to_string(),
                health: "healthy".to_string(),
                last_error: None,
                staleness_hours: 0.3,
                data_freshness: "fresh".to_string(),
            },
            history: vec![
                ExecutionRecord {
                    execution_id: "exec-dr-001".to_string(),
                    started_at: (now - chrono::Duration::hours(46)).to_rfc3339(),
                    completed_at: Some((now - chrono::Duration::hours(46)).to_rfc3339()),
                    duration_ms: 450,
                    status: "success".to_string(),
                    rows_produced: Some(365),
                    bytes_written: Some(28400),
                    cost_usd: 0.000450,
                    binary_cached: false,
                    compile_ms: 0,
                    run_ms: 450,
                    error: None,
                    execution_location: "local".to_string(),
                    version: 1,
                },
                ExecutionRecord {
                    execution_id: "exec-dr-002".to_string(),
                    started_at: (now - chrono::Duration::hours(36)).to_rfc3339(),
                    completed_at: Some((now - chrono::Duration::hours(36)).to_rfc3339()),
                    duration_ms: 380,
                    status: "success".to_string(),
                    rows_produced: Some(365),
                    bytes_written: Some(28400),
                    cost_usd: 0.000380,
                    binary_cached: false,
                    compile_ms: 0,
                    run_ms: 380,
                    error: None,
                    execution_location: "local".to_string(),
                    version: 1,
                },
                ExecutionRecord {
                    execution_id: "exec-dr-003".to_string(),
                    started_at: (now - chrono::Duration::hours(20)).to_rfc3339(),
                    completed_at: Some((now - chrono::Duration::hours(20)).to_rfc3339()),
                    duration_ms: 410,
                    status: "success".to_string(),
                    rows_produced: Some(366),
                    bytes_written: Some(28500),
                    cost_usd: 0.000410,
                    binary_cached: false,
                    compile_ms: 0,
                    run_ms: 410,
                    error: None,
                    execution_location: "local".to_string(),
                    version: 2,
                },
                ExecutionRecord {
                    execution_id: "exec-dr-004".to_string(),
                    started_at: (now - chrono::Duration::hours(12)).to_rfc3339(),
                    completed_at: Some((now - chrono::Duration::hours(12)).to_rfc3339()),
                    duration_ms: 395,
                    status: "success".to_string(),
                    rows_produced: Some(366),
                    bytes_written: Some(28500),
                    cost_usd: 0.000395,
                    binary_cached: false,
                    compile_ms: 0,
                    run_ms: 395,
                    error: None,
                    execution_location: "local".to_string(),
                    version: 2,
                },
                ExecutionRecord {
                    execution_id: "exec-dr-005".to_string(),
                    started_at: (now - chrono::Duration::hours(1)).to_rfc3339(),
                    completed_at: Some((now - chrono::Duration::hours(1)).to_rfc3339()),
                    duration_ms: 420,
                    status: "success".to_string(),
                    rows_produced: Some(366),
                    bytes_written: Some(28500),
                    cost_usd: 0.000420,
                    binary_cached: false,
                    compile_ms: 0,
                    run_ms: 420,
                    error: None,
                    execution_location: "local".to_string(),
                    version: 3,
                },
            ],
            versions: vec![
                TransformVersion {
                    version: 1,
                    source_code: "SELECT date, SUM(amount) as revenue\nFROM orders\nGROUP BY date".to_string(),
                    source_hash: hash_source("SELECT date, SUM(amount) as revenue\nFROM orders\nGROUP BY date"),
                    created_at: (now - chrono::Duration::hours(48)).to_rfc3339(),
                    created_by: "user".to_string(),
                    change_description: "Initial transform".to_string(),
                    binary_size_bytes: None,
                    snapshot_ids: vec![1, 2],
                },
                TransformVersion {
                    version: 2,
                    source_code: "SELECT date, SUM(amount) as revenue, COUNT(*) as orders\nFROM orders\nGROUP BY date".to_string(),
                    source_hash: hash_source("SELECT date, SUM(amount) as revenue, COUNT(*) as orders\nFROM orders\nGROUP BY date"),
                    created_at: (now - chrono::Duration::hours(24)).to_rfc3339(),
                    created_by: "user".to_string(),
                    change_description: "Added order count metric".to_string(),
                    binary_size_bytes: None,
                    snapshot_ids: vec![3, 4],
                },
                TransformVersion {
                    version: 3,
                    source_code: "SELECT date, SUM(amount) as revenue, COUNT(*) as orders, AVG(amount) as avg_order\nFROM orders\nGROUP BY date\nORDER BY date DESC".to_string(),
                    source_hash: hash_source("SELECT date, SUM(amount) as revenue, COUNT(*) as orders, AVG(amount) as avg_order\nFROM orders\nGROUP BY date\nORDER BY date DESC"),
                    created_at: (now - chrono::Duration::hours(6)).to_rfc3339(),
                    created_by: "user".to_string(),
                    change_description: "Added avg order value and sorting".to_string(),
                    binary_size_bytes: None,
                    snapshot_ids: vec![5],
                },
            ],
            created_at: (now - chrono::Duration::hours(48)).to_rfc3339(),
            last_refresh: Some((now - chrono::Duration::hours(1)).to_rfc3339()),
            next_refresh: Some((now + chrono::Duration::minutes(40)).to_rfc3339()),
            estimated_cost_usd: 0.000420,
            total_executions: 5,
            total_cost_usd: 0.002055,
            incremental: false,
            watermark_column: None,
            last_watermark: None,
            executions_skipped: 0,
            cost_saved_usd: 0.0,
            auto_refresh: false,
            refresh_interval_seconds: 0,
        };

        // Fix version hashes to match actual source code
        let mut dr = daily_revenue;
        for v in &mut dr.versions {
            v.source_hash = hash_source(&v.source_code);
        }
        dr.transform.source_hash = dr.versions.last().map(|v| v.source_hash.clone()).unwrap_or_default();
        state.seed_executable_table(dr).await;

        // customer_segments — Rust transform (compilable, produces CSV → Parquet)
        let cs_v1_code = "fn main() {\n    let data = vec![\n        (\"C001\", \"Alice\", 2500.0, 12),\n        (\"C002\", \"Bob\", 450.0, 3),\n        (\"C003\", \"Charlie\", 8200.0, 45),\n    ];\n    println!(\"customer_id,name,lifetime_value,order_count\");\n    for (id, name, ltv, orders) in &data {\n        println!(\"{},{},{:.2},{}\", id, name, ltv, orders);\n    }\n}";
        let cs_v2_code = "fn main() {\n    let data = vec![\n        (\"C001\", \"Alice\", 2500.0, 12, \"2026-03-15\"),\n        (\"C002\", \"Bob\", 450.0, 3, \"2026-02-10\"),\n        (\"C003\", \"Charlie\", 8200.0, 45, \"2026-03-20\"),\n        (\"C004\", \"Diana\", 120.0, 1, \"2026-01-05\"),\n    ];\n    println!(\"customer_id,name,lifetime_value,order_count,last_order\");\n    for (id, name, ltv, orders, last) in &data {\n        let segment = if *ltv > 5000.0 { \"high_value\" } else if *ltv > 500.0 { \"medium\" } else { \"low\" };\n        println!(\"{},{},{:.2},{},{}\", id, name, ltv, orders, last);\n    }\n}";
        let cs_current_code = "fn main() {\n    let data = vec![\n        (\"C001\", \"Alice\", 2500.0, 12, \"2026-03-15\"),\n        (\"C002\", \"Bob\", 450.0, 3, \"2026-02-10\"),\n        (\"C003\", \"Charlie\", 8200.0, 45, \"2026-03-20\"),\n        (\"C004\", \"Diana\", 120.0, 1, \"2026-01-05\"),\n        (\"C005\", \"Eve\", 3100.0, 18, \"2025-12-01\"),\n    ];\n    println!(\"customer_id,name,lifetime_value,order_count,last_order,segment,churn_risk\");\n    for (id, name, ltv, orders, last_order) in &data {\n        let segment = if *ltv > 5000.0 { \"high_value\" } else if *ltv > 500.0 { \"medium_value\" } else { \"low_value\" };\n        let churn = if last_order < &\"2026-02-01\" { true } else { false };\n        println!(\"{},{},{:.2},{},{},{},{}\", id, name, ltv, orders, last_order, segment, churn);\n    }\n}";

        let customer_segments = ExecutableTable {
            table_name: "customer_segments".to_string(),
            table_location: "s3://warehouse/customer_segments".to_string(),
            transform: TableTransform {
                transform_type: "rust".to_string(),
                source_code: cs_current_code.to_string(),
                source_hash: hash_source(cs_current_code),
                binary_path: None,
                binary_size: None,
                binary_cached: false,
                compiler_version: None,
                target_arch: None,
            },
            schedule: Some("0 0 * * *".to_string()),
            quality_gates: vec![
                QualityGateRef {
                    gate_type: "not_null".to_string(),
                    column: Some("customer_id".to_string()),
                    threshold: None,
                    description: "customer_id must not be null".to_string(),
                },
                QualityGateRef {
                    gate_type: "unique".to_string(),
                    column: Some("customer_id".to_string()),
                    threshold: None,
                    description: "customer_id must be unique".to_string(),
                },
                QualityGateRef {
                    gate_type: "row_count".to_string(),
                    column: None,
                    threshold: Some(1.0),
                    description: "Must produce at least 1 row".to_string(),
                },
            ],
            input_tables: vec!["customers".to_string(), "orders".to_string()],
            status: ExecutableTableStatus {
                state: "active".to_string(),
                health: "healthy".to_string(),
                last_error: None,
                staleness_hours: 0.0,
                data_freshness: "unknown".to_string(),
            },
            history: Vec::new(),
            versions: vec![
                TransformVersion {
                    version: 1,
                    source_code: cs_v1_code.to_string(),
                    source_hash: hash_source(cs_v1_code),
                    created_at: (now - chrono::Duration::hours(96)).to_rfc3339(),
                    created_by: "user".to_string(),
                    change_description: "Initial LTV calculation".to_string(),
                    binary_size_bytes: None,
                    snapshot_ids: Vec::new(),
                },
                TransformVersion {
                    version: 2,
                    source_code: cs_v2_code.to_string(),
                    source_hash: hash_source(cs_v2_code),
                    created_at: (now - chrono::Duration::hours(48)).to_rfc3339(),
                    created_by: "user".to_string(),
                    change_description: "Added last_order date and segmentation".to_string(),
                    binary_size_bytes: None,
                    snapshot_ids: Vec::new(),
                },
                TransformVersion {
                    version: 3,
                    source_code: cs_current_code.to_string(),
                    source_hash: hash_source(cs_current_code),
                    created_at: (now - chrono::Duration::hours(24)).to_rfc3339(),
                    created_by: "user".to_string(),
                    change_description: "Added churn risk detection and 5th customer".to_string(),
                    binary_size_bytes: None,
                    snapshot_ids: Vec::new(),
                },
            ],
            created_at: (now - chrono::Duration::hours(96)).to_rfc3339(),
            last_refresh: None,
            next_refresh: None,
            estimated_cost_usd: 0.0,
            total_executions: 0,
            total_cost_usd: 0.0,
            incremental: false,
            watermark_column: None,
            last_watermark: None,
            executions_skipped: 0,
            cost_saved_usd: 0.0,
            auto_refresh: false,
            refresh_interval_seconds: 0,
        };

    state.seed_executable_table(customer_segments).await;
}

/// Sync all registered DataFusion tables into DuckDB for OLAP acceleration.
#[cfg(feature = "duckdb")]
async fn sync_tables_to_duckdb(state: &AppState) {
    let Some(ref duckdb_engine) = state.duckdb_engine else {
        return;
    };

    let ctx = state.ctx.read().await;
    let tables = match ctx.list_tables().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "DuckDB sync: failed to list tables");
            return;
        }
    };

    let mut sync_data = Vec::new();
    for table_name in &tables {
        // Read all data from DataFusion table
        let sql = format!("SELECT * FROM \"{}\"", table_name);
        match ctx.datafusion_ctx().sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) if !batches.is_empty() => {
                    sync_data.push((table_name.clone(), batches));
                }
                Ok(_) => {} // empty table, skip
                Err(e) => {
                    tracing::debug!(table = %table_name, error = %e, "DuckDB sync: skip table (read error)");
                }
            },
            Err(e) => {
                tracing::debug!(table = %table_name, error = %e, "DuckDB sync: skip table (sql error)");
            }
        }
    }
    drop(ctx);

    let table_count = sync_data.len();
    match duckdb_engine.sync_tables(sync_data).await {
        Ok(synced) => {
            tracing::info!(synced, total = table_count, "DuckDB: table sync complete");
        }
        Err(e) => {
            tracing::warn!(error = %e, "DuckDB: table sync failed");
        }
    }
}

/// Sync all registered DataFusion tables into Polars for DataFrame execution.
#[cfg(feature = "polars")]
async fn sync_tables_to_polars(state: &AppState) {
    let Some(ref polars_engine) = state.polars_engine else {
        return;
    };

    let ctx = state.ctx.read().await;
    let tables = match ctx.list_tables().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "Polars sync: failed to list tables");
            return;
        }
    };

    let mut sync_data = Vec::new();
    for table_name in &tables {
        let sql = format!("SELECT * FROM \"{}\"", table_name);
        match ctx.datafusion_ctx().sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) if !batches.is_empty() => {
                    sync_data.push((table_name.clone(), batches));
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(table = %table_name, error = %e, "Polars sync: skip table (read error)");
                }
            },
            Err(e) => {
                tracing::debug!(table = %table_name, error = %e, "Polars sync: skip table (sql error)");
            }
        }
    }
    drop(ctx);

    let table_count = sync_data.len();
    match polars_engine.sync_tables(sync_data).await {
        Ok(synced) => {
            tracing::info!(synced, total = table_count, "Polars: table sync complete");
        }
        Err(e) => {
            tracing::warn!(error = %e, "Polars: table sync failed");
        }
    }
}

#[derive(clap::Parser)]
#[command(
    name = "rustlake",
    about = "RustLake — The All-Rust Data Platform",
    version,
    long_about = "Single-binary data platform built on Apache Arrow, DataFusion, and Iceberg.\n\nRun `rustlake serve` to start the platform."
)]
struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(clap::Subcommand)]
enum CliCommand {
    /// Start the RustLake platform (API server + scheduler + all engines)
    Serve {
        /// Port to bind to
        #[arg(long, short, default_value = "3000")]
        port: u16,

        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    let cli = Cli::parse();

    // If no subcommand, default to `serve`
    let (host, port) = match cli.command {
        Some(CliCommand::Serve { host, port }) => (host, port),
        None => ("127.0.0.1".to_string(), 3000),
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("info,rustlake_api=debug,rustlake_engine=debug,rustlake_router=debug,tower_http=debug")
            }),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_level(true)
        .with_ansi(true)
        .with_timer(ChronoLocal::new("%H:%M:%S%.3f".to_string()))
        .init();

    // Load config (from RUSTLAKE_CONFIG env var or defaults)
    let config = match std::env::var("RUSTLAKE_CONFIG") {
        Ok(path) => RustLakeConfig::from_file(&path)?,
        Err(_) => RustLakeConfig::default(),
    };

    // CLI args override config
    let bind_addr = format!("{}:{}", host, port);
    let flight_enabled = config.flight.enabled;
    let flight_config = config.flight.clone();
    let cluster_config = config.cluster.clone();
    let k8s_config = config.k8s.clone();
    let node_role = config.cluster.node_role.clone();

    if let Err(e) = std::fs::create_dir_all("uploads") {
        tracing::warn!(error = %e, "Failed to create uploads/ directory");
    }

    // Create the query engine context
    let ctx = RustLakeContext::new(config.clone()).await?;

    let vector_index = load_product_vectors();
    let mut state = AppState::with_vector_index(ctx, vector_index);

    // Initialize DuckDB engine if compiled and enabled
    #[cfg(feature = "duckdb")]
    {
        let duckdb_enabled = std::env::var("RUSTLAKE_DUCKDB__ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(config.duckdb.enabled);

        if duckdb_enabled {
            let mut duckdb_config = config.duckdb.clone();
            duckdb_config.enabled = true;
            // Allow env overrides
            if let Ok(mem) = std::env::var("RUSTLAKE_DUCKDB__MEMORY_LIMIT") {
                duckdb_config.memory_limit = Some(mem);
            }
            if let Ok(threads) = std::env::var("RUSTLAKE_DUCKDB__THREADS") {
                if let Ok(n) = threads.parse::<usize>() {
                    duckdb_config.threads = Some(n);
                }
            }

            match rustlake_engine::duckdb_engine::DuckDbEngine::new(&duckdb_config) {
                Ok(engine) => {
                    let version = engine.version();
                    tracing::info!(
                        version = %version,
                        memory_limit = ?duckdb_config.memory_limit,
                        threads = ?duckdb_config.threads,
                        "DuckDB engine initialized"
                    );
                    state.duckdb_engine = Some(engine);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to initialize DuckDB engine");
                }
            }
        } else {
            tracing::info!("DuckDB engine disabled — enable with RUSTLAKE_DUCKDB__ENABLED=true");
        }
    }

    // Initialize Polars engine if compiled and enabled
    #[cfg(feature = "polars")]
    {
        let polars_enabled = std::env::var("RUSTLAKE_POLARS__ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(config.polars.enabled);

        if polars_enabled {
            match rustlake_engine::polars_engine::PolarsEngine::new() {
                Ok(engine) => {
                    tracing::info!(version = %engine.version(), "Polars engine initialized");
                    state.polars_engine = Some(engine);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to initialize Polars engine");
                }
            }
        } else {
            tracing::info!("Polars engine disabled — enable with RUSTLAKE_POLARS__ENABLED=true");
        }
    }

    let flight_metrics = FlightMetrics::default();
    state.flight_metrics = Some(flight_metrics.clone());

    // Load persisted data from disk
    let persisted = load_chat_messages_from_file();
    if !persisted.is_empty() {
        tracing::info!(count = persisted.len(), "Loaded persisted chat messages from feedback.jsonl");
        *state.chat_messages.get_mut() = persisted;
    }

    let transforms = load_user_transforms_from_file();
    if !transforms.is_empty() {
        tracing::info!(count = transforms.len(), "Loaded user transforms from JSONL");
        *state.user_transforms.get_mut() = transforms;
    } else {
        #[cfg(feature = "duckdb")]
        if let Some(ref db) = state.state_db {
            let cached = db.load_transforms();
            if !cached.is_empty() {
                tracing::info!(count = cached.len(), "Restored user transforms from DuckDB");
                *state.user_transforms.get_mut() = cached;
            }
        }
    }

    let jobs = load_scheduled_jobs_from_file();
    if !jobs.is_empty() {
        tracing::info!(count = jobs.len(), "Loaded scheduled jobs from JSONL");
        *state.scheduled_jobs.get_mut() = jobs;
    } else {
        #[cfg(feature = "duckdb")]
        if let Some(ref db) = state.state_db {
            let cached = db.load_jobs();
            if !cached.is_empty() {
                tracing::info!(count = cached.len(), "Restored scheduled jobs from DuckDB");
                *state.scheduled_jobs.get_mut() = cached;
            }
        }
    }

    let connections = state::load_connections_from_file();
    if !connections.is_empty() {
        tracing::info!(count = connections.len(), "Loaded persisted connections from connections.jsonl");
        *state.connections.get_mut() = connections;
    } else {
        // No JSONL — try restoring from DuckDB state store
        #[cfg(feature = "duckdb")]
        if let Some(ref db) = state.state_db {
            let cached = db.load_connections();
            if !cached.is_empty() {
                let mut restored: Vec<state::ConnectionEntry> = Vec::new();
                for c in &cached {
                    let tables = db.load_tables(&c.id);
                    restored.push(state::ConnectionEntry {
                        id: c.id.clone(),
                        name: c.name.clone(),
                        conn_type: c.conn_type.clone(),
                        host: c.host.clone(),
                        port: c.port,
                        database: c.database.clone(),
                        username: c.username.clone(),
                        status: "cached".to_string(),
                        tables,
                        created_at: chrono::Utc::now(),
                        source: c.source.clone(),
                        sync_status: "cached".to_string(),
                        sync_error: None,
                        sync_progress: None,
                        auth_method: c.auth_method.clone(),
                        connection_string: c.connection_string.clone(),
                        aws_access_key: None,
                        aws_secret_key: None,
                        aws_session_token: None,
                    });
                }
                tracing::info!(count = restored.len(), "Restored connections from DuckDB state store");
                *state.connections.get_mut() = restored;
            }
        }
    }

    // Load encrypted credentials and restore passwords
    {
        let passwords = state.credential_store.load_all_passwords();
        if !passwords.is_empty() {
            tracing::info!(count = passwords.len(), "Loaded encrypted passwords from credentials.enc");
            *state.connection_passwords.get_mut() = passwords;
        }

        let s3_creds = state.credential_store.load_all_s3_creds();
        if !s3_creds.is_empty() {
            tracing::info!(count = s3_creds.len(), "Loaded encrypted S3 credentials from credentials.enc");
            *state.migration_s3_creds.get_mut() = s3_creds;
        }
    }

    // Restore S3 configs from DuckDB + credentials
    #[cfg(feature = "duckdb")]
    {
        if let Some(ref db) = state.state_db {
            let s3_meta = db.load_s3_configs();
            if !s3_meta.is_empty() {
                let s3_creds = state.credential_store.load_all_s3_creds();
                let mut restored_configs: Vec<state::S3Config> = Vec::new();
                for (name, endpoint, bucket, region) in &s3_meta {
                    let creds = s3_creds.get(bucket.as_str());
                    let (access_key, secret_key) = match creds {
                        Some(c) => (c.access_key.clone(), c.secret_key.clone()),
                        None => (String::new(), String::new()),
                    };
                    let cached_tables = db.load_s3_tables(name);
                    let table_names: Vec<String> = cached_tables.iter().map(|(t, _, _)| t.clone()).collect();
                    let table_formats: std::collections::HashMap<String, String> = cached_tables.iter()
                        .map(|(t, f, _)| (t.clone(), f.clone()))
                        .collect();
                    restored_configs.push(state::S3Config {
                        name: name.clone(),
                        endpoint: endpoint.clone(),
                        access_key,
                        secret_key,
                        bucket: bucket.clone(),
                        region: region.clone(),
                        status: if table_names.is_empty() { "configured".to_string() } else { "ready".to_string() },
                        created_at: chrono::Utc::now(),
                        tables: table_names,
                        table_types: std::collections::HashMap::new(),
                        table_formats,
                        sync_status: "cached".to_string(),
                        sync_error: None,
                        scan_progress: None,
                        scan_detail: None,
                        scan_scanned: 0,
                        scan_total: 0,
                        scan_found: 0,
                        scan_elapsed_ms: 0,
                        format_counts: std::collections::HashMap::new(),
                    });
                }
                if !restored_configs.is_empty() {
                    // Configure DuckDB S3 credentials for native access on restored configs
                    if let Some(ref duckdb_engine) = state.duckdb_engine {
                        for cfg in &restored_configs {
                            if !cfg.access_key.is_empty() && !cfg.secret_key.is_empty() {
                                let endpoint = if cfg.endpoint.is_empty() { None } else { Some(cfg.endpoint.as_str()) };
                                match duckdb_engine.configure_s3(&cfg.access_key, &cfg.secret_key, &cfg.region, endpoint).await {
                                    Ok(()) => tracing::info!(bucket = %cfg.bucket, "DuckDB: S3 credentials configured on startup"),
                                    Err(e) => tracing::warn!(error = %e, bucket = %cfg.bucket, "DuckDB: S3 config failed on startup"),
                                }
                            }
                        }
                    }
                    tracing::info!(
                        count = restored_configs.len(),
                        total_tables = restored_configs.iter().map(|c| c.tables.len()).sum::<usize>(),
                        "Restored S3 configs from DuckDB state store"
                    );
                    *state.s3_configs.get_mut() = restored_configs;
                }
            }
        }
    }

    // Fetch S3 credentials from external API if configured
    if let Ok(creds_url) = std::env::var("RUSTLAKE_S3_CREDENTIALS_URL") {
        if !creds_url.is_empty() {
            match state::fetch_s3_credentials_from_api(&creds_url).await {
                Ok(bucket_map) => {
                    let count = bucket_map.len();
                    *state.migration_s3_creds.get_mut() = bucket_map;
                    tracing::info!(buckets = count, url = %creds_url, "Loaded S3 credentials from external API");
                }
                Err(e) => {
                    tracing::warn!(error = %e, url = %creds_url, "Failed to fetch S3 credentials from external API");
                }
            }
        }
    }

    // Initialize S3 binary cache for Rust notebook cells
    {
        let s3_configs = state.s3_configs.get_mut();
        if let Some(first_s3) = s3_configs.first() {
            // Use in-memory credentials if available, otherwise fall back to credential store
            let (ak, sk) = if !first_s3.access_key.is_empty() && !first_s3.secret_key.is_empty() {
                (first_s3.access_key.clone(), first_s3.secret_key.clone())
            } else {
                let all_creds = state.credential_store.load_all_s3_creds();
                if let Some(creds) = all_creds.get(&first_s3.bucket).or_else(|| all_creds.values().next()) {
                    (creds.access_key.clone(), creds.secret_key.clone())
                } else {
                    (String::new(), String::new())
                }
            };
            if !ak.is_empty() && !sk.is_empty() {
                rust_executor::init_s3_cache(
                    &first_s3.endpoint,
                    &first_s3.bucket,
                    &ak,
                    &sk,
                    &first_s3.region,
                ).await;
            } else {
                tracing::debug!("S3 binary cache: no credentials available for bucket {}", first_s3.bucket);
            }
        } else {
            // Try MinIO defaults
            let minio_endpoint = std::env::var("RUSTLAKE_MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
            let minio_bucket = std::env::var("RUSTLAKE_MINIO_BUCKET").unwrap_or_else(|_| "rustlake-warehouse".to_string());
            let minio_key = std::env::var("RUSTLAKE_MINIO_ACCESS_KEY").unwrap_or_else(|_| "rustlake".to_string());
            let minio_secret = std::env::var("RUSTLAKE_MINIO_SECRET_KEY").unwrap_or_else(|_| "rustlake123".to_string());
            rust_executor::init_s3_cache(&minio_endpoint, &minio_bucket, &minio_key, &minio_secret, "us-east-1").await;
        }
    }

    // Load cached data from DuckDB state store (instant startup)
    #[cfg(feature = "duckdb")]
    {
        if let Some(ref db) = state.state_db {
            let (conns, tables, s3s, pipes) = db.summary();
            if conns > 0 || s3s > 0 || pipes > 0 {
                tracing::info!(
                    connections = conns, cached_tables = tables,
                    s3_configs = s3s, pipelines = pipes,
                    "StateDb cache loaded from rustlake_state.duckdb"
                );
            }

            // Restore pipelines from DuckDB if we have none from JSONL
            let pipelines = state.streaming_pipelines.get_mut();
            if pipelines.is_empty() {
                let cached_pipelines = db.load_pipelines();
                if !cached_pipelines.is_empty() {
                    tracing::info!(count = cached_pipelines.len(), "Restored pipelines from StateDb");
                    *pipelines = cached_pipelines;
                }
            }

            // Restore cached table lists for connections that have none
            let connections = state.connections.get_mut();
            for conn in connections.iter_mut() {
                if conn.tables.is_empty() {
                    let cached = db.load_tables(&conn.id);
                    if !cached.is_empty() {
                        tracing::info!(
                            conn_id = %conn.id, name = %conn.name,
                            tables = cached.len(),
                            "Restored cached table list from StateDb"
                        );
                        conn.tables = cached;
                        conn.sync_status = "cached".to_string();
                    }
                }
            }
        }

        // ── Migrate JSONL → DuckDB (one-time) then delete JSONL files ──
        if let Some(ref db) = state.state_db {
            let mut migrated_any = false;

            // Migrate connections
            let connections = state.connections.get_mut();
            if !connections.is_empty() {
                match db.migrate_connections(connections) {
                    Ok(n) if n > 0 => migrated_any = true,
                    Err(e) => tracing::warn!(error = %e, "JSONL → DuckDB migration failed for connections"),
                    _ => {}
                }
            }
            // Migrate jobs
            let jobs = state.scheduled_jobs.get_mut();
            if !jobs.is_empty() {
                match db.migrate_jobs(jobs) {
                    Ok(n) if n > 0 => migrated_any = true,
                    Err(e) => tracing::warn!(error = %e, "JSONL → DuckDB migration failed for scheduled_jobs"),
                    _ => {}
                }
            }
            // Migrate transforms
            let transforms = state.user_transforms.get_mut();
            if !transforms.is_empty() {
                match db.migrate_transforms(transforms) {
                    Ok(n) if n > 0 => migrated_any = true,
                    Err(e) => tracing::warn!(error = %e, "JSONL → DuckDB migration failed for user_transforms"),
                    _ => {}
                }
            }

            // Delete JSONL source files after successful migration
            if migrated_any {
                for path in &["connections.jsonl", "scheduled_jobs.jsonl", "user_transforms.jsonl"] {
                    if std::path::Path::new(path).exists() {
                        match std::fs::remove_file(path) {
                            Ok(()) => tracing::info!(file = %path, "Deleted JSONL file after DuckDB migration"),
                            Err(e) => tracing::warn!(file = %path, error = %e, "Failed to delete JSONL file"),
                        }
                    }
                }
            }
        }
    }

    // Pre-register S3 object stores with DataFusion so CDC tables are immediately queryable
    {
        let s3_configs = state.s3_configs.get_mut();
        let ctx = state.ctx.get_mut();
        let df_ctx = ctx.datafusion_ctx();
        let mut registered = 0;
        for cfg in s3_configs.iter() {
            if cfg.access_key.is_empty() || cfg.secret_key.is_empty() {
                continue;
            }
            match iceberg_s3::build_s3_store(
                &cfg.bucket, &cfg.access_key, &cfg.secret_key, &cfg.region,
                if cfg.endpoint.is_empty() { None } else { Some(&cfg.endpoint) },
            ) {
                Ok(store) => {
                    if let Ok(url) = url::Url::parse(&format!("s3://{}", cfg.bucket)) {
                        df_ctx.runtime_env().register_object_store(&url, store);
                        registered += 1;
                    }
                }
                Err(e) => tracing::warn!(name = %cfg.name, error = %e, "Failed to pre-register S3 store"),
            }
        }
        if registered > 0 {
            tracing::info!(count = registered, "Pre-registered S3 object stores with DataFusion");
        }
    }

    let state = Arc::new(state);

    // Auto-reconnect previously saved connections (non-blocking)
    {
        let reconnect_state = state.clone();
        tokio::spawn(async move {
            routes::reconnect_saved_connections(reconnect_state).await;
        });
    }

    // Auto-bootstrap is disabled — developers start with a clean slate.
    // Use POST /api/v1/bootstrap to connect Docker services on demand.
    // To enable auto-bootstrap, set RUSTLAKE_AUTO_BOOTSTRAP=true.
    if std::env::var("RUSTLAKE_AUTO_BOOTSTRAP").unwrap_or_default() == "true" {
        let bootstrap_state = state.clone();
        tokio::spawn(async move {
            bootstrap_demo_connections(bootstrap_state.clone()).await;

            // Sync all registered tables to DuckDB after bootstrap
            #[cfg(feature = "duckdb")]
            {
                if bootstrap_state.duckdb_available() {
                    sync_tables_to_duckdb(&bootstrap_state).await;
                }
            }

            // Sync all registered tables to Polars after bootstrap
            #[cfg(feature = "polars")]
            {
                if bootstrap_state.polars_available() {
                    sync_tables_to_polars(&bootstrap_state).await;
                }
            }
        });
    }

    // ── Seed demo executable tables (always runs) ───────────────────
    seed_demo_executable_tables(&state).await;

    // ── Hardware & resource banner ──────────────────────────────────
    let cpu_cores = std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1);
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
                        .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()).map(|kb| kb * 1024))
                })
                .unwrap_or(0)
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        { 0u64 }
    };
    let os_name = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let hostname = hostname::get().map(|h| h.to_string_lossy().to_string()).unwrap_or_else(|_| "unknown".into());
    let pid = std::process::id();
    let mem_gb = total_memory_bytes as f64 / 1_073_741_824.0;
    let saved_conns = state.connections.try_read().map(|c| c.len()).unwrap_or(0);
    let saved_passwords = state.connection_passwords.try_read().map(|p| p.len()).unwrap_or(0);
    let saved_s3 = state.migration_s3_creds.try_read().map(|s| s.len()).unwrap_or(0);
    let cred_secured = std::env::var("RUSTLAKE_SECRET_KEY").map(|k| !k.is_empty()).unwrap_or(false);

    tracing::info!("╔══════════════════════════════════════════════════════════╗");
    tracing::info!("║              RustLake Data Platform v0.1.0               ║");
    tracing::info!("╚══════════════════════════════════════════════════════════╝");
    tracing::info!(
        host = %hostname, os = os_name, arch = arch, pid = pid,
        cores = cpu_cores, memory_gb = format!("{:.1}", mem_gb),
        "System"
    );
    tracing::info!(
        datafusion = "51", arrow = "57",
        duckdb = state.duckdb_available(), polars = state.polars_available(),
        flight = flight_enabled,
        "Engines"
    );
    let state_db_status = {
        #[cfg(feature = "duckdb")]
        {
            if state.state_db.is_some() { "active" } else { "unavailable" }
        }
        #[cfg(not(feature = "duckdb"))]
        { "disabled" }
    };
    tracing::info!(
        connections = saved_conns, passwords = saved_passwords,
        s3_creds = saved_s3, encrypted = cred_secured,
        state_db = state_db_status,
        "Restored state"
    );
    tracing::info!(role = ?node_role, bind = %bind_addr, "HTTP API server starting");
    tracing::debug!("RUST_LOG={}", std::env::var("RUST_LOG").unwrap_or_else(|_| "info,rustlake_*=debug,tower_http=debug".to_string()));

    // Build the Axum router
    let app = Router::new()
        .merge(routes::api_routes())
        .layer(CorsLayer::permissive())
        .layer(
            TraceLayer::new_for_http()
                .on_request(DefaultOnRequest::new().level(Level::DEBUG))
                .on_response(DefaultOnResponse::new().level(Level::DEBUG).latency_unit(tower_http::LatencyUnit::Millis)),
        )
        .with_state(state.clone());

    // State for background scheduler
    let scheduler_state = state;

    // Spawn background scheduler tick for executable table hot-swap + cost-aware scheduling + matview refresh
    {
        let sched_state = scheduler_state;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;

                // ── Feature 9: Executable table job execution ────────────
                let jobs = sched_state.scheduled_jobs.read().await.clone();
                for job in &jobs {
                    if !job.enabled || job.job_type != "executable_table" {
                        continue;
                    }
                    if let Some(next) = job.next_run {
                        if chrono::Utc::now() < next {
                            continue;
                        }
                    }
                    let tables = sched_state.executable_tables.read().await;
                    if let Some(table) = tables.iter().find(|t| t.table_name == job.target) {
                        let tt = table.transform.transform_type.clone();
                        let code = table.transform.source_code.clone();
                        let name = table.table_name.clone();
                        let input_tables = table.input_tables.clone();
                        let last_refresh = table.last_refresh.clone();
                        let estimated_cost = table.estimated_cost_usd;
                        let incremental = table.incremental;
                        let watermark_col = table.watermark_column.clone();
                        let last_watermark = table.last_watermark.clone();
                        let quality_gates = table.quality_gates.clone();
                        let prev_version = table.versions.iter()
                            .filter(|v| !v.change_description.starts_with("Auto-rollback"))
                            .rev().nth(1).cloned();
                        drop(tables);

                        // ── Feature 6: Cost-aware skip detection ─────
                        let mut should_skip = false;
                        if !input_tables.is_empty() {
                            let upstream_tables = sched_state.executable_tables.read().await;
                            let upstream_changed = input_tables.iter().any(|input_name| {
                                if let Some(ut) = upstream_tables.iter().find(|t| t.table_name == *input_name) {
                                    if let (Some(ref my_lr), Some(ref upstream_lr)) = (&last_refresh, &ut.last_refresh) {
                                        upstream_lr > my_lr // upstream refreshed after us
                                    } else {
                                        true // no refresh info → run
                                    }
                                } else {
                                    false
                                }
                            });
                            drop(upstream_tables);

                            if !upstream_changed {
                                should_skip = true;
                                let mut tables = sched_state.executable_tables.write().await;
                                if let Some(t) = tables.iter_mut().find(|t| t.table_name == name) {
                                    t.executions_skipped += 1;
                                    t.cost_saved_usd += estimated_cost;
                                }
                                tracing::info!(table=%name, "Scheduler: skipped (upstream unchanged), saved ${:.6}", estimated_cost);
                            }
                        }

                        if !should_skip {
                            // Determine if incremental or full
                            let exec_code = if incremental && watermark_col.is_some() && last_watermark.is_some() {
                                let wc = watermark_col.as_deref().unwrap_or("updated_at");
                                let wv = last_watermark.as_deref().unwrap_or("1970-01-01");
                                format!("{} WHERE {} > '{}'", code, wc, wv)
                            } else {
                                code.clone()
                            };

                            tracing::info!(table=%name, "Scheduler: executing executable table (hot-swap tick)");
                            if tt == "rust" {
                                let result = crate::rust_executor::execute_rust(&exec_code).await;
                                tracing::info!(table=%name, success=%result.success, run_ms=%result.run_ms, "Scheduler: Rust transform complete");
                            } else if tt == "sql" {
                                let ctx = sched_state.ctx.read().await;
                                match ctx.sql(&exec_code).await {
                                    Ok(batches) => {
                                        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                                        tracing::info!(table=%name, rows=%rows, "Scheduler: SQL transform complete");

                                        // ── Self-Healing: validate quality gates after execution ──
                                        let gate_failed = if !quality_gates.is_empty() {
                                            let gate_results = crate::executable_table::validate_gates(&quality_gates, &batches);
                                            let any_failed = gate_results.iter().any(|g| !g.passed);
                                            if any_failed {
                                                let failed_names: Vec<_> = gate_results.iter()
                                                    .filter(|g| !g.passed)
                                                    .map(|g| format!("{}: {}", g.gate_type, g.detail))
                                                    .collect();
                                                tracing::warn!(table=%name, gates=?failed_names, "Scheduler: quality gates FAILED");
                                            }
                                            any_failed
                                        } else {
                                            false
                                        };

                                        if gate_failed {
                                            // Auto-rollback to previous version
                                            if let Some(ref prev) = prev_version {
                                                let mut tables = sched_state.executable_tables.write().await;
                                                if let Some(t) = tables.iter_mut().find(|t| t.table_name == name) {
                                                    let new_ver = t.versions.iter().map(|v| v.version).max().unwrap_or(1) + 1;
                                                    t.versions.push(crate::executable_table::TransformVersion {
                                                        version: new_ver,
                                                        source_code: prev.source_code.clone(),
                                                        source_hash: prev.source_hash.clone(),
                                                        created_at: chrono::Utc::now().to_rfc3339(),
                                                        created_by: "auto-heal".to_string(),
                                                        change_description: format!("Auto-rollback to v{} (gate failure)", prev.version),
                                                        binary_size_bytes: prev.binary_size_bytes,
                                                        snapshot_ids: Vec::new(),
                                                    });
                                                    t.transform.source_code = prev.source_code.clone();
                                                    t.transform.source_hash = prev.source_hash.clone();
                                                    t.transform.binary_cached = false;
                                                    t.status.health = "warning".to_string();
                                                    tracing::warn!(table=%name, rolled_back_to=prev.version, "Scheduler: AUTO-ROLLBACK triggered (self-healing)");
                                                }
                                            } else {
                                                tracing::warn!(table=%name, "Scheduler: gate failure but no previous version to rollback to");
                                            }
                                        } else {
                                            // Gates passed (or no gates) — update last_refresh normally
                                            let mut tables = sched_state.executable_tables.write().await;
                                            if let Some(t) = tables.iter_mut().find(|t| t.table_name == name) {
                                                t.last_refresh = Some(chrono::Utc::now().to_rfc3339());
                                                if t.status.health == "warning" {
                                                    t.status.health = "healthy".to_string();
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => tracing::warn!(table=%name, error=%e, "Scheduler: SQL transform failed"),
                                }
                            }
                        }
                    } else {
                        drop(tables);
                    }
                }

                // ── Feature 3: Matview auto-refresh ──────────────────────
                {
                    let tables = sched_state.executable_tables.read().await;
                    let matview_candidates: Vec<(String, String, String)> = tables.iter()
                        .filter(|t| {
                            t.transform.transform_type == "matview" || (t.auto_refresh && t.refresh_interval_seconds > 0)
                        })
                        .filter(|t| {
                            if let Some(ref lr) = t.last_refresh {
                                if let Ok(last) = chrono::DateTime::parse_from_rfc3339(lr) {
                                    let elapsed = (chrono::Utc::now() - last.with_timezone(&chrono::Utc)).num_seconds() as u64;
                                    elapsed >= t.refresh_interval_seconds
                                } else { true }
                            } else { true }
                        })
                        .map(|t| (t.table_name.clone(), t.transform.transform_type.clone(), t.transform.source_code.clone()))
                        .collect();
                    drop(tables);

                    for (name, tt, code) in matview_candidates {
                        tracing::info!(table=%name, "Scheduler: auto-refreshing matview");
                        if tt == "sql" || tt == "matview" {
                            let ctx = sched_state.ctx.read().await;
                            match ctx.sql(&code).await {
                                Ok(batches) => {
                                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                                    tracing::info!(table=%name, rows=%rows, "Scheduler: matview refresh complete");
                                    let mut tables = sched_state.executable_tables.write().await;
                                    if let Some(t) = tables.iter_mut().find(|t| t.table_name == name) {
                                        t.last_refresh = Some(chrono::Utc::now().to_rfc3339());
                                        let exec_id = uuid::Uuid::new_v4().to_string();
                                        t.history.push(crate::executable_table::ExecutionRecord {
                                            execution_id: exec_id,
                                            started_at: chrono::Utc::now().to_rfc3339(),
                                            completed_at: Some(chrono::Utc::now().to_rfc3339()),
                                            duration_ms: 0,
                                            status: "success".to_string(),
                                            rows_produced: Some(rows as u64),
                                            bytes_written: None,
                                            cost_usd: 0.0,
                                            binary_cached: false,
                                            compile_ms: 0,
                                            run_ms: 0,
                                            error: None,
                                            execution_location: "local".to_string(),
                                            version: t.versions.iter().map(|v| v.version).max().unwrap_or(1),
                                        });
                                        t.total_executions += 1;
                                    }
                                }
                                Err(e) => tracing::warn!(table=%name, error=%e, "Scheduler: matview refresh failed"),
                            }
                        }
                    }
                }
            }
        });
        tracing::info!("Background scheduler started (60s tick for executable table hot-swap + matview + cost-aware)");
    }

    let listener = TcpListener::bind(&bind_addr).await?;

    match node_role {
        // ── Standalone mode (default) ────────────────────────────────
        // Single process: HTTP API + optional Flight RPC. No distribution.
        NodeRole::Standalone => {
            if flight_enabled {
                let flight_ctx = Arc::new(tokio::sync::RwLock::new(
                    RustLakeContext::new(config).await?,
                ));
                let flight_svc = RustLakeFlightService::new(flight_ctx, flight_metrics);

                let flight_addr = format!("{}:{}", flight_config.host, flight_config.port);
                tracing::info!("Arrow Flight gRPC server starting on {}", flight_addr);

                tokio::select! {
                    result = axum::serve(listener, app) => { result?; }
                    result = flight_svc.serve(&flight_config) => { result?; }
                }
            } else {
                tracing::info!("Arrow Flight server disabled — enable with RUSTLAKE_FLIGHT__ENABLED=true");
                axum::serve(listener, app).await?;
            }
        }

        // ── Coordinator mode ─────────────────────────────────────────
        // Accepts client queries, distributes to workers, merges results.
        // Runs: HTTP API + Flight RPC (with coordinator worker registry).
        NodeRole::Coordinator => {
            let flight_ctx = Arc::new(tokio::sync::RwLock::new(
                RustLakeContext::new(config).await?,
            ));

            // Create the coordinator that manages workers.
            let coordinator = Arc::new(Coordinator::new(
                cluster_config.clone(),
                flight_ctx.clone(),
            ));

            // Discover workers based on config (static list, self-register, or K8s DNS).
            discovery::discover_workers(coordinator.clone(), &cluster_config, &k8s_config).await;

            // Start the health check loop in the background.
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            let health_coordinator = coordinator.clone();
            tokio::spawn(async move {
                health_coordinator.health_check_loop(cancel_rx).await;
            });

            // If K8s discovery is enabled, also start the periodic re-discovery loop.
            if cluster_config.discovery == rustlake_core::config::DiscoveryMethod::Kubernetes {
                let k8s_coordinator = coordinator.clone();
                let k8s_cfg = k8s_config.clone();
                let k8s_cancel = cancel_tx.subscribe();
                tokio::spawn(async move {
                    discovery::k8s_discovery_loop(k8s_coordinator, k8s_cfg, k8s_cancel).await;
                });
            }

            // Build Flight service with coordinator attached.
            let flight_svc = RustLakeFlightService::new(flight_ctx, flight_metrics)
                .with_coordinator(coordinator.clone());

            let flight_addr = format!("{}:{}", flight_config.host, flight_config.port);
            tracing::info!("Coordinator Flight gRPC server starting on {}", flight_addr);
            tracing::info!("Workers can register via: do_action('register_worker', ...)");

            tokio::select! {
                result = axum::serve(listener, app) => { result?; }
                result = flight_svc.serve(&flight_config) => { result?; }
            }

            // Signal background loops to stop.
            let _ = cancel_tx.send(true);
        }

        // ── Worker mode ──────────────────────────────────────────────
        // Registers with coordinator, sends heartbeats, executes partition scans.
        // Runs: Flight RPC server (for coordinator to dispatch queries to).
        // Optionally runs HTTP API for local health checks.
        NodeRole::Worker => {
            let flight_ctx = Arc::new(tokio::sync::RwLock::new(
                RustLakeContext::new(config).await?,
            ));

            // Register with the coordinator.
            let advertised_addr = format!("{}:{}", flight_config.host, flight_config.port);
            let mut worker = WorkerNode::new(
                cluster_config,
                advertised_addr,
                flight_ctx.clone(),
            );

            match worker.register().await {
                Ok(id) => tracing::info!(worker_id = %id, "Registered with coordinator"),
                Err(e) => tracing::warn!(error = %e, "Failed to register with coordinator — running standalone"),
            }

            // Start heartbeat loop in the background.
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            tokio::spawn(async move {
                worker.heartbeat_loop(cancel_rx).await;
            });

            // Run Flight server (workers don't need the coordinator attachment).
            let flight_svc = RustLakeFlightService::new(flight_ctx, flight_metrics);

            let flight_addr = format!("{}:{}", flight_config.host, flight_config.port);
            tracing::info!("Worker Flight gRPC server starting on {}", flight_addr);

            // Run both HTTP (for health checks) and Flight.
            tokio::select! {
                result = axum::serve(listener, app) => { result?; }
                result = flight_svc.serve(&flight_config) => { result?; }
            }

            let _ = cancel_tx.send(true);
        }
    }

    Ok(())
}
