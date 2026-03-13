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
mod providers;
mod routes;
mod state;
mod trino_client;
mod trino_provider;
mod credential_store;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    // Load config (from file if provided, otherwise defaults)
    let config = match std::env::args().nth(1) {
        Some(path) => RustLakeConfig::from_file(&path)?,
        None => RustLakeConfig::default(),
    };

    let bind_addr = format!("{}:{}", config.api.host, config.api.port);
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
        tracing::info!(count = transforms.len(), "Loaded persisted user transforms from user_transforms.jsonl");
        *state.user_transforms.get_mut() = transforms;
    }

    let jobs = load_scheduled_jobs_from_file();
    if !jobs.is_empty() {
        tracing::info!(count = jobs.len(), "Loaded persisted scheduled jobs from scheduled_jobs.jsonl");
        *state.scheduled_jobs.get_mut() = jobs;
    }

    let connections = state::load_connections_from_file();
    if !connections.is_empty() {
        tracing::info!(count = connections.len(), "Loaded persisted connections from connections.jsonl");
        *state.connections.get_mut() = connections;
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
    tracing::info!(
        connections = saved_conns, passwords = saved_passwords,
        s3_creds = saved_s3, encrypted = cred_secured,
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
        .with_state(state);

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
