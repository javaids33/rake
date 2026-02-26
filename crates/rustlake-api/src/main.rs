use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
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

mod postgres;
mod routes;
mod state;

use state::{load_chat_messages_from_file, load_scheduled_jobs_from_file, load_user_transforms_from_file, AppState};

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
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

    // Build the Axum router
    let app = Router::new()
        .merge(routes::api_routes())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state));

    tracing::info!(
        role = ?node_role,
        "RustLake API server starting on {}",
        bind_addr
    );
    tracing::info!("Try: curl -X POST http://{}/api/v1/sql -H 'Content-Type: application/json' -d '{{\"sql\": \"SELECT 1 + 1 AS result\"}}'", bind_addr);

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
