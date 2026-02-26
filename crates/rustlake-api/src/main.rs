use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use rustlake_core::RustLakeConfig;
use rustlake_engine::RustLakeContext;
use rustlake_vector::embedding::SimpleEmbeddingGenerator;
use rustlake_vector::search::VectorIndex;

mod postgres;
mod routes;
mod state;

use state::{load_chat_messages_from_file, load_scheduled_jobs_from_file, load_user_transforms_from_file, AppState};

/// Pre-populate the vector index with product data from sample-data/products.csv.
///
/// Reads each product's name and category, generates a deterministic embedding
/// from the combined text, and indexes it so the demo can search immediately.
fn load_product_vectors() -> VectorIndex {
    const DIMENSIONS: usize = 128;
    let generator = SimpleEmbeddingGenerator::new(DIMENSIONS);
    let mut index = VectorIndex::new(DIMENSIONS);

    // Try multiple paths — the binary may run from the project root or from
    // the crates/rustlake-api directory.
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

    // Simple CSV parsing (no external csv crate needed for this small file).
    // Format: product_id,name,category,price,cost,stock_qty
    let mut lines = csv_content.lines();
    let _header = lines.next(); // skip header

    for line in lines {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 4 {
            continue;
        }

        let product_id = fields[0].trim();
        let name = fields[1].trim();
        let category = fields[2].trim();
        let price = fields[3].trim();

        // Combine name + category for richer embedding text
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
    // Initialize tracing
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

    // Create the uploads directory for file uploads
    if let Err(e) = std::fs::create_dir_all("uploads") {
        tracing::warn!(error = %e, "Failed to create uploads/ directory");
    }

    // Create the query engine context
    let ctx = RustLakeContext::new(config).await?;

    // Pre-populate the vector index with product data for the demo
    let vector_index = load_product_vectors();
    let mut state = AppState::with_vector_index(ctx, vector_index);

    // Load persisted chat messages from disk
    let persisted = load_chat_messages_from_file();
    if !persisted.is_empty() {
        tracing::info!(count = persisted.len(), "Loaded persisted chat messages from feedback.jsonl");
        *state.chat_messages.get_mut() = persisted;
    }

    // Load persisted user transforms from disk
    let transforms = load_user_transforms_from_file();
    if !transforms.is_empty() {
        tracing::info!(count = transforms.len(), "Loaded persisted user transforms from user_transforms.jsonl");
        *state.user_transforms.get_mut() = transforms;
    }

    // Load persisted scheduled jobs from disk
    let jobs = load_scheduled_jobs_from_file();
    if !jobs.is_empty() {
        tracing::info!(count = jobs.len(), "Loaded persisted scheduled jobs from scheduled_jobs.jsonl");
        *state.scheduled_jobs.get_mut() = jobs;
    }

    // Build the router
    let app = Router::new()
        .merge(routes::api_routes())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state));

    tracing::info!("RustLake API server starting on {}", bind_addr);
    tracing::info!("Try: curl -X POST http://{}/api/v1/sql -H 'Content-Type: application/json' -d '{{\"sql\": \"SELECT 1 + 1 AS result\"}}'", bind_addr);
    tracing::info!("Try: curl http://{}/api/v1/vector/status", bind_addr);
    tracing::info!("Try: curl -X POST http://{}/api/v1/vector/search -H 'Content-Type: application/json' -d '{{\"query\": \"wireless headphones\", \"k\": 5}}'", bind_addr);

    let listener = TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
