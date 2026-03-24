use anyhow::Result;
use rmcp::{ServiceExt, transport::stdio};
use tracing_subscriber::EnvFilter;

mod client;
mod server;
mod tools;

#[tokio::main]
async fn main() -> Result<()> {
    // CRITICAL: all logging to stderr — stdout is the JSON-RPC transport
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let api_url = std::env::var("RUSTLAKE_API_URL")
        .unwrap_or_else(|_| "http://localhost:3000".to_string());

    tracing::info!("Starting RustLake MCP server (api={})", api_url);

    let service = server::RustLakeMcp::new(api_url)
        .serve(stdio())
        .await
        .inspect_err(|e| {
            tracing::error!("serving error: {:?}", e);
        })?;

    service.waiting().await?;
    Ok(())
}
