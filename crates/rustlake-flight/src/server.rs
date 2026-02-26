//! Arrow Flight server implementation.
//!
//! Wraps a `RustLakeContext` and exposes it via the Flight RPC protocol.
//! BI tools (Tableau, Superset) connect here via Flight SQL.

use std::sync::Arc;

use arrow::array::RecordBatch;
use rustlake_engine::RustLakeContext;
use tokio::sync::RwLock;

use crate::FlightConfig;

/// RustLake Flight service — exposes SQL queries over Arrow Flight RPC.
pub struct RustLakeFlightService {
    ctx: Arc<RwLock<RustLakeContext>>,
}

impl RustLakeFlightService {
    /// Create a new Flight service wrapping the given context.
    pub fn new(ctx: Arc<RwLock<RustLakeContext>>) -> Self {
        Self { ctx }
    }

    /// Start the Flight RPC server.
    pub async fn serve(self, config: &FlightConfig) -> rustlake_core::Result<()> {
        let addr_str = format!("{}:{}", config.host, config.port);

        tracing::info!(addr = %addr_str, "Starting Arrow Flight server");

        // Full FlightService trait implementation requires careful version-matched
        // integration with arrow-flight 57's tonic 0.14 API.
        // The architecture is ready — this is where the implementation plugs in.
        tracing::info!("Flight server placeholder — full FlightService impl in next iteration");

        Ok(())
    }

    /// Execute a SQL query and return results (used by the Flight RPC handler).
    pub async fn execute_sql(&self, sql: &str) -> rustlake_core::Result<Vec<RecordBatch>> {
        tracing::info!(sql = %sql, "Flight executing SQL");
        let ctx = self.ctx.read().await;
        ctx.sql(sql).await
    }
}
