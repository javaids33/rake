//! Polars-compatible DataFrame engine.
//!
//! Provides a PolarsEngine with the same interface as DuckDbEngine (sql, register_arrow_table,
//! sync_tables, version). Internally routes queries through a dedicated DataFusion context.
//!
//! **Why not real Polars?** polars-arrow 0.53 depends on `chrono <= 0.4.41` while DataFusion 51
//! requires `chrono >= 0.4.42`. This is an upstream incompatibility. When polars 0.54+ resolves
//! the chrono conflict, this module will swap in real Polars execution (the API surface and all
//! UI/routing/scheduler integration is already complete).

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use rustlake_core::{Result, RustLakeError};
use tokio::sync::Mutex;

/// Polars-compatible DataFrame engine.
///
/// Uses a dedicated DataFusion `SessionContext` as the execution backend. This gives
/// an isolated engine instance (separate from the main DataFusion context) so engine
/// selection, routing, and benchmarking all work correctly.
pub struct PolarsEngine {
    ctx: Arc<Mutex<SessionContext>>,
}

impl PolarsEngine {
    /// Create a new Polars engine with a fresh execution context.
    pub fn new() -> Result<Self> {
        let ctx = SessionContext::new();
        Ok(Self {
            ctx: Arc::new(Mutex::new(ctx)),
        })
    }

    /// Execute a SQL query and return Arrow RecordBatches.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let ctx = self.ctx.lock().await;
        let df = ctx
            .sql(query)
            .await
            .map_err(|e| RustLakeError::Polars(format!("SQL execution failed: {}", e)))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| RustLakeError::Polars(format!("Collect failed: {}", e)))?;
        Ok(batches)
    }

    /// Register Arrow RecordBatches as a named table.
    pub async fn register_arrow_table(
        &self,
        name: &str,
        batches: &[RecordBatch],
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();
        let mem_table = datafusion::datasource::MemTable::try_new(
            schema,
            vec![batches.to_vec()],
        )
        .map_err(|e| RustLakeError::Polars(format!("Failed to create table '{}': {}", name, e)))?;

        let ctx = self.ctx.lock().await;
        ctx.register_table(name, Arc::new(mem_table))
            .map_err(|e| {
                RustLakeError::Polars(format!("Failed to register table '{}': {}", name, e))
            })?;
        Ok(())
    }

    /// Sync multiple tables into the Polars engine.
    /// Returns the number of tables successfully synced.
    pub async fn sync_tables(
        &self,
        tables: Vec<(String, Vec<RecordBatch>)>,
    ) -> Result<usize> {
        let mut synced = 0usize;
        for (name, batches) in tables {
            match self.register_arrow_table(&name, &batches).await {
                Ok(()) => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    tracing::debug!(table = %name, rows, "Polars: synced table");
                    synced += 1;
                }
                Err(e) => {
                    tracing::warn!(table = %name, error = %e, "Polars: failed to sync table");
                }
            }
        }
        Ok(synced)
    }

    /// Get engine version string.
    pub fn version(&self) -> String {
        "0.53-compat".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_polars_engine_basic() {
        let engine = PolarsEngine::new().unwrap();
        let batches = engine.sql("SELECT 1 + 1 AS result").await.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_register_and_query() {
        let engine = PolarsEngine::new().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();

        engine
            .register_arrow_table("users", &[batch])
            .await
            .unwrap();

        let result = engine
            .sql("SELECT COUNT(*) as cnt FROM users")
            .await
            .unwrap();
        assert_eq!(result[0].num_rows(), 1);
    }
}
