use arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};
use rustlake_core::config::RustLakeConfig;
use rustlake_core::{Result, RustLakeError};

/// The main query execution context. Wraps DataFusion's SessionContext
/// and integrates with the RustLake catalog and storage layers.
pub struct RustLakeContext {
    df_ctx: SessionContext,
    config: RustLakeConfig,
}

impl RustLakeContext {
    /// Create a new context with default in-memory catalog.
    pub async fn new(config: RustLakeConfig) -> Result<Self> {
        let mut df_config = datafusion::execution::context::SessionConfig::new()
            .with_batch_size(config.engine.batch_size)
            .with_target_partitions(config.engine.target_partitions);

        df_config = df_config.with_information_schema(true);
        df_config.options_mut().catalog.has_header = true;

        // Configure memory pool only when explicitly set (avoids tracking overhead on small queries)
        let df_ctx = if let Some(memory_limit) = config.engine.memory_limit {
            let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                .with_memory_limit(memory_limit, 1.0)
                .build_arc()
                .map_err(|e| RustLakeError::Engine(format!("Failed to create runtime: {}", e)))?;
            SessionContext::new_with_config_rt(df_config, runtime_env)
        } else {
            SessionContext::new_with_config(df_config)
        };

        Ok(Self { df_ctx, config })
    }

    /// Execute a SQL query and return results as Arrow RecordBatches.
    /// Automatically detects file paths in FROM clauses and registers them as tables.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        tracing::debug!(query, "Executing SQL");

        // Auto-register any file paths found in the query and rewrite SQL
        let rewritten = self.auto_register_files(query).await?;
        let effective_sql = rewritten.as_deref().unwrap_or(query);

        let df = self
            .df_ctx
            .sql(effective_sql)
            .await
            .map_err(|e| RustLakeError::Query(format!("SQL execution failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| RustLakeError::Query(format!("Failed to collect results: {}", e)))?;

        tracing::debug!(
            num_batches = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Query complete"
        );

        Ok(batches)
    }

    /// Scan SQL for quoted file paths, auto-register them as tables,
    /// and return rewritten SQL with paths replaced by table names.
    async fn auto_register_files(&self, query: &str) -> Result<Option<String>> {
        // Find all single-quoted strings that look like file paths
        let mut in_quote = false;
        let mut current = String::new();
        let mut replacements: Vec<(String, String)> = Vec::new();

        for ch in query.chars() {
            if ch == '\'' && !in_quote {
                in_quote = true;
                current.clear();
            } else if ch == '\'' && in_quote {
                if current.ends_with(".csv")
                    || current.ends_with(".parquet")
                    || current.ends_with(".parq")
                    || current.ends_with(".json")
                    || current.ends_with(".ndjson")
                {
                    // Build a unique table name from path components to avoid collisions
                    // e.g. "benchmarks/data/sf0.01/orders.csv" → "benchmarks_data_sf0_01_orders"
                    let table_name = current
                        .replace(['/', '\\', '.', '-'], "_")
                        .trim_end_matches("_csv")
                        .trim_end_matches("_parquet")
                        .trim_end_matches("_parq")
                        .trim_end_matches("_json")
                        .trim_end_matches("_ndjson")
                        .trim_start_matches('_')
                        .to_string();
                    replacements.push((current.clone(), table_name));
                }
                in_quote = false;
            } else if in_quote {
                current.push(ch);
            }
        }

        if replacements.is_empty() {
            return Ok(None);
        }

        let mut rewritten = query.to_string();

        for (path, table_name) in &replacements {
            // Register the file as a table if not already registered
            if self.df_ctx.table(table_name).await.is_err() {
                tracing::debug!(path = %path, table = %table_name, "Auto-registering file as table");

                if path.ends_with(".csv") {
                    self.df_ctx
                        .register_csv(table_name, path, CsvReadOptions::default())
                        .await
                        .map_err(|e| {
                            RustLakeError::Catalog(format!(
                                "Failed to auto-register '{}': {}",
                                path, e
                            ))
                        })?;
                } else if path.ends_with(".parquet") || path.ends_with(".parq") {
                    self.df_ctx
                        .register_parquet(table_name, path, ParquetReadOptions::default())
                        .await
                        .map_err(|e| {
                            RustLakeError::Catalog(format!(
                                "Failed to auto-register '{}': {}",
                                path, e
                            ))
                        })?;
                } else if path.ends_with(".json") || path.ends_with(".ndjson") {
                    self.df_ctx
                        .register_json(table_name, path, NdJsonReadOptions::default())
                        .await
                        .map_err(|e| {
                            RustLakeError::Catalog(format!(
                                "Failed to auto-register '{}': {}",
                                path, e
                            ))
                        })?;
                }
            }

            // Replace 'path/to/file.csv' with table_name in SQL
            rewritten = rewritten.replace(&format!("'{}'", path), table_name);
        }

        Ok(Some(rewritten))
    }

    /// Register a Parquet file or directory as a named table.
    pub async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        tracing::info!(name, path, "Registering Parquet table");

        self.df_ctx
            .register_parquet(name, path, ParquetReadOptions::default())
            .await
            .map_err(|e| {
                RustLakeError::Catalog(format!(
                    "Failed to register Parquet table '{}': {}",
                    name, e
                ))
            })?;

        Ok(())
    }

    /// Register a CSV file as a named table.
    pub async fn register_csv(&self, name: &str, path: &str) -> Result<()> {
        tracing::info!(name, path, "Registering CSV table");

        self.df_ctx
            .register_csv(name, path, CsvReadOptions::default())
            .await
            .map_err(|e| {
                RustLakeError::Catalog(format!("Failed to register CSV table '{}': {}", name, e))
            })?;

        Ok(())
    }

    /// Register a JSON (newline-delimited) file as a named table.
    pub async fn register_json(&self, name: &str, path: &str) -> Result<()> {
        tracing::info!(name, path, "Registering JSON table");

        self.df_ctx
            .register_json(name, path, NdJsonReadOptions::default())
            .await
            .map_err(|e| {
                RustLakeError::Catalog(format!(
                    "Failed to register JSON table '{}': {}",
                    name, e
                ))
            })?;

        Ok(())
    }

    /// Register a table from a path, auto-detecting format from extension.
    pub async fn register_table(&self, name: &str, path: &str) -> Result<()> {
        if path.ends_with(".parquet") || path.ends_with(".parq") {
            self.register_parquet(name, path).await
        } else if path.ends_with(".csv") {
            self.register_csv(name, path).await
        } else if path.ends_with(".json") || path.ends_with(".ndjson") {
            self.register_json(name, path).await
        } else {
            Err(RustLakeError::Catalog(format!(
                "Unknown file format for '{}'. Supported: .parquet, .csv, .json, .ndjson",
                path
            )))
        }
    }

    /// List all registered tables.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let catalog = self
            .df_ctx
            .catalog("datafusion")
            .ok_or_else(|| RustLakeError::Catalog("Default catalog not found".into()))?;

        let schema = catalog
            .schema("public")
            .ok_or_else(|| RustLakeError::Catalog("Default schema not found".into()))?;

        Ok(schema.table_names())
    }

    /// Get a reference to the underlying DataFusion context.
    pub fn datafusion_ctx(&self) -> &SessionContext {
        &self.df_ctx
    }

    /// Deregister a table from the DataFusion context.
    pub async fn deregister_table(&self, name: &str) -> Result<()> {
        self.df_ctx
            .deregister_table(name)
            .map_err(|e| rustlake_core::RustLakeError::Query(e.to_string()))?;
        Ok(())
    }

    /// Get the config.
    pub fn config(&self) -> &RustLakeConfig {
        &self.config
    }
}
