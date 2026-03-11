//! Federated data provider registry.
//!
//! Manages live `TableProvider` connections via `datafusion-table-providers`.
//! Each registered provider enables on-demand query execution with predicate and
//! projection pushdown to the source database — no eager `SELECT *` snapshots.
//!
//! Tables are registered under per-source DataFusion schemas (e.g., `pg`, `mysql`)
//! so that the registered name matches the physical source table name. This ensures
//! correct column qualifier resolution during predicate pushdown.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::MemorySchemaProvider;
use datafusion::execution::context::SessionContext;
use futures::stream::{self, StreamExt};
use tokio::sync::RwLock;

/// Maximum number of concurrent table registrations per connection.
const PARALLEL_TABLE_REGISTRATIONS: usize = 8;

/// A registered provider entry with connection metadata.
#[derive(Debug, Clone)]
pub struct ProviderEntry {
    /// Connection type (e.g., "postgres", "mysql", "sqlite").
    pub conn_type: String,
    /// Tables registered from this provider.
    pub tables: Vec<String>,
}

/// Central registry managing all federated data provider connections.
///
/// Wraps the provider-specific connection pools and table factories from
/// `datafusion-table-providers`, exposing a uniform interface for bootstrap
/// and on-demand table registration.
pub struct ProviderRegistry {
    entries: RwLock<HashMap<String, ProviderEntry>>,
}

impl ProviderRegistry {
    /// Create an empty provider registry.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// List all registered provider entries.
    pub async fn list_entries(&self) -> Vec<(String, ProviderEntry)> {
        self.entries
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Register a Postgres database as a federated provider.
    ///
    /// Discovers all public tables via a raw `tokio-postgres` query, then creates
    /// a live `TableProvider` for each via `datafusion-table-providers` with
    /// predicate/projection pushdown. Tables are registered with the given prefix.
    #[cfg(feature = "postgres")]
    pub async fn register_postgres(
        &self,
        connection_id: &str,
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
        prefix: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<String>, String> {
        use datafusion::common::TableReference;
        use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
        use datafusion_table_providers::postgres::PostgresTableFactory;
        use datafusion_table_providers::util::secrets::to_secret_map;

        // 1. Discover tables via raw tokio-postgres (lightweight, separate from pool)
        let table_names = discover_pg_tables(host, port, database, username, password).await?;

        // 2. Create the provider pool for live query execution
        let mut opts = HashMap::new();
        opts.insert("host".to_string(), host.to_string());
        opts.insert("port".to_string(), port.to_string());
        opts.insert("db".to_string(), database.to_string());
        opts.insert("user".to_string(), username.to_string());
        opts.insert("pass".to_string(), password.to_string());
        opts.insert("sslmode".to_string(), "disable".to_string());

        let pool = Arc::new(
            PostgresConnectionPool::new(to_secret_map(opts))
                .await
                .map_err(|e| format!("Postgres pool creation failed: {}", e))?,
        );

        let factory = PostgresTableFactory::new(pool);

        // 3. Register each table in a per-source schema so the registered name
        //    matches the physical table name (required for correct WHERE pushdown).
        let schema_provider = ensure_schema(ctx, prefix)?;

        // Resolve table providers in parallel (up to 8 concurrent)
        let factory = Arc::new(factory);
        let prefix_owned = prefix.to_string();
        let results: Vec<Option<(String, String, Arc<dyn datafusion::catalog::TableProvider>)>> = stream::iter(table_names.iter().cloned())
            .map(|table_name| {
                let factory = factory.clone();
                let prefix_owned = prefix_owned.clone();
                async move {
                    let table_ref = TableReference::partial("public", table_name.as_str());
                    match factory.table_provider(table_ref).await {
                        Ok(provider) => {
                            let df_name = format!("{}.{}", prefix_owned, table_name);
                            Some((table_name, df_name, provider))
                        }
                        Err(e) => {
                            tracing::warn!(table = %table_name, error = %e, "Failed to create table provider");
                            None
                        }
                    }
                }
            })
            .buffer_unordered(PARALLEL_TABLE_REGISTRATIONS)
            .collect()
            .await;

        let mut registered = Vec::new();
        for item in results.into_iter().flatten() {
            let (table_name, df_name, provider) = item;
            match schema_provider.register_table(table_name, provider) {
                Ok(_) => {
                    tracing::info!(table = %df_name, "Federated provider registered");
                    registered.push(df_name);
                }
                Err(e) => {
                    tracing::warn!(table = %df_name, error = %e, "Failed to register federated table");
                }
            }
        }

        self.entries.write().await.insert(
            connection_id.to_string(),
            ProviderEntry {
                conn_type: "postgres".to_string(),
                tables: registered.clone(),
            },
        );

        Ok(registered)
    }

    /// Register a MySQL database as a federated provider.
    #[cfg(feature = "mysql")]
    pub async fn register_mysql(
        &self,
        connection_id: &str,
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
        prefix: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<String>, String> {
        use datafusion::common::TableReference;
        use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
        use datafusion_table_providers::mysql::MySQLTableFactory;
        use datafusion_table_providers::util::secrets::to_secret_map;

        // 1. Discover tables via raw mysql_async (lightweight, separate from pool)
        let table_names = discover_mysql_tables(host, port, database, username, password).await?;

        // 2. Create the provider pool for live query execution
        let mut opts = HashMap::new();
        opts.insert("host".to_string(), host.to_string());
        opts.insert("tcp_port".to_string(), port.to_string());
        opts.insert("db".to_string(), database.to_string());
        opts.insert("user".to_string(), username.to_string());
        opts.insert("pass".to_string(), password.to_string());
        opts.insert("sslmode".to_string(), "disabled".to_string());

        let pool = Arc::new(
            MySQLConnectionPool::new(to_secret_map(opts))
                .await
                .map_err(|e| format!("MySQL pool creation failed: {}", e))?,
        );

        let factory = MySQLTableFactory::new(pool);

        // 3. Register each table in a per-source schema so the registered name
        //    matches the physical table name (required for correct WHERE pushdown).
        let schema_provider = ensure_schema(ctx, prefix)?;

        // Resolve table providers in parallel (up to 8 concurrent)
        let factory = Arc::new(factory);
        let prefix_owned = prefix.to_string();
        let database_owned = database.to_string();
        let results: Vec<Option<(String, String, Arc<dyn datafusion::catalog::TableProvider>)>> = stream::iter(table_names.iter().cloned())
            .map(|table_name| {
                let factory = factory.clone();
                let prefix_owned = prefix_owned.clone();
                let database_owned = database_owned.clone();
                async move {
                    let table_ref = TableReference::partial(database_owned.as_str(), table_name.as_str());
                    match factory.table_provider(table_ref).await {
                        Ok(provider) => {
                            let df_name = format!("{}.{}", prefix_owned, table_name);
                            Some((table_name, df_name, provider))
                        }
                        Err(e) => {
                            tracing::warn!(table = %table_name, error = %e, "Failed to create MySQL table provider");
                            None
                        }
                    }
                }
            })
            .buffer_unordered(PARALLEL_TABLE_REGISTRATIONS)
            .collect()
            .await;

        let mut registered = Vec::new();
        for item in results.into_iter().flatten() {
            let (table_name, df_name, provider) = item;
            match schema_provider.register_table(table_name, provider) {
                Ok(_) => {
                    tracing::info!(table = %df_name, "Federated provider registered");
                    registered.push(df_name);
                }
                Err(e) => {
                    tracing::warn!(table = %df_name, error = %e, "Failed to register federated table");
                }
            }
        }

        self.entries.write().await.insert(
            connection_id.to_string(),
            ProviderEntry {
                conn_type: "mysql".to_string(),
                tables: registered.clone(),
            },
        );

        Ok(registered)
    }

    /// Register a SQLite database file as a federated provider.
    #[cfg(feature = "sqlite")]
    pub async fn register_sqlite(
        &self,
        connection_id: &str,
        db_path: &str,
        prefix: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<String>, String> {
        use datafusion::common::TableReference;
        use datafusion_table_providers::sql::db_connection_pool::Mode;
        use datafusion_table_providers::sqlite::{SqliteTableFactory, SqliteTableProviderFactory};

        // Create the pool via the provider factory
        let spf = SqliteTableProviderFactory::new();
        let pool = Arc::new(
            spf.get_or_init_instance(db_path, Mode::File, std::time::Duration::from_secs(5))
                .await
                .map_err(|e| format!("SQLite pool creation failed: {}", e))?,
        );

        let factory = SqliteTableFactory::new(pool);

        // Discover tables from sqlite_master using rusqlite directly
        let table_names = discover_sqlite_tables(db_path)?;

        let schema_provider = ensure_schema(ctx, prefix)?;

        let mut registered = Vec::new();
        for table_name in &table_names {
            let table_ref = TableReference::bare(table_name.as_str());
            match factory.table_provider(table_ref).await {
                Ok(provider) => {
                    let df_name = format!("{}.{}", prefix, table_name);
                    match schema_provider.register_table(table_name.clone(), provider) {
                        Ok(_) => {
                            tracing::info!(table = %df_name, "Federated SQLite provider registered");
                            registered.push(df_name);
                        }
                        Err(e) => {
                            tracing::warn!(table = %df_name, error = %e, "Failed to register SQLite table");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(table = %table_name, error = %e, "Failed to create SQLite table provider");
                }
            }
        }

        self.entries.write().await.insert(
            connection_id.to_string(),
            ProviderEntry {
                conn_type: "sqlite".to_string(),
                tables: registered.clone(),
            },
        );

        Ok(registered)
    }

    /// Register all tables from a Trino connection as DataFusion TableProviders.
    ///
    /// Creates a per-catalog DataFusion schema (e.g., `trino_postgresql`, `trino_mysql`)
    /// and registers each Trino table with predicate/projection pushdown.
    /// Table names use `{schema}_{table}` format within the DataFusion schema, so
    /// `postgresql.public.customers` becomes `trino_postgresql.public_customers`.
    pub async fn register_trino(
        &self,
        connection_id: &str,
        trino_conn: &crate::trino_client::TrinoConnection,
        rest_client: std::sync::Arc<crate::trino_client::TrinoRestClient>,
        ctx: &SessionContext,
    ) -> Result<Vec<String>, String> {
        // Get catalog tree from DuckDB cache
        #[cfg(feature = "duckdb")]
        let tree = trino_conn.browse().await?;
        #[cfg(not(feature = "duckdb"))]
        return Err("DuckDB feature required for Trino provider".into());

        #[cfg(feature = "duckdb")]
        {
            use datafusion::catalog::SchemaProvider;

            // Pre-create all schema providers
            let mut schema_map: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
            for catalog in &tree.catalogs {
                let schema_prefix = format!("trino_{}", catalog.name);
                if !schema_map.contains_key(&schema_prefix) {
                    schema_map.insert(schema_prefix, ensure_schema(ctx, &format!("trino_{}", catalog.name))?);
                }
            }

            // Collect all (catalog, schema, table, prefix) tuples for parallel column resolution
            let mut table_tasks: Vec<(String, String, String, String)> = Vec::new();
            for catalog in &tree.catalogs {
                let schema_prefix = format!("trino_{}", catalog.name);
                for schema in &catalog.schemas {
                    for table_name in &schema.tables {
                        table_tasks.push((
                            catalog.name.clone(),
                            schema.name.clone(),
                            table_name.clone(),
                            schema_prefix.clone(),
                        ));
                    }
                }
            }

            // Resolve columns in parallel (up to 8 concurrent network calls)
            let results: Vec<Option<(String, String, String, Arc<dyn datafusion::catalog::TableProvider>)>> = stream::iter(table_tasks)
                .map(|(cat, sch, tbl, prefix)| {
                    let trino_conn = trino_conn;
                    let rest_client = rest_client.clone();
                    async move {
                        let columns = trino_conn
                            .columns(&cat, &sch, &tbl)
                            .await
                            .unwrap_or_default();

                        if columns.is_empty() {
                            tracing::warn!(
                                catalog = %cat, schema = %sch, table = %tbl,
                                "Skipping Trino table: no columns resolved"
                            );
                            return None;
                        }

                        let arrow_schema = crate::trino_provider::trino_columns_to_schema(&columns);
                        let provider = crate::trino_provider::TrinoTableProvider::new(
                            cat, sch.clone(), tbl.clone(), arrow_schema, rest_client,
                        );
                        let df_table_name = format!("{}_{}", sch, tbl);
                        let df_full = format!("{}.{}", prefix, df_table_name);
                        Some((df_table_name, df_full, prefix, Arc::new(provider) as Arc<dyn datafusion::catalog::TableProvider>))
                    }
                })
                .buffer_unordered(PARALLEL_TABLE_REGISTRATIONS)
                .collect()
                .await;

            // Register providers sequentially (schema_provider registration is not concurrent-safe)
            let mut registered = Vec::new();
            for item in results.into_iter().flatten() {
                let (df_table_name, df_full, prefix, provider) = item;
                if let Some(schema_prov) = schema_map.get(&prefix) {
                    match schema_prov.register_table(df_table_name, provider) {
                        Ok(_) => {
                            tracing::info!(table = %df_full, "Trino table provider registered");
                            registered.push(df_full);
                        }
                        Err(e) => {
                            tracing::warn!(table = %df_full, error = %e, "Failed to register Trino table provider");
                        }
                    }
                }
            }

            self.entries.write().await.insert(
                connection_id.to_string(),
                ProviderEntry {
                    conn_type: "trino".to_string(),
                    tables: registered.clone(),
                },
            );

            Ok(registered)
        }
    }
}

/// Ensure a named schema exists in the default DataFusion catalog.
///
/// Creates a `MemorySchemaProvider` under the default "datafusion" catalog so
/// that federated tables can be registered as `schema.table` — keeping the
/// registered name identical to the physical source table name. This is required
/// for correct column qualifier resolution during predicate pushdown.
pub fn ensure_schema(
    ctx: &SessionContext,
    schema_name: &str,
) -> Result<Arc<dyn datafusion::catalog::SchemaProvider>, String> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| "Default catalog 'datafusion' not found".to_string())?;

    // If the schema already exists, return it
    if let Some(existing) = catalog.schema(schema_name) {
        return Ok(existing);
    }

    let schema: Arc<dyn datafusion::catalog::SchemaProvider> =
        Arc::new(MemorySchemaProvider::new());
    catalog
        .register_schema(schema_name, schema.clone())
        .map_err(|e| format!("Failed to register schema '{}': {}", schema_name, e))?;

    Ok(schema)
}

/// Discover public table names from Postgres via a lightweight raw connection.
#[cfg(feature = "postgres")]
async fn discover_pg_tables(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<Vec<String>, String> {
    let conn_str = format!(
        "host={} port={} dbname={} user={} password={}",
        host, port, database, username, password
    );

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("Failed to connect to Postgres: {}", e))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "Postgres discovery connection error");
        }
    });

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW') \
             ORDER BY table_name",
            &[],
        )
        .await
        .map_err(|e| format!("Failed to discover tables: {}", e))?;

    let tables: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    Ok(tables)
}

/// Discover table names from MySQL via a lightweight raw connection.
#[cfg(feature = "mysql")]
async fn discover_mysql_tables(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<Vec<String>, String> {
    use mysql_async::prelude::*;
    use mysql_async::{Opts, OptsBuilder, Pool};

    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database))
        .user(Some(username))
        .pass(Some(password))
        .into();

    let pool = Pool::new(opts);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| format!("Failed to connect to MySQL: {}", e))?;

    let tables: Vec<String> = conn
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
        )
        .await
        .map_err(|e| format!("Failed to discover tables: {}", e))?;

    drop(conn);
    pool.disconnect().await.ok();

    Ok(tables)
}

/// Discover table names from a SQLite database file.
#[cfg(feature = "sqlite")]
fn discover_sqlite_tables(db_path: &str) -> Result<Vec<String>, String> {
    let conn = rusqlite::Connection::open(db_path)
        .map_err(|e| format!("Failed to open SQLite database: {}", e))?;

    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .map_err(|e| format!("SQLite table discovery failed: {}", e))?;

    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .map_err(|e| format!("SQLite table query failed: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(table_names)
}
