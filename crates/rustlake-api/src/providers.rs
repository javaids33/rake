//! Federated data provider registry.
//!
//! Manages live `TableProvider` connections via `datafusion-table-providers`.
//! Each registered provider enables on-demand query execution with predicate and
//! projection pushdown to the source database — no eager `SELECT *` snapshots.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use tokio::sync::RwLock;

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

        // 3. Register each table as a federated TableProvider
        let mut registered = Vec::new();
        for table_name in &table_names {
            let table_ref = TableReference::partial("public", table_name.as_str());
            match factory.table_provider(table_ref).await {
                Ok(provider) => {
                    let df_name = format!("{}_{}", prefix, table_name);
                    match ctx.register_table(&df_name, provider) {
                        Ok(_) => {
                            tracing::info!(table = %df_name, "Federated provider registered");
                            registered.push(df_name);
                        }
                        Err(e) => {
                            tracing::warn!(table = %df_name, error = %e, "Failed to register federated table");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(table = %table_name, error = %e, "Failed to create table provider");
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

        // 3. Register each table as a federated TableProvider
        let mut registered = Vec::new();
        for table_name in &table_names {
            let table_ref = TableReference::partial(database, table_name.as_str());
            match factory.table_provider(table_ref).await {
                Ok(provider) => {
                    let df_name = format!("{}_{}", prefix, table_name);
                    match ctx.register_table(&df_name, provider) {
                        Ok(_) => {
                            tracing::info!(table = %df_name, "Federated provider registered");
                            registered.push(df_name);
                        }
                        Err(e) => {
                            tracing::warn!(table = %df_name, error = %e, "Failed to register federated table");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(table = %table_name, error = %e, "Failed to create MySQL table provider");
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

        let mut registered = Vec::new();
        for table_name in &table_names {
            let table_ref = TableReference::bare(table_name.as_str());
            match factory.table_provider(table_ref).await {
                Ok(provider) => {
                    let df_name = format!("{}_{}", prefix, table_name);
                    match ctx.register_table(&df_name, provider) {
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
