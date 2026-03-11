//! Trino REST API client with DuckDB-backed metadata cache.
//!
//! Architecture:
//! - On connect: fetch schema/table names only (lightweight, ~5 API calls)
//! - Cache in local DuckDB file → subsequent reads are sub-ms
//! - On-demand: fetch column info and previews when user drills down
//! - Explicit refresh to re-sync with Trino
//! - All Trino REST calls go through `TrinoRestClient::query()`

use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ── Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrinoCatalogTree {
    pub catalogs: Vec<TrinoCatalogEntry>,
    pub cached_at: Option<String>,
    pub total_tables: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrinoCatalogEntry {
    pub name: String,
    pub schemas: Vec<TrinoSchemaEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrinoSchemaEntry {
    pub name: String,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrinoColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub ordinal: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrinoQueryResult {
    pub columns: Vec<String>,
    pub column_types: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub duration_ms: u128,
}

// ── DuckDB Metadata Cache ────────────────────────────────────────────

#[cfg(feature = "duckdb")]
pub struct TrinoCache {
    db: Arc<std::sync::Mutex<duckdb::Connection>>,
}

#[cfg(feature = "duckdb")]
impl TrinoCache {
    pub fn new(path: &str) -> Result<Self, String> {
        let conn = duckdb::Connection::open(path)
            .map_err(|e| format!("Failed to open cache DB: {}", e))?;
        let cache = Self {
            db: Arc::new(std::sync::Mutex::new(conn)),
        };
        cache.init_schema()?;
        Ok(cache)
    }

    fn init_schema(&self) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS trino_schemas (
                conn_id TEXT, catalog_name TEXT, schema_name TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (conn_id, catalog_name, schema_name)
            );
            CREATE TABLE IF NOT EXISTS trino_tables (
                conn_id TEXT, catalog_name TEXT, schema_name TEXT,
                table_name TEXT, table_type TEXT DEFAULT 'TABLE',
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (conn_id, catalog_name, schema_name, table_name)
            );
            CREATE TABLE IF NOT EXISTS trino_columns (
                conn_id TEXT, catalog_name TEXT, schema_name TEXT,
                table_name TEXT, column_name TEXT, data_type TEXT,
                is_nullable BOOLEAN DEFAULT TRUE, ordinal_position INTEGER DEFAULT 0,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (conn_id, catalog_name, schema_name, table_name, column_name)
            );
            CREATE TABLE IF NOT EXISTS trino_preview_meta (
                conn_id TEXT, catalog_name TEXT, schema_name TEXT,
                table_name TEXT, row_count INTEGER, cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (conn_id, catalog_name, schema_name, table_name)
            );"
        ).map_err(|e| format!("Cache schema init failed: {}", e))?;
        Ok(())
    }

    // ── Cache reads ──

    pub fn get_catalog_tree(&self, conn_id: &str) -> Option<TrinoCatalogTree> {
        let db = self.db.lock().ok()?;
        // Get all schemas
        let mut stmt = db.prepare(
            "SELECT DISTINCT catalog_name, schema_name FROM trino_schemas WHERE conn_id = ? ORDER BY catalog_name, schema_name"
        ).ok()?;
        let schema_rows: Vec<(String, String)> = stmt.query_map(duckdb::params![conn_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }).ok()?.filter_map(|r| r.ok()).collect();

        if schema_rows.is_empty() { return None; }

        // Get all tables
        let mut stmt = db.prepare(
            "SELECT catalog_name, schema_name, table_name FROM trino_tables WHERE conn_id = ? ORDER BY catalog_name, schema_name, table_name"
        ).ok()?;
        let table_rows: Vec<(String, String, String)> = stmt.query_map(duckdb::params![conn_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        }).ok()?.filter_map(|r| r.ok()).collect();

        // Get cached_at
        let cached_at: Option<String> = db.query_row(
            "SELECT MAX(cached_at)::TEXT FROM trino_schemas WHERE conn_id = ?",
            duckdb::params![conn_id],
            |row| row.get(0),
        ).ok();

        // Build tree
        let mut catalogs: Vec<TrinoCatalogEntry> = Vec::new();
        let mut current_catalog: Option<String> = None;
        let mut current_schemas: Vec<TrinoSchemaEntry> = Vec::new();

        for (cat, schema) in &schema_rows {
            if current_catalog.as_ref() != Some(cat) {
                if let Some(prev_cat) = current_catalog.take() {
                    catalogs.push(TrinoCatalogEntry { name: prev_cat, schemas: std::mem::take(&mut current_schemas) });
                }
                current_catalog = Some(cat.clone());
            }
            let tables: Vec<String> = table_rows.iter()
                .filter(|(c, s, _)| c == cat && s == schema)
                .map(|(_, _, t)| t.clone())
                .collect();
            current_schemas.push(TrinoSchemaEntry { name: schema.clone(), tables });
        }
        if let Some(cat) = current_catalog {
            catalogs.push(TrinoCatalogEntry { name: cat, schemas: current_schemas });
        }

        let total_tables = table_rows.len();
        Some(TrinoCatalogTree { catalogs, cached_at, total_tables })
    }

    pub fn get_columns(&self, conn_id: &str, catalog: &str, schema: &str, table: &str) -> Option<Vec<TrinoColumnInfo>> {
        let db = self.db.lock().ok()?;
        let mut stmt = db.prepare(
            "SELECT column_name, data_type, is_nullable, ordinal_position FROM trino_columns
             WHERE conn_id = ? AND catalog_name = ? AND schema_name = ? AND table_name = ?
             ORDER BY ordinal_position"
        ).ok()?;
        let cols: Vec<TrinoColumnInfo> = stmt.query_map(
            duckdb::params![conn_id, catalog, schema, table], |row| {
                Ok(TrinoColumnInfo {
                    name: row.get(0)?,
                    data_type: row.get(1)?,
                    nullable: row.get::<_, bool>(2).unwrap_or(true),
                    ordinal: row.get::<_, i32>(3).unwrap_or(0),
                })
            }
        ).ok()?.filter_map(|r| r.ok()).collect();
        if cols.is_empty() { None } else { Some(cols) }
    }

    // ── Cache writes ──

    pub fn set_schemas(&self, conn_id: &str, catalog: &str, schemas: &[String]) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        // Clear old entries for this catalog
        db.execute(
            "DELETE FROM trino_schemas WHERE conn_id = ? AND catalog_name = ?",
            duckdb::params![conn_id, catalog],
        ).map_err(|e| e.to_string())?;
        let mut stmt = db.prepare(
            "INSERT INTO trino_schemas (conn_id, catalog_name, schema_name) VALUES (?, ?, ?)"
        ).map_err(|e| e.to_string())?;
        for s in schemas {
            stmt.execute(duckdb::params![conn_id, catalog, s]).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn set_tables(&self, conn_id: &str, catalog: &str, schema: &str, tables: &[String]) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute(
            "DELETE FROM trino_tables WHERE conn_id = ? AND catalog_name = ? AND schema_name = ?",
            duckdb::params![conn_id, catalog, schema],
        ).map_err(|e| e.to_string())?;
        let mut stmt = db.prepare(
            "INSERT INTO trino_tables (conn_id, catalog_name, schema_name, table_name) VALUES (?, ?, ?, ?)"
        ).map_err(|e| e.to_string())?;
        for t in tables {
            stmt.execute(duckdb::params![conn_id, catalog, schema, t]).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn set_columns(&self, conn_id: &str, catalog: &str, schema: &str, table: &str, columns: &[TrinoColumnInfo]) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute(
            "DELETE FROM trino_columns WHERE conn_id = ? AND catalog_name = ? AND schema_name = ? AND table_name = ?",
            duckdb::params![conn_id, catalog, schema, table],
        ).map_err(|e| e.to_string())?;
        let mut stmt = db.prepare(
            "INSERT INTO trino_columns (conn_id, catalog_name, schema_name, table_name, column_name, data_type, is_nullable, ordinal_position) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ).map_err(|e| e.to_string())?;
        for c in columns {
            stmt.execute(duckdb::params![conn_id, catalog, schema, table, c.name, c.data_type, c.nullable, c.ordinal]).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn invalidate_connection(&self, conn_id: &str) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute("DELETE FROM trino_schemas WHERE conn_id = ?", duckdb::params![conn_id]).map_err(|e| e.to_string())?;
        db.execute("DELETE FROM trino_tables WHERE conn_id = ?", duckdb::params![conn_id]).map_err(|e| e.to_string())?;
        db.execute("DELETE FROM trino_columns WHERE conn_id = ?", duckdb::params![conn_id]).map_err(|e| e.to_string())?;
        db.execute("DELETE FROM trino_preview_meta WHERE conn_id = ?", duckdb::params![conn_id]).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn cache_stats(&self, conn_id: &str) -> serde_json::Value {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return serde_json::json!({}),
        };
        let schemas: i64 = db.query_row("SELECT COUNT(*) FROM trino_schemas WHERE conn_id = ?", duckdb::params![conn_id], |r| r.get(0)).unwrap_or(0);
        let tables: i64 = db.query_row("SELECT COUNT(*) FROM trino_tables WHERE conn_id = ?", duckdb::params![conn_id], |r| r.get(0)).unwrap_or(0);
        let columns: i64 = db.query_row("SELECT COUNT(*) FROM trino_columns WHERE conn_id = ?", duckdb::params![conn_id], |r| r.get(0)).unwrap_or(0);
        let last_cached: Option<String> = db.query_row("SELECT MAX(cached_at)::TEXT FROM trino_schemas WHERE conn_id = ?", duckdb::params![conn_id], |r| r.get(0)).ok();
        serde_json::json!({
            "schemas_cached": schemas,
            "tables_cached": tables,
            "columns_cached": columns,
            "last_refresh": last_cached,
        })
    }
}

// ── Trino REST Client ────────────────────────────────────────────────

/// Authentication method for Trino connections.
#[derive(Debug, Clone)]
pub enum TrinoAuth {
    /// No authentication (X-Trino-User header only).
    None,
    /// HTTP Basic Auth (username + password).
    Basic(String),
    /// JWT Bearer token (for enterprise Trino/Starburst).
    Bearer(String),
}

pub struct TrinoRestClient {
    http: reqwest::Client,
    pub base_url: String,
    pub user: String,
    auth: TrinoAuth,
}

impl std::fmt::Debug for TrinoRestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoRestClient")
            .field("base_url", &self.base_url)
            .field("user", &self.user)
            .field("auth", &match &self.auth {
                TrinoAuth::None => "none",
                TrinoAuth::Basic(_) => "basic",
                TrinoAuth::Bearer(_) => "bearer",
            })
            .finish()
    }
}

impl TrinoRestClient {
    /// Create a new client with password-based auth (basic or none).
    pub fn new(base_url: String, user: String, password: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .danger_accept_invalid_certs(
                std::env::var("RUSTLAKE_TRINO_INSECURE").unwrap_or_default() == "true"
            )
            .build()
            .unwrap_or_default();
        let auth = if password.is_empty() {
            TrinoAuth::None
        } else if password.starts_with("ey") && password.contains('.') {
            // JWT tokens start with "ey" (base64 of {"alg":...})
            TrinoAuth::Bearer(password)
        } else {
            TrinoAuth::Basic(password)
        };
        Self { http, base_url, user, auth }
    }

    /// Apply auth headers to a request builder.
    fn apply_auth(&self, mut builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder = builder.header("X-Trino-User", &self.user);
        match &self.auth {
            TrinoAuth::None => builder,
            TrinoAuth::Basic(password) => builder.basic_auth(&self.user, Some(password)),
            TrinoAuth::Bearer(token) => builder.bearer_auth(token),
        }
    }

    pub async fn server_info(&self) -> Result<serde_json::Value, String> {
        let builder = self.http.get(&format!("{}/v1/info", self.base_url));
        let resp = self.apply_auth(builder)
            .send().await.map_err(|e| format!("Trino /v1/info failed: {}", e))?;
        resp.json().await.map_err(|e| format!("Parse error: {}", e))
    }

    pub async fn list_catalogs(&self) -> Result<Vec<String>, String> {
        let rows = self.query("SHOW CATALOGS", "system").await?;
        Ok(rows.into_iter()
            .filter_map(|r| r.first().and_then(|v| v.as_str()).map(|s| s.trim().to_string()))
            .filter(|s| s != "system")
            .collect())
    }

    pub async fn list_schemas(&self, catalog: &str) -> Result<Vec<String>, String> {
        let rows = self.query(&format!("SHOW SCHEMAS FROM \"{}\"", catalog), catalog).await?;
        Ok(rows.into_iter()
            .filter_map(|r| r.first().and_then(|v| v.as_str()).map(|s| s.trim().to_string()))
            .filter(|s| s != "information_schema" && s != "pg_catalog" && s != "performance_schema")
            .collect())
    }

    pub async fn list_tables(&self, catalog: &str, schema: &str) -> Result<Vec<String>, String> {
        let rows = self.query(&format!("SHOW TABLES FROM \"{}\".\"{}\"", catalog, schema), catalog).await?;
        Ok(rows.into_iter()
            .filter_map(|r| r.first().and_then(|v| v.as_str()).map(|s| s.trim().to_string()))
            .collect())
    }

    pub async fn describe_table(&self, catalog: &str, schema: &str, table: &str) -> Result<Vec<TrinoColumnInfo>, String> {
        let rows = self.query(&format!("DESCRIBE \"{}\".\"{}\".\"{}\"\n", catalog, schema, table), catalog).await?;
        Ok(rows.into_iter().enumerate().map(|(i, row)| {
            TrinoColumnInfo {
                name: row.first().and_then(|v| v.as_str()).unwrap_or("?").trim().to_string(),
                data_type: row.get(1).and_then(|v| v.as_str()).unwrap_or("varchar").trim().to_string(),
                nullable: row.get(2).and_then(|v| v.as_str()).map(|s| s.contains("YES")).unwrap_or(true),
                ordinal: i as i32,
            }
        }).collect())
    }

    pub async fn execute_query(&self, sql: &str, catalog: &str) -> Result<TrinoQueryResult, String> {
        let start = std::time::Instant::now();
        // Get columns from the first response
        let req_builder = self.http.post(&format!("{}/v1/statement", self.base_url))
            .header("X-Trino-Catalog", catalog)
            .body(sql.to_string());
        let resp = self.apply_auth(req_builder)
            .send().await.map_err(|e| format!("Trino query failed: {}", e))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Trino HTTP {}: {}", status, &body[..body.len().min(500)]));
        }
        let body: serde_json::Value = resp.json().await.map_err(|e| format!("Parse: {}", e))?;

        let mut all_data: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut columns: Vec<String> = Vec::new();
        let mut column_types: Vec<String> = Vec::new();

        if let Some(cols) = body.get("columns").and_then(|c| c.as_array()) {
            columns = cols.iter().filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())).collect();
            column_types = cols.iter().filter_map(|c| c.get("type").and_then(|n| n.as_str()).map(|s| s.to_string())).collect();
        }
        if let Some(data) = body.get("data").and_then(|d| d.as_array()) {
            all_data.extend(data.iter().cloned().filter_map(|v| v.as_array().cloned()));
        }

        // Follow nextUri chain
        let mut next_uri = body.get("nextUri").and_then(|v| v.as_str()).map(|s| s.to_string());
        for _ in 0..200 {
            let Some(uri) = next_uri.take() else { break };
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let poll = self.http.get(&uri);
            let poll_resp = self.apply_auth(poll)
                .send().await.map_err(|e| format!("Poll: {}", e))?;
            let poll_body: serde_json::Value = poll_resp.json().await.map_err(|e| format!("Poll parse: {}", e))?;

            if columns.is_empty() {
                if let Some(cols) = poll_body.get("columns").and_then(|c| c.as_array()) {
                    columns = cols.iter().filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())).collect();
                    column_types = cols.iter().filter_map(|c| c.get("type").and_then(|n| n.as_str()).map(|s| s.to_string())).collect();
                }
            }
            if let Some(data) = poll_body.get("data").and_then(|d| d.as_array()) {
                all_data.extend(data.iter().cloned().filter_map(|v| v.as_array().cloned()));
            }
            next_uri = poll_body.get("nextUri").and_then(|v| v.as_str()).map(|s| s.to_string());
            let state = poll_body.get("stats").and_then(|s| s.get("state")).and_then(|v| v.as_str()).unwrap_or("");
            if state == "FINISHED" || state == "FAILED" {
                if state == "FAILED" {
                    let err = poll_body.get("error").and_then(|e| e.get("message")).and_then(|m| m.as_str()).unwrap_or("Unknown error");
                    return Err(format!("Trino query failed: {}", err));
                }
                break;
            }
        }

        let row_count = all_data.len();
        Ok(TrinoQueryResult { columns, column_types, rows: all_data, row_count, duration_ms: start.elapsed().as_millis() })
    }

    /// Execute a SQL query and return raw row data.
    pub async fn query(&self, sql: &str, catalog: &str) -> Result<Vec<Vec<serde_json::Value>>, String> {
        let result = self.execute_query(sql, catalog).await?;
        Ok(result.rows)
    }
}

// ── Trino Connection (cache + REST combined) ─────────────────────────

#[allow(dead_code)] // Fields used by routes.rs handlers
pub struct TrinoConnection {
    pub id: String,
    pub name: String,
    pub rest: TrinoRestClient,
    pub default_catalog: String,
    #[cfg(feature = "duckdb")]
    pub cache: Arc<TrinoCache>,
}

impl TrinoConnection {
    /// Fetch catalog tree: cache-first, network-fallback.
    #[cfg(feature = "duckdb")]
    pub async fn browse(&self) -> Result<TrinoCatalogTree, String> {
        // Try cache
        let cache = self.cache.clone();
        let conn_id = self.id.clone();
        let cached = tokio::task::spawn_blocking(move || cache.get_catalog_tree(&conn_id))
            .await.unwrap_or(None);
        if let Some(tree) = cached {
            if tree.total_tables > 0 {
                return Ok(tree);
            }
        }
        // Cache miss — fetch from Trino and cache
        self.refresh_cache().await?;
        let cache = self.cache.clone();
        let conn_id = self.id.clone();
        tokio::task::spawn_blocking(move || cache.get_catalog_tree(&conn_id))
            .await.unwrap_or(None)
            .ok_or_else(|| "Failed to build catalog tree after refresh".to_string())
    }

    /// Fetch columns for a table: cache-first, network-fallback.
    #[cfg(feature = "duckdb")]
    pub async fn columns(&self, catalog: &str, schema: &str, table: &str) -> Result<Vec<TrinoColumnInfo>, String> {
        let cache = self.cache.clone();
        let (conn_id, cat, sch, tbl) = (self.id.clone(), catalog.to_string(), schema.to_string(), table.to_string());
        let cached = tokio::task::spawn_blocking(move || cache.get_columns(&conn_id, &cat, &sch, &tbl))
            .await.unwrap_or(None);
        if let Some(cols) = cached {
            return Ok(cols);
        }
        // Fetch from Trino
        let cols = self.rest.describe_table(catalog, schema, table).await?;
        // Cache
        let cache = self.cache.clone();
        let (conn_id, cat, sch, tbl) = (self.id.clone(), catalog.to_string(), schema.to_string(), table.to_string());
        let cols_clone = cols.clone();
        tokio::task::spawn_blocking(move || {
            let _ = cache.set_columns(&conn_id, &cat, &sch, &tbl, &cols_clone);
        }).await.ok();
        Ok(cols)
    }

    /// Preview table data (not cached in DuckDB — goes to Trino with LIMIT).
    pub async fn preview(&self, catalog: &str, schema: &str, table: &str, limit: usize) -> Result<TrinoQueryResult, String> {
        let sql = format!("SELECT * FROM \"{}\".\"{}\".\"{}\"\nLIMIT {}", catalog, schema, table, limit);
        self.rest.execute_query(&sql, catalog).await
    }

    /// Execute arbitrary SQL through Trino.
    pub async fn query(&self, sql: &str, catalog: &str) -> Result<TrinoQueryResult, String> {
        self.rest.execute_query(sql, catalog).await
    }

    /// Re-fetch all metadata from Trino and update cache.
    #[cfg(feature = "duckdb")]
    pub async fn refresh_cache(&self) -> Result<usize, String> {
        tracing::info!(conn_id = %self.id, "Refreshing Trino cache from REST API");
        // Invalidate old cache
        let cache = self.cache.clone();
        let conn_id = self.id.clone();
        tokio::task::spawn_blocking(move || cache.invalidate_connection(&conn_id))
            .await.map_err(|e| e.to_string())??;

        let catalogs = self.rest.list_catalogs().await?;
        let mut total_tables = 0usize;

        for catalog in &catalogs {
            let schemas = self.rest.list_schemas(catalog).await.unwrap_or_default();
            // Cache schemas
            let cache = self.cache.clone();
            let (cid, cat) = (self.id.clone(), catalog.clone());
            let schemas_clone = schemas.clone();
            tokio::task::spawn_blocking(move || cache.set_schemas(&cid, &cat, &schemas_clone))
                .await.map_err(|e| e.to_string())??;

            for schema in &schemas {
                let tables = self.rest.list_tables(catalog, schema).await.unwrap_or_default();
                total_tables += tables.len();
                // Cache tables
                let cache = self.cache.clone();
                let (cid, cat, sch) = (self.id.clone(), catalog.clone(), schema.clone());
                let tables_clone = tables.clone();
                tokio::task::spawn_blocking(move || cache.set_tables(&cid, &cat, &sch, &tables_clone))
                    .await.map_err(|e| e.to_string())??;
            }
        }

        tracing::info!(conn_id = %self.id, catalogs = catalogs.len(), tables = total_tables, "Trino cache refreshed");
        Ok(total_tables)
    }

    /// Cache statistics.
    #[cfg(feature = "duckdb")]
    pub async fn stats(&self) -> serde_json::Value {
        let cache = self.cache.clone();
        let conn_id = self.id.clone();
        tokio::task::spawn_blocking(move || cache.cache_stats(&conn_id))
            .await.unwrap_or_else(|_| serde_json::json!({}))
    }
}
