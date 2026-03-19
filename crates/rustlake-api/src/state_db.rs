//! DuckDB-backed persistent state store.
//!
//! Caches connection metadata, discovered tables, column schemas, S3 configs,
//! and streaming pipelines so the server starts instantly from cache and
//! re-syncs external databases in the background.

use std::sync::{Arc, Mutex};

/// Persistent state store backed by a local DuckDB file.
#[cfg(feature = "duckdb")]
pub struct StateDb {
    db: Arc<Mutex<duckdb::Connection>>,
}

#[cfg(feature = "duckdb")]
impl StateDb {
    /// Open (or create) the state database at the given path.
    pub fn open(path: &str) -> Result<Self, String> {
        let conn = duckdb::Connection::open(path)
            .map_err(|e| format!("StateDb open '{}': {}", path, e))?;
        let store = Self { db: Arc::new(Mutex::new(conn)) };
        store.init_schema()?;
        Ok(store)
    }

    /// Create all tables if they don't exist.
    fn init_schema(&self) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute_batch(
            "
            -- Connections: one row per external database connection
            CREATE TABLE IF NOT EXISTS connections (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                conn_type TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL DEFAULT 5432,
                database_name TEXT NOT NULL DEFAULT '',
                username TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'connected',
                source TEXT NOT NULL DEFAULT 'user',
                auth_method TEXT NOT NULL DEFAULT 'scram',
                connection_string TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Discovered tables per connection
            CREATE TABLE IF NOT EXISTS connection_tables (
                conn_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                schema_name TEXT,
                table_type TEXT DEFAULT 'TABLE',
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (conn_id, table_name)
            );

            -- Column schemas per table (optional — for immediate catalog display)
            CREATE TABLE IF NOT EXISTS connection_columns (
                conn_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                data_type TEXT NOT NULL DEFAULT 'TEXT',
                is_nullable BOOLEAN DEFAULT TRUE,
                ordinal_position INTEGER DEFAULT 0,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (conn_id, table_name, column_name)
            );

            -- S3 storage configurations
            CREATE TABLE IF NOT EXISTS s3_configs (
                name TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL DEFAULT '',
                bucket TEXT NOT NULL,
                region TEXT NOT NULL DEFAULT 'us-east-1',
                status TEXT NOT NULL DEFAULT 'configured',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- S3 discovered tables
            CREATE TABLE IF NOT EXISTS s3_tables (
                config_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                schema_name TEXT,
                format TEXT DEFAULT 'iceberg',
                table_type TEXT DEFAULT 'TABLE',
                s3_location TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (config_name, table_name)
            );

            -- Streaming pipelines
            CREATE TABLE IF NOT EXISTS pipelines (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_config TEXT NOT NULL DEFAULT '{}',
                transform_sql TEXT,
                sink_table TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'created',
                events_processed BIGINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Scheduled jobs
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL DEFAULT '{}'
            );

            -- User transforms
            CREATE TABLE IF NOT EXISTS user_transforms (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL DEFAULT '{}'
            );

            -- Migration metadata — tracks which JSONL files have been imported
            CREATE TABLE IF NOT EXISTS _migrations (
                key TEXT PRIMARY KEY,
                migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            "
        ).map_err(|e| format!("StateDb schema init: {}", e))?;
        Ok(())
    }

    // ── Connection CRUD ─────────────────────────────────────────────

    /// Save or update a connection.
    pub fn upsert_connection(&self, conn: &super::state::ConnectionEntry) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute(
            "INSERT OR REPLACE INTO connections (id, name, conn_type, host, port, database_name, username, status, source, auth_method, connection_string, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
            duckdb::params![
                conn.id, conn.name, conn.conn_type, conn.host, conn.port as i32,
                conn.database, conn.username, conn.status, conn.source,
                conn.auth_method, conn.connection_string.as_deref().unwrap_or(""),
            ],
        ).map_err(|e| format!("Upsert connection '{}': {}", conn.id, e))?;
        Ok(())
    }

    /// Remove a connection and its cached tables/columns.
    pub fn delete_connection(&self, conn_id: &str) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute("DELETE FROM connection_columns WHERE conn_id = ?", duckdb::params![conn_id])
            .map_err(|e| e.to_string())?;
        db.execute("DELETE FROM connection_tables WHERE conn_id = ?", duckdb::params![conn_id])
            .map_err(|e| e.to_string())?;
        db.execute("DELETE FROM connections WHERE id = ?", duckdb::params![conn_id])
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Save discovered tables for a connection (replaces old cache).
    pub fn cache_tables(&self, conn_id: &str, tables: &[String]) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute("DELETE FROM connection_tables WHERE conn_id = ?", duckdb::params![conn_id])
            .map_err(|e| e.to_string())?;
        let mut stmt = db.prepare(
            "INSERT INTO connection_tables (conn_id, table_name) VALUES (?, ?)"
        ).map_err(|e| e.to_string())?;
        for t in tables {
            stmt.execute(duckdb::params![conn_id, t]).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Save column schema for a table.
    #[allow(dead_code)]
    pub fn cache_columns(&self, conn_id: &str, table_name: &str, columns: &[(String, String, bool)]) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute(
            "DELETE FROM connection_columns WHERE conn_id = ? AND table_name = ?",
            duckdb::params![conn_id, table_name],
        ).map_err(|e| e.to_string())?;
        let mut stmt = db.prepare(
            "INSERT INTO connection_columns (conn_id, table_name, column_name, data_type, is_nullable, ordinal_position) VALUES (?, ?, ?, ?, ?, ?)"
        ).map_err(|e| e.to_string())?;
        for (i, (col_name, col_type, nullable)) in columns.iter().enumerate() {
            stmt.execute(duckdb::params![conn_id, table_name, col_name, col_type, nullable, i as i32])
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Load all connections from cache.
    #[allow(dead_code)]
    pub fn load_connections(&self) -> Vec<CachedConnection> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return vec![],
        };
        let mut stmt = match db.prepare(
            "SELECT id, name, conn_type, host, port, database_name, username, status, source, auth_method, connection_string FROM connections ORDER BY created_at"
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        let rows = stmt.query_map([], |row| {
            Ok(CachedConnection {
                id: row.get(0)?,
                name: row.get(1)?,
                conn_type: row.get(2)?,
                host: row.get(3)?,
                port: row.get::<_, i32>(4)? as u16,
                database: row.get(5)?,
                username: row.get(6)?,
                status: row.get(7)?,
                source: row.get(8)?,
                auth_method: row.get(9)?,
                connection_string: row.get::<_, String>(10).ok().filter(|s| !s.is_empty()),
            })
        });
        match rows {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => vec![],
        }
    }

    /// Load cached table names for a connection.
    pub fn load_tables(&self, conn_id: &str) -> Vec<String> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return vec![],
        };
        let mut stmt = match db.prepare("SELECT table_name FROM connection_tables WHERE conn_id = ? ORDER BY table_name") {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        match stmt.query_map(duckdb::params![conn_id], |row| row.get(0)) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(_) => vec![],
        }
    }

    /// Load cached columns for a table.
    #[allow(dead_code)]
    pub fn load_columns(&self, conn_id: &str, table_name: &str) -> Vec<(String, String, bool)> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return vec![],
        };
        let mut stmt = match db.prepare(
            "SELECT column_name, data_type, is_nullable FROM connection_columns WHERE conn_id = ? AND table_name = ? ORDER BY ordinal_position"
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        match stmt.query_map(duckdb::params![conn_id, table_name], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, bool>(2)?))
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(_) => vec![],
        }
    }

    // ── S3 Config CRUD ──────────────────────────────────────────────

    /// Load all S3 configs from DuckDB.
    pub fn load_s3_configs(&self) -> Vec<(String, String, String, String)> {
        let db = match self.db.lock() { Ok(db) => db, Err(_) => return vec![] };
        let mut stmt = match db.prepare("SELECT name, endpoint, bucket, region FROM s3_configs ORDER BY name") {
            Ok(s) => s, Err(_) => return vec![],
        };
        match stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?))
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(_) => vec![],
        }
    }

    /// Save or update an S3 config.
    pub fn upsert_s3_config(&self, name: &str, endpoint: &str, bucket: &str, region: &str) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute(
            "INSERT OR REPLACE INTO s3_configs (name, endpoint, bucket, region) VALUES (?, ?, ?, ?)",
            duckdb::params![name, endpoint, bucket, region],
        ).map_err(|e| format!("Upsert S3 config '{}': {}", name, e))?;
        Ok(())
    }

    /// Save discovered S3 tables (replaces old cache).
    pub fn cache_s3_tables(&self, config_name: &str, tables: &[(String, String, String)]) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute("DELETE FROM s3_tables WHERE config_name = ?", duckdb::params![config_name])
            .map_err(|e| e.to_string())?;
        let mut stmt = db.prepare(
            "INSERT INTO s3_tables (config_name, table_name, format, s3_location) VALUES (?, ?, ?, ?)"
        ).map_err(|e| e.to_string())?;
        for (table_name, format, location) in tables {
            stmt.execute(duckdb::params![config_name, table_name, format, location])
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Load S3 table names for a config.
    #[allow(dead_code)]
    pub fn load_s3_tables(&self, config_name: &str) -> Vec<(String, String, String)> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return vec![],
        };
        let mut stmt = match db.prepare(
            "SELECT table_name, format, COALESCE(s3_location, '') FROM s3_tables WHERE config_name = ? ORDER BY table_name"
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        match stmt.query_map(duckdb::params![config_name], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(_) => vec![],
        }
    }

    // ── Pipeline CRUD ───────────────────────────────────────────────

    /// Save or update a pipeline.
    pub fn upsert_pipeline(&self, p: &super::state::StreamingPipeline) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        let source_json = serde_json::to_string(&p.source_config).unwrap_or_default();
        db.execute(
            "INSERT OR REPLACE INTO pipelines (id, name, source_type, source_config, transform_sql, sink_table, status, events_processed)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                p.id, p.name, p.source_type, source_json,
                p.transform_sql.as_deref().unwrap_or(""),
                p.sink_table, p.status, p.events_processed as i64,
            ],
        ).map_err(|e| format!("Upsert pipeline '{}': {}", p.id, e))?;
        Ok(())
    }

    /// Delete a pipeline.
    pub fn delete_pipeline(&self, id: &str) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute("DELETE FROM pipelines WHERE id = ?", duckdb::params![id])
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Load all pipelines.
    pub fn load_pipelines(&self) -> Vec<super::state::StreamingPipeline> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return vec![],
        };
        let mut stmt = match db.prepare(
            "SELECT id, name, source_type, source_config, transform_sql, sink_table, status, events_processed, created_at FROM pipelines ORDER BY created_at"
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        let rows = stmt.query_map([], |row| {
            let source_json: String = row.get(3)?;
            let transform_sql: String = row.get(4)?;
            Ok(super::state::StreamingPipeline {
                id: row.get(0)?,
                name: row.get(1)?,
                source_type: row.get(2)?,
                source_config: serde_json::from_str(&source_json).unwrap_or_default(),
                transform_sql: if transform_sql.is_empty() { None } else { Some(transform_sql) },
                sink_table: row.get(5)?,
                status: row.get(6)?,
                events_processed: row.get::<_, i64>(7)? as u64,
                created_at: chrono::Utc::now(),
                snapshot_docs: None,
                snapshot_completed_at: None,
                files_written: 0,
                phase: String::new(),
            })
        });
        match rows {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => vec![],
        }
    }

    // ── Stats ───────────────────────────────────────────────────────

    /// Get summary counts for logging.
    pub fn summary(&self) -> (usize, usize, usize, usize) {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return (0, 0, 0, 0),
        };
        let count = |sql: &str| -> usize {
            db.query_row(sql, [], |row| row.get::<_, i64>(0))
                .unwrap_or(0) as usize
        };
        (
            count("SELECT COUNT(*) FROM connections"),
            count("SELECT COUNT(*) FROM connection_tables"),
            count("SELECT COUNT(*) FROM s3_configs"),
            count("SELECT COUNT(*) FROM pipelines"),
        )
    }

    // ── JSONL Migration ─────────────────────────────────────────────

    /// Check if a particular JSONL migration has been completed.
    pub fn is_migrated(&self, key: &str) -> bool {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return false,
        };
        db.query_row(
            "SELECT COUNT(*) FROM _migrations WHERE key = ?",
            duckdb::params![key],
            |row| row.get::<_, i64>(0),
        ).unwrap_or(0) > 0
    }

    /// Mark a JSONL migration as completed.
    pub fn mark_migrated(&self, key: &str) -> Result<(), String> {
        let db = self.db.lock().map_err(|e| e.to_string())?;
        db.execute(
            "INSERT OR REPLACE INTO _migrations (key) VALUES (?)",
            duckdb::params![key],
        ).map_err(|e| format!("Mark migration '{}': {}", key, e))?;
        Ok(())
    }

    /// Load scheduled jobs from DuckDB.
    pub fn load_jobs(&self) -> Vec<super::state::ScheduledJob> {
        let db = match self.db.lock() { Ok(db) => db, Err(_) => return vec![] };
        let mut stmt = match db.prepare("SELECT data FROM scheduled_jobs") { Ok(s) => s, Err(_) => return vec![] };
        let rows = stmt.query_map([], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        });
        match rows {
            Ok(r) => r.filter_map(|r| r.ok())
                .filter_map(|json| serde_json::from_str(&json).ok())
                .collect(),
            Err(_) => vec![],
        }
    }

    /// Load user transforms from DuckDB.
    pub fn load_transforms(&self) -> Vec<super::state::UserTransform> {
        let db = match self.db.lock() { Ok(db) => db, Err(_) => return vec![] };
        let mut stmt = match db.prepare("SELECT data FROM user_transforms") { Ok(s) => s, Err(_) => return vec![] };
        let rows = stmt.query_map([], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        });
        match rows {
            Ok(r) => r.filter_map(|r| r.ok())
                .filter_map(|json| serde_json::from_str(&json).ok())
                .collect(),
            Err(_) => vec![],
        }
    }

    /// Migrate scheduled jobs from Vec into DuckDB (stores as JSON blobs).
    pub fn migrate_jobs(&self, jobs: &[super::state::ScheduledJob]) -> Result<usize, String> {
        if self.is_migrated("scheduled_jobs") { return Ok(0); }
        let db = self.db.lock().map_err(|e| e.to_string())?;
        let mut count = 0;
        let mut stmt = db.prepare("INSERT OR REPLACE INTO scheduled_jobs (id, data) VALUES (?, ?)")
            .map_err(|e| e.to_string())?;
        for job in jobs {
            let json = serde_json::to_string(job).unwrap_or_default();
            stmt.execute(duckdb::params![job.id, json]).map_err(|e| e.to_string())?;
            count += 1;
        }
        drop(stmt);
        drop(db);
        self.mark_migrated("scheduled_jobs")?;
        tracing::info!(count, "Migrated scheduled_jobs from JSONL → DuckDB");
        Ok(count)
    }

    /// Migrate user transforms from Vec into DuckDB (stores as JSON blobs).
    pub fn migrate_transforms(&self, transforms: &[super::state::UserTransform]) -> Result<usize, String> {
        if self.is_migrated("user_transforms") { return Ok(0); }
        let db = self.db.lock().map_err(|e| e.to_string())?;
        let mut count = 0;
        let mut stmt = db.prepare("INSERT OR REPLACE INTO user_transforms (id, data) VALUES (?, ?)")
            .map_err(|e| e.to_string())?;
        for t in transforms {
            let json = serde_json::to_string(t).unwrap_or_default();
            let id = t.name.clone();
            stmt.execute(duckdb::params![id, json]).map_err(|e| e.to_string())?;
            count += 1;
        }
        drop(stmt);
        drop(db);
        self.mark_migrated("user_transforms")?;
        tracing::info!(count, "Migrated user_transforms from JSONL → DuckDB");
        Ok(count)
    }

    /// Migrate connections from Vec into DuckDB.
    pub fn migrate_connections(&self, connections: &[super::state::ConnectionEntry]) -> Result<usize, String> {
        if self.is_migrated("connections") { return Ok(0); }
        let mut count = 0;
        for conn in connections {
            self.upsert_connection(conn)?;
            if !conn.tables.is_empty() {
                self.cache_tables(&conn.id, &conn.tables)?;
            }
            count += 1;
        }
        self.mark_migrated("connections")?;
        tracing::info!(count, "Migrated connections from JSONL → DuckDB");
        Ok(count)
    }
}

/// A connection loaded from the DuckDB cache.
#[cfg(feature = "duckdb")]
#[allow(dead_code)]
pub struct CachedConnection {
    pub id: String,
    pub name: String,
    pub conn_type: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub status: String,
    pub source: String,
    pub auth_method: String,
    pub connection_string: Option<String>,
}

#[cfg(test)]
#[cfg(feature = "duckdb")]
mod tests {
    use super::*;

    fn temp_db() -> StateDb {
        StateDb::open(":memory:").expect("in-memory DuckDB should open")
    }

    #[test]
    fn test_open_and_init_schema() {
        let db = temp_db();
        let (conns, tables, s3s, pipes) = db.summary();
        assert_eq!(conns, 0);
        assert_eq!(tables, 0);
        assert_eq!(s3s, 0);
        assert_eq!(pipes, 0);
    }

    #[test]
    fn test_upsert_and_load_connection() {
        let db = temp_db();
        let entry = crate::state::ConnectionEntry {
            id: "test-1".into(),
            name: "Test Postgres".into(),
            conn_type: "postgres".into(),
            host: "localhost".into(),
            port: 5432,
            database: "testdb".into(),
            username: "user".into(),
            status: "connected".into(),
            tables: vec![],
            created_at: chrono::Utc::now(),
            source: "user".into(),
            sync_status: "ready".into(),
            sync_error: None,
            sync_progress: None,
            auth_method: "scram".into(),
            connection_string: None,
            aws_access_key: None,
            aws_secret_key: None,
            aws_session_token: None,
        };
        db.upsert_connection(&entry).unwrap();

        let (conns, _, _, _) = db.summary();
        assert_eq!(conns, 1);

        let loaded = db.load_connections();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Test Postgres");
        assert_eq!(loaded[0].conn_type, "postgres");
        assert_eq!(loaded[0].host, "localhost");
    }

    #[test]
    fn test_upsert_connection_is_idempotent() {
        let db = temp_db();
        let entry = crate::state::ConnectionEntry {
            id: "test-1".into(),
            name: "Version 1".into(),
            conn_type: "postgres".into(),
            host: "host1".into(),
            port: 5432,
            database: "db".into(),
            username: "u".into(),
            status: "connected".into(),
            tables: vec![],
            created_at: chrono::Utc::now(),
            source: "user".into(),
            sync_status: "ready".into(),
            sync_error: None,
            sync_progress: None,
            auth_method: "scram".into(),
            connection_string: None,
            aws_access_key: None,
            aws_secret_key: None,
            aws_session_token: None,
        };
        db.upsert_connection(&entry).unwrap();

        // Update same ID
        let mut entry2 = entry.clone();
        entry2.name = "Version 2".into();
        entry2.host = "host2".into();
        db.upsert_connection(&entry2).unwrap();

        let (conns, _, _, _) = db.summary();
        assert_eq!(conns, 1); // Still 1, not 2

        let loaded = db.load_connections();
        assert_eq!(loaded[0].name, "Version 2");
        assert_eq!(loaded[0].host, "host2");
    }

    #[test]
    fn test_delete_connection() {
        let db = temp_db();
        let entry = crate::state::ConnectionEntry {
            id: "del-me".into(),
            name: "Delete Me".into(),
            conn_type: "mysql".into(),
            host: "localhost".into(),
            port: 3306,
            database: "db".into(),
            username: "u".into(),
            status: "connected".into(),
            tables: vec![],
            created_at: chrono::Utc::now(),
            source: "user".into(),
            sync_status: "ready".into(),
            sync_error: None,
            sync_progress: None,
            auth_method: "scram".into(),
            connection_string: None,
            aws_access_key: None,
            aws_secret_key: None,
            aws_session_token: None,
        };
        db.upsert_connection(&entry).unwrap();
        db.cache_tables("del-me", &["table1".into(), "table2".into()]).unwrap();

        assert_eq!(db.summary().0, 1);
        assert_eq!(db.load_tables("del-me").len(), 2);

        db.delete_connection("del-me").unwrap();
        assert_eq!(db.summary().0, 0);
        assert_eq!(db.load_tables("del-me").len(), 0);
    }

    #[test]
    fn test_cache_and_load_tables() {
        let db = temp_db();
        let tables = vec!["users".into(), "orders".into(), "products".into()];
        db.cache_tables("conn-1", &tables).unwrap();

        let loaded = db.load_tables("conn-1");
        assert_eq!(loaded.len(), 3);
        assert!(loaded.contains(&"orders".to_string()));
        assert!(loaded.contains(&"users".to_string()));
    }

    #[test]
    fn test_cache_tables_replaces_old() {
        let db = temp_db();
        db.cache_tables("conn-1", &["old1".into(), "old2".into()]).unwrap();
        assert_eq!(db.load_tables("conn-1").len(), 2);

        db.cache_tables("conn-1", &["new1".into()]).unwrap();
        let loaded = db.load_tables("conn-1");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], "new1");
    }

    #[test]
    fn test_cache_and_load_columns() {
        let db = temp_db();
        let cols = vec![
            ("id".into(), "INTEGER".into(), false),
            ("name".into(), "TEXT".into(), true),
            ("email".into(), "TEXT".into(), true),
        ];
        db.cache_columns("conn-1", "users", &cols).unwrap();

        let loaded = db.load_columns("conn-1", "users");
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].0, "id");
        assert_eq!(loaded[0].1, "INTEGER");
        assert!(!loaded[0].2); // not nullable
        assert!(loaded[2].2); // nullable
    }

    #[test]
    fn test_s3_config_crud() {
        let db = temp_db();
        db.upsert_s3_config("prod", "https://s3.amazonaws.com", "my-bucket", "us-east-1").unwrap();

        let (_, _, s3s, _) = db.summary();
        assert_eq!(s3s, 1);
    }

    #[test]
    fn test_s3_table_cache() {
        let db = temp_db();
        db.upsert_s3_config("test", "", "test-bucket", "us-east-1").unwrap();

        let tables = vec![
            ("events".into(), "iceberg".into(), "warehouse/events".into()),
            ("users".into(), "delta".into(), "warehouse/users".into()),
        ];
        db.cache_s3_tables("test", &tables).unwrap();

        let loaded = db.load_s3_tables("test");
        assert_eq!(loaded.len(), 2);
        // Sorted alphabetically by table_name: events < users
        assert_eq!(loaded[0].0, "events");
        assert_eq!(loaded[0].1, "iceberg");
        assert_eq!(loaded[1].0, "users");
        assert_eq!(loaded[1].1, "delta");
    }

    #[test]
    fn test_pipeline_crud() {
        let db = temp_db();
        let pipeline = crate::state::StreamingPipeline {
            id: "pipe-1".into(),
            name: "kafka-events".into(),
            source_type: "kafka".into(),
            source_config: serde_json::json!({"broker": "localhost:9092", "topic": "events"}),
            transform_sql: Some("SELECT * FROM source WHERE event_type != 'heartbeat'".into()),
            sink_table: "iceberg://warehouse.events".into(),
            status: "created".into(),
            events_processed: 0,
            created_at: chrono::Utc::now(),
            snapshot_docs: None,
            snapshot_completed_at: None,
            files_written: 0,
            phase: String::new(),
        };
        db.upsert_pipeline(&pipeline).unwrap();

        let loaded = db.load_pipelines();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "kafka-events");
        assert_eq!(loaded[0].source_type, "kafka");
        assert_eq!(loaded[0].sink_table, "iceberg://warehouse.events");
        assert!(loaded[0].transform_sql.is_some());

        db.delete_pipeline("pipe-1").unwrap();
        assert_eq!(db.load_pipelines().len(), 0);
    }

    #[test]
    fn test_multiple_connections() {
        let db = temp_db();
        for i in 0..5 {
            let entry = crate::state::ConnectionEntry {
                id: format!("conn-{}", i),
                name: format!("Connection {}", i),
                conn_type: "postgres".into(),
                host: "localhost".into(),
                port: 5432 + i as u16,
                database: format!("db{}", i),
                username: "user".into(),
                status: "connected".into(),
                tables: vec![],
                created_at: chrono::Utc::now(),
                source: "user".into(),
                sync_status: "ready".into(),
                sync_error: None,
                sync_progress: None,
                auth_method: "scram".into(),
                connection_string: None,
                aws_access_key: None,
                aws_secret_key: None,
                aws_session_token: None,
            };
            db.upsert_connection(&entry).unwrap();
        }
        assert_eq!(db.summary().0, 5);
        assert_eq!(db.load_connections().len(), 5);
    }
}
