use std::time::Duration;

/// HTTP client wrapper for all RustLake API calls.
#[derive(Clone)]
pub struct RustLakeClient {
    http: reqwest::Client,
    base: String,
}

impl RustLakeClient {
    pub fn new(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");
        Self {
            http,
            base: base_url.trim_end_matches('/').to_string(),
        }
    }

    async fn get(&self, path: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}{}", self.base, path);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("HTTP GET {} failed: {}", url, e))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read response body: {}", e))?;
        if !status.is_success() {
            return Err(format!("HTTP {} from {}: {}", status, path, body));
        }
        serde_json::from_str(&body)
            .map_err(|e| format!("Invalid JSON from {}: {} (body: {})", path, e, &body[..body.len().min(200)]))
    }

    async fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        let url = format!("{}{}", self.base, path);
        let resp = self
            .http
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("HTTP POST {} failed: {}", url, e))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read response body: {}", e))?;
        if !status.is_success() {
            return Err(format!("HTTP {} from {}: {}", status, path, text));
        }
        serde_json::from_str(&text)
            .map_err(|e| format!("Invalid JSON from {}: {} (body: {})", path, e, &text[..text.len().min(200)]))
    }

    // ── Query & Debug ──────────────────────────────────────────────

    pub async fn sql_query(&self, sql: &str, engine: &str) -> Result<serde_json::Value, String> {
        self.post(
            "/api/v1/sql",
            &serde_json::json!({ "sql": sql, "engine": engine }),
        )
        .await
    }

    pub async fn sql_explain(&self, sql: &str) -> Result<serde_json::Value, String> {
        self.post(
            "/api/v1/sql/explain",
            &serde_json::json!({ "sql": sql }),
        )
        .await
    }

    pub async fn query_history(&self, limit: usize) -> Result<serde_json::Value, String> {
        self.get(&format!("/api/v1/query/history?limit={}", limit))
            .await
    }

    // ── Schema & Discovery ─────────────────────────────────────────

    pub async fn list_tables(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/tables").await
    }

    pub async fn table_schema(&self, name: &str) -> Result<serde_json::Value, String> {
        self.get(&format!("/api/v1/tables/{}/schema", name)).await
    }

    pub async fn table_preview(&self, name: &str) -> Result<serde_json::Value, String> {
        self.get(&format!("/api/v1/tables/{}/preview", name)).await
    }

    pub async fn table_stats(&self, name: &str) -> Result<serde_json::Value, String> {
        self.get(&format!("/api/v1/tables/{}/stats", name)).await
    }

    // ── Connections ────────────────────────────────────────────────

    pub async fn list_connections(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/connections").await
    }

    pub async fn connection_status(&self, id: &str) -> Result<serde_json::Value, String> {
        self.get(&format!("/api/v1/connections/{}", id)).await
    }

    // ── Streaming & CDC ────────────────────────────────────────────

    pub async fn list_pipelines(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/streaming/pipelines").await
    }

    // ── Glaciers (Executable Tables) ───────────────────────────────

    pub async fn list_executable_tables(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/executable-tables").await
    }

    pub async fn executable_table_properties(&self, name: &str) -> Result<serde_json::Value, String> {
        self.get(&format!("/api/v1/executable-tables/{}/properties", name))
            .await
    }

    pub async fn column_lineage(&self, name: &str) -> Result<serde_json::Value, String> {
        self.get(&format!(
            "/api/v1/executable-tables/{}/column-lineage",
            name
        ))
        .await
    }

    // ── System ─────────────────────────────────────────────────────

    pub async fn system_info(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/system/info").await
    }

    pub async fn system_resources(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/system/resources").await
    }

    pub async fn list_engines(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/engines").await
    }

    // ── Scheduling ─────────────────────────────────────────────────

    pub async fn list_schedules(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/schedules").await
    }

    pub async fn schedule_runs(&self) -> Result<serde_json::Value, String> {
        self.get("/api/v1/schedules/runs").await
    }

    // ── S3 Storage ─────────────────────────────────────────────────

    pub async fn s3_browse(&self, id: &str, prefix: &str) -> Result<serde_json::Value, String> {
        self.get(&format!(
            "/api/v1/storage/s3/{}/browse?prefix={}",
            id, prefix
        ))
        .await
    }

    // ── Actions: Pipeline CRUD ─────────────────────────────────────

    pub async fn create_pipeline(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        self.post("/api/v1/streaming/pipelines", body).await
    }

    pub async fn start_pipeline(&self, id: &str) -> Result<serde_json::Value, String> {
        self.post(
            &format!("/api/v1/streaming/pipelines/{}/start", id),
            &serde_json::json!({}),
        )
        .await
    }

    pub async fn stop_pipeline(&self, id: &str) -> Result<serde_json::Value, String> {
        self.post(
            &format!("/api/v1/streaming/pipelines/{}/stop", id),
            &serde_json::json!({}),
        )
        .await
    }

    pub async fn delete_pipeline(&self, id: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/api/v1/streaming/pipelines/{}", self.base, id);
        let resp = self
            .http
            .delete(&url)
            .send()
            .await
            .map_err(|e| format!("HTTP DELETE {} failed: {}", url, e))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read response body: {}", e))?;
        if !status.is_success() {
            return Err(format!("HTTP {} from DELETE pipeline {}: {}", status, id, text));
        }
        serde_json::from_str(&text)
            .map_err(|e| format!("Invalid JSON: {} (body: {})", e, &text[..text.len().min(200)]))
    }

    // ── Actions: Glacier CRUD ──────────────────────────────────────

    pub async fn create_executable_table(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        self.post("/api/v1/executable-tables", body).await
    }

    pub async fn create_glacier_from_pipeline(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        self.post("/api/v1/executable-tables/from-pipeline", body)
            .await
    }

    pub async fn execute_executable_table(&self, name: &str) -> Result<serde_json::Value, String> {
        self.post(
            &format!("/api/v1/executable-tables/{}/execute", name),
            &serde_json::json!({}),
        )
        .await
    }

    pub async fn cascade_replay(&self, name: &str) -> Result<serde_json::Value, String> {
        self.post(
            &format!("/api/v1/executable-tables/{}/cascade-replay", name),
            &serde_json::json!({}),
        )
        .await
    }

    // ── Actions: Connections ───────────────────────────────────────

    pub async fn create_connection(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        self.post("/api/v1/connections", body).await
    }

    // ── Health ─────────────────────────────────────────────────────

    pub async fn health(&self) -> Result<serde_json::Value, String> {
        self.get("/health").await
    }
}
