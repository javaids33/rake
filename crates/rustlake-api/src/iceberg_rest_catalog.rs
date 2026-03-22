//! Apache Iceberg REST Catalog implementation.
//!
//! Implements the [Iceberg REST Catalog API spec](https://iceberg.apache.org/concepts/catalog/#rest-catalog)
//! so that Trino, Spark, Flink, and PyIceberg can use RustLake as their catalog server.
//!
//! All state is held in a process-global `LazyLock<RwLock<CatalogState>>` so that
//! routes can be mounted without modifying `AppState`.
//!
//! # Endpoints (all under `/api/v1/iceberg/v1`)
//!
//! | Method   | Path                                  | Description              |
//! |----------|---------------------------------------|--------------------------|
//! | GET      | `/v1/config`                          | Catalog configuration    |
//! | GET      | `/v1/namespaces`                      | List namespaces          |
//! | POST     | `/v1/namespaces`                      | Create namespace         |
//! | GET      | `/v1/namespaces/{ns}`                 | Get namespace metadata   |
//! | DELETE   | `/v1/namespaces/{ns}`                 | Drop namespace           |
//! | GET      | `/v1/namespaces/{ns}/tables`          | List tables              |
//! | POST     | `/v1/namespaces/{ns}/tables`          | Create table             |
//! | GET      | `/v1/namespaces/{ns}/tables/{table}`  | Load table metadata      |
//! | DELETE   | `/v1/namespaces/{ns}/tables/{table}`  | Drop table               |

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::state::AppState;

// ---------------------------------------------------------------------------
// Global catalog state (avoids modifying AppState)
// ---------------------------------------------------------------------------

static CATALOG: LazyLock<RwLock<CatalogState>> =
    LazyLock::new(|| RwLock::new(CatalogState::new()));

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// A multi-level namespace identifier (e.g. `["production", "analytics"]`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Namespace {
    /// Ordered list of namespace levels.
    pub levels: Vec<String>,
}

impl Namespace {
    /// Create a single-level namespace.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            levels: vec![name.into()],
        }
    }

    /// Encode the namespace as a dot-separated string for map keys.
    pub fn to_key(&self) -> String {
        self.levels.join(".")
    }

    /// Parse a dot-separated string back into a `Namespace`.
    pub fn from_key(key: &str) -> Self {
        Self {
            levels: key.split('.').map(|s| s.to_string()).collect(),
        }
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.levels.join("."))
    }
}

/// Metadata associated with a namespace (arbitrary key-value properties).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceMetadata {
    /// The namespace identifier.
    pub namespace: Vec<String>,
    /// User-defined properties.
    pub properties: HashMap<String, String>,
}

/// Full identifier for a table within a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIdentifier {
    /// The namespace the table belongs to.
    pub namespace: Vec<String>,
    /// The table name.
    pub name: String,
}

impl TableIdentifier {
    /// Create a new table identifier.
    pub fn new(namespace: Vec<String>, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }

    /// Flat string key for HashMap lookups: `ns1.ns2::table_name`.
    pub fn to_key(&self) -> String {
        format!("{}::{}", self.namespace.join("."), self.name)
    }
}

impl std::fmt::Display for TableIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace.join("."), self.name)
    }
}

/// Stored table entry: metadata location + full metadata JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableEntry {
    /// Where the metadata file lives (e.g. `s3://bucket/warehouse/ns/table/metadata/v1.metadata.json`).
    pub metadata_location: String,
    /// Full Iceberg table metadata as a JSON value.
    pub metadata: serde_json::Value,
    /// When this entry was last updated.
    pub last_updated_ms: i64,
}

/// Catalog-level configuration returned by `GET /v1/config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Overrides that the client must apply.
    pub overrides: HashMap<String, String>,
    /// Defaults that the client may use if it has no local value.
    pub defaults: HashMap<String, String>,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        let mut defaults = HashMap::new();
        defaults.insert("warehouse".to_string(), "rustlake".to_string());
        defaults.insert(
            "uri".to_string(),
            "http://localhost:3000/api/v1/iceberg/v1".to_string(),
        );
        Self {
            overrides: HashMap::new(),
            defaults,
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory catalog state
// ---------------------------------------------------------------------------

/// All catalog state held in memory.
#[derive(Debug)]
pub struct CatalogState {
    /// Namespace key → metadata.
    namespaces: HashMap<String, NamespaceMetadata>,
    /// Table key (`ns::table`) → table entry.
    tables: HashMap<String, TableEntry>,
    /// Catalog-level configuration.
    config: CatalogConfig,
    /// Monotonically increasing snapshot ID generator.
    next_snapshot_id: i64,
}

impl CatalogState {
    /// Create a new empty catalog with a default namespace.
    pub fn new() -> Self {
        let mut namespaces = HashMap::new();
        // Seed a "default" namespace so there's always somewhere to put tables.
        let default_ns = NamespaceMetadata {
            namespace: vec!["default".to_string()],
            properties: {
                let mut p = HashMap::new();
                p.insert("owner".to_string(), "rustlake".to_string());
                p
            },
        };
        namespaces.insert("default".to_string(), default_ns);

        Self {
            namespaces,
            tables: HashMap::new(),
            config: CatalogConfig::default(),
            next_snapshot_id: 1,
        }
    }

    /// Allocate a new snapshot ID.
    fn alloc_snapshot_id(&mut self) -> i64 {
        let id = self.next_snapshot_id;
        self.next_snapshot_id += 1;
        id
    }
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// `POST /v1/namespaces` request body.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Multi-level namespace identifier.
    pub namespace: Vec<String>,
    /// Optional properties to set on the namespace.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Response for namespace operations that return the namespace.
#[derive(Debug, Serialize)]
pub struct CreateNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

/// `GET /v1/namespaces` response.
#[derive(Debug, Serialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
}

/// `GET /v1/namespaces/{ns}` response.
#[derive(Debug, Serialize)]
pub struct GetNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

/// Query parameters for `GET /v1/namespaces`.
#[derive(Debug, Deserialize)]
pub struct ListNamespacesQuery {
    /// Optional parent namespace filter.
    pub parent: Option<String>,
}

/// `GET /v1/namespaces/{ns}/tables` response.
#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
}

/// Schema field definition for table creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// Unique field ID.
    pub id: i32,
    /// Field name.
    pub name: String,
    /// Iceberg type string (e.g., "long", "string", "double", "boolean", "timestamp").
    #[serde(rename = "type")]
    pub field_type: serde_json::Value,
    /// Whether the field is required.
    #[serde(default = "default_true")]
    pub required: bool,
    /// Optional documentation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

fn default_true() -> bool {
    true
}

/// Iceberg schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSchema {
    /// Schema type — always "struct".
    #[serde(rename = "type", default = "default_struct_type")]
    pub schema_type: String,
    /// Schema ID.
    #[serde(default)]
    pub schema_id: i32,
    /// Column definitions.
    #[serde(default)]
    pub fields: Vec<SchemaField>,
    /// Identifier field IDs (for key columns).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identifier_field_ids: Vec<i32>,
}

fn default_struct_type() -> String {
    "struct".to_string()
}

/// Partition spec for table creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// Spec ID.
    #[serde(default)]
    pub spec_id: i32,
    /// Partition fields.
    #[serde(default)]
    pub fields: Vec<PartitionField>,
}

/// A single partition field in a partition spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    /// Source column ID.
    pub source_id: i32,
    /// Partition field ID.
    pub field_id: i32,
    /// Partition field name.
    pub name: String,
    /// Transform function: "identity", "bucket[N]", "truncate[N]", "year", "month", "day", "hour".
    pub transform: String,
}

/// Sort order for table creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    /// Order ID.
    #[serde(default)]
    pub order_id: i32,
    /// Sort fields.
    #[serde(default)]
    pub fields: Vec<SortField>,
}

/// A single field in a sort order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    /// Source column ID.
    pub source_id: i32,
    /// Transform applied before sorting.
    pub transform: String,
    /// Sort direction: "asc" or "desc".
    pub direction: String,
    /// Null ordering: "nulls-first" or "nulls-last".
    pub null_order: String,
}

/// `POST /v1/namespaces/{ns}/tables` request body.
#[derive(Debug, Deserialize)]
pub struct CreateTableRequest {
    /// Table name (within the namespace from the path).
    pub name: String,
    /// Table schema.
    pub schema: IcebergSchema,
    /// Optional partition spec.
    #[serde(default)]
    pub partition_spec: Option<PartitionSpec>,
    /// Optional sort order.
    #[serde(default)]
    pub write_order: Option<SortOrder>,
    /// Stage-create flag — if true, table is created in a staging state.
    #[serde(default)]
    pub stage_create: bool,
    /// Optional table properties.
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Optional explicit metadata location.
    #[serde(default)]
    pub location: Option<String>,
}

/// `GET /v1/namespaces/{ns}/tables/{table}` response.
#[derive(Debug, Serialize)]
pub struct LoadTableResponse {
    /// Location of the metadata file.
    pub metadata_location: String,
    /// Full table metadata JSON.
    pub metadata: serde_json::Value,
}

/// Standard Iceberg REST error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorDetail,
}

/// Detail inside an error response.
#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    /// Human-readable error message.
    pub message: String,
    /// Error type classification.
    #[serde(rename = "type")]
    pub error_type: String,
    /// HTTP status code.
    pub code: u16,
}

impl ErrorResponse {
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                message: message.into(),
                error_type: "NoSuchNamespaceException".to_string(),
                code: 404,
            },
        }
    }

    fn table_not_found(message: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                message: message.into(),
                error_type: "NoSuchTableException".to_string(),
                code: 404,
            },
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                message: message.into(),
                error_type: "AlreadyExistsException".to_string(),
                code: 409,
            },
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                message: message.into(),
                error_type: "BadRequestException".to_string(),
                code: 400,
            },
        }
    }

    fn not_empty(message: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                message: message.into(),
                error_type: "NamespaceNotEmptyException".to_string(),
                code: 409,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// `GET /v1/config` — return catalog configuration.
///
/// Clients call this first to discover overrides and defaults.
async fn get_config(
    State(_state): State<Arc<AppState>>,
) -> Json<CatalogConfig> {
    tracing::debug!("Iceberg REST: GET /v1/config");
    let catalog = CATALOG.read().await;
    Json(catalog.config.clone())
}

/// `GET /v1/namespaces` — list all namespaces, optionally filtered by parent.
async fn list_namespaces(
    State(_state): State<Arc<AppState>>,
    Query(params): Query<ListNamespacesQuery>,
) -> Json<ListNamespacesResponse> {
    tracing::debug!(parent = ?params.parent, "Iceberg REST: GET /v1/namespaces");
    let catalog = CATALOG.read().await;

    let namespaces: Vec<Vec<String>> = if let Some(ref parent) = params.parent {
        // Filter to namespaces whose prefix matches the parent.
        let parent_ns = Namespace::from_key(parent);
        catalog
            .namespaces
            .values()
            .filter(|meta| {
                meta.namespace.len() > parent_ns.levels.len()
                    && meta.namespace[..parent_ns.levels.len()] == parent_ns.levels[..]
            })
            .map(|meta| meta.namespace.clone())
            .collect()
    } else {
        catalog
            .namespaces
            .values()
            .map(|meta| meta.namespace.clone())
            .collect()
    };

    tracing::info!(count = namespaces.len(), "Listed namespaces");
    Json(ListNamespacesResponse { namespaces })
}

/// `POST /v1/namespaces` — create a new namespace.
async fn create_namespace(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<CreateNamespaceResponse>), (StatusCode, Json<ErrorResponse>)> {
    if req.namespace.is_empty() {
        tracing::warn!("Iceberg REST: create_namespace called with empty namespace");
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("Namespace must not be empty")),
        ));
    }

    let key = req.namespace.join(".");
    tracing::info!(namespace = %key, "Iceberg REST: POST /v1/namespaces");

    let mut catalog = CATALOG.write().await;

    if catalog.namespaces.contains_key(&key) {
        tracing::warn!(namespace = %key, "Namespace already exists");
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse::conflict(format!(
                "Namespace already exists: {}",
                key
            ))),
        ));
    }

    let metadata = NamespaceMetadata {
        namespace: req.namespace.clone(),
        properties: req.properties.clone(),
    };
    catalog.namespaces.insert(key.clone(), metadata);

    tracing::info!(namespace = %key, "Namespace created");
    Ok((
        StatusCode::OK,
        Json(CreateNamespaceResponse {
            namespace: req.namespace,
            properties: req.properties,
        }),
    ))
}

/// `GET /v1/namespaces/{ns}` — get namespace metadata.
async fn get_namespace(
    State(_state): State<Arc<AppState>>,
    Path(ns): Path<String>,
) -> Result<Json<GetNamespaceResponse>, (StatusCode, Json<ErrorResponse>)> {
    // The namespace may be URL-encoded with %1F (unit separator) or dots.
    let key = decode_namespace_path(&ns);
    tracing::debug!(namespace = %key, "Iceberg REST: GET /v1/namespaces/{ns}");

    let catalog = CATALOG.read().await;
    match catalog.namespaces.get(&key) {
        Some(meta) => {
            tracing::info!(namespace = %key, "Namespace found");
            Ok(Json(GetNamespaceResponse {
                namespace: meta.namespace.clone(),
                properties: meta.properties.clone(),
            }))
        }
        None => {
            tracing::warn!(namespace = %key, "Namespace not found");
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "Namespace does not exist: {}",
                    key
                ))),
            ))
        }
    }
}

/// `DELETE /v1/namespaces/{ns}` — drop a namespace (must be empty).
async fn drop_namespace(
    State(_state): State<Arc<AppState>>,
    Path(ns): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let key = decode_namespace_path(&ns);
    tracing::info!(namespace = %key, "Iceberg REST: DELETE /v1/namespaces/{ns}");

    let mut catalog = CATALOG.write().await;

    // Check the namespace exists.
    if !catalog.namespaces.contains_key(&key) {
        tracing::warn!(namespace = %key, "Cannot drop — namespace not found");
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "Namespace does not exist: {}",
                key
            ))),
        ));
    }

    // Ensure no tables remain in this namespace.
    let ns_prefix = format!("{}::", key);
    let has_tables = catalog.tables.keys().any(|k| k.starts_with(&ns_prefix));
    if has_tables {
        tracing::warn!(namespace = %key, "Cannot drop — namespace is not empty");
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse::not_empty(format!(
                "Namespace is not empty: {}",
                key
            ))),
        ));
    }

    catalog.namespaces.remove(&key);
    tracing::info!(namespace = %key, "Namespace dropped");
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /v1/namespaces/{ns}/tables` — list tables in a namespace.
async fn list_tables(
    State(_state): State<Arc<AppState>>,
    Path(ns): Path<String>,
) -> Result<Json<ListTablesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let key = decode_namespace_path(&ns);
    tracing::debug!(namespace = %key, "Iceberg REST: GET /v1/namespaces/{ns}/tables");

    let catalog = CATALOG.read().await;

    // Verify namespace exists.
    if !catalog.namespaces.contains_key(&key) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "Namespace does not exist: {}",
                key
            ))),
        ));
    }

    let ns_prefix = format!("{}::", key);
    let ns_levels: Vec<String> = key.split('.').map(|s| s.to_string()).collect();

    let identifiers: Vec<TableIdentifier> = catalog
        .tables
        .keys()
        .filter(|k| k.starts_with(&ns_prefix))
        .map(|k| {
            let table_name = k.strip_prefix(&ns_prefix).unwrap_or(k);
            TableIdentifier::new(ns_levels.clone(), table_name)
        })
        .collect();

    tracing::info!(namespace = %key, count = identifiers.len(), "Listed tables");
    Ok(Json(ListTablesResponse { identifiers }))
}

/// `POST /v1/namespaces/{ns}/tables` — create a table.
async fn create_table(
    State(_state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    Json(req): Json<CreateTableRequest>,
) -> Result<(StatusCode, Json<LoadTableResponse>), (StatusCode, Json<ErrorResponse>)> {
    let ns_key = decode_namespace_path(&ns);
    tracing::info!(
        namespace = %ns_key,
        table = %req.name,
        fields = req.schema.fields.len(),
        "Iceberg REST: POST /v1/namespaces/{ns}/tables"
    );

    if req.name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("Table name must not be empty")),
        ));
    }

    let mut catalog = CATALOG.write().await;

    // Verify namespace exists.
    if !catalog.namespaces.contains_key(&ns_key) {
        tracing::warn!(namespace = %ns_key, "Cannot create table — namespace not found");
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "Namespace does not exist: {}",
                ns_key
            ))),
        ));
    }

    let ns_levels: Vec<String> = ns_key.split('.').map(|s| s.to_string()).collect();
    let table_id = TableIdentifier::new(ns_levels.clone(), &req.name);
    let table_key = table_id.to_key();

    if catalog.tables.contains_key(&table_key) {
        tracing::warn!(table = %table_key, "Table already exists");
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse::conflict(format!(
                "Table already exists: {}",
                table_id
            ))),
        ));
    }

    // Build metadata location.
    let location = req.location.clone().unwrap_or_else(|| {
        format!(
            "s3://rustlake-warehouse/{}/{}/data",
            ns_key.replace('.', "/"),
            req.name
        )
    });
    let metadata_location = format!(
        "s3://rustlake-warehouse/{}/{}/metadata/v1.metadata.json",
        ns_key.replace('.', "/"),
        req.name
    );

    // Build partition spec JSON.
    let partition_spec = req.partition_spec.as_ref().map(|ps| {
        serde_json::json!({
            "spec-id": ps.spec_id,
            "fields": ps.fields.iter().map(|f| {
                serde_json::json!({
                    "source-id": f.source_id,
                    "field-id": f.field_id,
                    "name": f.name,
                    "transform": f.transform,
                })
            }).collect::<Vec<_>>()
        })
    }).unwrap_or_else(|| {
        serde_json::json!({
            "spec-id": 0,
            "fields": []
        })
    });

    // Build sort order JSON.
    let sort_order = req.write_order.as_ref().map(|so| {
        serde_json::json!({
            "order-id": so.order_id,
            "fields": so.fields.iter().map(|f| {
                serde_json::json!({
                    "source-id": f.source_id,
                    "transform": f.transform,
                    "direction": f.direction,
                    "null-order": f.null_order,
                })
            }).collect::<Vec<_>>()
        })
    }).unwrap_or_else(|| {
        serde_json::json!({
            "order-id": 0,
            "fields": []
        })
    });

    // Build full Iceberg v2 table metadata.
    let snapshot_id = catalog.alloc_snapshot_id();
    let now_ms = Utc::now().timestamp_millis();

    let schema_json = serde_json::json!({
        "type": "struct",
        "schema-id": req.schema.schema_id,
        "fields": req.schema.fields.iter().map(|f| {
            let mut field = serde_json::json!({
                "id": f.id,
                "name": f.name,
                "type": f.field_type,
                "required": f.required,
            });
            if let Some(ref doc) = f.doc {
                field.as_object_mut().unwrap().insert("doc".to_string(), serde_json::json!(doc));
            }
            field
        }).collect::<Vec<_>>(),
        "identifier-field-ids": req.schema.identifier_field_ids,
    });

    let mut properties = req.properties.clone();
    if !properties.contains_key("format-version") {
        properties.insert("format-version".to_string(), "2".to_string());
    }

    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": now_ms,
        "last-column-id": req.schema.fields.iter().map(|f| f.id).max().unwrap_or(0),
        "current-schema-id": req.schema.schema_id,
        "schemas": [schema_json],
        "default-spec-id": partition_spec.get("spec-id").and_then(|v| v.as_i64()).unwrap_or(0),
        "partition-specs": [partition_spec],
        "last-partition-id": 999,
        "default-sort-order-id": sort_order.get("order-id").and_then(|v| v.as_i64()).unwrap_or(0),
        "sort-orders": [sort_order],
        "properties": properties,
        "current-snapshot-id": -1,
        "refs": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "statistics": [],
        "partition-statistics": [],
    });

    let entry = TableEntry {
        metadata_location: metadata_location.clone(),
        metadata: metadata.clone(),
        last_updated_ms: now_ms,
    };
    catalog.tables.insert(table_key.clone(), entry);

    tracing::info!(
        table = %table_id,
        snapshot_id = snapshot_id,
        metadata_location = %metadata_location,
        "Table created"
    );

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location,
            metadata,
        }),
    ))
}

/// `GET /v1/namespaces/{ns}/tables/{table}` — load table metadata.
async fn load_table(
    State(_state): State<Arc<AppState>>,
    Path((ns, table)): Path<(String, String)>,
) -> Result<Json<LoadTableResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ns_key = decode_namespace_path(&ns);
    let table_key = format!("{}::{}", ns_key, table);
    tracing::debug!(table = %table_key, "Iceberg REST: GET /v1/namespaces/{ns}/tables/{table}");

    let catalog = CATALOG.read().await;

    // Verify namespace exists.
    if !catalog.namespaces.contains_key(&ns_key) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "Namespace does not exist: {}",
                ns_key
            ))),
        ));
    }

    match catalog.tables.get(&table_key) {
        Some(entry) => {
            tracing::info!(table = %table_key, "Table loaded");
            Ok(Json(LoadTableResponse {
                metadata_location: entry.metadata_location.clone(),
                metadata: entry.metadata.clone(),
            }))
        }
        None => {
            tracing::warn!(table = %table_key, "Table not found");
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::table_not_found(format!(
                    "Table does not exist: {}.{}",
                    ns_key, table
                ))),
            ))
        }
    }
}

/// `DELETE /v1/namespaces/{ns}/tables/{table}` — drop a table.
async fn drop_table(
    State(_state): State<Arc<AppState>>,
    Path((ns, table)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let ns_key = decode_namespace_path(&ns);
    let table_key = format!("{}::{}", ns_key, table);
    tracing::info!(table = %table_key, "Iceberg REST: DELETE /v1/namespaces/{ns}/tables/{table}");

    let mut catalog = CATALOG.write().await;

    // Verify namespace exists.
    if !catalog.namespaces.contains_key(&ns_key) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "Namespace does not exist: {}",
                ns_key
            ))),
        ));
    }

    if catalog.tables.remove(&table_key).is_some() {
        tracing::info!(table = %table_key, "Table dropped");
        Ok(StatusCode::NO_CONTENT)
    } else {
        tracing::warn!(table = %table_key, "Cannot drop — table not found");
        Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::table_not_found(format!(
                "Table does not exist: {}.{}",
                ns_key, table
            ))),
        ))
    }
}

// ---------------------------------------------------------------------------
// Utility routes
// ---------------------------------------------------------------------------

/// `GET /v1/namespaces/{ns}/tables/{table}/metrics` — placeholder for report-metrics endpoint.
///
/// Spark and Flink clients may POST scan/commit metrics here. We accept and discard for now.
async fn report_metrics(
    State(_state): State<Arc<AppState>>,
    Path((ns, table)): Path<(String, String)>,
) -> StatusCode {
    tracing::debug!(
        namespace = %ns,
        table = %table,
        "Iceberg REST: metrics report (accepted, not stored)"
    );
    StatusCode::NO_CONTENT
}

/// `HEAD /v1/namespaces/{ns}/tables/{table}` — check if a table exists.
async fn table_exists(
    State(_state): State<Arc<AppState>>,
    Path((ns, table)): Path<(String, String)>,
) -> StatusCode {
    let ns_key = decode_namespace_path(&ns);
    let table_key = format!("{}::{}", ns_key, table);
    tracing::debug!(table = %table_key, "Iceberg REST: HEAD table exists check");

    let catalog = CATALOG.read().await;
    if catalog.tables.contains_key(&table_key) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// `HEAD /v1/namespaces/{ns}` — check if a namespace exists.
async fn namespace_exists(
    State(_state): State<Arc<AppState>>,
    Path(ns): Path<String>,
) -> StatusCode {
    let key = decode_namespace_path(&ns);
    tracing::debug!(namespace = %key, "Iceberg REST: HEAD namespace exists check");

    let catalog = CATALOG.read().await;
    if catalog.namespaces.contains_key(&key) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// ---------------------------------------------------------------------------
// Internal catalog API for use by other RustLake modules
// ---------------------------------------------------------------------------

/// Register an existing Iceberg table in the REST catalog.
///
/// Called by the CDC pipeline after writing Iceberg metadata to S3.
/// This makes the table discoverable by Trino/Spark/Flink via the REST catalog.
pub async fn register_table_in_catalog(
    namespace: &str,
    table_name: &str,
    metadata_location: &str,
    metadata: serde_json::Value,
) -> Result<(), String> {
    let ns_key = namespace.to_string();
    let table_key = format!("{}::{}", ns_key, table_name);
    let now_ms = Utc::now().timestamp_millis();

    tracing::info!(
        namespace = %ns_key,
        table = %table_name,
        metadata_location = %metadata_location,
        "Registering table in Iceberg REST catalog"
    );

    let mut catalog = CATALOG.write().await;

    // Auto-create namespace if it doesn't exist.
    if !catalog.namespaces.contains_key(&ns_key) {
        let ns_levels: Vec<String> = ns_key.split('.').map(|s| s.to_string()).collect();
        catalog.namespaces.insert(
            ns_key.clone(),
            NamespaceMetadata {
                namespace: ns_levels,
                properties: {
                    let mut p = HashMap::new();
                    p.insert("owner".to_string(), "rustlake-cdc".to_string());
                    p.insert("created-by".to_string(), "auto-register".to_string());
                    p
                },
            },
        );
        tracing::info!(namespace = %ns_key, "Auto-created namespace for table registration");
    }

    let entry = TableEntry {
        metadata_location: metadata_location.to_string(),
        metadata,
        last_updated_ms: now_ms,
    };
    catalog.tables.insert(table_key, entry);

    tracing::info!(
        namespace = %ns_key,
        table = %table_name,
        "Table registered in Iceberg REST catalog"
    );
    Ok(())
}

/// Update the metadata for an existing table in the catalog.
///
/// Used after Iceberg metadata compaction or new snapshot commits.
pub async fn update_table_metadata(
    namespace: &str,
    table_name: &str,
    metadata_location: &str,
    metadata: serde_json::Value,
) -> Result<(), String> {
    let table_key = format!("{}::{}", namespace, table_name);
    let now_ms = Utc::now().timestamp_millis();

    let mut catalog = CATALOG.write().await;
    match catalog.tables.get_mut(&table_key) {
        Some(entry) => {
            entry.metadata_location = metadata_location.to_string();
            entry.metadata = metadata;
            entry.last_updated_ms = now_ms;
            tracing::info!(table = %table_key, "Table metadata updated");
            Ok(())
        }
        None => {
            let msg = format!("Table not found in catalog: {}", table_key);
            tracing::warn!("{}", msg);
            Err(msg)
        }
    }
}

/// List all tables across all namespaces in the catalog.
///
/// Returns `(namespace_key, table_name, metadata_location)` tuples.
pub async fn list_all_catalog_tables() -> Vec<(String, String, String)> {
    let catalog = CATALOG.read().await;
    catalog
        .tables
        .iter()
        .map(|(key, entry)| {
            let parts: Vec<&str> = key.splitn(2, "::").collect();
            let ns = parts.first().copied().unwrap_or("default").to_string();
            let table = parts.get(1).copied().unwrap_or("unknown").to_string();
            (ns, table, entry.metadata_location.clone())
        })
        .collect()
}

/// Get catalog statistics for the system info endpoint.
pub async fn catalog_stats() -> (usize, usize) {
    let catalog = CATALOG.read().await;
    (catalog.namespaces.len(), catalog.tables.len())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Decode a namespace path segment.
///
/// The Iceberg REST spec encodes multi-level namespaces using `%1F` (unit separator)
/// between levels. Some clients use dots. We normalize both to dots for our key format.
fn decode_namespace_path(raw: &str) -> String {
    // Simple percent-decode for %1F (unit separator)
    let decoded = raw.replace("%1F", "\x1f").replace("%1f", "\x1f");
    // Replace unit separator (U+001F) with dots.
    decoded.replace('\x1f', ".")
}

// ---------------------------------------------------------------------------
// Router constructor
// ---------------------------------------------------------------------------

/// Build the Iceberg REST Catalog router.
///
/// Mount this under `/api/v1/iceberg/v1` in the main application router:
///
/// ```rust,ignore
/// let app = Router::new()
///     .nest("/api/v1/iceberg/v1", iceberg_catalog_routes())
///     .with_state(state);
/// ```
pub fn iceberg_catalog_routes() -> Router<Arc<AppState>> {
    Router::new()
        // Catalog config
        .route("/v1/config", get(get_config))
        // Namespaces
        .route("/v1/namespaces", get(list_namespaces).post(create_namespace))
        .route(
            "/v1/namespaces/{ns}",
            get(get_namespace)
                .delete(drop_namespace)
                .head(namespace_exists),
        )
        // Tables
        .route(
            "/v1/namespaces/{ns}/tables",
            get(list_tables).post(create_table),
        )
        .route(
            "/v1/namespaces/{ns}/tables/{table}",
            get(load_table)
                .delete(drop_table)
                .head(table_exists),
        )
        // Metrics (accept and discard)
        .route(
            "/v1/namespaces/{ns}/tables/{table}/metrics",
            post(report_metrics),
        )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_key_roundtrip() {
        let ns = Namespace::new("analytics");
        assert_eq!(ns.to_key(), "analytics");
        assert_eq!(Namespace::from_key("analytics").levels, vec!["analytics"]);

        let multi = Namespace {
            levels: vec!["prod".to_string(), "analytics".to_string()],
        };
        assert_eq!(multi.to_key(), "prod.analytics");
        assert_eq!(
            Namespace::from_key("prod.analytics").levels,
            vec!["prod", "analytics"]
        );
    }

    #[test]
    fn test_table_identifier_key() {
        let id = TableIdentifier::new(vec!["default".to_string()], "orders");
        assert_eq!(id.to_key(), "default::orders");
        assert_eq!(id.to_string(), "default.orders");
    }

    #[test]
    fn test_decode_namespace_path_dot() {
        assert_eq!(decode_namespace_path("prod.analytics"), "prod.analytics");
    }

    #[test]
    fn test_decode_namespace_path_unit_separator() {
        // %1F is the unit separator character
        assert_eq!(decode_namespace_path("prod%1Fanalytics"), "prod.analytics");
    }

    #[test]
    fn test_catalog_state_new_has_default_namespace() {
        let state = CatalogState::new();
        assert!(state.namespaces.contains_key("default"));
        assert!(state.tables.is_empty());
    }

    #[test]
    fn test_catalog_config_defaults() {
        let config = CatalogConfig::default();
        assert_eq!(config.defaults.get("warehouse").unwrap(), "rustlake");
        assert!(config.overrides.is_empty());
    }

    #[test]
    fn test_error_response_types() {
        let not_found = ErrorResponse::not_found("test");
        assert_eq!(not_found.error.code, 404);
        assert_eq!(not_found.error.error_type, "NoSuchNamespaceException");

        let conflict = ErrorResponse::conflict("test");
        assert_eq!(conflict.error.code, 409);

        let bad_req = ErrorResponse::bad_request("test");
        assert_eq!(bad_req.error.code, 400);

        let table_nf = ErrorResponse::table_not_found("test");
        assert_eq!(table_nf.error.error_type, "NoSuchTableException");

        let not_empty = ErrorResponse::not_empty("test");
        assert_eq!(not_empty.error.error_type, "NamespaceNotEmptyException");
    }

    #[test]
    fn test_snapshot_id_allocation() {
        let mut state = CatalogState::new();
        assert_eq!(state.alloc_snapshot_id(), 1);
        assert_eq!(state.alloc_snapshot_id(), 2);
        assert_eq!(state.alloc_snapshot_id(), 3);
    }

    #[tokio::test]
    async fn test_register_and_list_tables() {
        // Clear global state for test isolation.
        {
            let mut catalog = CATALOG.write().await;
            catalog.namespaces.clear();
            catalog.tables.clear();
            catalog.namespaces.insert(
                "default".to_string(),
                NamespaceMetadata {
                    namespace: vec!["default".to_string()],
                    properties: HashMap::new(),
                },
            );
        }

        let metadata = serde_json::json!({"format-version": 2});
        register_table_in_catalog(
            "default",
            "orders",
            "s3://bucket/orders/metadata/v1.metadata.json",
            metadata.clone(),
        )
        .await
        .expect("registration should succeed");

        let tables = list_all_catalog_tables().await;
        assert!(tables.iter().any(|(ns, t, _)| ns == "default" && t == "orders"),
            "Expected to find default.orders in catalog tables: {:?}", tables);

        let (ns_count, table_count) = catalog_stats().await;
        assert!(ns_count >= 1, "Expected at least 1 namespace, got {}", ns_count);
        assert!(table_count >= 1, "Expected at least 1 table, got {}", table_count);
    }

    #[tokio::test]
    async fn test_update_table_metadata() {
        {
            let mut catalog = CATALOG.write().await;
            catalog.namespaces.clear();
            catalog.tables.clear();
            catalog.namespaces.insert(
                "test_ns".to_string(),
                NamespaceMetadata {
                    namespace: vec!["test_ns".to_string()],
                    properties: HashMap::new(),
                },
            );
        }

        let meta_v1 = serde_json::json!({"format-version": 2, "version": 1});
        register_table_in_catalog(
            "test_ns",
            "events",
            "s3://bucket/events/metadata/v1.metadata.json",
            meta_v1,
        )
        .await
        .unwrap();

        let meta_v2 = serde_json::json!({"format-version": 2, "version": 2});
        update_table_metadata(
            "test_ns",
            "events",
            "s3://bucket/events/metadata/v2.metadata.json",
            meta_v2,
        )
        .await
        .unwrap();

        let catalog = CATALOG.read().await;
        let entry = catalog.tables.get("test_ns::events").unwrap();
        assert_eq!(
            entry.metadata_location,
            "s3://bucket/events/metadata/v2.metadata.json"
        );
        assert_eq!(entry.metadata["version"], 2);
    }

    #[tokio::test]
    async fn test_auto_create_namespace_on_register() {
        {
            let mut catalog = CATALOG.write().await;
            catalog.namespaces.clear();
            catalog.tables.clear();
        }

        let metadata = serde_json::json!({"format-version": 2});
        register_table_in_catalog(
            "auto_ns",
            "test_table",
            "s3://bucket/test/metadata.json",
            metadata,
        )
        .await
        .unwrap();

        let catalog = CATALOG.read().await;
        assert!(catalog.namespaces.contains_key("auto_ns"));
        assert!(catalog.tables.contains_key("auto_ns::test_table"));
    }
}
