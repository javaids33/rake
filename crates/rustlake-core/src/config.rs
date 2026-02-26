//! Platform-wide configuration, loaded from TOML files and environment variables.

use serde::{Deserialize, Serialize};

/// Top-level RustLake configuration.
///
/// Composed of per-subsystem sections. All fields have sensible defaults,
/// so `RustLakeConfig::default()` produces a working local-filesystem config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RustLakeConfig {
    /// Storage backend configuration (local, S3, GCS, Azure).
    #[serde(default)]
    pub storage: StorageConfig,
    /// Query engine tuning parameters.
    #[serde(default)]
    pub engine: EngineConfig,
    /// HTTP API server bind address.
    #[serde(default)]
    pub api: ApiConfig,
    /// Streaming ingestion settings.
    #[serde(default)]
    pub stream: StreamConfig,
    /// Vector/AI layer settings.
    #[serde(default)]
    pub vector: VectorConfig,
    /// Arrow Flight RPC server settings.
    #[serde(default)]
    pub flight: FlightConfig,
}

impl RustLakeConfig {
    /// Load configuration from a TOML file at the given path.
    pub fn from_file(path: &str) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
}

/// Configuration for the storage layer (object store backend).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The storage backend to use. Defaults to local filesystem at `./data`.
    #[serde(default = "default_storage_backend")]
    pub backend: StorageBackend,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
        }
    }
}

fn default_storage_backend() -> StorageBackend {
    StorageBackend::Local {
        path: "./data".to_string(),
    }
}

/// Object storage backend variants.
///
/// Tagged by `"type"` in TOML/JSON so the deserializer can distinguish them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageBackend {
    /// Local filesystem storage.
    Local {
        /// Root directory for data files.
        path: String,
    },
    /// Amazon S3 (or S3-compatible) storage.
    S3 {
        /// S3 bucket name.
        bucket: String,
        /// AWS region (e.g., `us-east-1`).
        region: String,
        /// Optional custom endpoint URL (for MinIO, LocalStack, etc.).
        #[serde(default)]
        endpoint: Option<String>,
    },
    /// Google Cloud Storage.
    Gcs {
        /// GCS bucket name.
        bucket: String,
    },
    /// Azure Blob Storage.
    Azure {
        /// Azure storage container name.
        container: String,
        /// Azure storage account name.
        account: String,
    },
}

/// DataFusion query engine tuning parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Number of rows per Arrow RecordBatch. Defaults to 8192.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Target number of output partitions (parallelism). Defaults to CPU count.
    #[serde(default = "default_target_partitions")]
    pub target_partitions: usize,
    /// Optional memory limit in bytes for the query engine.
    #[serde(default)]
    pub memory_limit: Option<usize>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            target_partitions: default_target_partitions(),
            memory_limit: None,
        }
    }
}

fn default_batch_size() -> usize {
    8192
}
fn default_target_partitions() -> usize {
    num_cpus()
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// HTTP API server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Bind address for the HTTP server. Defaults to `127.0.0.1`.
    #[serde(default = "default_host")]
    pub host: String,
    /// Port for the HTTP server. Defaults to 3000.
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    3000
}

/// Streaming ingestion configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Whether the streaming engine is enabled. Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,
    /// Number of records per micro-batch. Defaults to 1000.
    #[serde(default = "default_stream_batch_size")]
    pub batch_size: usize,
    /// Interval in seconds between checkpoint commits. Defaults to 30.
    #[serde(default = "default_stream_checkpoint_interval")]
    pub checkpoint_interval_secs: u64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: default_stream_batch_size(),
            checkpoint_interval_secs: default_stream_checkpoint_interval(),
        }
    }
}

fn default_stream_batch_size() -> usize {
    1000
}
fn default_stream_checkpoint_interval() -> u64 {
    30
}

/// Vector/AI layer configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfig {
    /// Whether the vector engine is enabled. Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,
    /// Default embedding dimensionality. Defaults to 384.
    #[serde(default = "default_embedding_dimensions")]
    pub default_dimensions: usize,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_dimensions: default_embedding_dimensions(),
        }
    }
}

fn default_embedding_dimensions() -> usize {
    384
}

/// Arrow Flight RPC server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightConfig {
    /// Whether the Flight server is enabled. Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,
    /// Bind address for the Flight gRPC server. Defaults to `127.0.0.1`.
    #[serde(default = "default_flight_host")]
    pub host: String,
    /// Port for the Flight gRPC server. Defaults to 50051.
    #[serde(default = "default_flight_port")]
    pub port: u16,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: default_flight_host(),
            port: default_flight_port(),
        }
    }
}

fn default_flight_host() -> String {
    "127.0.0.1".to_string()
}
fn default_flight_port() -> u16 {
    50051
}
