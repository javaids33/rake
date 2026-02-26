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
    /// Distributed cluster topology and scaling settings.
    #[serde(default)]
    pub cluster: ClusterConfig,
    /// Kubernetes-specific discovery settings (only used when `cluster.discovery` is `Kubernetes`).
    #[serde(default)]
    pub k8s: K8sDiscoveryConfig,
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
    /// Maximum gRPC message size in bytes. Defaults to 64 MB.
    #[serde(default = "default_flight_max_message_size")]
    pub max_message_size: usize,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: default_flight_host(),
            port: default_flight_port(),
            max_message_size: default_flight_max_message_size(),
        }
    }
}

fn default_flight_host() -> String {
    "127.0.0.1".to_string()
}
fn default_flight_port() -> u16 {
    50051
}
fn default_flight_max_message_size() -> usize {
    64 * 1024 * 1024 // 64 MB
}

/// Role of this node in the distributed cluster topology.
///
/// - `Standalone`: single-process mode (default) — runs both query planning and execution
/// - `Coordinator`: accepts client queries, plans execution, distributes partitions to workers
/// - `Worker`: receives partition scan tasks from coordinator, executes and returns results
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Standalone,
    Coordinator,
    Worker,
}

impl Default for NodeRole {
    fn default() -> Self {
        Self::Standalone
    }
}

/// Information about a worker node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Worker's Flight RPC address (e.g., `"10.0.1.5:50051"`).
    pub address: String,
    /// Optional human-readable label.
    #[serde(default)]
    pub label: Option<String>,
}

/// Distributed cluster configuration for multi-node execution.
///
/// Controls how RustLake nodes discover each other and distribute queries.
/// In standalone mode (default), all fields are ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Role of this node. Defaults to `Standalone`.
    #[serde(default)]
    pub node_role: NodeRole,
    /// Address of the coordinator node. Workers use this to register.
    /// Only required when `node_role` is `Worker`.
    #[serde(default)]
    pub coordinator_addr: Option<String>,
    /// Static list of worker addresses (for non-K8s deployments).
    /// Only used when `node_role` is `Coordinator` and `discovery` is `Static`.
    #[serde(default)]
    pub workers: Vec<WorkerConfig>,
    /// Worker discovery mechanism.
    #[serde(default)]
    pub discovery: DiscoveryMethod,
    /// Interval in seconds between worker heartbeats. Defaults to 10.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
    /// Seconds without a heartbeat before a worker is marked unhealthy. Defaults to 30.
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
    /// Maximum number of concurrent partition scans per worker. Defaults to 4.
    #[serde(default = "default_max_partitions_per_worker")]
    pub max_partitions_per_worker: usize,
    /// Buffer size in bytes for shuffle exchange between nodes. Defaults to 128 MB.
    #[serde(default = "default_shuffle_buffer_size")]
    pub shuffle_buffer_size: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_role: NodeRole::default(),
            coordinator_addr: None,
            workers: Vec::new(),
            discovery: DiscoveryMethod::default(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
            max_partitions_per_worker: default_max_partitions_per_worker(),
            shuffle_buffer_size: default_shuffle_buffer_size(),
        }
    }
}

fn default_heartbeat_interval() -> u64 {
    10
}
fn default_heartbeat_timeout() -> u64 {
    30
}
fn default_max_partitions_per_worker() -> usize {
    4
}
fn default_shuffle_buffer_size() -> usize {
    128 * 1024 * 1024 // 128 MB
}

/// How the coordinator discovers worker nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryMethod {
    /// Workers are listed explicitly in `cluster.workers`.
    Static,
    /// Workers register themselves via gRPC (self-registration).
    Register,
    /// Discover workers via Kubernetes headless service DNS SRV records.
    Kubernetes,
}

impl Default for DiscoveryMethod {
    fn default() -> Self {
        Self::Register
    }
}

/// Kubernetes-specific discovery configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sDiscoveryConfig {
    /// Kubernetes namespace. Defaults to `"default"`.
    #[serde(default = "default_k8s_namespace")]
    pub namespace: String,
    /// Headless service name for worker discovery.
    #[serde(default = "default_k8s_service")]
    pub service_name: String,
    /// Port name in the K8s service spec. Defaults to `"flight"`.
    #[serde(default = "default_k8s_port_name")]
    pub port_name: String,
    /// Polling interval in seconds for DNS re-resolution. Defaults to 15.
    #[serde(default = "default_k8s_poll_interval")]
    pub poll_interval_secs: u64,
}

impl Default for K8sDiscoveryConfig {
    fn default() -> Self {
        Self {
            namespace: default_k8s_namespace(),
            service_name: default_k8s_service(),
            port_name: default_k8s_port_name(),
            poll_interval_secs: default_k8s_poll_interval(),
        }
    }
}

fn default_k8s_namespace() -> String {
    "default".to_string()
}
fn default_k8s_service() -> String {
    "rustlake-workers".to_string()
}
fn default_k8s_port_name() -> String {
    "flight".to_string()
}
fn default_k8s_poll_interval() -> u64 {
    15
}
