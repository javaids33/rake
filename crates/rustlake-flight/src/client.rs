//! Arrow Flight client for connecting to remote RustLake Flight servers.
//!
//! Supports connection pooling for persistent gRPC channels to avoid
//! TCP handshake + TLS negotiation overhead on repeated queries.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use rustlake_core::{Result, RustLakeError};
use tokio::sync::Mutex;
use tonic::transport::Channel;

/// Client for connecting to a RustLake Flight server.
///
/// Creates a new gRPC connection per query. For persistent connections,
/// use `FlightClientPool`.
pub struct FlightClient {
    endpoint: String,
}

impl FlightClient {
    /// Create a new client targeting the given endpoint.
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
        }
    }

    /// Execute a SQL query on the remote server and return results.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let channel = tonic::transport::Channel::from_shared(self.endpoint.clone())
            .map_err(|e| RustLakeError::Engine(format!("Invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Connection failed: {}", e)))?;

        let mut client = FlightServiceClient::new(channel);

        let ticket = Ticket::new(sql.as_bytes().to_vec());
        let response = client
            .do_get(ticket)
            .await
            .map_err(|e| RustLakeError::Engine(format!("DoGet failed: {}", e)))?;

        let stream = response.into_inner();
        let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let batches: Vec<RecordBatch> = decoder
            .try_collect()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Flight decode error: {}", e)))?;

        Ok(batches)
    }
}

/// A cached gRPC channel with creation timestamp.
struct CachedChannel {
    channel: Channel,
    created_at: Instant,
}

/// Connection pool for Flight clients.
///
/// Maintains persistent gRPC channels to worker endpoints, reusing connections
/// across queries to avoid TCP handshake + TLS overhead. Channels are evicted
/// after `max_idle_duration` or when connection errors occur.
pub struct FlightClientPool {
    /// Cached channels keyed by endpoint URL.
    channels: Arc<Mutex<HashMap<String, CachedChannel>>>,
    /// Maximum time a channel can be idle before being evicted.
    max_idle_duration: Duration,
    /// Connection timeout for new channels.
    connect_timeout: Duration,
    /// Keep-alive interval for persistent connections.
    keep_alive_interval: Duration,
}

impl FlightClientPool {
    /// Create a new connection pool with default settings.
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            max_idle_duration: Duration::from_secs(300), // 5 minutes
            connect_timeout: Duration::from_secs(10),
            keep_alive_interval: Duration::from_secs(30),
        }
    }

    /// Create a pool with custom timeouts.
    pub fn with_timeouts(
        max_idle_secs: u64,
        connect_timeout_secs: u64,
        keep_alive_secs: u64,
    ) -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            max_idle_duration: Duration::from_secs(max_idle_secs),
            connect_timeout: Duration::from_secs(connect_timeout_secs),
            keep_alive_interval: Duration::from_secs(keep_alive_secs),
        }
    }

    /// Get or create a gRPC channel to the given endpoint.
    async fn get_channel(&self, endpoint: &str) -> Result<Channel> {
        let mut channels = self.channels.lock().await;

        // Check for cached channel.
        if let Some(cached) = channels.get(endpoint) {
            if cached.created_at.elapsed() < self.max_idle_duration {
                return Ok(cached.channel.clone());
            }
            // Channel expired — remove and create new.
            channels.remove(endpoint);
        }

        // Create new channel with connection settings.
        let channel = tonic::transport::Channel::from_shared(endpoint.to_string())
            .map_err(|e| RustLakeError::Engine(format!("Invalid endpoint: {}", e)))?
            .connect_timeout(self.connect_timeout)
            .keep_alive_timeout(self.keep_alive_interval)
            .connect()
            .await
            .map_err(|e| {
                RustLakeError::Engine(format!(
                    "Connection to {} failed: {}",
                    endpoint, e
                ))
            })?;

        channels.insert(
            endpoint.to_string(),
            CachedChannel {
                channel: channel.clone(),
                created_at: Instant::now(),
            },
        );

        Ok(channel)
    }

    /// Execute a SQL query on a remote worker using a pooled connection.
    pub async fn query(&self, endpoint: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        let channel = self.get_channel(endpoint).await?;
        let mut client = FlightServiceClient::new(channel);

        let ticket = Ticket::new(sql.as_bytes().to_vec());
        let response = client
            .do_get(ticket)
            .await
            .map_err(|e| {
                // On connection error, evict the channel so next attempt reconnects.
                let channels = self.channels.clone();
                let ep = endpoint.to_string();
                tokio::spawn(async move {
                    channels.lock().await.remove(&ep);
                });
                RustLakeError::Engine(format!("DoGet failed: {}", e))
            })?;

        let stream = response.into_inner();
        let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let batches: Vec<RecordBatch> = decoder
            .try_collect()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Flight decode error: {}", e)))?;

        Ok(batches)
    }

    /// Evict all cached channels (useful on coordinator shutdown).
    pub async fn clear(&self) {
        self.channels.lock().await.clear();
    }

    /// Evict expired channels.
    pub async fn evict_expired(&self) {
        let mut channels = self.channels.lock().await;
        channels.retain(|_, cached| cached.created_at.elapsed() < self.max_idle_duration);
    }

    /// Number of cached channels.
    pub async fn pool_size(&self) -> usize {
        self.channels.lock().await.len()
    }
}

impl Default for FlightClientPool {
    fn default() -> Self {
        Self::new()
    }
}
