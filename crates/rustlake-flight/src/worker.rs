//! Worker node for distributed query execution.
//!
//! A worker registers with the coordinator, sends periodic heartbeats,
//! and executes partition scans dispatched by the coordinator. Results
//! are returned to the coordinator via Arrow Flight `do_get`.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use futures::TryStreamExt;
use rustlake_core::config::ClusterConfig;
use rustlake_engine::RustLakeContext;
use serde::Serialize;
use tokio::sync::RwLock;

/// Worker node that participates in distributed query execution.
pub struct WorkerNode {
    /// Cluster configuration.
    config: ClusterConfig,
    /// Worker's own Flight RPC address (advertised to the coordinator).
    advertised_addr: String,
    /// Worker ID assigned by the coordinator after registration.
    worker_id: Option<String>,
    /// Local query context for executing partition scans.
    #[allow(dead_code)]
    ctx: Arc<RwLock<RustLakeContext>>,
}

/// Heartbeat payload sent to the coordinator.
#[derive(Debug, Serialize)]
#[allow(dead_code)]
struct HeartbeatPayload {
    worker_id: String,
    cpu_usage_pct: f32,
    memory_used_bytes: u64,
    active_partitions: u32,
}

impl WorkerNode {
    /// Create a new worker node.
    ///
    /// # Arguments
    /// * `config` — Cluster configuration (must have `coordinator_addr` set)
    /// * `advertised_addr` — This worker's Flight RPC address (e.g., `"0.0.0.0:50051"`)
    /// * `ctx` — Local query context for executing scans
    pub fn new(
        config: ClusterConfig,
        advertised_addr: String,
        ctx: Arc<RwLock<RustLakeContext>>,
    ) -> Self {
        Self {
            config,
            advertised_addr,
            worker_id: None,
            ctx,
        }
    }

    /// Register this worker with the coordinator.
    ///
    /// Sends a `register_worker` action to the coordinator's Flight RPC endpoint
    /// with this worker's address and resource info. The coordinator responds with
    /// an assigned worker ID.
    pub async fn register(&mut self) -> rustlake_core::Result<String> {
        let coordinator_addr = self
            .config
            .coordinator_addr
            .as_ref()
            .ok_or_else(|| {
                rustlake_core::RustLakeError::Config(
                    "Worker node requires cluster.coordinator_addr to be set".into(),
                )
            })?
            .clone();

        tracing::info!(
            coordinator = %coordinator_addr,
            advertised = %self.advertised_addr,
            "Registering with coordinator"
        );

        let channel = tonic::transport::Channel::from_shared(coordinator_addr.clone())
            .map_err(|e| rustlake_core::RustLakeError::Config(format!("Invalid coordinator address: {}", e)))?
            .connect()
            .await
            .map_err(|e| {
                rustlake_core::RustLakeError::Engine(format!(
                    "Failed to connect to coordinator at {}: {}",
                    coordinator_addr, e
                ))
            })?;

        let mut client = FlightServiceClient::new(channel);

        // Build the registration payload.
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let payload = serde_json::json!({
            "endpoint": format!("http://{}", self.advertised_addr),
            "label": format!("worker-{}", &self.advertised_addr),
            "cpu_cores": cpu_cores,
            "memory_bytes": 0u64,
        });

        let action = Action {
            r#type: "register_worker".to_string(),
            body: serde_json::to_vec(&payload)
                .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Serialization error: {}", e)))?
                .into(),
        };

        let response = client
            .do_action(action)
            .await
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Registration failed: {}", e)))?;

        // Read the first result to get our worker ID.
        let mut stream = response.into_inner();
        let result = stream
            .try_next()
            .await
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Registration stream error: {}", e)))?
            .ok_or_else(|| rustlake_core::RustLakeError::Engine("Empty registration response".into()))?;

        let worker_id = String::from_utf8(result.body.to_vec())
            .map_err(|_| rustlake_core::RustLakeError::Engine("Invalid worker ID in response".into()))?;

        tracing::info!(worker_id = %worker_id, "Successfully registered with coordinator");
        self.worker_id = Some(worker_id.clone());
        Ok(worker_id)
    }

    /// Start sending periodic heartbeats to the coordinator.
    ///
    /// Runs until the cancellation signal is received. If a heartbeat fails,
    /// the worker attempts to re-register.
    pub async fn heartbeat_loop(&self, cancel: tokio::sync::watch::Receiver<bool>) {
        let Some(ref coordinator_addr) = self.config.coordinator_addr else {
            tracing::error!("No coordinator address — cannot send heartbeats");
            return;
        };
        let Some(ref worker_id) = self.worker_id else {
            tracing::error!("Worker not registered — cannot send heartbeats");
            return;
        };

        let interval = Duration::from_secs(self.config.heartbeat_interval_secs);
        let mut ticker = tokio::time::interval(interval);
        let mut consecutive_failures = 0u32;

        loop {
            ticker.tick().await;
            if *cancel.borrow() {
                tracing::info!("Heartbeat loop shutting down");
                break;
            }

            match self.send_heartbeat(coordinator_addr, worker_id).await {
                Ok(()) => {
                    consecutive_failures = 0;
                    tracing::trace!(worker_id = %worker_id, "Heartbeat sent");
                }
                Err(e) => {
                    consecutive_failures += 1;
                    tracing::warn!(
                        worker_id = %worker_id,
                        failures = consecutive_failures,
                        error = %e,
                        "Heartbeat failed"
                    );

                    if consecutive_failures >= 3 {
                        tracing::error!(
                            "3 consecutive heartbeat failures — worker may be evicted"
                        );
                    }
                }
            }
        }
    }

    /// Send a single heartbeat to the coordinator.
    async fn send_heartbeat(
        &self,
        coordinator_addr: &str,
        worker_id: &str,
    ) -> rustlake_core::Result<()> {
        let channel = tonic::transport::Channel::from_shared(coordinator_addr.to_string())
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Invalid address: {}", e)))?
            .connect()
            .await
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Connection failed: {}", e)))?;

        let mut client = FlightServiceClient::new(channel);

        let action = Action {
            r#type: "heartbeat".to_string(),
            body: worker_id.as_bytes().to_vec().into(),
        };

        client
            .do_action(action)
            .await
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Heartbeat RPC failed: {}", e)))?;

        Ok(())
    }

    /// Gracefully deregister from the coordinator before shutdown.
    pub async fn deregister(&self) -> rustlake_core::Result<()> {
        let Some(ref coordinator_addr) = self.config.coordinator_addr else {
            return Ok(());
        };
        let Some(ref worker_id) = self.worker_id else {
            return Ok(());
        };

        tracing::info!(worker_id = %worker_id, "Deregistering from coordinator");

        let channel = tonic::transport::Channel::from_shared(coordinator_addr.to_string())
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Invalid address: {}", e)))?
            .connect()
            .await
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Connection failed: {}", e)))?;

        let mut client = FlightServiceClient::new(channel);

        let action = Action {
            r#type: "deregister_worker".to_string(),
            body: worker_id.as_bytes().to_vec().into(),
        };

        client
            .do_action(action)
            .await
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Deregister failed: {}", e)))?;

        Ok(())
    }

    /// Get the assigned worker ID (None if not registered).
    pub fn worker_id(&self) -> Option<&str> {
        self.worker_id.as_deref()
    }
}
