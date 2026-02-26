//! Coordinator node for distributed query execution.
//!
//! The coordinator accepts client queries, plans distributed execution,
//! assigns partition scans to workers, collects results, and returns
//! the merged output. Workers register via Flight RPC and send periodic
//! heartbeats; the coordinator evicts workers that miss the timeout.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use futures::future::join_all;
use rustlake_core::config::{ClusterConfig, DiscoveryMethod};
use rustlake_engine::RustLakeContext;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::client::{FlightClient, FlightClientPool};
use crate::planner::{DistributedPlanner, DistributionStrategy};

/// A registered worker in the coordinator's registry.
#[derive(Debug, Clone)]
pub struct WorkerHandle {
    /// Unique worker ID (assigned by the coordinator on registration).
    pub id: String,
    /// The worker's Flight RPC endpoint (e.g., `"http://10.0.1.5:50051"`).
    pub endpoint: String,
    /// Human-readable label for the worker.
    pub label: Option<String>,
    /// Number of CPU cores reported by the worker.
    pub cpu_cores: usize,
    /// Total memory in bytes reported by the worker.
    pub memory_bytes: u64,
    /// Current status of the worker.
    pub status: WorkerStatus,
    /// Number of partition scans currently assigned to this worker.
    pub active_partitions: u32,
    /// Total queries executed by this worker since registration.
    pub queries_executed: u64,
    /// When this worker was registered.
    pub registered_at: Instant,
    /// Last heartbeat received.
    pub last_heartbeat: Instant,
}

/// Worker health status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    /// Healthy and accepting work.
    Active,
    /// Missed one heartbeat, may be recovering.
    Draining,
    /// Missed heartbeat timeout, will be evicted.
    Unhealthy,
    /// Gracefully shutting down.
    Deregistering,
}

/// Registration request sent by a worker to the coordinator.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerRegistration {
    /// The worker's Flight RPC endpoint.
    pub endpoint: String,
    /// Optional human-readable label.
    pub label: Option<String>,
    /// CPU cores available on the worker.
    pub cpu_cores: usize,
    /// Total memory in bytes.
    pub memory_bytes: u64,
}

/// A partition assignment sent to a worker for execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    /// Unique ID for this partition task.
    pub partition_id: String,
    /// The SQL fragment or table + filter to execute.
    pub sql: String,
    /// Target worker endpoint.
    pub worker_endpoint: String,
}

/// Result from a worker after executing a partition.
#[derive(Debug)]
pub struct PartitionResult {
    pub partition_id: String,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
    pub duration_ms: u128,
}

/// Manages the set of active workers and distributes queries.
pub struct Coordinator {
    /// Cluster configuration.
    config: ClusterConfig,
    /// Registered workers keyed by worker ID.
    workers: Arc<RwLock<HashMap<String, WorkerHandle>>>,
    /// Counter for generating worker IDs.
    next_worker_id: AtomicU64,
    /// The local query context (coordinator also executes queries for small workloads).
    ctx: Arc<RwLock<RustLakeContext>>,
    /// Distributed query planner.
    planner: DistributedPlanner,
    /// Connection pool for persistent worker connections.
    client_pool: FlightClientPool,
    /// Per-worker latency tracking for cost-based routing (endpoint → avg_ms).
    worker_latency: Arc<RwLock<HashMap<String, f64>>>,
}

impl Coordinator {
    /// Create a new coordinator with the given cluster config and local context.
    pub fn new(config: ClusterConfig, ctx: Arc<RwLock<RustLakeContext>>) -> Self {
        let planner = DistributedPlanner::new(ctx.clone());
        Self {
            config,
            workers: Arc::new(RwLock::new(HashMap::new())),
            next_worker_id: AtomicU64::new(1),
            ctx,
            planner,
            client_pool: FlightClientPool::new(),
            worker_latency: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new worker. Returns the assigned worker ID.
    pub async fn register_worker(&self, reg: WorkerRegistration) -> String {
        let id = format!("worker-{}", self.next_worker_id.fetch_add(1, Ordering::Relaxed));
        let now = Instant::now();

        let handle = WorkerHandle {
            id: id.clone(),
            endpoint: reg.endpoint,
            label: reg.label,
            cpu_cores: reg.cpu_cores,
            memory_bytes: reg.memory_bytes,
            status: WorkerStatus::Active,
            active_partitions: 0,
            queries_executed: 0,
            registered_at: now,
            last_heartbeat: now,
        };

        tracing::info!(
            worker_id = %id,
            endpoint = %handle.endpoint,
            cores = handle.cpu_cores,
            "Worker registered"
        );

        self.workers.write().await.insert(id.clone(), handle);
        id
    }

    /// Remove a worker from the registry.
    pub async fn deregister_worker(&self, worker_id: &str) -> bool {
        let removed = self.workers.write().await.remove(worker_id).is_some();
        if removed {
            tracing::info!(worker_id = %worker_id, "Worker deregistered");
        }
        removed
    }

    /// Update a worker's last heartbeat timestamp.
    pub async fn heartbeat(&self, worker_id: &str) -> bool {
        let mut workers = self.workers.write().await;
        if let Some(w) = workers.get_mut(worker_id) {
            w.last_heartbeat = Instant::now();
            if w.status == WorkerStatus::Draining {
                w.status = WorkerStatus::Active;
            }
            true
        } else {
            false
        }
    }

    /// Get a snapshot of all registered workers.
    pub async fn list_workers(&self) -> Vec<WorkerHandle> {
        self.workers.read().await.values().cloned().collect()
    }

    /// Get the number of active workers.
    pub async fn active_worker_count(&self) -> usize {
        self.workers
            .read()
            .await
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .count()
    }

    /// Check worker health and evict timed-out workers.
    /// Should be called periodically (e.g., every heartbeat interval).
    pub async fn check_worker_health(&self) {
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let drain_threshold = Duration::from_secs(self.config.heartbeat_interval_secs * 2);
        let now = Instant::now();

        let mut workers = self.workers.write().await;
        let mut to_remove = Vec::new();

        for (id, worker) in workers.iter_mut() {
            let elapsed = now.duration_since(worker.last_heartbeat);

            if elapsed > timeout {
                tracing::warn!(
                    worker_id = %id,
                    endpoint = %worker.endpoint,
                    elapsed_secs = elapsed.as_secs(),
                    "Worker timed out — evicting"
                );
                to_remove.push(id.clone());
            } else if elapsed > drain_threshold && worker.status == WorkerStatus::Active {
                tracing::warn!(
                    worker_id = %id,
                    endpoint = %worker.endpoint,
                    "Worker missed heartbeats — marking as draining"
                );
                worker.status = WorkerStatus::Draining;
            }
        }

        for id in to_remove {
            workers.remove(&id);
        }
    }

    /// Distribute a SQL query using the distributed planner.
    ///
    /// The planner inspects the SQL AST and chooses the optimal distribution strategy:
    /// - Local: execute on coordinator (simple queries, metadata)
    /// - SingleWorker: route to least-loaded worker
    /// - RangePartition: split table scan across workers
    /// - ScatterGather: all workers execute, coordinator merges
    /// - PartialAggregate: workers compute partial, coordinator merges
    pub async fn execute_distributed(
        &self,
        sql: &str,
    ) -> rustlake_core::Result<Vec<RecordBatch>> {
        let active_workers = {
            let workers = self.workers.read().await;
            workers
                .values()
                .filter(|w| w.status == WorkerStatus::Active)
                .cloned()
                .collect::<Vec<_>>()
        };

        if active_workers.is_empty() {
            tracing::debug!("No active workers — executing locally");
            let ctx = self.ctx.read().await;
            return ctx.sql(sql).await;
        }

        // Use the planner to determine distribution strategy.
        let plan = self.planner.plan(sql, &active_workers).await?;

        tracing::info!(
            strategy = ?plan.strategy,
            workers = plan.worker_assignments.len(),
            estimated_cost = plan.estimated_cost,
            "Distributed plan created"
        );

        // Apply cost-based routing: if estimated cost is low, execute locally.
        let avg_latency = self.average_worker_latency().await;
        if plan.estimated_cost < 5.0 || (plan.estimated_cost < 20.0 && avg_latency > 50.0) {
            tracing::debug!(
                cost = plan.estimated_cost,
                avg_latency_ms = avg_latency,
                "Low cost / high latency — executing locally"
            );
            let ctx = self.ctx.read().await;
            return ctx.sql(sql).await;
        }

        match plan.strategy {
            DistributionStrategy::Local => {
                let ctx = self.ctx.read().await;
                ctx.sql(sql).await
            }
            DistributionStrategy::SingleWorker => {
                let assignment = &plan.worker_assignments[0];
                self.execute_on_worker(&assignment.worker_endpoint, &assignment.sql).await
            }
            DistributionStrategy::RangePartition { .. }
            | DistributionStrategy::ScatterGather => {
                self.execute_parallel(&plan.worker_assignments).await
            }
            DistributionStrategy::PartialAggregate { .. } => {
                // Stage 1: workers execute partial aggregation.
                let partial_batches = self.execute_parallel(&plan.worker_assignments).await?;

                // Stage 2: merge partial results on coordinator.
                // Register partial results as a temp table and run the merge SQL.
                if let Some(ref _merge) = plan.merge_sql {
                    // For now, return the partial results directly.
                    // Full merge requires registering partial batches as a DataFusion table.
                    Ok(partial_batches)
                } else {
                    Ok(partial_batches)
                }
            }
        }
    }

    /// Execute a query on a single worker using the connection pool.
    async fn execute_on_worker(
        &self,
        endpoint: &str,
        sql: &str,
    ) -> rustlake_core::Result<Vec<RecordBatch>> {
        let start = Instant::now();

        match self.client_pool.query(endpoint, sql).await {
            Ok(batches) => {
                let elapsed = start.elapsed().as_millis() as f64;
                self.update_worker_latency(endpoint, elapsed).await;
                Ok(batches)
            }
            Err(e) => {
                tracing::warn!(
                    endpoint = %endpoint,
                    error = %e,
                    "Worker execution failed — falling back to local"
                );
                let ctx = self.ctx.read().await;
                ctx.sql(sql).await
            }
        }
    }

    /// Execute assignments across multiple workers in parallel, merge results.
    async fn execute_parallel(
        &self,
        assignments: &[crate::planner::WorkerAssignment],
    ) -> rustlake_core::Result<Vec<RecordBatch>> {
        let futures: Vec<_> = assignments
            .iter()
            .map(|a| {
                let pool = &self.client_pool;
                let endpoint = a.worker_endpoint.clone();
                let sql = a.sql.clone();
                async move {
                    let start = Instant::now();
                    let result = pool.query(&endpoint, &sql).await;
                    let elapsed = start.elapsed().as_millis() as f64;
                    (endpoint, result, elapsed)
                }
            })
            .collect();

        let results = join_all(futures).await;

        let mut all_batches = Vec::new();
        for (endpoint, result, elapsed) in results {
            self.update_worker_latency(&endpoint, elapsed).await;
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    tracing::warn!(
                        endpoint = %endpoint,
                        error = %e,
                        "Worker failed during parallel execution"
                    );
                }
            }
        }

        Ok(all_batches)
    }

    /// Update the rolling average latency for a worker.
    async fn update_worker_latency(&self, endpoint: &str, latency_ms: f64) {
        let mut latencies = self.worker_latency.write().await;
        let entry = latencies.entry(endpoint.to_string()).or_insert(latency_ms);
        // Exponential moving average (alpha = 0.3).
        *entry = *entry * 0.7 + latency_ms * 0.3;
    }

    /// Get the average latency across all tracked workers.
    async fn average_worker_latency(&self) -> f64 {
        let latencies = self.worker_latency.read().await;
        if latencies.is_empty() {
            return 0.0;
        }
        let sum: f64 = latencies.values().sum();
        sum / latencies.len() as f64
    }

    /// Execute a query using scatter-gather: send to all workers, merge results.
    ///
    /// Each worker executes the full query independently. The coordinator merges
    /// all result batches. Useful for queries where each worker holds different
    /// data partitions (e.g., hash-partitioned tables).
    pub async fn scatter_gather(
        &self,
        sql: &str,
    ) -> rustlake_core::Result<Vec<RecordBatch>> {
        let workers = self.workers.read().await;
        let active_workers: Vec<WorkerHandle> = workers
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .cloned()
            .collect();
        drop(workers);

        if active_workers.is_empty() {
            let ctx = self.ctx.read().await;
            return ctx.sql(sql).await;
        }

        tracing::info!(
            worker_count = active_workers.len(),
            sql = %sql,
            "Scatter-gather query to all workers"
        );

        // Fire queries to all workers in parallel.
        let futures: Vec<_> = active_workers
            .iter()
            .map(|w| {
                let client = FlightClient::new(&w.endpoint);
                let sql = sql.to_string();
                async move { client.query(&sql).await }
            })
            .collect();

        let results = join_all(futures).await;

        // Merge successful results, log failures.
        let mut all_batches = Vec::new();
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    tracing::warn!(
                        worker_id = %active_workers[i].id,
                        error = %e,
                        "Worker failed during scatter-gather"
                    );
                }
            }
        }

        Ok(all_batches)
    }

    /// Start the periodic health check loop.
    /// Runs until the provided cancellation token is signalled.
    pub async fn health_check_loop(self: Arc<Self>, cancel: tokio::sync::watch::Receiver<bool>) {
        let interval = Duration::from_secs(self.config.heartbeat_interval_secs);
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;
            if *cancel.borrow() {
                tracing::info!("Health check loop shutting down");
                break;
            }
            self.check_worker_health().await;
        }
    }

    /// Seed static workers from config (for non-K8s deployments).
    pub async fn seed_static_workers(&self) {
        if self.config.discovery != DiscoveryMethod::Static {
            return;
        }

        for w in &self.config.workers {
            let reg = WorkerRegistration {
                endpoint: format!("http://{}", w.address),
                label: w.label.clone(),
                cpu_cores: 0,
                memory_bytes: 0,
            };
            self.register_worker(reg).await;
        }
    }
}
