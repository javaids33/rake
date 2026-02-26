//! Arrow Flight server implementation.
//!
//! Wraps a `RustLakeContext` and exposes it via the Flight RPC protocol.
//! BI tools (Tableau, Superset) connect here via Flight SQL.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use rustlake_engine::RustLakeContext;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::{Coordinator, WorkerRegistration};
use crate::FlightConfig;

/// Shared metrics exposed to the API layer for real-time Flight status reporting.
#[derive(Debug, Clone)]
pub struct FlightMetrics {
    /// Number of in-flight gRPC connections (incremented on do_get, decremented when stream ends).
    pub active_connections: Arc<AtomicU64>,
    /// Total queries executed via Flight since startup.
    pub queries_served: Arc<AtomicU64>,
    /// Whether the tonic server is currently accepting connections.
    pub running: Arc<AtomicBool>,
}

impl Default for FlightMetrics {
    fn default() -> Self {
        Self {
            active_connections: Arc::new(AtomicU64::new(0)),
            queries_served: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// RustLake Flight service — exposes SQL queries over Arrow Flight RPC.
pub struct RustLakeFlightService {
    ctx: Arc<RwLock<RustLakeContext>>,
    metrics: FlightMetrics,
    /// Optional coordinator for managing distributed workers.
    /// Set when this node runs as a coordinator.
    coordinator: Option<Arc<Coordinator>>,
}

impl RustLakeFlightService {
    /// Create a new Flight service wrapping the given context and metrics handle.
    pub fn new(ctx: Arc<RwLock<RustLakeContext>>, metrics: FlightMetrics) -> Self {
        Self {
            ctx,
            metrics,
            coordinator: None,
        }
    }

    /// Attach a coordinator for handling worker registration and distributed queries.
    pub fn with_coordinator(mut self, coordinator: Arc<Coordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
    }

    /// Start the Flight RPC server on the configured address.
    ///
    /// Sets `metrics.running` to `true` while serving, resets on exit.
    pub async fn serve(self, config: &FlightConfig) -> rustlake_core::Result<()> {
        let addr = format!("{}:{}", config.host, config.port)
            .parse()
            .map_err(|e| rustlake_core::RustLakeError::Config(format!("Bad flight address: {}", e)))?;

        tracing::info!(%addr, "Starting Arrow Flight gRPC server");

        self.metrics.running.store(true, Ordering::SeqCst);
        let running_flag = self.metrics.running.clone();

        let svc = FlightServiceServer::new(self)
            .max_encoding_message_size(config.max_message_size)
            .max_decoding_message_size(config.max_message_size);

        let result = tonic::transport::Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .map_err(|e| rustlake_core::RustLakeError::Engine(format!("Flight server error: {}", e)));

        running_flag.store(false, Ordering::SeqCst);
        result
    }

    /// Execute a SQL query and return results (used by the Flight RPC handlers).
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Status> {
        tracing::info!(sql = %sql, "Flight executing SQL");
        let ctx = self.ctx.read().await;
        ctx.sql(sql)
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))
    }
}

#[tonic::async_trait]
impl FlightService for RustLakeFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    /// Handshake — not implemented (no auth layer yet).
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not implemented — no auth layer"))
    }

    /// List flights — not implemented.
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights not implemented"))
    }

    /// Get flight info for a SQL query — runs the query and returns schema + row count.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let sql = std::str::from_utf8(&descriptor.cmd)
            .map_err(|_| Status::invalid_argument("FlightDescriptor.cmd must be valid UTF-8 SQL"))?;

        let batches = self.execute_sql(sql).await?;

        let schema = if let Some(first) = batches.first() {
            first.schema()
        } else {
            Arc::new(arrow::datatypes::Schema::empty())
        };

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("Schema encoding error: {}", e)))?
            .with_descriptor(descriptor)
            .with_total_records(total_rows as i64);

        Ok(Response::new(info))
    }

    /// Poll flight info — not implemented.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }

    /// Get schema — not implemented.
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("GetSchema not implemented"))
    }

    /// Execute a SQL query and stream results as Flight data.
    ///
    /// The `Ticket` bytes are interpreted as a UTF-8 SQL string.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = std::str::from_utf8(&ticket.ticket)
            .map_err(|_| Status::invalid_argument("Ticket must contain valid UTF-8 SQL"))?;

        self.metrics.active_connections.fetch_add(1, Ordering::Relaxed);
        self.metrics.queries_served.fetch_add(1, Ordering::Relaxed);
        let active_conns = self.metrics.active_connections.clone();

        let batches = self.execute_sql(sql).await?;

        let schema = if let Some(first) = batches.first() {
            first.schema()
        } else {
            Arc::new(arrow::datatypes::Schema::empty())
        };

        // Encode RecordBatches into FlightData frames.
        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Flight encoding error: {}", e)))
            .chain(stream::once(async move {
                // Decrement active connections when the client finishes reading.
                active_conns.fetch_sub(1, Ordering::Relaxed);
                // Return Err to signal end without adding a data frame — the
                // stream terminates naturally after the last FlightData.
                Err(Status::ok("stream complete"))
            }))
            // Filter out the synthetic "ok" status — clients see a clean EOF.
            .take_while(|item| {
                let keep = match item {
                    Err(s) if s.code() == tonic::Code::Ok => false,
                    _ => true,
                };
                futures::future::ready(keep)
            });

        Ok(Response::new(Box::pin(flight_stream)))
    }

    /// Put — not implemented (read-only server).
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("DoPut not implemented — read-only server"))
    }

    /// Exchange — bidirectional data shuffle for distributed joins and aggregations.
    ///
    /// The coordinator sends partition data to workers, and workers send their
    /// partition results back. The exchange protocol:
    /// 1. First FlightData message is a descriptor with the exchange type (e.g., "shuffle")
    /// 2. Subsequent messages contain RecordBatch data
    /// 3. Response stream contains the worker's processed results
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let inbound = request.into_inner();

        // Collect all inbound FlightData into RecordBatches.
        let mut decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            inbound.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let mut input_batches: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = decoder
            .try_next()
            .await
            .map_err(|e| Status::internal(format!("Exchange decode error: {}", e)))?
        {
            input_batches.push(batch);
        }

        if input_batches.is_empty() {
            // No data — return an empty stream.
            return Ok(Response::new(Box::pin(stream::empty())));
        }

        // For now, the exchange is a pass-through: the worker processes the
        // received batches and returns them. In a full implementation, this would
        // apply hash-partitioning, repartitioning, or aggregation.
        let schema = input_batches[0].schema();

        let batch_stream = stream::iter(input_batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Exchange encoding error: {}", e)));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    /// Execute a named action.
    ///
    /// Supported actions:
    /// - `"healthcheck"` — returns `"ok"` if the server is alive
    /// - `"sql"` — runs the action body as SQL, returns a summary
    /// - `"register_worker"` — (coordinator only) register a new worker node
    /// - `"heartbeat"` — (coordinator only) update worker heartbeat
    /// - `"deregister_worker"` — (coordinator only) remove a worker node
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "healthcheck" => {
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from("ok"),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "sql" => {
                let sql = std::str::from_utf8(&action.body)
                    .map_err(|_| Status::invalid_argument("Action body must be valid UTF-8 SQL"))?;
                let batches = self.execute_sql(sql).await?;
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let msg = format!("Query returned {} rows", total_rows);
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from(msg),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "register_worker" => {
                let coordinator = self.coordinator.as_ref().ok_or_else(|| {
                    Status::failed_precondition("This node is not a coordinator")
                })?;
                let reg: WorkerRegistration = serde_json::from_slice(&action.body)
                    .map_err(|e| Status::invalid_argument(format!("Invalid registration payload: {}", e)))?;
                let worker_id = coordinator.register_worker(reg).await;
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from(worker_id),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "heartbeat" => {
                let coordinator = self.coordinator.as_ref().ok_or_else(|| {
                    Status::failed_precondition("This node is not a coordinator")
                })?;
                let worker_id = std::str::from_utf8(&action.body)
                    .map_err(|_| Status::invalid_argument("Worker ID must be valid UTF-8"))?;
                let ok = coordinator.heartbeat(worker_id).await;
                if !ok {
                    return Err(Status::not_found(format!("Worker {} not registered", worker_id)));
                }
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from("ok"),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            "deregister_worker" => {
                let coordinator = self.coordinator.as_ref().ok_or_else(|| {
                    Status::failed_precondition("This node is not a coordinator")
                })?;
                let worker_id = std::str::from_utf8(&action.body)
                    .map_err(|_| Status::invalid_argument("Worker ID must be valid UTF-8"))?;
                coordinator.deregister_worker(worker_id).await;
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from("ok"),
                };
                Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
            }
            other => Err(Status::invalid_argument(format!(
                "Unknown action type: {other}"
            ))),
        }
    }

    /// List supported action types.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let mut actions = vec![
            Ok(ActionType {
                r#type: "healthcheck".to_string(),
                description: "Check if the Flight server is alive".to_string(),
            }),
            Ok(ActionType {
                r#type: "sql".to_string(),
                description: "Execute SQL and return a summary".to_string(),
            }),
        ];
        // Include coordinator-only actions if this node is a coordinator.
        if self.coordinator.is_some() {
            actions.push(Ok(ActionType {
                r#type: "register_worker".to_string(),
                description: "Register a worker node (JSON body with endpoint, cpu_cores, memory_bytes)".to_string(),
            }));
            actions.push(Ok(ActionType {
                r#type: "heartbeat".to_string(),
                description: "Worker heartbeat (body = worker_id)".to_string(),
            }));
            actions.push(Ok(ActionType {
                r#type: "deregister_worker".to_string(),
                description: "Remove a worker node (body = worker_id)".to_string(),
            }));
        }
        Ok(Response::new(Box::pin(stream::iter(actions))))
    }
}
