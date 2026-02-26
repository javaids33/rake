//! Arrow Flight RPC server and client for RustLake.
//!
//! Enables distributed query execution via Arrow Flight protocol and
//! provides JDBC/ODBC compatibility via Flight SQL.
//!
//! ## Modules
//!
//! - `server` — Flight RPC service (do_get, do_action, do_exchange, etc.)
//! - `client` — Flight RPC client + connection pool for persistent channels
//! - `coordinator` — Coordinator node: worker registry, query distribution
//! - `worker` — Worker node: registration, heartbeats, partition execution
//! - `planner` — Distributed query planner: partition-aware SQL planning
//! - `exchange` — DataFusion ExecutionPlan nodes for Flight-based data exchange
//! - `discovery` — Worker discovery (static, self-register, Kubernetes DNS)
//! - `sql` — Flight SQL protocol for JDBC/ODBC client compatibility

pub mod client;
pub mod coordinator;
pub mod discovery;
pub mod exchange;
pub mod planner;
pub mod server;
pub mod sql;
pub mod worker;

// Re-export FlightConfig from core (single source of truth).
pub use rustlake_core::config::FlightConfig;
