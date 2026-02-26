//! Arrow Flight RPC server and client for RustLake.
//!
//! Enables distributed query execution via Arrow Flight protocol and
//! provides JDBC/ODBC compatibility via Flight SQL.

pub mod client;
pub mod server;

/// Configuration for the Flight server.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FlightConfig {
    /// Host to bind the Flight server to.
    #[serde(default = "default_flight_host")]
    pub host: String,
    /// Port for the Flight server.
    #[serde(default = "default_flight_port")]
    pub port: u16,
    /// Maximum message size in bytes.
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            host: default_flight_host(),
            port: default_flight_port(),
            max_message_size: default_max_message_size(),
        }
    }
}

fn default_flight_host() -> String {
    "127.0.0.1".to_string()
}
fn default_flight_port() -> u16 {
    50051
}
fn default_max_message_size() -> usize {
    64 * 1024 * 1024
} // 64MB
