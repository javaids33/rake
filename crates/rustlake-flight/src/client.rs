//! Arrow Flight client for connecting to remote RustLake Flight servers.

use arrow::array::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use rustlake_core::{Result, RustLakeError};

/// Client for connecting to a RustLake Flight server.
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

        // Decode the Flight stream into RecordBatches using arrow-flight's decoder
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
