//! Flight SQL protocol support for JDBC/ODBC client compatibility.
//!
//! Implements the Arrow Flight SQL specification so that BI tools (Tableau,
//! Superset, DBeaver) and JDBC/ODBC drivers can connect to RustLake as a
//! standard SQL database.
//!
//! Flight SQL extends the base Flight RPC protocol with typed commands
//! encoded as Protobuf messages in `FlightDescriptor.cmd`. This module
//! handles decoding those commands and delegating to the query engine.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementSubstraitPlan, CommandStatementUpdate, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use futures::stream::{self, BoxStream};
use futures::TryStreamExt;
use rustlake_engine::RustLakeContext;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// Flight SQL service implementation for RustLake.
///
/// Wraps a `RustLakeContext` and exposes it via the Flight SQL protocol,
/// enabling JDBC/ODBC clients to execute SQL queries, inspect catalogs,
/// and retrieve schema information.
pub struct RustLakeFlightSqlService {
    ctx: Arc<RwLock<RustLakeContext>>,
}

impl RustLakeFlightSqlService {
    /// Create a new Flight SQL service wrapping the given context.
    pub fn new(ctx: Arc<RwLock<RustLakeContext>>) -> Self {
        Self { ctx }
    }

    /// Execute SQL and return RecordBatches.
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let ctx = self.ctx.read().await;
        ctx.sql(sql)
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))
    }
}

#[tonic::async_trait]
impl FlightSqlService for RustLakeFlightSqlService {
    type FlightService = Self;

    /// Handshake — accept all connections (no auth yet).
    async fn do_handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<BoxStream<'static, Result<HandshakeResponse, Status>>>,
        Status,
    > {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::new(),
        };
        Ok(Response::new(Box::pin(stream::once(async { Ok(response) }))))
    }

    /// Execute a SQL statement and return FlightInfo with the result schema.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<BoxStream<'static, Result<arrow_flight::FlightData, Status>>>, Status>
    {
        let handle = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::internal("Invalid statement handle"))?;

        let batches = self.execute_sql(&handle).await?;

        let schema = if let Some(first) = batches.first() {
            first.schema()
        } else {
            Arc::new(Schema::empty())
        };

        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    /// Get FlightInfo for a SQL statement.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = &query.query;
        let batches = self.execute_sql(sql).await?;

        let schema = if let Some(first) = batches.first() {
            first.schema()
        } else {
            Arc::new(Schema::empty())
        };

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let ticket = Ticket::new(sql.as_bytes().to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_total_records(total_rows as i64)
            .with_endpoint(endpoint);

        Ok(Response::new(info))
    }

    /// Get catalog names.
    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
        ]));
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;
        Ok(Response::new(info))
    }

    /// Get database schemas.
    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;
        Ok(Response::new(info))
    }

    /// Get table list.
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("GetSqlInfo not yet implemented"))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_type", DataType::Utf8, false),
        ]));
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?;
        Ok(Response::new(info))
    }

    /// Create a prepared statement — stores the SQL for later execution.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let handle = query.query.into_bytes();
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: bytes::Bytes::new(),
            parameter_schema: bytes::Bytes::new(),
        })
    }

    /// Close a prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    /// Execute a prepared statement query.
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<BoxStream<'static, Result<arrow_flight::FlightData, Status>>>, Status>
    {
        let sql = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::internal("Invalid prepared statement handle"))?;

        let batches = self.execute_sql(&sql).await?;

        let schema = if let Some(first) = batches.first() {
            first.schema()
        } else {
            Arc::new(Schema::empty())
        };

        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    /// Get FlightInfo for a prepared statement.
    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = String::from_utf8(cmd.prepared_statement_handle.to_vec())
            .map_err(|_| Status::internal("Invalid prepared statement handle"))?;

        let batches = self.execute_sql(&sql).await?;
        let schema = if let Some(first) = batches.first() {
            first.schema()
        } else {
            Arc::new(Schema::empty())
        };
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let ticket = Ticket::new(sql.as_bytes().to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_total_records(total_rows as i64)
            .with_endpoint(endpoint);

        Ok(Response::new(info))
    }

    // --- Unimplemented features (stubs for protocol compliance) ---

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("XDBC type info not supported"))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Primary keys not supported"))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Exported keys not supported"))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Imported keys not supported"))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Cross reference not supported"))
    }

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Statement update not supported"))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Prepared statement update not supported"))
    }

    async fn do_put_substrait_plan(
        &self,
        _ticket: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Substrait plans not supported"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
