use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::router::tool::ToolRouter,
    model::*,
    service::RequestContext,
    tool_handler,
};
use serde_json::json;

use crate::client::RustLakeClient;

#[derive(Clone)]
pub struct RustLakeMcp {
    pub(crate) client: RustLakeClient,
    pub(crate) tool_router: ToolRouter<Self>,
}

impl RustLakeMcp {
    fn resource(uri: &str, name: &str, description: &str) -> Resource {
        RawResource::new(uri, name.to_string())
            .with_description(description)
            .no_annotation()
    }
}

#[tool_handler]
impl ServerHandler for RustLakeMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new("rustlake-mcp", env!("CARGO_PKG_VERSION")))
        .with_protocol_version(ProtocolVersion::V_2024_11_05)
        .with_instructions(
            "RustLake MCP server: execute SQL queries, inspect table schemas, \
             monitor streaming pipelines, check Glacier table health, and debug \
             the RustLake data platform. Connects to the running RustLake API. \
             \n\nKey tools: sql_query (run SQL), list_tables (catalog), \
             table_schema/preview/stats (inspect data), list_connections, \
             list_pipelines (CDC/Kafka), list_glaciers (executable tables), \
             system_info, list_engines."
                .to_string(),
        )
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult {
            resources: vec![
                Self::resource(
                    "rustlake://system/info",
                    "System Info",
                    "RustLake server version, uptime, query count, and engine status",
                ),
                Self::resource(
                    "rustlake://tables",
                    "Table Catalog",
                    "All registered tables across all schemas",
                ),
                Self::resource(
                    "rustlake://connections",
                    "Connections",
                    "All data source connections and their status",
                ),
                Self::resource(
                    "rustlake://pipelines",
                    "Pipelines",
                    "Streaming/CDC pipeline status and metrics",
                ),
                Self::resource(
                    "rustlake://glaciers",
                    "Glaciers",
                    "Executable tables with health, freshness, and version info",
                ),
            ],
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let uri = &request.uri;
        let result = match uri.as_str() {
            "rustlake://system/info" => self.client.system_info().await,
            "rustlake://tables" => self.client.list_tables().await,
            "rustlake://connections" => self.client.list_connections().await,
            "rustlake://pipelines" => self.client.list_pipelines().await,
            "rustlake://glaciers" => self.client.list_executable_tables().await,
            _ => {
                return Err(McpError::resource_not_found(
                    "resource_not_found",
                    Some(json!({ "uri": uri })),
                ));
            }
        };

        match result {
            Ok(json) => {
                let text = serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string());
                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    text,
                    uri.clone(),
                )]))
            }
            Err(e) => {
                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    format!("Error fetching resource: {}", e),
                    uri.clone(),
                )]))
            }
        }
    }
}
