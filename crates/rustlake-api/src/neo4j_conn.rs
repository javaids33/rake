//! Neo4j graph database connector via HTTP REST API.
//!
//! Connects to Neo4j's HTTP transaction API, executes Cypher queries,
//! and converts results to Arrow RecordBatch for DataFusion integration.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Configuration for connecting to a Neo4j instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Neo4jConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub use_ssl: bool,
}

impl Default for Neo4jConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 7474,
            username: "neo4j".to_string(),
            password: "neo4j".to_string(),
            database: "neo4j".to_string(),
            use_ssl: false,
        }
    }
}

/// A live connection handle to Neo4j backed by an HTTP client.
pub struct Neo4jConnection {
    client: reqwest::Client,
    config: Neo4jConfig,
    base_url: String,
}

/// A node returned from a Cypher query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Neo4jNode {
    pub id: i64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, serde_json::Value>,
}

/// A relationship returned from a Cypher query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Neo4jRelationship {
    pub id: i64,
    pub start_node: i64,
    pub end_node: i64,
    pub rel_type: String,
    pub properties: HashMap<String, serde_json::Value>,
}

/// Combined result of a Cypher query containing both tabular and graph data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Neo4jResult {
    pub nodes: Vec<Neo4jNode>,
    pub relationships: Vec<Neo4jRelationship>,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// A graph node prepared for visualization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub group: String,
    pub properties: HashMap<String, String>,
    pub size: f64,
}

/// A graph edge prepared for visualization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub label: String,
    pub properties: HashMap<String, String>,
}

/// Graph data ready for front-end visualization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphData {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

impl Neo4jConnection {
    /// Build the transaction commit URL for the configured database.
    fn tx_commit_url(&self) -> String {
        format!("{}/db/{}/tx/commit", self.base_url, self.config.database)
    }

    /// Execute a raw HTTP POST against the transaction endpoint.
    async fn post_statements(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let resp = self
            .client
            .post(&self.tx_commit_url())
            .basic_auth(&self.config.username, Some(&self.config.password))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json;charset=UTF-8")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Neo4j HTTP request failed: {e}"))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read Neo4j response body: {e}"))?;

        if !status.is_success() {
            return Err(format!(
                "Neo4j returned HTTP {status}: {text}"
            ));
        }

        let json: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("Invalid JSON from Neo4j: {e}"))?;

        // Check for Neo4j-level errors embedded in the response.
        if let Some(errors) = json.get("errors") {
            if let Some(arr) = errors.as_array() {
                if !arr.is_empty() {
                    let msgs: Vec<String> = arr
                        .iter()
                        .filter_map(|e| {
                            let code = e.get("code").and_then(|c| c.as_str()).unwrap_or("?");
                            let msg = e.get("message").and_then(|m| m.as_str()).unwrap_or("?");
                            Some(format!("[{code}] {msg}"))
                        })
                        .collect();
                    return Err(format!("Neo4j errors: {}", msgs.join("; ")));
                }
            }
        }

        Ok(json)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Test connectivity to a Neo4j instance and return a connection handle.
pub async fn connect(config: &Neo4jConfig) -> Result<Neo4jConnection, String> {
    let scheme = if config.use_ssl { "https" } else { "http" };
    let base_url = format!("{scheme}://{}:{}", config.host, config.port);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {e}"))?;

    let conn = Neo4jConnection {
        client,
        config: config.clone(),
        base_url,
    };

    // Verify connectivity with a trivial query.
    let body = serde_json::json!({
        "statements": [{"statement": "RETURN 1 AS ping"}]
    });

    conn.post_statements(body).await.map_err(|e| {
        format!(
            "Cannot connect to Neo4j at {}:{} — {e}",
            config.host, config.port
        )
    })?;

    Ok(conn)
}

/// Execute a Cypher query and return combined tabular + graph results.
pub async fn execute_cypher(
    conn: &Neo4jConnection,
    cypher: &str,
) -> Result<Neo4jResult, String> {
    let body = serde_json::json!({
        "statements": [{
            "statement": cypher,
            "resultDataContents": ["row", "graph"]
        }]
    });

    let json = conn.post_statements(body).await?;

    let result_obj = json
        .get("results")
        .and_then(|r| r.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| "No results in Neo4j response".to_string())?;

    // --- columns ---
    let columns: Vec<String> = result_obj
        .get("columns")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // --- rows, nodes, relationships ---
    let data_array = result_obj
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();

    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut nodes: Vec<Neo4jNode> = Vec::new();
    let mut relationships: Vec<Neo4jRelationship> = Vec::new();
    let mut seen_node_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut seen_rel_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for datum in &data_array {
        // Tabular row data.
        if let Some(row) = datum.get("row").and_then(|r| r.as_array()) {
            rows.push(row.clone());
        }

        // Graph data.
        if let Some(graph) = datum.get("graph") {
            if let Some(ns) = graph.get("nodes").and_then(|n| n.as_array()) {
                for n in ns {
                    let id = parse_neo4j_id(n);
                    if seen_node_ids.contains(&id) {
                        continue;
                    }
                    seen_node_ids.insert(id);
                    nodes.push(parse_node(n));
                }
            }
            if let Some(rs) = graph.get("relationships").and_then(|r| r.as_array()) {
                for r in rs {
                    let id = parse_neo4j_id(r);
                    if seen_rel_ids.contains(&id) {
                        continue;
                    }
                    seen_rel_ids.insert(id);
                    relationships.push(parse_relationship(r));
                }
            }
        }
    }

    Ok(Neo4jResult {
        nodes,
        relationships,
        columns,
        rows,
    })
}

/// Discover all node labels in the database.
pub async fn discover_labels(conn: &Neo4jConnection) -> Result<Vec<String>, String> {
    let result = execute_cypher(conn, "CALL db.labels()").await?;
    let labels = result
        .rows
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()).map(String::from))
        .collect();
    Ok(labels)
}

/// Discover all relationship types in the database.
pub async fn discover_relationship_types(
    conn: &Neo4jConnection,
) -> Result<Vec<String>, String> {
    let result = execute_cypher(conn, "CALL db.relationshipTypes()").await?;
    let types = result
        .rows
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()).map(String::from))
        .collect();
    Ok(types)
}

/// Discover schema: for each label, return the list of property keys observed.
pub async fn discover_schema(
    conn: &Neo4jConnection,
) -> Result<Vec<(String, Vec<String>)>, String> {
    let labels = discover_labels(conn).await?;
    let mut schema: Vec<(String, Vec<String>)> = Vec::with_capacity(labels.len());

    for label in &labels {
        // Use a sample query to discover property keys for this label.
        let cypher = format!(
            "MATCH (n:`{label}`) WITH n LIMIT 100 UNWIND keys(n) AS k RETURN DISTINCT k ORDER BY k"
        );
        let result = execute_cypher(conn, &cypher).await?;
        let keys: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_str()).map(String::from))
            .collect();
        schema.push((label.clone(), keys));
    }

    Ok(schema)
}

/// Convert tabular Cypher results into an Arrow RecordBatch.
pub fn cypher_result_to_recordbatch(result: &Neo4jResult) -> Result<RecordBatch, String> {
    if result.columns.is_empty() {
        return Err("No columns in Cypher result".to_string());
    }

    if result.rows.is_empty() {
        // Return an empty batch with string columns.
        let fields: Vec<Field> = result
            .columns
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = result
            .columns
            .iter()
            .map(|_| {
                let mut builder = StringBuilder::new();
                Arc::new(builder.finish()) as ArrayRef
            })
            .collect();
        return RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("Failed to build empty RecordBatch: {e}"));
    }

    // Detect column types from the first row.
    let first_row = &result.rows[0];
    let col_types: Vec<DataType> = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let val = first_row.get(i);
            infer_arrow_type(val)
        })
        .collect();

    let num_rows = result.rows.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(result.columns.len());

    for (col_idx, dt) in col_types.iter().enumerate() {
        match dt {
            DataType::Int64 => {
                let values: Vec<Option<i64>> = result
                    .rows
                    .iter()
                    .map(|row| row.get(col_idx).and_then(|v| v.as_i64()))
                    .collect();
                arrays.push(Arc::new(Int64Array::from(values)) as ArrayRef);
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = result
                    .rows
                    .iter()
                    .map(|row| row.get(col_idx).and_then(|v| v.as_f64()))
                    .collect();
                arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
            }
            DataType::Boolean => {
                let values: Vec<Option<bool>> = result
                    .rows
                    .iter()
                    .map(|row| row.get(col_idx).and_then(|v| v.as_bool()))
                    .collect();
                arrays.push(Arc::new(BooleanArray::from(values)) as ArrayRef);
            }
            _ => {
                // Default: stringify everything.
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in &result.rows {
                    match row.get(col_idx) {
                        Some(serde_json::Value::Null) | None => builder.append_null(),
                        Some(serde_json::Value::String(s)) => builder.append_value(s),
                        Some(v) => builder.append_value(&v.to_string()),
                    }
                }
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
        }
    }

    let fields: Vec<Field> = result
        .columns
        .iter()
        .zip(col_types.iter())
        .map(|(name, dt)| Field::new(name, dt.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("Failed to build RecordBatch: {e}"))
}

/// Extract graph nodes and edges from Cypher results for visualization.
pub fn cypher_result_to_graph(result: &Neo4jResult) -> GraphData {
    let nodes: Vec<GraphNode> = result
        .nodes
        .iter()
        .map(|n| {
            let label = n.labels.first().cloned().unwrap_or_else(|| "Node".to_string());
            let group = label.clone();
            let properties: HashMap<String, String> = n
                .properties
                .iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), s)
                })
                .collect();

            // Size based on property count — more connected nodes appear larger.
            let size = 10.0 + (properties.len() as f64) * 2.0;

            // Prefer a "name" or "title" property as the display label.
            let display = properties
                .get("name")
                .or_else(|| properties.get("title"))
                .cloned()
                .unwrap_or_else(|| format!("{label}:{}", n.id));

            GraphNode {
                id: n.id.to_string(),
                label: display,
                group,
                properties,
                size,
            }
        })
        .collect();

    let edges: Vec<GraphEdge> = result
        .relationships
        .iter()
        .map(|r| {
            let properties: HashMap<String, String> = r
                .properties
                .iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), s)
                })
                .collect();

            GraphEdge {
                source: r.start_node.to_string(),
                target: r.end_node.to_string(),
                label: r.rel_type.clone(),
                properties,
            }
        })
        .collect();

    GraphData { nodes, edges }
}

/// Return the total number of nodes in the database.
pub async fn node_count(conn: &Neo4jConnection) -> Result<u64, String> {
    let result = execute_cypher(conn, "MATCH (n) RETURN count(n) AS cnt").await?;
    let count = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    Ok(count)
}

/// Return the total number of relationships in the database.
pub async fn relationship_count(conn: &Neo4jConnection) -> Result<u64, String> {
    let result =
        execute_cypher(conn, "MATCH ()-[r]->() RETURN count(r) AS cnt").await?;
    let count = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    Ok(count)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Parse a Neo4j node from the graph portion of the HTTP response.
fn parse_node(value: &serde_json::Value) -> Neo4jNode {
    let id = parse_neo4j_id(value);
    let labels: Vec<String> = value
        .get("labels")
        .and_then(|l| l.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let properties = parse_properties(value);

    Neo4jNode {
        id,
        labels,
        properties,
    }
}

/// Parse a Neo4j relationship from the graph portion of the HTTP response.
fn parse_relationship(value: &serde_json::Value) -> Neo4jRelationship {
    let id = parse_neo4j_id(value);
    let start_node = value
        .get("startNode")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| value.get("startNode").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    let end_node = value
        .get("endNode")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| value.get("endNode").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    let rel_type = value
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("RELATED_TO")
        .to_string();
    let properties = parse_properties(value);

    Neo4jRelationship {
        id,
        start_node,
        end_node,
        rel_type,
        properties,
    }
}

/// Extract the `id` field from a Neo4j graph entity (node or relationship).
/// The HTTP API may return `id` as a string or integer.
fn parse_neo4j_id(value: &serde_json::Value) -> i64 {
    value
        .get("id")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
        .unwrap_or(0)
}

/// Extract the `properties` map from a Neo4j graph entity.
fn parse_properties(value: &serde_json::Value) -> HashMap<String, serde_json::Value> {
    value
        .get("properties")
        .and_then(|p| p.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Infer an Arrow DataType from a JSON value.
fn infer_arrow_type(value: Option<&serde_json::Value>) -> DataType {
    match value {
        Some(serde_json::Value::Number(n)) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        Some(serde_json::Value::Bool(_)) => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_arrow_type() {
        assert_eq!(
            infer_arrow_type(Some(&serde_json::json!(42))),
            DataType::Int64
        );
        assert_eq!(
            infer_arrow_type(Some(&serde_json::json!(3.14))),
            DataType::Float64
        );
        assert_eq!(
            infer_arrow_type(Some(&serde_json::json!(true))),
            DataType::Boolean
        );
        assert_eq!(
            infer_arrow_type(Some(&serde_json::json!("hello"))),
            DataType::Utf8
        );
        assert_eq!(infer_arrow_type(None), DataType::Utf8);
    }

    #[test]
    fn test_cypher_result_to_recordbatch_empty() {
        let result = Neo4jResult {
            nodes: vec![],
            relationships: vec![],
            columns: vec!["name".to_string(), "age".to_string()],
            rows: vec![],
        };
        let batch = cypher_result_to_recordbatch(&result).expect("should succeed");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_cypher_result_to_recordbatch_mixed() {
        let result = Neo4jResult {
            nodes: vec![],
            relationships: vec![],
            columns: vec![
                "name".to_string(),
                "age".to_string(),
                "score".to_string(),
                "active".to_string(),
            ],
            rows: vec![
                vec![
                    serde_json::json!("Alice"),
                    serde_json::json!(30),
                    serde_json::json!(9.5),
                    serde_json::json!(true),
                ],
                vec![
                    serde_json::json!("Bob"),
                    serde_json::json!(25),
                    serde_json::json!(8.2),
                    serde_json::json!(false),
                ],
            ],
        };
        let batch = cypher_result_to_recordbatch(&result).expect("should succeed");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(3).data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_cypher_result_to_graph() {
        let result = Neo4jResult {
            nodes: vec![
                Neo4jNode {
                    id: 1,
                    labels: vec!["Person".to_string()],
                    properties: HashMap::from([
                        ("name".to_string(), serde_json::json!("Alice")),
                    ]),
                },
                Neo4jNode {
                    id: 2,
                    labels: vec!["Company".to_string()],
                    properties: HashMap::from([
                        ("name".to_string(), serde_json::json!("Acme")),
                    ]),
                },
            ],
            relationships: vec![Neo4jRelationship {
                id: 100,
                start_node: 1,
                end_node: 2,
                rel_type: "WORKS_AT".to_string(),
                properties: HashMap::new(),
            }],
            columns: vec![],
            rows: vec![],
        };

        let graph = cypher_result_to_graph(&result);
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes[0].label, "Alice");
        assert_eq!(graph.nodes[0].group, "Person");
        assert_eq!(graph.nodes[1].label, "Acme");
        assert_eq!(graph.edges[0].label, "WORKS_AT");
        assert_eq!(graph.edges[0].source, "1");
        assert_eq!(graph.edges[0].target, "2");
    }

    #[test]
    fn test_parse_node() {
        let json = serde_json::json!({
            "id": "42",
            "labels": ["Person", "Employee"],
            "properties": {"name": "Charlie", "age": 28}
        });
        let node = parse_node(&json);
        assert_eq!(node.id, 42);
        assert_eq!(node.labels, vec!["Person", "Employee"]);
        assert_eq!(
            node.properties.get("name"),
            Some(&serde_json::json!("Charlie"))
        );
    }

    #[test]
    fn test_parse_relationship() {
        let json = serde_json::json!({
            "id": 99,
            "startNode": "1",
            "endNode": "2",
            "type": "KNOWS",
            "properties": {"since": 2020}
        });
        let rel = parse_relationship(&json);
        assert_eq!(rel.id, 99);
        assert_eq!(rel.start_node, 1);
        assert_eq!(rel.end_node, 2);
        assert_eq!(rel.rel_type, "KNOWS");
    }

    #[test]
    fn test_default_config() {
        let cfg = Neo4jConfig::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 7474);
        assert_eq!(cfg.database, "neo4j");
        assert!(!cfg.use_ssl);
    }
}
