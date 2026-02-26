//! Column-level lineage tracking.

use std::collections::HashMap;

use petgraph::graph::{DiGraph, NodeIndex};
use serde::Serialize;

/// A node in the lineage graph — represents a table or column.
#[derive(Debug, Clone, Serialize)]
pub struct LineageNode {
    /// Fully qualified name (e.g., "schema.table.column").
    pub fqn: String,
    /// Node type.
    pub node_type: LineageNodeType,
}

/// Type of lineage node.
#[derive(Debug, Clone, Serialize)]
pub enum LineageNodeType {
    /// A raw data source (e.g., a Kafka topic or external table).
    Source,
    /// A transformation model that produces a derived table.
    Model,
    /// An individual column within a table or model.
    Column,
}

/// Directed graph tracking data lineage between tables and columns.
pub struct LineageGraph {
    graph: DiGraph<LineageNode, String>,
    node_map: HashMap<String, NodeIndex>,
}

impl LineageGraph {
    /// Create an empty lineage graph.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_map: HashMap::new(),
        }
    }

    /// Add a node to the graph.
    pub fn add_node(&mut self, fqn: &str, node_type: LineageNodeType) {
        if !self.node_map.contains_key(fqn) {
            let node = LineageNode {
                fqn: fqn.to_string(),
                node_type,
            };
            let idx = self.graph.add_node(node);
            self.node_map.insert(fqn.to_string(), idx);
        }
    }

    /// Add a lineage edge: `from` feeds into `to`.
    pub fn add_edge(&mut self, from: &str, to: &str, transform: &str) {
        if let (Some(&from_idx), Some(&to_idx)) = (self.node_map.get(from), self.node_map.get(to)) {
            self.graph.add_edge(from_idx, to_idx, transform.to_string());
        }
    }

    /// Get all upstream dependencies of a node.
    pub fn upstream(&self, fqn: &str) -> Vec<&LineageNode> {
        let Some(&idx) = self.node_map.get(fqn) else {
            return vec![];
        };

        self.graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .map(|n| &self.graph[n])
            .collect()
    }

    /// Get all downstream dependents of a node.
    pub fn downstream(&self, fqn: &str) -> Vec<&LineageNode> {
        let Some(&idx) = self.node_map.get(fqn) else {
            return vec![];
        };

        self.graph
            .neighbors_directed(idx, petgraph::Direction::Outgoing)
            .map(|n| &self.graph[n])
            .collect()
    }

    /// Get total number of nodes.
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Get total number of edges.
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }
}

impl Default for LineageGraph {
    fn default() -> Self {
        Self::new()
    }
}
