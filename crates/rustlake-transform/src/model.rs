//! Model definitions for the transformation layer.

use serde::{Deserialize, Serialize};

/// A transformation model — a SQL query that produces a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    /// Unique model name (e.g., "stg_orders").
    pub name: String,
    /// Raw SQL template (may contain ref/source macros).
    pub sql: String,
    /// Model configuration.
    #[serde(default)]
    pub config: ModelConfig,
    /// Description of the model.
    #[serde(default)]
    pub description: String,
    /// Column documentation.
    #[serde(default)]
    pub columns: Vec<ColumnDoc>,
}

/// Configuration options for a model.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Materialization strategy.
    #[serde(default = "default_materialization")]
    pub materialized: Materialization,
    /// Target schema/namespace.
    #[serde(default)]
    pub schema: Option<String>,
    /// Tags for organizing models.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// How a model is materialized.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Materialization {
    /// Create a view (default).
    #[default]
    View,
    /// Create a table (full refresh).
    Table,
    /// Incremental append.
    Incremental,
    /// Ephemeral (inline CTE, not materialized).
    Ephemeral,
}

fn default_materialization() -> Materialization {
    Materialization::View
}

/// Documentation for a column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDoc {
    /// Column name.
    pub name: String,
    /// Human-readable description of the column.
    pub description: String,
    /// Names of tests to run on this column (e.g., "not_null", "unique").
    #[serde(default)]
    pub tests: Vec<String>,
}
