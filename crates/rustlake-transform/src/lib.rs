//! dbt-compatible transformation layer for RustLake.
//!
//! Reads model definitions (SQL + YAML), builds dependency DAGs,
//! resolves `ref()` and `source()` macros, and compiles SQL.

pub mod compiler;
pub mod lineage;
pub mod model;

pub use compiler::SqlCompiler;
pub use lineage::LineageGraph;
pub use model::{Model, ModelConfig};
