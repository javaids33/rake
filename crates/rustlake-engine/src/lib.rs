//! DataFusion-based query engine for RustLake.
//!
//! Provides [`RustLakeContext`], which wraps a DataFusion `SessionContext` and
//! adds auto-registration of file-based tables, catalog integration, and
//! configuration from [`RustLakeConfig`](rustlake_core::RustLakeConfig).

mod context;
#[cfg(feature = "duckdb")]
pub mod duckdb_engine;
#[cfg(feature = "polars")]
pub mod polars_engine;

pub use context::RustLakeContext;
