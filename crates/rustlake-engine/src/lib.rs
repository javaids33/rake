//! DataFusion-based query engine for RustLake.
//!
//! Provides [`RustLakeContext`], which wraps a DataFusion `SessionContext` and
//! adds auto-registration of file-based tables, catalog integration, and
//! configuration from [`RustLakeConfig`](rustlake_core::RustLakeConfig).

mod context;

pub use context::RustLakeContext;
