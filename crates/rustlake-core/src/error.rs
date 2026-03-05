//! Unified error type for all RustLake crates.

use thiserror::Error;

/// The unified error type used across all RustLake crates.
///
/// Each variant carries a human-readable message. Where possible, errors
/// include context such as the table name, file path, or operation attempted.
#[derive(Error, Debug)]
pub enum RustLakeError {
    /// SQL query parsing or execution error.
    #[error("Query error: {0}")]
    Query(String),

    /// Object storage I/O error (S3, GCS, local filesystem).
    #[error("Storage error: {0}")]
    Storage(String),

    /// Catalog metadata error (table not found, namespace issues).
    #[error("Catalog error: {0}")]
    Catalog(String),

    /// Configuration parsing or validation error.
    #[error("Config error: {0}")]
    Config(String),

    /// Query engine internal error.
    #[error("Engine error: {0}")]
    Engine(String),

    /// Error from the Apache Arrow library.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Error from the DataFusion query engine.
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// Standard I/O error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    /// TOML configuration file parse error.
    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),

    /// Error from the DuckDB OLAP engine.
    #[error("DuckDB error: {0}")]
    DuckDb(String),

    /// Error from the Polars DataFrame engine.
    #[error("Polars error: {0}")]
    Polars(String),

    /// Error from a federated data provider (Postgres, MySQL, SQLite, etc.).
    #[error("Provider error: {0}")]
    Provider(String),

    /// Catch-all for errors that don't fit other variants.
    #[error("{0}")]
    Other(String),
}

/// A `Result` type alias using [`RustLakeError`] as the error type.
pub type Result<T> = std::result::Result<T, RustLakeError>;
