//! Core shared types, configuration, and error handling for RustLake.
//!
//! Every other crate in the workspace depends on `rustlake-core`. It provides
//! the [`RustLakeConfig`] configuration struct, the [`RustLakeError`] error enum,
//! and common Arrow/DataFusion re-exports.

pub mod config;
pub mod error;

pub use config::RustLakeConfig;
pub use error::{Result, RustLakeError};
