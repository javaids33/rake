//! Object storage abstraction for RustLake.
//!
//! Wraps the `object_store` crate to provide a unified interface over
//! local filesystem, S3, GCS, and Azure Blob Storage backends.

mod provider;

pub use provider::StorageProvider;
