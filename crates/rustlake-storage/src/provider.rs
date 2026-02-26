use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use rustlake_core::config::{StorageBackend, StorageConfig};
use rustlake_core::{Result, RustLakeError};

/// Provides access to the configured object store backend.
///
/// Constructed from a [`StorageConfig`], this struct holds an `Arc<dyn ObjectStore>`
/// and the root path prefix for all data operations.
pub struct StorageProvider {
    /// The underlying object store implementation.
    store: Arc<dyn ObjectStore>,
    /// Root path prefix within the object store.
    root: ObjectStorePath,
}

impl StorageProvider {
    /// Create a new storage provider from the given configuration.
    ///
    /// Currently supports `Local` and `S3` backends. GCS and Azure return
    /// an error until their implementations are complete.
    pub fn new(config: &StorageConfig) -> Result<Self> {
        match &config.backend {
            StorageBackend::Local { path } => {
                // Ensure the directory exists
                std::fs::create_dir_all(path).map_err(|e| {
                    RustLakeError::Storage(format!("Failed to create data dir '{}': {}", path, e))
                })?;

                let abs_path = std::fs::canonicalize(path).map_err(|e| {
                    RustLakeError::Storage(format!("Failed to resolve path '{}': {}", path, e))
                })?;

                let store = LocalFileSystem::new_with_prefix(&abs_path).map_err(|e| {
                    RustLakeError::Storage(format!("Failed to create local store: {}", e))
                })?;

                Ok(Self {
                    store: Arc::new(store),
                    root: ObjectStorePath::from(""),
                })
            }
            StorageBackend::S3 {
                bucket,
                region,
                endpoint,
            } => {
                use object_store::aws::AmazonS3Builder;

                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region);

                if let Some(ep) = endpoint {
                    builder = builder.with_endpoint(ep).with_allow_http(true);
                }

                let store = builder.build().map_err(|e| {
                    RustLakeError::Storage(format!("Failed to create S3 store: {}", e))
                })?;

                Ok(Self {
                    store: Arc::new(store),
                    root: ObjectStorePath::from(""),
                })
            }
            StorageBackend::Gcs { bucket: _ } => Err(RustLakeError::Storage(
                "GCS support not yet implemented".into(),
            )),
            StorageBackend::Azure {
                container: _,
                account: _,
            } => Err(RustLakeError::Storage(
                "Azure support not yet implemented".into(),
            )),
        }
    }

    /// Return a shared reference to the underlying `ObjectStore`.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Return the root path prefix for this storage provider.
    pub fn root_path(&self) -> &ObjectStorePath {
        &self.root
    }
}
