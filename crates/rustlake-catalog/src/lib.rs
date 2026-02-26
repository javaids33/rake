pub mod memory;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use rustlake_core::Result;

/// Trait for table catalog backends.
/// Implementations can back onto in-memory maps, Iceberg REST catalogs,
/// Delta Lake logs, or any other metadata store.
#[async_trait]
pub trait TableCatalog: Send + Sync {
    /// List all table names in a namespace.
    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>>;

    /// Retrieve a DataFusion TableProvider for the given table.
    async fn get_table(&self, namespace: &str, name: &str) -> Result<Arc<dyn TableProvider>>;

    /// Register a new table backed by a TableProvider.
    async fn register_table(
        &self,
        namespace: &str,
        name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<()>;

    /// Drop a table from the catalog.
    async fn drop_table(&self, namespace: &str, name: &str) -> Result<()>;

    /// List all namespaces.
    async fn list_namespaces(&self) -> Result<Vec<String>>;
}
