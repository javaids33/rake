use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use rustlake_core::{Result, RustLakeError};
use tokio::sync::RwLock;

use crate::TableCatalog;

/// Table storage: namespace -> (table_name -> provider).
type NamespaceMap = HashMap<String, HashMap<String, Arc<dyn TableProvider>>>;

/// In-memory catalog for development, testing, and single-node deployments.
/// Tables are registered dynamically and lost on restart.
pub struct MemoryCatalog {
    tables: RwLock<NamespaceMap>,
}

impl MemoryCatalog {
    /// Create a new empty in-memory catalog with no namespaces or tables.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableCatalog for MemoryCatalog {
    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let tables = self.tables.read().await;
        Ok(tables
            .get(namespace)
            .map(|ns| ns.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn get_table(&self, namespace: &str, name: &str) -> Result<Arc<dyn TableProvider>> {
        let tables = self.tables.read().await;
        tables
            .get(namespace)
            .and_then(|ns| ns.get(name))
            .cloned()
            .ok_or_else(|| {
                RustLakeError::Catalog(format!("Table '{}.{}' not found", namespace, name))
            })
    }

    async fn register_table(
        &self,
        namespace: &str,
        name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<()> {
        let mut tables = self.tables.write().await;
        tables
            .entry(namespace.to_string())
            .or_default()
            .insert(name.to_string(), provider);
        tracing::info!(namespace, name, "Registered table");
        Ok(())
    }

    async fn drop_table(&self, namespace: &str, name: &str) -> Result<()> {
        let mut tables = self.tables.write().await;
        if let Some(ns) = tables.get_mut(namespace) {
            ns.remove(name);
        }
        tracing::info!(namespace, name, "Dropped table");
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let tables = self.tables.read().await;
        Ok(tables.keys().cloned().collect())
    }
}
