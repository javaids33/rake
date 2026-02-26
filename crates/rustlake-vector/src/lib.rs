//! Vector/AI layer for RustLake.
//!
//! Provides embedding generation, vector similarity search, and AI UDFs
//! that integrate with DataFusion's query engine.

pub mod embedding;
pub mod search;

use async_trait::async_trait;
use rustlake_core::Result;

// Re-export key types for convenience
pub use embedding::SimpleEmbeddingGenerator;
pub use search::{IndexSearchResult, VectorIndex};

/// Configuration for an embedding provider.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmbeddingConfig {
    /// Which embedding provider to use.
    pub provider: EmbeddingProvider,
    /// Model name/ID (e.g., "text-embedding-3-small").
    pub model: String,
    /// Number of dimensions in the output embedding vector.
    pub dimensions: usize,
}

/// Supported embedding providers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum EmbeddingProvider {
    /// OpenAI API (text-embedding-3-small, etc.)
    OpenAI { api_key_env: String },
    /// Self-hosted via Ollama.
    Ollama { endpoint: String },
    /// Local mock for testing.
    Mock,
}

/// A provider that generates embedding vectors from text.
#[async_trait]
pub trait EmbeddingGenerator: Send + Sync {
    /// Generate embeddings for a batch of text inputs.
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>;

    /// Get the dimensionality of embeddings produced by this generator.
    fn dimensions(&self) -> usize;

    /// Get the model name.
    fn model_name(&self) -> &str;
}

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine distance).
    Cosine,
    /// L2 (Euclidean) distance.
    L2,
    /// Negative dot product distance.
    DotProduct,
}
