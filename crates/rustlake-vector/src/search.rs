//! Vector similarity search.

use crate::DistanceMetric;

/// Result of a brute-force vector similarity search (index-based).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchResult {
    /// Index of the matched vector in the original dataset.
    pub index: usize,
    /// Distance/similarity score.
    pub score: f32,
}

/// A rich search result returned by `VectorIndex::search`, including metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexSearchResult {
    /// Unique identifier of the document.
    pub id: String,
    /// Original text that was indexed.
    pub text: String,
    /// Cosine similarity score (higher is more similar, range [-1, 1]).
    pub score: f64,
    /// Arbitrary metadata associated with the document.
    pub metadata: serde_json::Value,
}

/// An entry stored in the vector index.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexEntry {
    /// Unique identifier for this document.
    pub id: String,
    /// Original text that was embedded.
    pub text: String,
    /// The embedding vector.
    pub embedding: Vec<f32>,
    /// Arbitrary metadata (e.g., category, price, source).
    pub metadata: serde_json::Value,
}

/// In-memory vector index that supports adding documents and searching by
/// cosine similarity.
///
/// This is a brute-force implementation suitable for small-to-medium datasets
/// (up to ~100k documents). For production use at scale, this should be
/// replaced by an approximate nearest neighbor index (IVF-PQ, HNSW) via LanceDB.
///
/// # Examples
///
/// ```
/// use rustlake_vector::search::VectorIndex;
///
/// let mut index = VectorIndex::new(3);
/// index.add("doc1".into(), "hello world".into(), vec![1.0, 0.0, 0.0], serde_json::json!({}));
/// index.add("doc2".into(), "goodbye world".into(), vec![0.9, 0.1, 0.0], serde_json::json!({}));
///
/// let results = index.search(&[1.0, 0.0, 0.0], 1);
/// assert_eq!(results[0].id, "doc1");
/// ```
pub struct VectorIndex {
    /// Dimensionality of the vectors in this index.
    dimensions: usize,
    /// All indexed entries.
    entries: Vec<IndexEntry>,
}

impl VectorIndex {
    /// Create a new empty vector index with the given dimensionality.
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            entries: Vec::new(),
        }
    }

    /// Add a document to the index.
    ///
    /// # Panics
    ///
    /// Panics if the embedding dimensionality does not match the index.
    pub fn add(
        &mut self,
        id: String,
        text: String,
        embedding: Vec<f32>,
        metadata: serde_json::Value,
    ) {
        assert_eq!(
            embedding.len(),
            self.dimensions,
            "Embedding dimensions mismatch: expected {}, got {}",
            self.dimensions,
            embedding.len(),
        );
        self.entries.push(IndexEntry {
            id,
            text,
            embedding,
            metadata,
        });
    }

    /// Search the index for the `k` most similar documents to the query embedding.
    ///
    /// Returns results sorted by descending cosine similarity score.
    pub fn search(&self, query_embedding: &[f32], k: usize) -> Vec<IndexSearchResult> {
        assert_eq!(
            query_embedding.len(),
            self.dimensions,
            "Query embedding dimensions mismatch: expected {}, got {}",
            self.dimensions,
            query_embedding.len(),
        );

        let mut scored: Vec<(usize, f64)> = self
            .entries
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let sim = cosine_similarity(query_embedding, &entry.embedding) as f64;
                (i, sim)
            })
            .collect();

        // Sort by descending similarity
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);

        scored
            .into_iter()
            .map(|(i, score)| {
                let entry = &self.entries[i];
                IndexSearchResult {
                    id: entry.id.clone(),
                    text: entry.text.clone(),
                    score,
                    metadata: entry.metadata.clone(),
                }
            })
            .collect()
    }

    /// Return the number of indexed documents.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the dimensionality of the index.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
}

// ── Standalone distance functions ─────────────────────────────────────

/// Compute cosine similarity between two vectors.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot / (norm_a * norm_b)
}

/// Compute L2 (Euclidean) distance between two vectors.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vector dimensions must match");
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Brute-force k-nearest-neighbor search.
///
/// For production use, this should be replaced by an index-backed search
/// (IVF-PQ, HNSW) via LanceDB.
pub fn knn_search(
    query: &[f32],
    vectors: &[Vec<f32>],
    k: usize,
    metric: DistanceMetric,
) -> Vec<SearchResult> {
    let mut results: Vec<SearchResult> = vectors
        .iter()
        .enumerate()
        .map(|(i, vec)| {
            let score = match metric {
                DistanceMetric::Cosine => 1.0 - cosine_similarity(query, vec),
                DistanceMetric::L2 => l2_distance(query, vec),
                DistanceMetric::DotProduct => -query
                    .iter()
                    .zip(vec.iter())
                    .map(|(a, b)| a * b)
                    .sum::<f32>(),
            };
            SearchResult { index: i, score }
        })
        .collect();

    results.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(k);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert!((cosine_similarity(&a, &b)).abs() < 1e-6);
    }

    #[test]
    fn test_knn_search() {
        let query = vec![1.0, 0.0, 0.0];
        let vectors = vec![
            vec![1.0, 0.0, 0.0],  // identical
            vec![0.9, 0.1, 0.0],  // very close
            vec![0.0, 1.0, 0.0],  // orthogonal
            vec![-1.0, 0.0, 0.0], // opposite
        ];

        let results = knn_search(&query, &vectors, 2, DistanceMetric::Cosine);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].index, 0); // identical should be first
        assert_eq!(results[1].index, 1); // close should be second
    }

    #[test]
    fn test_vector_index_add_and_search() {
        let mut index = VectorIndex::new(3);
        index.add(
            "a".into(),
            "red shoes".into(),
            vec![1.0, 0.0, 0.0],
            serde_json::json!({"category": "footwear"}),
        );
        index.add(
            "b".into(),
            "blue shoes".into(),
            vec![0.9, 0.1, 0.0],
            serde_json::json!({"category": "footwear"}),
        );
        index.add(
            "c".into(),
            "green hat".into(),
            vec![0.0, 0.0, 1.0],
            serde_json::json!({"category": "headwear"}),
        );

        assert_eq!(index.len(), 3);
        assert_eq!(index.dimensions(), 3);

        let results = index.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "a"); // most similar
        assert_eq!(results[1].id, "b"); // second most similar
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn test_vector_index_empty() {
        let index = VectorIndex::new(3);
        assert!(index.is_empty());
        let results = index.search(&[1.0, 0.0, 0.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_vector_index_k_larger_than_entries() {
        let mut index = VectorIndex::new(2);
        index.add(
            "a".into(),
            "foo".into(),
            vec![1.0, 0.0],
            serde_json::json!({}),
        );
        let results = index.search(&[1.0, 0.0], 10);
        assert_eq!(results.len(), 1);
    }
}
