//! Embedding generation — mock and simple hash-based providers for development/testing.

use async_trait::async_trait;
use rustlake_core::Result;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::EmbeddingGenerator;

/// Mock embedding generator — produces deterministic vectors for testing.
pub struct MockEmbeddingGenerator {
    dimensions: usize,
}

impl MockEmbeddingGenerator {
    /// Create a new mock generator with the given dimensionality.
    pub fn new(dimensions: usize) -> Self {
        Self { dimensions }
    }
}

#[async_trait]
impl EmbeddingGenerator for MockEmbeddingGenerator {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|text| {
                // Produce a deterministic vector based on text hash
                let hash = text
                    .bytes()
                    .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
                (0..self.dimensions)
                    .map(|i| {
                        let val = ((hash.wrapping_mul(i as u64 + 1)) % 1000) as f32 / 1000.0;
                        val * 2.0 - 1.0 // Normalize to [-1, 1]
                    })
                    .collect()
            })
            .collect())
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn model_name(&self) -> &str {
        "mock-embedding-v1"
    }
}

/// Default number of dimensions for the simple embedding generator.
const DEFAULT_DIMENSIONS: usize = 128;

/// A simple hash-based embedding generator that produces deterministic vectors.
///
/// Uses a word-overlap approach: each word contributes to specific dimensions
/// based on its hash, so texts sharing words will produce similar embeddings.
/// This is suitable for demos and testing without requiring an external ML model.
///
/// # Examples
///
/// ```
/// use rustlake_vector::embedding::SimpleEmbeddingGenerator;
///
/// let gen = SimpleEmbeddingGenerator::new(128);
/// let emb = gen.generate_embedding("red running shoes");
/// assert_eq!(emb.len(), 128);
/// ```
pub struct SimpleEmbeddingGenerator {
    dimensions: usize,
}

impl SimpleEmbeddingGenerator {
    /// Create a new simple embedding generator with the given dimensionality.
    pub fn new(dimensions: usize) -> Self {
        Self { dimensions }
    }

    /// Create a generator with the default 128 dimensions.
    pub fn default_dimensions() -> Self {
        Self {
            dimensions: DEFAULT_DIMENSIONS,
        }
    }

    /// Get the dimensionality of produced embeddings.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Hash a single word into a u64 using `DefaultHasher`.
    fn hash_word(word: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        word.hash(&mut hasher);
        hasher.finish()
    }

    /// Generate a deterministic embedding for the given text.
    ///
    /// The approach:
    /// 1. Tokenize the text into lowercase words.
    /// 2. For each word, hash it to select which dimensions to activate and
    ///    by how much (positive or negative contribution).
    /// 3. Accumulate contributions from all words.
    /// 4. L2-normalize the resulting vector so cosine similarity is meaningful.
    ///
    /// Similar texts (sharing words) will produce similar vectors.
    pub fn generate_embedding(&self, text: &str) -> Vec<f32> {
        let mut vector = vec![0.0f32; self.dimensions];
        let words = tokenize(text);

        if words.is_empty() {
            return vector;
        }

        for word in &words {
            let hash = Self::hash_word(word);

            // Each word activates multiple dimensions using different hash-derived seeds.
            // We use 8 activations per word to spread the signal.
            for activation in 0u64..8 {
                let seed = hash
                    .wrapping_mul(activation.wrapping_add(1))
                    .wrapping_add(activation);
                let dim = (seed % self.dimensions as u64) as usize;
                // Determine sign and magnitude from higher bits of the seed
                let sign_seed = seed.wrapping_mul(2654435761); // Knuth multiplicative hash
                let sign = if sign_seed % 2 == 0 { 1.0f32 } else { -1.0f32 };
                let magnitude = ((seed >> 16) % 1000) as f32 / 1000.0 * 0.5 + 0.5;
                vector[dim] += sign * magnitude;
            }

            // Also add a global "word presence" signal using char-level features.
            // This helps distinguish words with different character compositions.
            for (ci, ch) in word.chars().enumerate() {
                let char_hash = (ch as u64)
                    .wrapping_mul(7919)
                    .wrapping_add(ci as u64 * 104729);
                let dim = (char_hash % self.dimensions as u64) as usize;
                let contribution = 0.1 / (ci as f32 + 1.0);
                vector[dim] += contribution;
            }
        }

        // L2-normalize so that cosine similarity works properly
        l2_normalize(&mut vector);
        vector
    }

    /// Generate embeddings for a batch of texts.
    pub fn generate_batch(&self, texts: &[&str]) -> Vec<Vec<f32>> {
        texts.iter().map(|t| self.generate_embedding(t)).collect()
    }
}

#[async_trait]
impl EmbeddingGenerator for SimpleEmbeddingGenerator {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        Ok(self.generate_batch(texts))
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn model_name(&self) -> &str {
        "simple-hash-v1"
    }
}

/// Tokenize text into lowercase words, stripping punctuation.
fn tokenize(text: &str) -> Vec<String> {
    text.split_whitespace()
        .map(|w| {
            w.to_lowercase()
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_string()
        })
        .filter(|w| !w.is_empty())
        .collect()
}

/// L2-normalize a vector in place.
fn l2_normalize(vec: &mut [f32]) {
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-10 {
        for x in vec.iter_mut() {
            *x /= norm;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::cosine_similarity;

    #[test]
    fn test_simple_embedding_deterministic() {
        let gen = SimpleEmbeddingGenerator::new(128);
        let emb1 = gen.generate_embedding("running shoes");
        let emb2 = gen.generate_embedding("running shoes");
        assert_eq!(emb1, emb2, "Same text must produce identical embeddings");
    }

    #[test]
    fn test_simple_embedding_dimensions() {
        let gen = SimpleEmbeddingGenerator::new(64);
        let emb = gen.generate_embedding("hello world");
        assert_eq!(emb.len(), 64);
    }

    #[test]
    fn test_similar_text_higher_similarity() {
        let gen = SimpleEmbeddingGenerator::new(128);
        let shoes = gen.generate_embedding("running shoes ultralight");
        let shoes2 = gen.generate_embedding("running shoes lightweight");
        let oven = gen.generate_embedding("cast iron dutch oven");

        let sim_similar = cosine_similarity(&shoes, &shoes2);
        let sim_different = cosine_similarity(&shoes, &oven);

        assert!(
            sim_similar > sim_different,
            "Similar texts should have higher cosine similarity. similar={}, different={}",
            sim_similar,
            sim_different,
        );
    }

    #[test]
    fn test_normalized_vector() {
        let gen = SimpleEmbeddingGenerator::new(128);
        let emb = gen.generate_embedding("wireless headphones");
        let norm: f32 = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-5,
            "Embedding should be L2-normalized, got norm={}",
            norm,
        );
    }

    #[test]
    fn test_generate_batch() {
        let gen = SimpleEmbeddingGenerator::new(128);
        let texts = vec!["hello world", "foo bar"];
        let batch = gen.generate_batch(&texts);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].len(), 128);
        assert_eq!(batch[1].len(), 128);
    }

    #[test]
    fn test_empty_text() {
        let gen = SimpleEmbeddingGenerator::new(128);
        let emb = gen.generate_embedding("");
        assert_eq!(emb.len(), 128);
        // All zeros for empty text
        assert!(emb.iter().all(|x| *x == 0.0));
    }
}
