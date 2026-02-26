//! Query classification and workload routing for RustLake.
//!
//! Analyzes incoming SQL using DataFusion's parser to inspect the AST,
//! classifies the workload type, and routes to the optimal execution engine.

mod classifier;

pub use classifier::{QueryClassifier, QueryType};
