//! Query classification and workload routing for RustLake.
//!
//! Analyzes incoming SQL using DataFusion's parser to inspect the AST,
//! classifies the workload type, and routes to the optimal execution engine.
//! The [`cost_model`] module provides cost-based estimation to choose the
//! fastest engine for a given query profile.

mod classifier;
pub mod cost_model;
pub mod profiler;

pub use classifier::{ClassificationResult, EngineTarget, QueryClassifier, QueryType};
pub use cost_model::{CostEstimate, CostModel, EngineBaseline, FragmentEstimate, SplitPlanEstimate};
pub use profiler::{
    AdaptiveQueryProfile, ColumnProfile, EngineRecommendation, ExecutionRecord,
    ExecutionStrategy, PlanFragment, QueryProfile, QueryProfiler, SourceType,
    TableProfileStats, TableReference, hash_sql,
};
