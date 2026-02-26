//! DAG-based workflow orchestration for RustLake.
//!
//! Provides a [`DagScheduler`] that executes tasks in topological order,
//! supporting dependencies between SQL transformations, ingestion jobs,
//! and other pipeline steps.

pub mod dag;

pub use dag::{DagScheduler, TaskDef, TaskStatus};
