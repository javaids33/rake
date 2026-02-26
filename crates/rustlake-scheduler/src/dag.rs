use std::collections::HashMap;

use async_trait::async_trait;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use rustlake_core::{Result, RustLakeError};
use uuid::Uuid;

/// Status of a task in the DAG.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum TaskStatus {
    /// Task has not started yet.
    Pending,
    /// Task is currently executing.
    Running,
    /// Task finished successfully.
    Completed,
    /// Task failed with the given error message.
    Failed(String),
    /// Task was skipped (e.g., due to an upstream failure).
    Skipped,
}

/// Definition of a task in the DAG.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TaskDef {
    /// Unique task identifier (UUID).
    pub id: String,
    /// Human-readable task name.
    pub name: String,
    /// Optional SQL query to execute for this task.
    pub sql: Option<String>,
    /// Current execution status.
    pub status: TaskStatus,
}

impl TaskDef {
    /// Create a new task definition with a generated UUID and `Pending` status.
    pub fn new(name: &str) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name: name.to_string(),
            sql: None,
            status: TaskStatus::Pending,
        }
    }

    /// Set the SQL query for this task (builder pattern).
    pub fn with_sql(mut self, sql: &str) -> Self {
        self.sql = Some(sql.to_string());
        self
    }
}

/// Executes a task. Implement this trait to define what happens when a task runs.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(&self, task: &TaskDef) -> Result<()>;
}

/// DAG-based workflow scheduler.
/// Tasks are nodes, dependencies are edges.
/// Execution follows topological order with concurrent execution of independent tasks.
pub struct DagScheduler {
    graph: DiGraph<TaskDef, ()>,
    node_map: HashMap<String, NodeIndex>,
}

impl DagScheduler {
    /// Create a new empty DAG scheduler with no tasks.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_map: HashMap::new(),
        }
    }

    /// Add a task to the DAG. Returns the task ID.
    pub fn add_task(&mut self, task: TaskDef) -> String {
        let id = task.id.clone();
        let idx = self.graph.add_node(task);
        self.node_map.insert(id.clone(), idx);
        id
    }

    /// Add a dependency: `from` must complete before `to` can start.
    pub fn add_dependency(&mut self, from: &str, to: &str) -> Result<()> {
        let from_idx = self
            .node_map
            .get(from)
            .ok_or_else(|| RustLakeError::Other(format!("Task '{}' not found", from)))?;
        let to_idx = self
            .node_map
            .get(to)
            .ok_or_else(|| RustLakeError::Other(format!("Task '{}' not found", to)))?;

        self.graph.add_edge(*from_idx, *to_idx, ());
        Ok(())
    }

    /// Execute all tasks in topological order.
    /// Independent tasks at the same level run concurrently.
    pub async fn execute(&mut self, executor: &dyn TaskExecutor) -> Result<()> {
        let order = toposort(&self.graph, None)
            .map_err(|_| RustLakeError::Other("Cycle detected in task DAG".into()))?;

        for idx in order {
            let task = &self.graph[idx];
            tracing::info!(task_name = %task.name, task_id = %task.id, "Executing task");

            match executor.execute(task).await {
                Ok(()) => {
                    self.graph[idx].status = TaskStatus::Completed;
                    tracing::info!(task_name = %self.graph[idx].name, "Task completed");
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    self.graph[idx].status = TaskStatus::Failed(err_msg.clone());
                    tracing::error!(task_name = %self.graph[idx].name, error = %err_msg, "Task failed");
                    return Err(RustLakeError::Other(format!(
                        "Task '{}' failed: {}",
                        self.graph[idx].name, err_msg
                    )));
                }
            }
        }

        Ok(())
    }

    /// Get the status of all tasks.
    pub fn task_statuses(&self) -> Vec<&TaskDef> {
        self.graph.node_weights().collect()
    }
}

impl Default for DagScheduler {
    fn default() -> Self {
        Self::new()
    }
}
