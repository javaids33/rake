//! Server-side notebook execution engine.
//!
//! Runs notebook cells sequentially, building a DAG from cell dependencies.
//! Supports scheduling notebooks as ETL jobs with optimized execution plans.

use serde::{Deserialize, Serialize};

/// A notebook submitted for server-side execution.
#[derive(Debug, Clone, Deserialize)]
pub struct NotebookSubmission {
    pub id: String,
    pub name: String,
    pub cells: Vec<CellSubmission>,
}

/// A single cell in a submitted notebook.
#[derive(Debug, Clone, Deserialize)]
pub struct CellSubmission {
    pub id: String,
    #[serde(rename = "type")]
    pub cell_type: String, // "sql", "python", "rust", "markdown"
    pub source: String,
    pub depends_on: Vec<String>, // cell IDs this cell depends on
}

/// Result of executing a full notebook.
#[derive(Debug, Clone, Serialize)]
pub struct NotebookRunResult {
    pub notebook_id: String,
    pub notebook_name: String,
    pub status: String, // "success", "partial", "failed"
    pub cell_results: Vec<CellRunResult>,
    pub total_duration_ms: u64,
    pub dag_order: Vec<String>, // cell IDs in execution order
    pub optimizations_applied: Vec<String>,
}

/// Result of executing a single cell.
#[derive(Debug, Clone, Serialize)]
pub struct CellRunResult {
    pub cell_id: String,
    pub cell_type: String,
    pub status: String, // "success", "skipped", "failed"
    pub duration_ms: u64,
    pub row_count: Option<usize>,
    pub error: Option<String>,
    pub output_preview: Option<String>,
}

/// An optimized execution plan for a notebook.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub stages: Vec<ExecutionStage>,
    pub total_cells: usize,
    pub parallelizable_cells: usize,
    pub estimated_duration_ms: u64,
    pub optimizations: Vec<Optimization>,
}

/// A stage in the execution plan (cells in same stage can run in parallel).
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionStage {
    pub stage_id: usize,
    pub cell_ids: Vec<String>,
    pub cell_types: Vec<String>,
    pub can_parallelize: bool,
    pub estimated_ms: u64,
}

/// An optimization suggestion.
#[derive(Debug, Clone, Serialize)]
pub struct Optimization {
    pub optimization_type: String, // "merge_sql", "cache_result", "parallelize", "skip_markdown"
    pub description: String,
    pub cells_affected: Vec<String>,
    pub estimated_speedup_ms: u64,
}

/// A notebook scheduled as an ETL job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotebookJob {
    pub job_id: String,
    pub notebook_id: String,
    pub notebook_name: String,
    pub schedule: String, // cron expression
    pub enabled: bool,
    pub last_run: Option<String>,
    pub last_status: Option<String>,
    pub last_duration_ms: Option<u64>,
    pub created_at: String,
    /// Cells to execute (empty = all cells)
    pub cells_to_run: Vec<String>,
    /// Optimization level: "none", "basic", "aggressive"
    pub optimization_level: String,
    /// Engine preference: "auto", "datafusion", "duckdb"
    pub engine: String,
    /// Tags for filtering
    pub tags: Vec<String>,
}

/// Build an execution plan from notebook cells.
pub fn build_execution_plan(notebook: &NotebookSubmission) -> ExecutionPlan {
    let mut stages: Vec<ExecutionStage> = Vec::new();
    let mut executed: Vec<String> = Vec::new();
    let mut remaining: Vec<&CellSubmission> = notebook.cells.iter().collect();
    let mut stage_id = 0;
    let mut optimizations = Vec::new();

    // Skip markdown cells
    let markdown_cells: Vec<String> = remaining
        .iter()
        .filter(|c| c.cell_type == "markdown")
        .map(|c| c.id.clone())
        .collect();
    if !markdown_cells.is_empty() {
        optimizations.push(Optimization {
            optimization_type: "skip_markdown".to_string(),
            description: format!(
                "Skipping {} markdown cells (documentation only)",
                markdown_cells.len()
            ),
            cells_affected: markdown_cells,
            estimated_speedup_ms: 0,
        });
        remaining.retain(|c| c.cell_type != "markdown");
    }

    // Check for consecutive SQL cells that could be merged
    let mut consecutive_sql: Vec<Vec<String>> = Vec::new();
    let mut current_run: Vec<String> = Vec::new();
    for cell in &notebook.cells {
        if cell.cell_type == "sql" {
            current_run.push(cell.id.clone());
        } else {
            if current_run.len() >= 2 {
                consecutive_sql.push(current_run.clone());
            }
            current_run.clear();
        }
    }
    if current_run.len() >= 2 {
        consecutive_sql.push(current_run);
    }
    for run in &consecutive_sql {
        optimizations.push(Optimization {
            optimization_type: "merge_sql".to_string(),
            description: format!(
                "Could merge {} consecutive SQL cells into a multi-statement batch",
                run.len()
            ),
            cells_affected: run.clone(),
            estimated_speedup_ms: (run.len() as u64 - 1) * 50, // save ~50ms per merged cell
        });
    }

    // Build stages using topological sort
    while !remaining.is_empty() {
        let ready: Vec<&CellSubmission> = remaining
            .iter()
            .filter(|c| c.depends_on.iter().all(|dep| executed.contains(dep)))
            .cloned()
            .collect();

        if ready.is_empty() {
            // No cells ready — remaining cells have unresolvable dependencies
            // Add them all to a final stage
            let ids: Vec<String> = remaining.iter().map(|c| c.id.clone()).collect();
            let types: Vec<String> = remaining.iter().map(|c| c.cell_type.clone()).collect();
            stages.push(ExecutionStage {
                stage_id,
                cell_ids: ids,
                cell_types: types,
                can_parallelize: false,
                estimated_ms: remaining.len() as u64 * 200,
            });
            break;
        }

        let can_parallelize = ready.len() > 1 && ready.iter().all(|c| c.cell_type == "sql");
        if can_parallelize {
            optimizations.push(Optimization {
                optimization_type: "parallelize".to_string(),
                description: format!(
                    "Stage {} has {} independent SQL cells that can run in parallel",
                    stage_id,
                    ready.len()
                ),
                cells_affected: ready.iter().map(|c| c.id.clone()).collect(),
                estimated_speedup_ms: (ready.len() as u64 - 1) * 100,
            });
        }

        let ids: Vec<String> = ready.iter().map(|c| c.id.clone()).collect();
        let types: Vec<String> = ready.iter().map(|c| c.cell_type.clone()).collect();
        let estimated_ms = ready
            .iter()
            .map(|c| match c.cell_type.as_str() {
                "sql" => 100,
                "rust" => 1500,
                "python" => 500,
                _ => 50,
            })
            .max()
            .unwrap_or(100); // parallel = max, not sum

        stages.push(ExecutionStage {
            stage_id,
            cell_ids: ids.clone(),
            cell_types: types,
            can_parallelize,
            estimated_ms,
        });

        for id in &ids {
            executed.push(id.clone());
        }
        remaining.retain(|c| !ids.contains(&c.id));
        stage_id += 1;
    }

    let parallelizable_cells = stages
        .iter()
        .filter(|s| s.can_parallelize)
        .map(|s| s.cell_ids.len())
        .sum();

    let estimated_duration_ms = stages.iter().map(|s| s.estimated_ms).sum();

    ExecutionPlan {
        total_cells: notebook.cells.len(),
        parallelizable_cells,
        estimated_duration_ms,
        stages,
        optimizations,
    }
}

/// Execute a notebook server-side (runs SQL cells via DataFusion).
pub async fn execute_notebook(
    notebook: &NotebookSubmission,
    ctx: &tokio::sync::RwLock<rustlake_engine::RustLakeContext>,
) -> NotebookRunResult {
    let start = std::time::Instant::now();
    let plan = build_execution_plan(notebook);
    let mut cell_results = Vec::new();
    let mut dag_order = Vec::new();
    let mut all_success = true;

    let optimizations_applied: Vec<String> =
        plan.optimizations.iter().map(|o| o.description.clone()).collect();

    for stage in &plan.stages {
        for cell_id in &stage.cell_ids {
            dag_order.push(cell_id.clone());

            let cell = match notebook.cells.iter().find(|c| c.id == *cell_id) {
                Some(c) => c,
                None => continue,
            };

            if cell.cell_type == "markdown" {
                cell_results.push(CellRunResult {
                    cell_id: cell_id.clone(),
                    cell_type: "markdown".to_string(),
                    status: "skipped".to_string(),
                    duration_ms: 0,
                    row_count: None,
                    error: None,
                    output_preview: None,
                });
                continue;
            }

            let cell_start = std::time::Instant::now();

            if cell.cell_type == "sql" {
                let context = ctx.read().await;
                match context.sql(&cell.source).await {
                    Ok(batches) => {
                        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                        cell_results.push(CellRunResult {
                            cell_id: cell_id.clone(),
                            cell_type: "sql".to_string(),
                            status: "success".to_string(),
                            duration_ms: cell_start.elapsed().as_millis() as u64,
                            row_count: Some(row_count),
                            error: None,
                            output_preview: Some(format!("{} rows returned", row_count)),
                        });
                    }
                    Err(e) => {
                        all_success = false;
                        cell_results.push(CellRunResult {
                            cell_id: cell_id.clone(),
                            cell_type: "sql".to_string(),
                            status: "failed".to_string(),
                            duration_ms: cell_start.elapsed().as_millis() as u64,
                            row_count: None,
                            error: Some(e.to_string()),
                            output_preview: None,
                        });
                    }
                }
            } else if cell.cell_type == "rust" {
                let result = crate::rust_executor::execute_rust(&cell.source).await;
                if result.success {
                    cell_results.push(CellRunResult {
                        cell_id: cell_id.clone(),
                        cell_type: "rust".to_string(),
                        status: "success".to_string(),
                        duration_ms: result.duration_ms,
                        row_count: None,
                        error: None,
                        output_preview: Some(result.stdout.chars().take(200).collect()),
                    });
                } else {
                    all_success = false;
                    cell_results.push(CellRunResult {
                        cell_id: cell_id.clone(),
                        cell_type: "rust".to_string(),
                        status: "failed".to_string(),
                        duration_ms: result.duration_ms,
                        row_count: None,
                        error: result.error,
                        output_preview: None,
                    });
                }
            } else {
                // Python and other types — skip for now
                cell_results.push(CellRunResult {
                    cell_id: cell_id.clone(),
                    cell_type: cell.cell_type.clone(),
                    status: "skipped".to_string(),
                    duration_ms: 0,
                    row_count: None,
                    error: None,
                    output_preview: Some(
                        "Server-side execution not available for this cell type".to_string(),
                    ),
                });
            }
        }
    }

    NotebookRunResult {
        notebook_id: notebook.id.clone(),
        notebook_name: notebook.name.clone(),
        status: if all_success { "success" } else { "partial" }.to_string(),
        cell_results,
        total_duration_ms: start.elapsed().as_millis() as u64,
        dag_order,
        optimizations_applied,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_plan_linear() {
        let nb = NotebookSubmission {
            id: "nb1".into(),
            name: "Test".into(),
            cells: vec![
                CellSubmission {
                    id: "c1".into(),
                    cell_type: "sql".into(),
                    source: "SELECT 1".into(),
                    depends_on: vec![],
                },
                CellSubmission {
                    id: "c2".into(),
                    cell_type: "sql".into(),
                    source: "SELECT 2".into(),
                    depends_on: vec!["c1".into()],
                },
            ],
        };
        let plan = build_execution_plan(&nb);
        assert_eq!(plan.stages.len(), 2);
        assert_eq!(plan.stages[0].cell_ids, vec!["c1"]);
        assert_eq!(plan.stages[1].cell_ids, vec!["c2"]);
    }

    #[test]
    fn test_build_plan_parallel() {
        let nb = NotebookSubmission {
            id: "nb1".into(),
            name: "Test".into(),
            cells: vec![
                CellSubmission {
                    id: "c1".into(),
                    cell_type: "sql".into(),
                    source: "SELECT 1".into(),
                    depends_on: vec![],
                },
                CellSubmission {
                    id: "c2".into(),
                    cell_type: "sql".into(),
                    source: "SELECT 2".into(),
                    depends_on: vec![],
                },
                CellSubmission {
                    id: "c3".into(),
                    cell_type: "sql".into(),
                    source: "SELECT 3".into(),
                    depends_on: vec!["c1".into(), "c2".into()],
                },
            ],
        };
        let plan = build_execution_plan(&nb);
        assert_eq!(plan.stages.len(), 2);
        assert!(plan.stages[0].can_parallelize);
        assert_eq!(plan.stages[0].cell_ids.len(), 2); // c1 and c2 in parallel
        assert_eq!(plan.stages[1].cell_ids, vec!["c3"]);
    }

    #[test]
    fn test_skip_markdown() {
        let nb = NotebookSubmission {
            id: "nb1".into(),
            name: "Test".into(),
            cells: vec![
                CellSubmission {
                    id: "c1".into(),
                    cell_type: "markdown".into(),
                    source: "# Title".into(),
                    depends_on: vec![],
                },
                CellSubmission {
                    id: "c2".into(),
                    cell_type: "sql".into(),
                    source: "SELECT 1".into(),
                    depends_on: vec![],
                },
            ],
        };
        let plan = build_execution_plan(&nb);
        assert!(plan
            .optimizations
            .iter()
            .any(|o| o.optimization_type == "skip_markdown"));
    }
}
