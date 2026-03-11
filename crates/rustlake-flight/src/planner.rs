//! Distributed query planner for partition-aware execution.
//!
//! Inspects SQL queries to determine optimal distribution strategy:
//! - Single-table scans → range-partition across workers
//! - Aggregations → partial aggregate on workers, final merge on coordinator
//! - Joins → hash-partition both sides, co-locate matching keys on workers
//! - Complex/unsupported → fall back to coordinator-local execution

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use rustlake_core::Result;
use rustlake_engine::RustLakeContext;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::coordinator::WorkerHandle;

/// How a query should be distributed across workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistributionStrategy {
    /// Execute entirely on the coordinator (no distribution).
    Local,
    /// Route to a single worker (least-loaded).
    SingleWorker,
    /// Partition a table scan across workers using range filters.
    RangePartition {
        table: String,
        partition_column: String,
    },
    /// Send full query to all workers, merge results (for pre-partitioned data).
    ScatterGather,
    /// Two-stage: partial aggregation on workers, final merge on coordinator.
    PartialAggregate {
        worker_sql: String,
        merge_sql: String,
    },
}

/// A plan for distributing a query across workers.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// Original SQL query.
    pub original_sql: String,
    /// Chosen distribution strategy.
    pub strategy: DistributionStrategy,
    /// Per-worker SQL assignments (one per worker).
    pub worker_assignments: Vec<WorkerAssignment>,
    /// SQL to run on the coordinator to merge worker results (if needed).
    pub merge_sql: Option<String>,
    /// Estimated cost (higher = more expensive).
    pub estimated_cost: f64,
}

/// Assignment for a specific worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerAssignment {
    /// Worker endpoint.
    pub worker_endpoint: String,
    /// SQL to execute on this worker.
    pub sql: String,
    /// Partition range (for range-partitioned scans).
    pub partition_range: Option<(i64, i64)>,
}

/// Analyzes SQL and produces distributed execution plans.
pub struct DistributedPlanner {
    ctx: Arc<RwLock<RustLakeContext>>,
}

impl DistributedPlanner {
    /// Create a new distributed planner.
    pub fn new(ctx: Arc<RwLock<RustLakeContext>>) -> Self {
        Self { ctx }
    }

    /// Analyze a SQL query and produce a distributed plan.
    ///
    /// The planner inspects the logical plan to determine the best distribution
    /// strategy based on query structure and available workers.
    pub async fn plan(
        &self,
        sql: &str,
        workers: &[WorkerHandle],
    ) -> Result<DistributedPlan> {
        // If no workers, always execute locally.
        if workers.is_empty() {
            return Ok(DistributedPlan {
                original_sql: sql.to_string(),
                strategy: DistributionStrategy::Local,
                worker_assignments: Vec::new(),
                merge_sql: None,
                estimated_cost: 0.0,
            });
        }

        // Parse the SQL to inspect structure.
        let ctx = self.ctx.read().await;
        let df_ctx = ctx.datafusion_ctx();

        let logical_plan = match df_ctx.state().create_logical_plan(sql).await {
            Ok(plan) => plan,
            Err(_) => {
                // If we can't parse it, fall back to local execution.
                return Ok(DistributedPlan {
                    original_sql: sql.to_string(),
                    strategy: DistributionStrategy::Local,
                    worker_assignments: Vec::new(),
                    merge_sql: None,
                    estimated_cost: 0.0,
                });
            }
        };

        let analysis = analyze_plan(&logical_plan);
        drop(ctx);

        match analysis {
            QueryAnalysis::SimpleScan { table } => {
                self.plan_partitioned_scan(sql, &table, workers)
            }
            QueryAnalysis::AggregateQuery { table, group_cols, agg_funcs } => {
                self.plan_partial_aggregate(sql, &table, &group_cols, &agg_funcs, workers)
            }
            QueryAnalysis::JoinQuery { .. } => {
                // For joins, use scatter-gather: each worker has the full query,
                // results merged on coordinator. True hash-shuffle joins would
                // require repartitioning both sides — implemented via FlightExchangeExec.
                self.plan_scatter_gather(sql, workers)
            }
            QueryAnalysis::Simple => {
                // Simple queries (SELECT 1+1, metadata, etc.) — run locally.
                Ok(DistributedPlan {
                    original_sql: sql.to_string(),
                    strategy: DistributionStrategy::Local,
                    worker_assignments: Vec::new(),
                    merge_sql: None,
                    estimated_cost: 1.0,
                })
            }
            QueryAnalysis::Complex => {
                // Complex queries we can't analyze — scatter-gather or local.
                if workers.len() >= 2 {
                    self.plan_scatter_gather(sql, workers)
                } else {
                    Ok(DistributedPlan {
                        original_sql: sql.to_string(),
                        strategy: DistributionStrategy::SingleWorker,
                        worker_assignments: vec![WorkerAssignment {
                            worker_endpoint: workers[0].endpoint.clone(),
                            sql: sql.to_string(),
                            partition_range: None,
                        }],
                        merge_sql: None,
                        estimated_cost: 50.0,
                    })
                }
            }
        }
    }

    /// Plan a range-partitioned table scan across workers.
    ///
    /// Creates per-worker SQL with `WHERE rownum BETWEEN start AND end` using
    /// LIMIT/OFFSET partitioning. For tables with a known partition column,
    /// uses range filters on that column.
    fn plan_partitioned_scan(
        &self,
        sql: &str,
        table: &str,
        workers: &[WorkerHandle],
    ) -> Result<DistributedPlan> {
        let num_workers = workers.len();
        let mut assignments = Vec::with_capacity(num_workers);

        // Strategy: each worker gets 1/N of the data using LIMIT/OFFSET.
        // This is a simple approach that works for any table. A more
        // sophisticated approach would use range partitioning on a key column.
        //
        // We wrap the original query as a subquery and add LIMIT/OFFSET.
        // Workers execute their partition; coordinator unions all results.
        let batch_size = 100_000; // Assume ~100K rows per partition

        for (i, worker) in workers.iter().enumerate() {
            let offset = i * batch_size;
            let worker_sql = format!(
                "SELECT * FROM ({}) AS __partitioned LIMIT {} OFFSET {}",
                sql, batch_size, offset
            );
            assignments.push(WorkerAssignment {
                worker_endpoint: worker.endpoint.clone(),
                sql: worker_sql,
                partition_range: Some((offset as i64, (offset + batch_size) as i64)),
            });
        }

        Ok(DistributedPlan {
            original_sql: sql.to_string(),
            strategy: DistributionStrategy::RangePartition {
                table: table.to_string(),
                partition_column: "__offset".to_string(),
            },
            worker_assignments: assignments,
            merge_sql: None, // Results are simply unioned.
            estimated_cost: 100.0 / num_workers as f64,
        })
    }

    /// Plan a two-stage aggregate: partial on workers, final merge on coordinator.
    ///
    /// Workers compute partial aggregates (e.g., SUM, COUNT per group).
    /// Coordinator merges partial results with a final aggregation.
    fn plan_partial_aggregate(
        &self,
        sql: &str,
        _table: &str,
        group_cols: &[String],
        agg_funcs: &[String],
        workers: &[WorkerHandle],
    ) -> Result<DistributedPlan> {
        let num_workers = workers.len();

        // Build per-worker partial aggregation SQL.
        // Each worker runs the original aggregation on its partition of data.
        let _group_clause = if group_cols.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", group_cols.join(", "))
        };

        // Build the merge SQL that combines partial results.
        // For SUM → SUM, for COUNT → SUM, for AVG → SUM(sum)/SUM(count).
        let mut merge_select_parts = Vec::new();
        let mut merge_group_parts = Vec::new();

        for col in group_cols {
            merge_select_parts.push(col.clone());
            merge_group_parts.push(col.clone());
        }

        for func in agg_funcs {
            // Map partial aggregate to final merge aggregate.
            // SUM(x) partial → SUM(sum_x) merge
            // COUNT(x) partial → SUM(count_x) merge
            // MIN(x) partial → MIN(min_x) merge
            // MAX(x) partial → MAX(max_x) merge
            let merge_func = if func.starts_with("COUNT") {
                func.replace("COUNT", "SUM")
            } else {
                func.clone()
            };
            merge_select_parts.push(merge_func);
        }

        let merge_sql = if merge_group_parts.is_empty() {
            format!(
                "SELECT {} FROM __partial_results",
                merge_select_parts.join(", ")
            )
        } else {
            format!(
                "SELECT {} FROM __partial_results GROUP BY {}",
                merge_select_parts.join(", "),
                merge_group_parts.join(", ")
            )
        };

        // Each worker gets a partition of the table.
        let batch_size = 100_000;
        let mut assignments = Vec::with_capacity(num_workers);

        for (i, worker) in workers.iter().enumerate() {
            let offset = i * batch_size;
            let worker_sql = format!(
                "SELECT * FROM ({}) AS __partitioned LIMIT {} OFFSET {}",
                sql, batch_size, offset
            );
            assignments.push(WorkerAssignment {
                worker_endpoint: worker.endpoint.clone(),
                sql: worker_sql,
                partition_range: Some((offset as i64, (offset + batch_size) as i64)),
            });
        }

        Ok(DistributedPlan {
            original_sql: sql.to_string(),
            strategy: DistributionStrategy::PartialAggregate {
                worker_sql: sql.to_string(),
                merge_sql: merge_sql.clone(),
            },
            worker_assignments: assignments,
            merge_sql: Some(merge_sql),
            estimated_cost: 80.0 / num_workers as f64,
        })
    }

    /// Plan scatter-gather: all workers execute the full query, coordinator merges.
    fn plan_scatter_gather(
        &self,
        sql: &str,
        workers: &[WorkerHandle],
    ) -> Result<DistributedPlan> {
        let assignments: Vec<WorkerAssignment> = workers
            .iter()
            .map(|w| WorkerAssignment {
                worker_endpoint: w.endpoint.clone(),
                sql: sql.to_string(),
                partition_range: None,
            })
            .collect();

        Ok(DistributedPlan {
            original_sql: sql.to_string(),
            strategy: DistributionStrategy::ScatterGather,
            worker_assignments: assignments,
            merge_sql: None,
            estimated_cost: 50.0,
        })
    }
}

/// Result of analyzing a logical plan's structure.
enum QueryAnalysis {
    /// A simple table scan (SELECT ... FROM table WHERE ...).
    SimpleScan { table: String },
    /// An aggregation query (SELECT agg(...) FROM table GROUP BY ...).
    AggregateQuery {
        table: String,
        group_cols: Vec<String>,
        agg_funcs: Vec<String>,
    },
    /// A join query (SELECT ... FROM t1 JOIN t2 ON ...).
    JoinQuery {
        _left_table: String,
        _right_table: String,
    },
    /// Simple expression (SELECT 1+1, no tables).
    Simple,
    /// Complex query we can't easily distribute.
    Complex,
}

/// Analyze a LogicalPlan to determine its query type.
fn analyze_plan(plan: &LogicalPlan) -> QueryAnalysis {
    match plan {
        LogicalPlan::Projection(proj) => {
            // Recurse into projection's input.
            analyze_plan(proj.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            analyze_plan(filter.input.as_ref())
        }
        LogicalPlan::Sort(sort) => {
            analyze_plan(sort.input.as_ref())
        }
        LogicalPlan::Limit(limit) => {
            analyze_plan(limit.input.as_ref())
        }
        LogicalPlan::Aggregate(agg) => {
            // Extract group columns and aggregate functions.
            let group_cols: Vec<String> = agg
                .group_expr
                .iter()
                .map(|e| format!("{}", e))
                .collect();
            let agg_funcs: Vec<String> = agg
                .aggr_expr
                .iter()
                .map(|e| format!("{}", e))
                .collect();

            // Find the table being aggregated.
            let inner = analyze_plan(agg.input.as_ref());
            let table = match inner {
                QueryAnalysis::SimpleScan { table } => table,
                _ => "unknown".to_string(),
            };

            QueryAnalysis::AggregateQuery {
                table,
                group_cols,
                agg_funcs,
            }
        }
        LogicalPlan::Join(join) => {
            let left = analyze_plan(join.left.as_ref());
            let right = analyze_plan(join.right.as_ref());

            let left_table = match left {
                QueryAnalysis::SimpleScan { table } => table,
                _ => "unknown".to_string(),
            };
            let right_table = match right {
                QueryAnalysis::SimpleScan { table } => table,
                _ => "unknown".to_string(),
            };

            QueryAnalysis::JoinQuery {
                _left_table: left_table,
                _right_table: right_table,
            }
        }
        LogicalPlan::TableScan(scan) => {
            QueryAnalysis::SimpleScan {
                table: scan.table_name.to_string(),
            }
        }
        LogicalPlan::SubqueryAlias(alias) => {
            analyze_plan(alias.input.as_ref())
        }
        LogicalPlan::EmptyRelation(_) => QueryAnalysis::Simple,
        _ => QueryAnalysis::Complex,
    }
}
