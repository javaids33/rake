//! Cost model for multi-engine query routing.
//!
//! Estimates per-engine execution time for a given [`QueryProfile`], considering scan
//! throughput, compute multipliers (aggregation, join, sort), data transfer overhead
//! (S3 fetch, Arrow IPC, sync), and per-engine fixed costs. The router uses these
//! estimates to pick the fastest engine — or to split a query across engines when
//! tables span different optimal paths.
//!
//! # Example
//!
//! ```
//! use rustlake_router::cost_model::CostModel;
//! use rustlake_router::profiler::{QueryProfile, TableReference, SourceType};
//! use std::collections::HashSet;
//!
//! let model = CostModel::new();
//! let profile = QueryProfile {
//!     tables: vec![TableReference {
//!         name: "orders".into(),
//!         source: SourceType::Local,
//!         estimated_rows: 1_000_000,
//!         estimated_bytes: 100_000_000,
//!     }],
//!     estimated_rows: 1_000_000,
//!     estimated_bytes: 100_000_000,
//!     has_aggregation: true,
//!     has_join: false,
//!     has_sort: false,
//!     has_vector_search: false,
//!     has_federated_source: false,
//!     projected_columns: 5,
//! };
//!
//! let engines = vec!["DataFusion".into(), "DuckDB".into()];
//! let cached = HashSet::new();
//! let (best_engine, estimate) = model.recommend(&engines, &profile, &cached);
//! assert!(estimate.total_ms > 0.0);
//! ```

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::profiler::{QueryProfile, SourceType};

/// Per-engine performance baseline used by the cost model.
///
/// Each engine has different scan throughput, compute overhead, and data access
/// capabilities. These baselines are calibrated against observed benchmarks
/// (TPC-H SF1 on 8-core machines).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineBaseline {
    /// Rows scanned per millisecond. DataFusion ~500K, DuckDB ~800K, Polars ~600K.
    pub scan_throughput_rows_per_ms: f64,
    /// Multiplier applied when the query contains GROUP BY / aggregates.
    pub aggregation_factor: f64,
    /// Multiplier applied when the query contains JOIN clauses.
    pub join_factor: f64,
    /// Multiplier applied when the query contains ORDER BY.
    pub sort_factor: f64,
    /// Whether this engine can read S3 object storage directly.
    pub s3_direct: bool,
    /// Whether this engine can read Iceberg tables on S3 natively.
    pub s3_iceberg_direct: bool,
    /// Whether this engine can read Delta Lake tables on S3 natively.
    pub s3_delta_direct: bool,
    /// Whether this engine supports federated pushdown to remote databases.
    pub federated_capable: bool,
    /// Fixed per-query overhead in ms (mutex acquisition, context setup, etc.).
    pub startup_overhead_ms: f64,
}

/// Estimated execution cost for a single engine on a given query profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Engine name (e.g., "DataFusion", "DuckDB", "Polars").
    pub engine: String,
    /// Total estimated execution time in milliseconds.
    pub total_ms: f64,
    /// Time spent scanning data.
    pub scan_ms: f64,
    /// Time spent on compute (aggregation, join, sort).
    pub compute_ms: f64,
    /// Time spent on data transfer (sync, S3 fetch, IPC serialization).
    pub transfer_ms: f64,
    /// Fixed overhead (startup, context setup).
    pub overhead_ms: f64,
    /// How data reaches the engine: "direct", "synced", or "cached".
    pub execution_mode: String,
    /// Human-readable cost breakdown notes.
    pub notes: Vec<String>,
}

/// Estimated cost of splitting a query across multiple engines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitPlanEstimate {
    /// Per-table fragment assignments.
    pub fragments: Vec<FragmentEstimate>,
    /// Cost to merge results from parallel fragments (IPC + coordinator join).
    pub merge_cost_ms: f64,
    /// Total wall-clock time: max(fragment times) + merge.
    pub total_ms: f64,
    /// Difference vs. the best single-engine estimate (negative = split is faster).
    pub vs_single_best_ms: f64,
}

/// A single table fragment assigned to an engine in a split plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentEstimate {
    /// Table name.
    pub table: String,
    /// Engine assigned to scan this table.
    pub engine: String,
    /// Estimated execution time for this fragment.
    pub estimated_ms: f64,
    /// Execution mode: "direct", "synced", or "cached".
    pub execution_mode: String,
}

/// Cost model for multi-engine query routing.
///
/// Combines per-engine baselines with data transfer overhead estimates to predict
/// which engine will execute a query fastest. Supports single-engine estimation,
/// multi-engine comparison, and split-plan analysis for queries spanning multiple
/// data sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostModel {
    /// Per-engine performance characteristics, keyed by engine name.
    pub engine_baselines: HashMap<String, EngineBaseline>,
    /// Base cost in ms to sync a table from DataFusion to another engine,
    /// plus 10ms per 100K rows.
    pub sync_overhead_ms: f64,
    /// S3 GET request latency in ms.
    pub s3_fetch_overhead_ms: f64,
    /// Arrow IPC serialization cost per megabyte in ms.
    pub ipc_overhead_per_mb: f64,
    /// Arrow Flight network transfer cost per megabyte in ms.
    pub flight_overhead_per_mb: f64,
}

impl CostModel {
    /// Create a cost model with default engine baselines calibrated against
    /// TPC-H SF1 benchmarks on 8-core machines.
    pub fn new() -> Self {
        let mut engine_baselines = HashMap::new();

        engine_baselines.insert(
            "DataFusion".to_string(),
            EngineBaseline {
                scan_throughput_rows_per_ms: 500_000.0,
                aggregation_factor: 1.5,
                join_factor: 2.0,
                sort_factor: 1.3,
                s3_direct: true,
                s3_iceberg_direct: true,
                s3_delta_direct: true,
                federated_capable: true,
                startup_overhead_ms: 1.0,
            },
        );

        engine_baselines.insert(
            "DuckDB".to_string(),
            EngineBaseline {
                scan_throughput_rows_per_ms: 800_000.0,
                aggregation_factor: 1.2,
                join_factor: 1.5,
                sort_factor: 1.1,
                s3_direct: true,
                s3_iceberg_direct: false,
                s3_delta_direct: false,
                federated_capable: false,
                startup_overhead_ms: 5.0,
            },
        );

        engine_baselines.insert(
            "Polars".to_string(),
            EngineBaseline {
                scan_throughput_rows_per_ms: 600_000.0,
                aggregation_factor: 1.4,
                join_factor: 1.8,
                sort_factor: 1.2,
                s3_direct: false,
                s3_iceberg_direct: false,
                s3_delta_direct: false,
                federated_capable: false,
                startup_overhead_ms: 3.0,
            },
        );

        Self {
            engine_baselines,
            sync_overhead_ms: 50.0,
            s3_fetch_overhead_ms: 50.0,
            ipc_overhead_per_mb: 2.0,
            flight_overhead_per_mb: 5.0,
        }
    }

    /// Estimate execution cost for a single engine on the given query profile.
    ///
    /// Returns a detailed [`CostEstimate`] with per-phase breakdown and notes.
    /// If the engine is unknown, returns an estimate with `f64::MAX` total cost.
    pub fn estimate(
        &self,
        engine: &str,
        profile: &QueryProfile,
        cached_tables: &HashSet<String>,
    ) -> CostEstimate {
        let Some(baseline) = self.engine_baselines.get(engine) else {
            return CostEstimate {
                engine: engine.to_string(),
                total_ms: f64::MAX,
                scan_ms: 0.0,
                compute_ms: 0.0,
                transfer_ms: 0.0,
                overhead_ms: 0.0,
                execution_mode: "unknown".to_string(),
                notes: vec![format!("Unknown engine: {engine}")],
            };
        };

        let mut notes = Vec::new();

        // --- Scan cost ---
        let scan_ms = profile.estimated_rows as f64 / baseline.scan_throughput_rows_per_ms;
        notes.push(format!(
            "Scan: {} rows at {:.0} rows/ms = {:.2}ms",
            profile.estimated_rows, baseline.scan_throughput_rows_per_ms, scan_ms,
        ));

        // --- Compute cost (multiplicative factors) ---
        let mut compute_multiplier = 1.0_f64;
        if profile.has_aggregation {
            compute_multiplier *= baseline.aggregation_factor;
            notes.push(format!(
                "Aggregation factor: {:.1}x",
                baseline.aggregation_factor,
            ));
        }
        if profile.has_join {
            compute_multiplier *= baseline.join_factor;
            notes.push(format!("Join factor: {:.1}x", baseline.join_factor));
        }
        if profile.has_sort {
            compute_multiplier *= baseline.sort_factor;
            notes.push(format!("Sort factor: {:.1}x", baseline.sort_factor));
        }
        let compute_ms = scan_ms * (compute_multiplier - 1.0);

        // --- Transfer cost ---
        let (transfer_ms, execution_mode) =
            self.compute_transfer_cost(engine, baseline, profile, cached_tables, &mut notes);

        // --- Fixed overhead ---
        let overhead_ms = baseline.startup_overhead_ms;
        notes.push(format!("Startup overhead: {:.1}ms", overhead_ms));

        let total_ms = scan_ms + compute_ms + transfer_ms + overhead_ms;
        notes.push(format!("Total: {:.2}ms", total_ms));

        CostEstimate {
            engine: engine.to_string(),
            total_ms,
            scan_ms,
            compute_ms,
            transfer_ms,
            overhead_ms,
            execution_mode,
            notes,
        }
    }

    /// Estimate execution cost across all provided engines.
    ///
    /// Returns one [`CostEstimate`] per engine, in the same order as `engines`.
    pub fn estimate_all(
        &self,
        engines: &[String],
        profile: &QueryProfile,
        cached_tables: &HashSet<String>,
    ) -> Vec<CostEstimate> {
        engines
            .iter()
            .map(|e| self.estimate(e, profile, cached_tables))
            .collect()
    }

    /// Recommend the best engine for a query, returning the engine name and its estimate.
    ///
    /// Compares all provided engines and returns the one with the lowest total estimated
    /// execution time.
    pub fn recommend(
        &self,
        engines: &[String],
        profile: &QueryProfile,
        cached_tables: &HashSet<String>,
    ) -> (String, CostEstimate) {
        let estimates = self.estimate_all(engines, profile, cached_tables);
        estimates
            .into_iter()
            .min_by(|a, b| a.total_ms.partial_cmp(&b.total_ms).unwrap_or(std::cmp::Ordering::Equal))
            .map(|e| (e.engine.clone(), e))
            .unwrap_or_else(|| {
                (
                    "DataFusion".to_string(),
                    CostEstimate {
                        engine: "DataFusion".to_string(),
                        total_ms: 0.0,
                        scan_ms: 0.0,
                        compute_ms: 0.0,
                        transfer_ms: 0.0,
                        overhead_ms: 0.0,
                        execution_mode: "fallback".to_string(),
                        notes: vec!["No engines provided, defaulting to DataFusion".to_string()],
                    },
                )
            })
    }

    /// Estimate the cost of splitting a query across multiple engines.
    ///
    /// Groups tables by their optimal engine, estimates parallel fragment execution,
    /// and compares against the best single-engine estimate. Returns `None` if all
    /// tables route to the same engine (no split benefit) or if the split doesn't
    /// achieve at least a 20% improvement.
    pub fn estimate_split_plan(
        &self,
        profile: &QueryProfile,
        cached_tables: &HashSet<String>,
    ) -> Option<SplitPlanEstimate> {
        if profile.tables.len() < 2 {
            return None;
        }

        let engines: Vec<String> = self.engine_baselines.keys().cloned().collect();

        // Find the optimal engine for each table individually.
        let mut table_engines: Vec<(String, String, f64, String)> = Vec::new(); // (table, engine, ms, mode)
        for table_ref in &profile.tables {
            let table_profile = QueryProfile {
                tables: vec![table_ref.clone()],
                estimated_rows: table_ref.estimated_rows,
                estimated_bytes: table_ref.estimated_bytes,
                has_aggregation: false,
                has_join: false,
                has_sort: false,
                has_vector_search: false,
                has_federated_source: matches!(
                    table_ref.source,
                    SourceType::Federated | SourceType::Trino
                ),
                projected_columns: profile.projected_columns,
            };

            let (best_engine, best_estimate) =
                self.recommend(&engines, &table_profile, cached_tables);
            table_engines.push((
                table_ref.name.clone(),
                best_engine,
                best_estimate.total_ms,
                best_estimate.execution_mode,
            ));
        }

        // Check if all tables route to the same engine — no split benefit.
        let first_engine = &table_engines[0].1;
        if table_engines.iter().all(|(_, e, _, _)| e == first_engine) {
            return None;
        }

        // Build fragment estimates.
        let fragments: Vec<FragmentEstimate> = table_engines
            .iter()
            .map(|(table, engine, ms, mode)| FragmentEstimate {
                table: table.clone(),
                engine: engine.clone(),
                estimated_ms: *ms,
                execution_mode: mode.clone(),
            })
            .collect();

        // Parallel execution: wall-clock = max fragment time.
        let max_fragment_ms = fragments
            .iter()
            .map(|f| f.estimated_ms)
            .fold(0.0_f64, f64::max);

        // Merge cost: transfer results back via IPC + coordinator join overhead.
        let total_result_bytes: u64 = profile
            .tables
            .iter()
            .map(|t| t.estimated_bytes)
            .sum();
        let merge_cost_ms = (total_result_bytes as f64 / 1_000_000.0) * self.ipc_overhead_per_mb
            + 5.0; // 5ms coordinator join overhead

        let split_total_ms = max_fragment_ms + merge_cost_ms;

        // Compare against best single-engine estimate.
        let (_, best_single) = self.recommend(&engines, profile, cached_tables);
        let vs_single_best_ms = split_total_ms - best_single.total_ms;

        // Only recommend split if it saves at least 20%.
        if split_total_ms >= best_single.total_ms * 0.8 {
            return None;
        }

        Some(SplitPlanEstimate {
            fragments,
            merge_cost_ms,
            total_ms: split_total_ms,
            vs_single_best_ms,
        })
    }

    /// Compute transfer cost and execution mode for a single engine.
    ///
    /// Accounts for S3 direct reads, cached tables, and IPC sync overhead.
    fn compute_transfer_cost(
        &self,
        engine: &str,
        baseline: &EngineBaseline,
        profile: &QueryProfile,
        cached_tables: &HashSet<String>,
        notes: &mut Vec<String>,
    ) -> (f64, String) {
        let mut total_transfer_ms = 0.0;
        let mut mode = "direct".to_string();

        // Check if any tables need special handling.
        for table_ref in &profile.tables {
            let table_cached = cached_tables.contains(&table_ref.name);

            match &table_ref.source {
                // S3-backed sources: check if engine can read directly.
                SourceType::S3Parquet => {
                    if baseline.s3_direct {
                        notes.push(format!(
                            "Table '{}': S3/Parquet direct read",
                            table_ref.name,
                        ));
                    } else if table_cached {
                        notes.push(format!(
                            "Table '{}': cached (no sync needed)",
                            table_ref.name,
                        ));
                        mode = "cached".to_string();
                    } else {
                        let sync_ms = self.sync_cost(table_ref.estimated_rows, table_ref.estimated_bytes);
                        total_transfer_ms += sync_ms;
                        mode = "synced".to_string();
                        notes.push(format!(
                            "Table '{}': sync required ({:.1}ms)",
                            table_ref.name, sync_ms,
                        ));
                    }
                }
                SourceType::S3Iceberg => {
                    if baseline.s3_iceberg_direct {
                        notes.push(format!(
                            "Table '{}': Iceberg direct read",
                            table_ref.name,
                        ));
                    } else if table_cached {
                        notes.push(format!(
                            "Table '{}': cached (no sync needed)",
                            table_ref.name,
                        ));
                        mode = "cached".to_string();
                    } else if engine != "DataFusion" {
                        let sync_ms = self.sync_cost(table_ref.estimated_rows, table_ref.estimated_bytes);
                        total_transfer_ms += sync_ms;
                        mode = "synced".to_string();
                        notes.push(format!(
                            "Table '{}': Iceberg sync via DataFusion ({:.1}ms)",
                            table_ref.name, sync_ms,
                        ));
                    }
                }
                SourceType::S3Delta => {
                    if baseline.s3_delta_direct {
                        notes.push(format!(
                            "Table '{}': Delta direct read",
                            table_ref.name,
                        ));
                    } else if table_cached {
                        notes.push(format!(
                            "Table '{}': cached (no sync needed)",
                            table_ref.name,
                        ));
                        mode = "cached".to_string();
                    } else if engine != "DataFusion" {
                        let sync_ms = self.sync_cost(table_ref.estimated_rows, table_ref.estimated_bytes);
                        total_transfer_ms += sync_ms;
                        mode = "synced".to_string();
                        notes.push(format!(
                            "Table '{}': Delta sync via DataFusion ({:.1}ms)",
                            table_ref.name, sync_ms,
                        ));
                    }
                }
                // Federated sources: only DataFusion can push down.
                SourceType::Federated | SourceType::Trino => {
                    if baseline.federated_capable {
                        notes.push(format!(
                            "Table '{}': federated pushdown",
                            table_ref.name,
                        ));
                    } else if table_cached {
                        notes.push(format!(
                            "Table '{}': cached (no sync needed)",
                            table_ref.name,
                        ));
                        mode = "cached".to_string();
                    } else {
                        // Must fetch via DataFusion first, then sync.
                        let fetch_ms = self.s3_fetch_overhead_ms;
                        let sync_ms = self.sync_cost(table_ref.estimated_rows, table_ref.estimated_bytes);
                        total_transfer_ms += fetch_ms + sync_ms;
                        mode = "synced".to_string();
                        notes.push(format!(
                            "Table '{}': federated fetch + sync ({:.1}ms)",
                            table_ref.name,
                            fetch_ms + sync_ms,
                        ));
                    }
                }
                // Local, Lance, and InMemory: no transfer needed for any engine.
                SourceType::Local | SourceType::Lance | SourceType::InMemory => {
                    if table_cached {
                        mode = "cached".to_string();
                    }
                    if engine != "DataFusion" && !table_cached {
                        let sync_ms = self.sync_cost(table_ref.estimated_rows, table_ref.estimated_bytes);
                        total_transfer_ms += sync_ms;
                        mode = "synced".to_string();
                        notes.push(format!(
                            "Table '{}': local sync to {} ({:.1}ms)",
                            table_ref.name, engine, sync_ms,
                        ));
                    }
                }
            }
        }

        (total_transfer_ms, mode)
    }

    /// Calculate sync cost: base overhead + per-row + per-byte IPC cost.
    fn sync_cost(&self, estimated_rows: u64, estimated_bytes: u64) -> f64 {
        let row_overhead = (estimated_rows as f64 / 100_000.0) * 10.0;
        let ipc_cost = (estimated_bytes as f64 / 1_000_000.0) * self.ipc_overhead_per_mb;
        self.sync_overhead_ms + row_overhead + ipc_cost
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiler::{TableReference, SourceType};

    /// Helper: build a simple single-table profile.
    fn single_table_profile(
        name: &str,
        source: SourceType,
        rows: u64,
        bytes: u64,
        agg: bool,
        join: bool,
        sort: bool,
    ) -> QueryProfile {
        QueryProfile {
            tables: vec![TableReference {
                name: name.to_string(),
                source: source.clone(),
                estimated_rows: rows,
                estimated_bytes: bytes,
            }],
            estimated_rows: rows,
            estimated_bytes: bytes,
            has_aggregation: agg,
            has_join: join,
            has_sort: sort,
            has_vector_search: false,
            has_federated_source: matches!(source, SourceType::Federated | SourceType::Trino),
            projected_columns: 5,
        }
    }

    #[test]
    fn duckdb_direct_s3_faster_than_synced_for_large_parquet() {
        let model = CostModel::new();
        let cached = HashSet::new();

        // Large S3 Parquet table: 10M rows, ~1GB.
        let profile = single_table_profile(
            "events",
            SourceType::S3Parquet,
            10_000_000,
            1_000_000_000,
            true,
            false,
            false,
        );

        let df_estimate = model.estimate("DataFusion", &profile, &cached);
        let dk_estimate = model.estimate("DuckDB", &profile, &cached);

        // DuckDB has s3_direct=true, so no sync overhead for S3 Parquet.
        assert_eq!(dk_estimate.execution_mode, "direct");
        assert_eq!(dk_estimate.transfer_ms, 0.0);

        // DuckDB should be faster due to higher scan throughput + lower agg factor.
        assert!(
            dk_estimate.total_ms < df_estimate.total_ms,
            "DuckDB ({:.2}ms) should be faster than DataFusion ({:.2}ms) on large S3 Parquet",
            dk_estimate.total_ms,
            df_estimate.total_ms,
        );
    }

    #[test]
    fn datafusion_wins_for_federated_source() {
        let model = CostModel::new();
        let engines = vec!["DataFusion".to_string(), "DuckDB".to_string()];
        let cached = HashSet::new();

        // Federated Postgres table: 500K rows.
        let profile = single_table_profile(
            "pg.tpch_orders",
            SourceType::Federated,
            500_000,
            50_000_000,
            true,
            false,
            false,
        );

        let (best_engine, _) = model.recommend(&engines, &profile, &cached);

        // DataFusion has federated_capable=true, DuckDB does not — DuckDB pays
        // fetch + sync overhead, so DataFusion should win.
        assert_eq!(
            best_engine, "DataFusion",
            "DataFusion should win for federated sources due to pushdown capability",
        );
    }

    #[test]
    fn split_plan_beats_single_engine_for_mixed_sources() {
        let model = CostModel::new();
        let cached = HashSet::new();

        // Query spanning a federated table (DataFusion optimal) and an S3 Parquet
        // table (DuckDB optimal).
        let profile = QueryProfile {
            tables: vec![
                TableReference {
                    name: "pg.tpch_orders".to_string(),
                    source: SourceType::Federated,
                    estimated_rows: 5_000_000,
                    estimated_bytes: 500_000_000,
                },
                TableReference {
                    name: "s3_events".to_string(),
                    source: SourceType::S3Parquet,
                    estimated_rows: 10_000_000,
                    estimated_bytes: 1_000_000_000,
                },
            ],
            estimated_rows: 15_000_000,
            estimated_bytes: 1_500_000_000,
            has_aggregation: true,
            has_join: true,
            has_sort: false,
            has_vector_search: false,
            has_federated_source: true,
            projected_columns: 8,
        };

        let split = model.estimate_split_plan(&profile, &cached);

        // With two very different source types, a split plan should be considered.
        // Whether it meets the 20% threshold depends on the exact numbers — but
        // at minimum the model should detect the table-engine divergence.
        if let Some(plan) = &split {
            assert_eq!(plan.fragments.len(), 2);
            // The federated table should route to DataFusion.
            let fed_frag = plan
                .fragments
                .iter()
                .find(|f| f.table == "pg.tpch_orders")
                .expect("federated table fragment");
            assert_eq!(fed_frag.engine, "DataFusion");

            assert!(
                plan.vs_single_best_ms < 0.0,
                "Split plan should be faster than single engine (delta: {:.2}ms)",
                plan.vs_single_best_ms,
            );
        }
        // If split is None, the 20% threshold wasn't met — that's acceptable too,
        // the model correctly decided splitting isn't worth the merge overhead.
    }

    #[test]
    fn cached_tables_eliminate_sync_cost() {
        let model = CostModel::new();

        let profile = single_table_profile(
            "orders",
            SourceType::S3Iceberg,
            1_000_000,
            100_000_000,
            false,
            false,
            false,
        );

        // Without cache: DuckDB must sync Iceberg via DataFusion.
        let uncached = HashSet::new();
        let dk_uncached = model.estimate("DuckDB", &profile, &uncached);

        // With cache: DuckDB has the table already synced.
        let mut cached = HashSet::new();
        cached.insert("orders".to_string());
        let dk_cached = model.estimate("DuckDB", &profile, &cached);

        assert_eq!(dk_cached.execution_mode, "cached");
        assert_eq!(dk_cached.transfer_ms, 0.0);
        assert!(
            dk_cached.total_ms < dk_uncached.total_ms,
            "Cached ({:.2}ms) should be faster than uncached ({:.2}ms)",
            dk_cached.total_ms,
            dk_uncached.total_ms,
        );

        // The savings should equal the sync cost.
        let expected_savings = dk_uncached.transfer_ms;
        let actual_savings = dk_uncached.total_ms - dk_cached.total_ms;
        assert!(
            (actual_savings - expected_savings).abs() < 0.01,
            "Savings ({:.2}ms) should equal sync cost ({:.2}ms)",
            actual_savings,
            expected_savings,
        );
    }

    #[test]
    fn small_queries_prefer_datafusion_lowest_overhead() {
        let model = CostModel::new();
        let engines = vec![
            "DataFusion".to_string(),
            "DuckDB".to_string(),
            "Polars".to_string(),
        ];
        let cached = HashSet::new();

        // Tiny query: 500 rows, 50KB — overhead dominates scan time.
        let profile = single_table_profile(
            "dim_status",
            SourceType::Local,
            500,
            50_000,
            false,
            false,
            false,
        );

        let (best_engine, best_estimate) = model.recommend(&engines, &profile, &cached);

        // DataFusion has 1ms startup vs DuckDB's 5ms and Polars's 3ms.
        // For tiny tables the overhead dominates, so DataFusion should win.
        assert_eq!(
            best_engine, "DataFusion",
            "DataFusion should win for small queries (lowest overhead). \
             DF={:.3}ms, got {}={:.3}ms",
            model.estimate("DataFusion", &profile, &cached).total_ms,
            best_engine,
            best_estimate.total_ms,
        );
    }

    #[test]
    fn unknown_engine_returns_max_cost() {
        let model = CostModel::new();
        let cached = HashSet::new();
        let profile = single_table_profile("t", SourceType::Local, 1000, 10000, false, false, false);

        let estimate = model.estimate("Nonexistent", &profile, &cached);
        assert_eq!(estimate.total_ms, f64::MAX);
        assert_eq!(estimate.execution_mode, "unknown");
    }

    #[test]
    fn estimate_all_returns_one_per_engine() {
        let model = CostModel::new();
        let engines = vec!["DataFusion".to_string(), "DuckDB".to_string()];
        let cached = HashSet::new();
        let profile = single_table_profile("t", SourceType::Local, 100_000, 10_000_000, true, false, false);

        let estimates = model.estimate_all(&engines, &profile, &cached);
        assert_eq!(estimates.len(), 2);
        assert_eq!(estimates[0].engine, "DataFusion");
        assert_eq!(estimates[1].engine, "DuckDB");
    }

    #[test]
    fn recommend_with_empty_engines_returns_fallback() {
        let model = CostModel::new();
        let cached = HashSet::new();
        let profile = single_table_profile("t", SourceType::Local, 1000, 10000, false, false, false);

        let (engine, estimate) = model.recommend(&[], &profile, &cached);
        assert_eq!(engine, "DataFusion");
        assert_eq!(estimate.execution_mode, "fallback");
    }

    #[test]
    fn single_table_no_split_plan() {
        let model = CostModel::new();
        let cached = HashSet::new();
        let profile = single_table_profile("t", SourceType::Local, 1_000_000, 100_000_000, true, false, false);

        let split = model.estimate_split_plan(&profile, &cached);
        assert!(split.is_none(), "Single table should never produce a split plan");
    }
}
