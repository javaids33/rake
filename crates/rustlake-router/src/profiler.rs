//! Query profiler for adaptive multi-engine routing.
//!
//! Collects table-level statistics (row counts, file sizes, column cardinality),
//! estimates per-query cost, and recommends the optimal execution engine based on
//! a combination of static heuristics and historical execution feedback.
//!
//! # Architecture
//!
//! The profiler sits between the SQL classifier and the engine dispatcher. After
//! the [`QueryClassifier`](crate::QueryClassifier) determines the *type* of a
//! query (OLAP, DDL, streaming, etc.), the `QueryProfiler` refines the routing
//! decision by incorporating:
//!
//! - **Table profiles** — cached metadata such as row count, byte size, partition
//!   count, and per-column statistics (cardinality, null fraction, min/max).
//! - **Selectivity estimation** — heuristic predicate analysis to predict how
//!   much data survives WHERE clauses.
//! - **Execution history** — a ring buffer of the last 1 000 query executions
//!   used to learn which engine performs best for similar workloads.
//!
//! The existing [`QueryProfile`] and [`TableReference`] types (used by the
//! [`cost_model`](crate::cost_model)) are defined here alongside the new
//! adaptive profiling types. The [`QueryProfiler`] produces an
//! [`AdaptiveQueryProfile`] that includes engine recommendations and execution
//! strategies.
//!
//! # Example
//!
//! ```rust
//! use rustlake_router::profiler::{QueryProfiler, TableProfileStats, SourceType};
//! use std::time::Duration;
//!
//! let profiler = QueryProfiler::new();
//!
//! // Register table metadata (typically populated from Iceberg manifests).
//! profiler.update_table_profile("events", TableProfileStats::new(
//!     1_000_000, 512_000_000, 64, 12, SourceType::S3Iceberg,
//! ));
//!
//! let engines = vec!["DataFusion".into(), "DuckDB".into()];
//! let profile = profiler.profile_query(
//!     "SELECT region, COUNT(*) FROM events GROUP BY region",
//!     &engines,
//! );
//!
//! println!("recommended: {} (confidence {:.0}%)",
//!     profile.recommended_engine.primary_engine,
//!     profile.recommended_engine.confidence * 100.0,
//! );
//! ```

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use tracing::debug;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of execution records retained for history-based scoring.
const MAX_EXECUTION_HISTORY: usize = 1_000;

/// Number of recent history records considered when computing per-engine
/// performance adjustments.
const HISTORY_LOOKBACK: usize = 100;

/// Multiplier applied to an engine's score when historical data shows it was
/// at least 2x faster than competitors on similar queries.
const HISTORY_BOOST_FACTOR: f64 = 1.30;

/// Default TTL for a table profile before it is considered stale.
const DEFAULT_PROFILE_TTL: Duration = Duration::from_secs(300);

// ===========================================================================
// Existing types (used by cost_model.rs)
// ===========================================================================

/// Where the data for a table physically lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceType {
    /// Local filesystem or memory-registered table.
    Local,
    /// S3/MinIO object storage — raw Parquet files.
    S3Parquet,
    /// S3/MinIO object storage — Iceberg table format.
    S3Iceberg,
    /// S3/MinIO object storage — Delta Lake table format.
    S3Delta,
    /// Federated relational database (Postgres, MySQL, SQLite).
    Federated,
    /// In-memory table (MemTable, CDC snapshot).
    InMemory,
    /// Trino-backed table (remote OLAP federation).
    Trino,
    /// Lance vector table format.
    Lance,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => write!(f, "Local"),
            Self::S3Parquet => write!(f, "S3/Parquet"),
            Self::S3Iceberg => write!(f, "S3/Iceberg"),
            Self::S3Delta => write!(f, "S3/Delta"),
            Self::Federated => write!(f, "Federated"),
            Self::InMemory => write!(f, "InMemory"),
            Self::Trino => write!(f, "Trino"),
            Self::Lance => write!(f, "Lance"),
        }
    }
}

/// A table referenced by a query, with its source and estimated size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReference {
    /// Fully-qualified table name (e.g., `pg.tpch_orders`).
    pub name: String,
    /// Where this table's data lives.
    pub source: SourceType,
    /// Estimated row count (from catalog statistics or heuristics).
    pub estimated_rows: u64,
    /// Estimated size in bytes (from catalog statistics or heuristics).
    pub estimated_bytes: u64,
}

/// Structural profile of a SQL query, used by the cost model to estimate execution time.
///
/// Captures the query's complexity characteristics without the full AST — just the
/// dimensions that affect engine selection and cost estimation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryProfile {
    /// Tables referenced by the query.
    pub tables: Vec<TableReference>,
    /// Total estimated rows across all tables (after predicate pushdown).
    pub estimated_rows: u64,
    /// Total estimated bytes across all tables.
    pub estimated_bytes: u64,
    /// Whether the query contains GROUP BY / aggregate functions.
    pub has_aggregation: bool,
    /// Whether the query contains JOIN clauses.
    pub has_join: bool,
    /// Whether the query contains ORDER BY.
    pub has_sort: bool,
    /// Whether the query contains vector_search or similarity operations.
    pub has_vector_search: bool,
    /// Whether the query references federated (remote database) tables.
    pub has_federated_source: bool,
    /// Number of columns projected (0 = SELECT *).
    pub projected_columns: usize,
}

// ===========================================================================
// New adaptive profiling types
// ===========================================================================

// ---------------------------------------------------------------------------
// ColumnProfile
// ---------------------------------------------------------------------------

/// Per-column statistics used for selectivity estimation and cost modeling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfile {
    /// Fraction of NULL values in the column (0.0 = no nulls, 1.0 = all null).
    pub null_fraction: f64,
    /// Estimated number of distinct non-null values (HyperLogLog or exact).
    pub distinct_count_estimate: Option<u64>,
    /// Serialized minimum value (format depends on the column's data type).
    pub min_value: Option<String>,
    /// Serialized maximum value.
    pub max_value: Option<String>,
}

// ---------------------------------------------------------------------------
// TableProfileStats
// ---------------------------------------------------------------------------

/// Cached metadata for a single table, typically populated from Iceberg
/// manifest files or post-query statistics updates.
///
/// Named `TableProfileStats` to avoid collision with the cost-model's
/// [`TableReference`] which serves a different purpose (per-query context
/// vs. cached metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableProfileStats {
    /// Total row count from manifests or the most recent query.
    pub total_rows: u64,
    /// Sum of data file sizes in bytes.
    pub total_bytes: u64,
    /// Number of Parquet data files backing this table.
    pub num_files: u32,
    /// Number of partitions (1 for unpartitioned tables).
    pub partition_count: u32,
    /// Estimated average row size (`total_bytes / total_rows`), zero when empty.
    pub avg_row_size_bytes: u32,
    /// Where the table's data physically resides.
    pub source_type: SourceType,
    /// Per-column statistics keyed by column name.
    pub column_stats: HashMap<String, ColumnProfile>,
    /// When this profile was last computed or refreshed.
    #[serde(skip, default = "Instant::now")]
    pub last_profiled: Instant,
    /// Duration after which this profile is considered stale.
    pub ttl: Duration,
}

impl TableProfileStats {
    /// Create a new table profile with the given metadata.
    ///
    /// `avg_row_size_bytes` is computed automatically. `column_stats` starts
    /// empty and can be populated later via direct field access.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rustlake_router::profiler::{TableProfileStats, SourceType};
    ///
    /// let profile = TableProfileStats::new(1_000_000, 512_000_000, 64, 12, SourceType::S3Iceberg);
    /// assert_eq!(profile.avg_row_size_bytes, 512);
    /// ```
    pub fn new(
        total_rows: u64,
        total_bytes: u64,
        num_files: u32,
        partition_count: u32,
        source_type: SourceType,
    ) -> Self {
        let avg_row_size_bytes = if total_rows > 0 {
            (total_bytes / total_rows) as u32
        } else {
            0
        };
        Self {
            total_rows,
            total_bytes,
            num_files,
            partition_count,
            avg_row_size_bytes,
            source_type,
            column_stats: HashMap::new(),
            last_profiled: Instant::now(),
            ttl: DEFAULT_PROFILE_TTL,
        }
    }

    /// Returns `true` when the profile is older than its configured TTL.
    pub fn is_stale(&self) -> bool {
        self.last_profiled.elapsed() > self.ttl
    }
}

// ---------------------------------------------------------------------------
// ExecutionStrategy / PlanFragment
// ---------------------------------------------------------------------------

/// How the query should be physically executed across engines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStrategy {
    /// Route the entire query to a single engine.
    SingleEngine {
        /// Target engine name.
        engine: String,
    },
    /// Split the logical plan so different fragments run on different engines.
    SplitPlan {
        /// Ordered list of plan fragments.
        fragments: Vec<PlanFragment>,
    },
    /// The chosen engine reads S3 directly — no DataFusion scan middleman.
    DirectS3 {
        /// Engine with native S3 support (e.g. DuckDB).
        engine: String,
    },
    /// One engine fetches data, another handles compute.
    FederatedFetch {
        /// Engine responsible for fetching rows from the remote source.
        source_engine: String,
        /// Engine responsible for the compute-heavy portion (agg, join, sort).
        compute_engine: String,
    },
}

/// A fragment of a split execution plan assigned to one engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanFragment {
    /// Table this fragment operates on.
    pub table: String,
    /// High-level operation: `"scan"`, `"aggregate"`, `"join"`, `"sort"`.
    pub operation: String,
    /// Engine assigned to execute this fragment.
    pub engine: String,
    /// Estimated output row count for this fragment.
    pub estimated_rows: u64,
}

// ---------------------------------------------------------------------------
// EngineRecommendation
// ---------------------------------------------------------------------------

/// The profiler's recommendation for how to execute a particular query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRecommendation {
    /// Primary engine name: `"DataFusion"`, `"DuckDB"`, or `"Polars"`.
    pub primary_engine: String,
    /// Confidence in the recommendation (0.0 = guess, 1.0 = certain).
    pub confidence: f64,
    /// Human-readable explanation of why this engine was chosen.
    pub reasoning: String,
    /// Detailed execution strategy.
    pub execution_strategy: ExecutionStrategy,
    /// Predicted wall-clock execution time in milliseconds.
    pub estimated_cost_ms: u64,
}

// ---------------------------------------------------------------------------
// AdaptiveQueryProfile
// ---------------------------------------------------------------------------

/// Pre-execution cost estimate for a specific SQL query, produced by the
/// [`QueryProfiler`].
///
/// Unlike the simpler [`QueryProfile`] (used by the cost model), this
/// includes the profiler's full engine recommendation with confidence and
/// execution strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveQueryProfile {
    /// Estimated rows after predicate selectivity is applied.
    pub estimated_rows: u64,
    /// Estimated bytes to process.
    pub estimated_bytes: u64,
    /// Number of tables referenced in the query.
    pub num_tables: u32,
    /// Whether the query contains a JOIN.
    pub has_join: bool,
    /// Whether the query contains an aggregation (GROUP BY / aggregate fn).
    pub has_aggregation: bool,
    /// Whether the query contains an ORDER BY.
    pub has_sort: bool,
    /// Fraction of data that survives predicates (0.0 to 1.0).
    pub selectivity: f64,
    /// Table names referenced by the query.
    pub tables: Vec<String>,
    /// Source types of the referenced tables.
    pub source_types: Vec<SourceType>,
    /// The profiler's engine recommendation.
    pub recommended_engine: EngineRecommendation,
}

// ---------------------------------------------------------------------------
// ExecutionRecord
// ---------------------------------------------------------------------------

/// A completed query execution record used for history-based learning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// Hash of the normalized SQL pattern (constants replaced with `?`).
    pub query_hash: u64,
    /// Engine that executed the query.
    pub engine: String,
    /// Wall-clock execution time in milliseconds.
    pub execution_ms: u64,
    /// Total rows scanned by the engine.
    pub rows_scanned: u64,
    /// Rows returned to the client.
    pub rows_returned: u64,
    /// Bytes scanned from storage.
    pub bytes_scanned: u64,
    /// When the execution completed.
    #[serde(skip, default = "Instant::now")]
    pub timestamp: Instant,
    /// Source types involved in this execution.
    pub source_types: Vec<SourceType>,
}

// ---------------------------------------------------------------------------
// QueryProfiler
// ---------------------------------------------------------------------------

/// Adaptive query profiler that combines table metadata, selectivity
/// estimation, and execution history to recommend the optimal engine for
/// each query.
///
/// Thread-safe: all internal state is behind [`Arc<RwLock<_>>`] so the
/// profiler can be shared across request handlers.
pub struct QueryProfiler {
    /// Cached table profiles keyed by table name.
    table_profiles: Arc<RwLock<HashMap<String, TableProfileStats>>>,
    /// Ring buffer of recent execution records for history-based scoring.
    execution_history: Arc<RwLock<VecDeque<ExecutionRecord>>>,
}

impl QueryProfiler {
    /// Create a new, empty profiler with no cached profiles or history.
    pub fn new() -> Self {
        Self {
            table_profiles: Arc::new(RwLock::new(HashMap::new())),
            execution_history: Arc::new(RwLock::new(VecDeque::with_capacity(
                MAX_EXECUTION_HISTORY,
            ))),
        }
    }

    /// Insert or replace the cached profile for a table.
    ///
    /// Called when Iceberg manifest metadata is read, or after a query reveals
    /// updated statistics (row counts, file counts, etc.).
    pub fn update_table_profile(&self, name: &str, profile: TableProfileStats) {
        let mut profiles = self.table_profiles.write().expect("profiler lock poisoned");
        debug!(
            table = name,
            rows = profile.total_rows,
            bytes = profile.total_bytes,
            "updated table profile"
        );
        profiles.insert(name.to_string(), profile);
    }

    /// Retrieve the cached profile for a table, or `None` if not profiled.
    pub fn get_table_profile(&self, name: &str) -> Option<TableProfileStats> {
        let profiles = self.table_profiles.read().expect("profiler lock poisoned");
        profiles.get(name).cloned()
    }

    /// Profile a SQL query and return a cost estimate with an engine
    /// recommendation.
    ///
    /// This is the main entry point. It:
    /// 1. Extracts referenced table names from the SQL text.
    /// 2. Looks up cached profiles for each table.
    /// 3. Estimates selectivity from WHERE predicates.
    /// 4. Scores each available engine.
    /// 5. Returns an [`AdaptiveQueryProfile`] with the winning recommendation.
    pub fn profile_query(
        &self,
        sql: &str,
        available_engines: &[String],
    ) -> AdaptiveQueryProfile {
        let upper = sql.to_uppercase();

        let has_join = upper.contains("JOIN");
        let has_aggregation = upper.contains("GROUP BY")
            || upper.contains("SUM(")
            || upper.contains("COUNT(")
            || upper.contains("AVG(")
            || upper.contains("MIN(")
            || upper.contains("MAX(");
        let has_sort = upper.contains("ORDER BY");

        // Extract table names (heuristic: FROM/JOIN <table>).
        let tables = Self::extract_table_names(sql);

        // Look up profiles and compute aggregate stats.
        let profiles = self.table_profiles.read().expect("profiler lock poisoned");
        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut source_types: Vec<SourceType> = Vec::new();
        let mut combined_selectivity: f64 = 1.0;

        for table in &tables {
            if let Some(tp) = profiles.get(table.as_str()) {
                let sel = self.estimate_selectivity(sql, table, tp);
                combined_selectivity *= sel;
                total_rows += tp.total_rows;
                total_bytes += tp.total_bytes;
                source_types.push(tp.source_type);
            }
        }
        drop(profiles);

        // Apply selectivity.
        let estimated_rows = (total_rows as f64 * combined_selectivity).ceil() as u64;
        let estimated_bytes = (total_bytes as f64 * combined_selectivity).ceil() as u64;

        let mut profile = AdaptiveQueryProfile {
            estimated_rows,
            estimated_bytes,
            num_tables: tables.len() as u32,
            has_join,
            has_aggregation,
            has_sort,
            selectivity: combined_selectivity,
            tables,
            source_types,
            // Placeholder — filled below.
            recommended_engine: EngineRecommendation {
                primary_engine: "DataFusion".to_string(),
                confidence: 0.0,
                reasoning: String::new(),
                execution_strategy: ExecutionStrategy::SingleEngine {
                    engine: "DataFusion".to_string(),
                },
                estimated_cost_ms: 0,
            },
        };

        let recommendation = self.build_recommendation(&profile, available_engines);
        profile.recommended_engine = recommendation;
        profile
    }

    /// Record a completed execution for history-based learning.
    ///
    /// The ring buffer evicts the oldest record when it exceeds
    /// [`MAX_EXECUTION_HISTORY`].
    pub fn record_execution(&self, record: ExecutionRecord) {
        let mut history = self.execution_history.write().expect("profiler lock poisoned");
        if history.len() >= MAX_EXECUTION_HISTORY {
            history.pop_front();
        }
        debug!(
            engine = record.engine,
            ms = record.execution_ms,
            rows = record.rows_returned,
            "recorded execution"
        );
        history.push_back(record);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Estimate predicate selectivity for a query on a specific table.
    ///
    /// Uses simple heuristics:
    /// - No WHERE -> 1.0
    /// - Equality on profiled column -> 1 / distinct_count
    /// - Range predicate (< > BETWEEN) -> 0.33
    /// - LIKE -> 0.10
    /// - AND -> multiply selectivities
    /// - OR  -> 1 - product(1 - s_i)
    fn estimate_selectivity(
        &self,
        sql: &str,
        _table: &str,
        profile: &TableProfileStats,
    ) -> f64 {
        let upper = sql.to_uppercase();

        // If no WHERE clause, everything passes.
        let where_pos = match upper.find("WHERE") {
            Some(pos) => pos,
            None => return 1.0,
        };

        // Extract the WHERE clause text (up to GROUP BY / ORDER BY / LIMIT / ;).
        let after_where = &upper[where_pos + 5..];
        let clause_end = ["GROUP BY", "ORDER BY", "LIMIT", "HAVING", ";"]
            .iter()
            .filter_map(|kw| after_where.find(kw))
            .min()
            .unwrap_or(after_where.len());
        let where_clause = &after_where[..clause_end];

        // Determine if predicates are combined with OR at the top level.
        let has_top_level_or = Self::has_top_level_or(where_clause);

        if has_top_level_or {
            // Split on OR and combine: 1 - product(1 - s_i).
            let parts: Vec<&str> = where_clause.split(" OR ").collect();
            let mut complement_product = 1.0_f64;
            for part in &parts {
                let sel = self.estimate_predicate_selectivity(part.trim(), profile);
                complement_product *= 1.0 - sel;
            }
            (1.0 - complement_product).clamp(0.0, 1.0)
        } else {
            // Split on AND and multiply.
            let parts: Vec<&str> = where_clause.split(" AND ").collect();
            let mut combined = 1.0_f64;
            for part in &parts {
                let sel = self.estimate_predicate_selectivity(part.trim(), profile);
                combined *= sel;
            }
            combined.clamp(0.0, 1.0)
        }
    }

    /// Estimate selectivity for a single predicate fragment.
    fn estimate_predicate_selectivity(
        &self,
        predicate: &str,
        profile: &TableProfileStats,
    ) -> f64 {
        let pred = predicate.trim();

        // LIKE pattern.
        if pred.contains("LIKE") {
            return 0.10;
        }

        // BETWEEN range.
        if pred.contains("BETWEEN") {
            return 0.33;
        }

        // IN list — approximate as N * equality selectivity.
        if pred.contains(" IN ") || pred.contains(" IN(") {
            let num_values = pred.matches(',').count() as f64 + 1.0;
            let col_name = Self::extract_column_from_predicate(pred);
            let base = self.column_equality_selectivity(&col_name, profile);
            return (base * num_values).min(1.0);
        }

        // Range operators.
        if pred.contains(">=")
            || pred.contains("<=")
            || pred.contains(" > ")
            || pred.contains(" < ")
        {
            return 0.33;
        }

        // Equality (=).
        if pred.contains('=') && !pred.contains("!=") && !pred.contains("<>") {
            let col_name = Self::extract_column_from_predicate(pred);
            return self.column_equality_selectivity(&col_name, profile);
        }

        // Inequality (!= / <>).
        if pred.contains("!=") || pred.contains("<>") {
            let col_name = Self::extract_column_from_predicate(pred);
            return 1.0 - self.column_equality_selectivity(&col_name, profile);
        }

        // Fallback for unrecognized predicates.
        0.5
    }

    /// Compute equality selectivity for a column: 1 / distinct_count.
    fn column_equality_selectivity(
        &self,
        col_name: &str,
        profile: &TableProfileStats,
    ) -> f64 {
        if col_name.is_empty() {
            return 0.01; // conservative default
        }
        if let Some(cs) = profile.column_stats.get(col_name) {
            if let Some(dc) = cs.distinct_count_estimate {
                if dc > 0 {
                    return 1.0 / dc as f64;
                }
            }
        }
        // No stats available — assume moderate cardinality.
        0.01
    }

    /// Extract the column name from a simple predicate like `COL = 5`.
    fn extract_column_from_predicate(pred: &str) -> String {
        // Take the left-hand side of the first operator.
        let operators = [">=", "<=", "!=", "<>", "=", ">", "<"];
        for op in &operators {
            if let Some(pos) = pred.find(op) {
                let lhs = pred[..pos].trim();
                // Strip table alias prefix (e.g. `t.col` -> `col`).
                let col = lhs.rsplit('.').next().unwrap_or(lhs);
                return col
                    .trim()
                    .trim_matches(|c: char| !c.is_alphanumeric() && c != '_')
                    .to_uppercase();
            }
        }
        String::new()
    }

    /// Check if the WHERE clause contains a top-level OR (not inside parens).
    fn has_top_level_or(clause: &str) -> bool {
        let mut depth = 0i32;
        let bytes = clause.as_bytes();
        let or_bytes = b" OR ";
        for i in 0..bytes.len() {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            if depth == 0 && i + 4 <= bytes.len() && &bytes[i..i + 4] == or_bytes {
                return true;
            }
        }
        false
    }

    /// Score an engine for a given query profile, using heuristics plus
    /// historical execution data.
    ///
    /// Scoring rules:
    /// - **DuckDB** scores high for large datasets (>50K rows), aggregation,
    ///   full scans, and S3 sources (direct access). Penalized for federated
    ///   sources and sync overhead.
    /// - **DataFusion** scores high for federated sources, multiple source types,
    ///   small datasets (<10K rows), and in-memory tables.
    /// - **Polars** scores high for medium datasets (10K-500K rows) with simple
    ///   transformations, penalized for S3 and federated sources.
    /// - History bonus: if an engine was >=2x faster on similar recent queries,
    ///   its score is boosted by 30%.
    fn score_engine(
        &self,
        engine: &str,
        query: &AdaptiveQueryProfile,
        history: &[ExecutionRecord],
    ) -> f64 {
        let mut score: f64 = 50.0; // baseline

        match engine {
            "DuckDB" => {
                // DuckDB excels at large OLAP scans.
                if query.estimated_rows > 50_000 {
                    score += 30.0;
                }
                if query.has_aggregation {
                    score += 20.0;
                }
                if query.selectivity > 0.5 {
                    // Full or near-full scan — DuckDB's strength.
                    score += 15.0;
                }
                // Direct S3 access bonus.
                let s3_tables = query
                    .source_types
                    .iter()
                    .filter(|st| {
                        matches!(st, SourceType::S3Parquet | SourceType::S3Iceberg)
                    })
                    .count();
                if s3_tables > 0 {
                    score += 20.0;
                }
                // Penalty: federated sources require DataFusion to fetch first.
                let federated_count = query
                    .source_types
                    .iter()
                    .filter(|st| {
                        matches!(st, SourceType::Federated | SourceType::Trino)
                    })
                    .count();
                if federated_count > 0 {
                    score -= 25.0;
                }
                // Sync cost penalty (DataFusion -> DuckDB data transfer).
                let sync_penalty_ms =
                    50.0 + (query.estimated_rows as f64 / 100_000.0) * 10.0;
                score -= sync_penalty_ms.min(40.0);
            }
            "DataFusion" => {
                // DataFusion is the coordinator — handles federation natively.
                let federated_count = query
                    .source_types
                    .iter()
                    .filter(|st| {
                        matches!(st, SourceType::Federated | SourceType::Trino)
                    })
                    .count();
                if federated_count > 0 {
                    score += 30.0;
                }
                // Multiple source types -> DataFusion is the join coordinator.
                let unique_sources: HashSet<_> = query.source_types.iter().collect();
                if unique_sources.len() > 1 {
                    score += 20.0;
                }
                // Small datasets are fast enough on DataFusion — no sync overhead.
                if query.estimated_rows < 10_000 {
                    score += 20.0;
                }
                // In-memory data avoids any transfer.
                let in_mem = query
                    .source_types
                    .iter()
                    .filter(|st| matches!(st, SourceType::InMemory))
                    .count();
                if in_mem > 0 {
                    score += 10.0;
                }
            }
            "Polars" => {
                // Polars is strong for medium-sized datasets with simple transforms.
                if query.estimated_rows >= 10_000 && query.estimated_rows <= 500_000 {
                    score += 20.0;
                }
                if !query.has_join && !query.has_sort {
                    score += 10.0;
                }
                // Polars doesn't read S3 directly in our setup — penalty.
                let s3_count = query
                    .source_types
                    .iter()
                    .filter(|st| {
                        matches!(
                            st,
                            SourceType::S3Parquet
                                | SourceType::S3Iceberg
                                | SourceType::S3Delta
                        )
                    })
                    .count();
                if s3_count > 0 {
                    score -= 15.0;
                }
                // Federated sources also need DataFusion intermediation.
                let federated_count = query
                    .source_types
                    .iter()
                    .filter(|st| {
                        matches!(st, SourceType::Federated | SourceType::Trino)
                    })
                    .count();
                if federated_count > 0 {
                    score -= 20.0;
                }
            }
            _ => {
                // Unknown engine — no adjustments.
            }
        }

        // History-based adjustment: check if this engine was >=2x faster than
        // competitors on similar queries (matching source types).
        let similar_records: Vec<&ExecutionRecord> = history
            .iter()
            .rev()
            .take(HISTORY_LOOKBACK)
            .filter(|r| {
                r.source_types
                    .iter()
                    .any(|st| query.source_types.contains(st))
            })
            .collect();

        if similar_records.len() >= 3 {
            let this_engine_times: Vec<u64> = similar_records
                .iter()
                .filter(|r| r.engine == engine)
                .map(|r| r.execution_ms)
                .collect();
            let other_engine_times: Vec<u64> = similar_records
                .iter()
                .filter(|r| r.engine != engine)
                .map(|r| r.execution_ms)
                .collect();

            if !this_engine_times.is_empty() && !other_engine_times.is_empty() {
                let avg_this = this_engine_times.iter().sum::<u64>() as f64
                    / this_engine_times.len() as f64;
                let avg_other = other_engine_times.iter().sum::<u64>() as f64
                    / other_engine_times.len() as f64;

                if avg_this > 0.0 && avg_other / avg_this >= 2.0 {
                    score *= HISTORY_BOOST_FACTOR;
                }
            }
        }

        score.max(0.0)
    }

    /// Build the final [`EngineRecommendation`] by scoring all available
    /// engines and selecting the winner.
    fn build_recommendation(
        &self,
        query: &AdaptiveQueryProfile,
        available_engines: &[String],
    ) -> EngineRecommendation {
        let history = self.execution_history.read().expect("profiler lock poisoned");
        let history_slice: Vec<ExecutionRecord> = history.iter().cloned().collect();
        drop(history);

        // Score each engine.
        let mut scores: Vec<(String, f64)> = available_engines
            .iter()
            .map(|eng| {
                let s = self.score_engine(eng, query, &history_slice);
                (eng.clone(), s)
            })
            .collect();
        scores.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        });

        let (best_engine, best_score) = scores
            .first()
            .cloned()
            .unwrap_or_else(|| ("DataFusion".to_string(), 50.0));
        let second_score = scores.get(1).map(|(_, s)| *s).unwrap_or(0.0);

        // Confidence: how much better the winner is vs runner-up.
        let spread = if second_score > 0.0 {
            (best_score - second_score) / best_score
        } else {
            1.0
        };
        let confidence = spread.clamp(0.1, 1.0);

        // Determine execution strategy.
        let has_federated = query
            .source_types
            .iter()
            .any(|st| matches!(st, SourceType::Federated | SourceType::Trino));
        let has_s3 = query.source_types.iter().any(|st| {
            matches!(
                st,
                SourceType::S3Parquet | SourceType::S3Iceberg | SourceType::S3Delta
            )
        });
        let unique_sources: HashSet<_> = query.source_types.iter().collect();

        let strategy = if has_federated && has_s3 && unique_sources.len() > 1 {
            // Mixed federated + S3 -> split plan.
            let mut fragments = Vec::new();
            for (i, table) in query.tables.iter().enumerate() {
                let st = query
                    .source_types
                    .get(i)
                    .copied()
                    .unwrap_or(SourceType::InMemory);
                let frag_engine =
                    if matches!(st, SourceType::Federated | SourceType::Trino) {
                        "DataFusion".to_string()
                    } else {
                        best_engine.clone()
                    };
                fragments.push(PlanFragment {
                    table: table.clone(),
                    operation: "scan".to_string(),
                    engine: frag_engine,
                    estimated_rows: query.estimated_rows
                        / query.num_tables.max(1) as u64,
                });
            }
            ExecutionStrategy::SplitPlan { fragments }
        } else if has_federated {
            ExecutionStrategy::FederatedFetch {
                source_engine: "DataFusion".to_string(),
                compute_engine: best_engine.clone(),
            }
        } else if has_s3 && best_engine == "DuckDB" {
            ExecutionStrategy::DirectS3 {
                engine: "DuckDB".to_string(),
            }
        } else {
            ExecutionStrategy::SingleEngine {
                engine: best_engine.clone(),
            }
        };

        // Rough cost estimate (ms).
        let base_cost_ms = if query.estimated_rows == 0 {
            1
        } else {
            // ~1ms per 10K rows as a rough baseline, plus fixed overhead.
            let row_cost = query.estimated_rows / 10_000;
            let overhead: u64 = match best_engine.as_str() {
                "DuckDB" => 10,  // minimal overhead
                "Polars" => 15,
                _ => 5, // DataFusion
            };
            overhead + row_cost.max(1)
        };

        let reasoning =
            self.build_reasoning(&best_engine, query, best_score, second_score);

        EngineRecommendation {
            primary_engine: best_engine,
            confidence,
            reasoning,
            execution_strategy: strategy,
            estimated_cost_ms: base_cost_ms,
        }
    }

    /// Build a human-readable explanation of the engine choice.
    fn build_reasoning(
        &self,
        engine: &str,
        query: &AdaptiveQueryProfile,
        score: f64,
        runner_up: f64,
    ) -> String {
        let mut reasons = Vec::new();

        match engine {
            "DuckDB" => {
                if query.estimated_rows > 50_000 {
                    reasons.push(format!(
                        "large dataset (~{} rows) favors DuckDB's columnar scan",
                        query.estimated_rows
                    ));
                }
                if query.has_aggregation {
                    reasons.push(
                        "aggregation benefits from DuckDB's vectorized execution"
                            .into(),
                    );
                }
                let s3 = query.source_types.iter().any(|st| {
                    matches!(st, SourceType::S3Parquet | SourceType::S3Iceberg)
                });
                if s3 {
                    reasons.push(
                        "DuckDB can read S3 natively (no sync overhead)".into(),
                    );
                }
            }
            "DataFusion" => {
                let fed = query.source_types.iter().any(|st| {
                    matches!(st, SourceType::Federated | SourceType::Trino)
                });
                if fed {
                    reasons
                        .push("federated source requires DataFusion provider".into());
                }
                if query.estimated_rows < 10_000 {
                    reasons.push(
                        "small dataset — DataFusion avoids sync overhead".into(),
                    );
                }
                let unique: HashSet<_> = query.source_types.iter().collect();
                if unique.len() > 1 {
                    reasons.push(
                        "multiple source types — DataFusion coordinates cross-source joins"
                            .into(),
                    );
                }
            }
            "Polars" => {
                if query.estimated_rows >= 10_000 && query.estimated_rows <= 500_000
                {
                    reasons.push("medium dataset size suits Polars".into());
                }
            }
            _ => {}
        }

        if reasons.is_empty() {
            reasons.push(format!(
                "{} selected as best available engine",
                engine
            ));
        }

        let margin = if runner_up > 0.0 {
            format!(" (score {:.0} vs runner-up {:.0})", score, runner_up)
        } else {
            format!(" (score {:.0})", score)
        };

        reasons.join("; ") + &margin
    }

    /// Extract table names from SQL text using simple heuristics.
    ///
    /// Looks for identifiers following `FROM` and `JOIN` keywords.
    fn extract_table_names(sql: &str) -> Vec<String> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        let upper_tokens: Vec<String> =
            tokens.iter().map(|t| t.to_uppercase()).collect();
        let mut tables = Vec::new();

        for (i, token) in upper_tokens.iter().enumerate() {
            let is_table_keyword = token == "FROM" || token == "JOIN";
            if is_table_keyword {
                if let Some(next) = tokens.get(i + 1) {
                    let name = next.trim_matches(|c: char| {
                        c == '(' || c == ')' || c == ',' || c == ';'
                    });
                    // Skip sub-query keywords.
                    if !name.is_empty()
                        && !name.eq_ignore_ascii_case("SELECT")
                        && !name.eq_ignore_ascii_case("(SELECT")
                        && !name.starts_with('(')
                    {
                        tables.push(name.to_string());
                    }
                }
            }
        }

        // Deduplicate while preserving order.
        let mut seen = HashSet::new();
        tables.retain(|t| seen.insert(t.clone()));
        tables
    }
}

impl Default for QueryProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute a stable hash for a SQL string (used for
/// [`ExecutionRecord::query_hash`]).
///
/// Constants are *not* normalized in this implementation — callers that want
/// pattern-level deduplication should normalize beforehand.
pub fn hash_sql(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a profiler with a large S3 Iceberg table registered.
    fn profiler_with_s3_iceberg(name: &str, rows: u64) -> QueryProfiler {
        let profiler = QueryProfiler::new();
        profiler.update_table_profile(
            name,
            TableProfileStats::new(
                rows,
                rows * 512,
                64,
                12,
                SourceType::S3Iceberg,
            ),
        );
        profiler
    }

    /// Helper: create a profiler with a federated Postgres table.
    fn profiler_with_federated(name: &str, rows: u64) -> QueryProfiler {
        let profiler = QueryProfiler::new();
        profiler.update_table_profile(
            name,
            TableProfileStats::new(rows, rows * 256, 1, 1, SourceType::Federated),
        );
        profiler
    }

    /// Helper: all three engines.
    fn all_engines() -> Vec<String> {
        vec![
            "DataFusion".to_string(),
            "DuckDB".to_string(),
            "Polars".to_string(),
        ]
    }

    // -----------------------------------------------------------------------
    // Engine recommendation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_s3_iceberg_large_table_recommends_duckdb() {
        let profiler = profiler_with_s3_iceberg("events", 1_000_000);
        let profile = profiler.profile_query(
            "SELECT region, COUNT(*) FROM events GROUP BY region",
            &all_engines(),
        );

        assert_eq!(profile.recommended_engine.primary_engine, "DuckDB");
        assert!(
            profile.recommended_engine.confidence > 0.0,
            "confidence should be positive"
        );
        // Strategy should be DirectS3 since DuckDB reads S3 natively.
        assert!(
            matches!(
                profile.recommended_engine.execution_strategy,
                ExecutionStrategy::DirectS3 { .. }
            ),
            "expected DirectS3 strategy for S3 Iceberg table, got {:?}",
            profile.recommended_engine.execution_strategy
        );
    }

    #[test]
    fn test_federated_postgres_recommends_datafusion() {
        let profiler = profiler_with_federated("pg.customers", 50_000);
        let profile = profiler.profile_query(
            "SELECT * FROM pg.customers WHERE country = 'US'",
            &all_engines(),
        );

        assert_eq!(
            profile.recommended_engine.primary_engine, "DataFusion"
        );
        assert!(profile
            .recommended_engine
            .reasoning
            .contains("federated"));
    }

    #[test]
    fn test_mixed_sources_recommends_split_plan() {
        let profiler = QueryProfiler::new();
        profiler.update_table_profile(
            "pg.orders",
            TableProfileStats::new(
                100_000,
                50_000_000,
                1,
                1,
                SourceType::Federated,
            ),
        );
        profiler.update_table_profile(
            "events",
            TableProfileStats::new(
                1_000_000,
                512_000_000,
                64,
                12,
                SourceType::S3Iceberg,
            ),
        );

        let profile = profiler.profile_query(
            "SELECT o.id, e.type FROM pg.orders o JOIN events e ON o.id = e.order_id",
            &all_engines(),
        );

        assert!(
            matches!(
                profile.recommended_engine.execution_strategy,
                ExecutionStrategy::SplitPlan { .. }
            ),
            "expected SplitPlan for mixed federated + S3, got {:?}",
            profile.recommended_engine.execution_strategy
        );

        if let ExecutionStrategy::SplitPlan { fragments } =
            &profile.recommended_engine.execution_strategy
        {
            assert_eq!(
                fragments.len(),
                2,
                "should have one fragment per table"
            );
            // Federated table fragment should use DataFusion.
            let fed_frag =
                fragments.iter().find(|f| f.table == "pg.orders");
            assert!(fed_frag.is_some(), "should have pg.orders fragment");
            assert_eq!(fed_frag.unwrap().engine, "DataFusion");
        }
    }

    // -----------------------------------------------------------------------
    // Selectivity estimation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_selectivity_equality_with_known_cardinality() {
        let profiler = QueryProfiler::new();
        let mut profile = TableProfileStats::new(
            1_000_000,
            512_000_000,
            64,
            12,
            SourceType::S3Iceberg,
        );
        profile.column_stats.insert(
            "ID".to_string(),
            ColumnProfile {
                null_fraction: 0.0,
                distinct_count_estimate: Some(1_000),
                min_value: Some("1".to_string()),
                max_value: Some("1000".to_string()),
            },
        );
        profiler.update_table_profile("events", profile.clone());

        let sel = profiler.estimate_selectivity(
            "SELECT * FROM events WHERE id = 5",
            "events",
            &profile,
        );

        // With 1000 distinct values, equality selectivity = 1/1000 = 0.001
        let expected = 1.0 / 1_000.0;
        assert!(
            (sel - expected).abs() < 1e-9,
            "expected selectivity {}, got {}",
            expected,
            sel
        );
    }

    #[test]
    fn test_selectivity_no_where_clause() {
        let profiler = QueryProfiler::new();
        let profile =
            TableProfileStats::new(100_000, 50_000_000, 10, 4, SourceType::Local);

        let sel = profiler.estimate_selectivity(
            "SELECT * FROM events",
            "events",
            &profile,
        );
        assert!(
            (sel - 1.0).abs() < 1e-9,
            "no WHERE should give selectivity 1.0, got {}",
            sel
        );
    }

    #[test]
    fn test_selectivity_range_predicate() {
        let profiler = QueryProfiler::new();
        let profile =
            TableProfileStats::new(100_000, 50_000_000, 10, 4, SourceType::Local);

        let sel = profiler.estimate_selectivity(
            "SELECT * FROM events WHERE ts > '2024-01-01'",
            "events",
            &profile,
        );
        assert!(
            (sel - 0.33).abs() < 1e-9,
            "range predicate should give 0.33, got {}",
            sel
        );
    }

    #[test]
    fn test_selectivity_like_predicate() {
        let profiler = QueryProfiler::new();
        let profile =
            TableProfileStats::new(100_000, 50_000_000, 10, 4, SourceType::Local);

        let sel = profiler.estimate_selectivity(
            "SELECT * FROM events WHERE name LIKE '%test%'",
            "events",
            &profile,
        );
        assert!(
            (sel - 0.10).abs() < 1e-9,
            "LIKE predicate should give 0.10, got {}",
            sel
        );
    }

    #[test]
    fn test_selectivity_and_combination() {
        let profiler = QueryProfiler::new();
        let mut profile =
            TableProfileStats::new(100_000, 50_000_000, 10, 4, SourceType::Local);
        profile.column_stats.insert(
            "REGION".to_string(),
            ColumnProfile {
                null_fraction: 0.0,
                distinct_count_estimate: Some(10),
                min_value: None,
                max_value: None,
            },
        );

        // WHERE region = 'US' AND ts > '2024-01-01'
        // = (1/10) * 0.33 = 0.033
        let sel = profiler.estimate_selectivity(
            "SELECT * FROM events WHERE region = 'US' AND ts > '2024-01-01'",
            "events",
            &profile,
        );
        let expected = (1.0 / 10.0) * 0.33;
        assert!(
            (sel - expected).abs() < 1e-6,
            "expected {}, got {}",
            expected,
            sel
        );
    }

    #[test]
    fn test_selectivity_or_combination() {
        let profiler = QueryProfiler::new();
        let mut profile =
            TableProfileStats::new(100_000, 50_000_000, 10, 4, SourceType::Local);
        profile.column_stats.insert(
            "STATUS".to_string(),
            ColumnProfile {
                null_fraction: 0.0,
                distinct_count_estimate: Some(5),
                min_value: None,
                max_value: None,
            },
        );

        // WHERE status = 'active' OR status = 'pending'
        // = 1 - (1 - 1/5) * (1 - 1/5) = 1 - 0.8 * 0.8 = 0.36
        let sel = profiler.estimate_selectivity(
            "SELECT * FROM events WHERE status = 'active' OR status = 'pending'",
            "events",
            &profile,
        );
        let expected = 1.0 - (1.0 - 1.0 / 5.0) * (1.0 - 1.0 / 5.0);
        assert!(
            (sel - expected).abs() < 1e-6,
            "expected {}, got {}",
            expected,
            sel
        );
    }

    // -----------------------------------------------------------------------
    // History-based adjustment test
    // -----------------------------------------------------------------------

    #[test]
    fn test_history_based_boost() {
        let profiler = profiler_with_s3_iceberg("events", 100_000);

        // Record history showing DuckDB is 3x faster than DataFusion on S3.
        for _ in 0..10 {
            profiler.record_execution(ExecutionRecord {
                query_hash: 123,
                engine: "DuckDB".to_string(),
                execution_ms: 50,
                rows_scanned: 100_000,
                rows_returned: 1_000,
                bytes_scanned: 50_000_000,
                timestamp: Instant::now(),
                source_types: vec![SourceType::S3Iceberg],
            });
            profiler.record_execution(ExecutionRecord {
                query_hash: 124,
                engine: "DataFusion".to_string(),
                execution_ms: 150,
                rows_scanned: 100_000,
                rows_returned: 1_000,
                bytes_scanned: 50_000_000,
                timestamp: Instant::now(),
                source_types: vec![SourceType::S3Iceberg],
            });
        }

        // Build an AdaptiveQueryProfile for scoring.
        let query = AdaptiveQueryProfile {
            estimated_rows: 100_000,
            estimated_bytes: 50_000_000,
            num_tables: 1,
            has_join: false,
            has_aggregation: true,
            has_sort: false,
            selectivity: 1.0,
            tables: vec!["events".to_string()],
            source_types: vec![SourceType::S3Iceberg],
            recommended_engine: EngineRecommendation {
                primary_engine: String::new(),
                confidence: 0.0,
                reasoning: String::new(),
                execution_strategy: ExecutionStrategy::SingleEngine {
                    engine: String::new(),
                },
                estimated_cost_ms: 0,
            },
        };

        let history = profiler
            .execution_history
            .read().expect("profiler lock poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let duckdb_score =
            profiler.score_engine("DuckDB", &query, &history);
        let df_score =
            profiler.score_engine("DataFusion", &query, &history);

        // DuckDB should win by a clear margin.
        assert!(
            duckdb_score > df_score,
            "DuckDB ({:.1}) should outscore DataFusion ({:.1}) with history boost",
            duckdb_score,
            df_score
        );

        // Verify the history boost was applied.
        let profiler_no_hist =
            profiler_with_s3_iceberg("events", 100_000);
        let empty_history: Vec<ExecutionRecord> = Vec::new();
        let duckdb_no_hist =
            profiler_no_hist.score_engine("DuckDB", &query, &empty_history);

        assert!(
            duckdb_score > duckdb_no_hist,
            "history-boosted score ({:.1}) should exceed no-history score ({:.1})",
            duckdb_score,
            duckdb_no_hist
        );
    }

    // -----------------------------------------------------------------------
    // Table extraction tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_table_names_simple() {
        let tables = QueryProfiler::extract_table_names(
            "SELECT * FROM events WHERE id = 1",
        );
        assert_eq!(tables, vec!["events"]);
    }

    #[test]
    fn test_extract_table_names_join() {
        let tables = QueryProfiler::extract_table_names(
            "SELECT o.id FROM pg.orders o JOIN events e ON o.id = e.order_id",
        );
        assert_eq!(tables, vec!["pg.orders", "events"]);
    }

    #[test]
    fn test_extract_table_names_dedup() {
        let tables = QueryProfiler::extract_table_names(
            "SELECT * FROM events e1 JOIN events e2 ON e1.id = e2.parent_id",
        );
        assert_eq!(tables, vec!["events"]);
    }

    // -----------------------------------------------------------------------
    // Utility tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_hash_sql_deterministic() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 1");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_table_profile_staleness() {
        let mut profile =
            TableProfileStats::new(100, 1000, 1, 1, SourceType::Local);
        assert!(
            !profile.is_stale(),
            "fresh profile should not be stale"
        );

        profile.ttl = Duration::from_nanos(1);
        std::thread::sleep(Duration::from_millis(1));
        assert!(profile.is_stale(), "profile should be stale after TTL");
    }

    #[test]
    fn test_default_profiler() {
        let profiler = QueryProfiler::default();
        assert!(profiler.get_table_profile("nonexistent").is_none());
    }

    #[test]
    fn test_query_profile_fields() {
        let profiler = profiler_with_s3_iceberg("events", 1_000);
        let profile = profiler.profile_query(
            "SELECT region, SUM(amount) FROM events GROUP BY region ORDER BY region",
            &["DataFusion".to_string()],
        );
        assert!(profile.has_aggregation);
        assert!(profile.has_sort);
        assert!(!profile.has_join);
        assert_eq!(profile.num_tables, 1);
    }

    #[test]
    fn test_source_type_display() {
        assert_eq!(format!("{}", SourceType::S3Iceberg), "S3/Iceberg");
        assert_eq!(format!("{}", SourceType::Federated), "Federated");
        assert_eq!(format!("{}", SourceType::Trino), "Trino");
        assert_eq!(format!("{}", SourceType::InMemory), "InMemory");
        assert_eq!(format!("{}", SourceType::Lance), "Lance");
    }
}
