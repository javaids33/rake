//! Executable Lakehouse — self-maintaining Iceberg tables.
//!
//! Tables store compiled transform binaries alongside data in S3.
//! A lightweight scheduler executes binaries to refresh tables without clusters.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use arrow::record_batch::RecordBatch;

/// An executable table definition — a table that knows how to refresh itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutableTable {
    /// Table name in the catalog.
    pub table_name: String,
    /// S3 location of the table data.
    pub table_location: String,
    /// The transform that produces this table.
    pub transform: TableTransform,
    /// Refresh schedule (cron expression). None = manual only.
    pub schedule: Option<String>,
    /// Quality gates that must pass before committing new data.
    pub quality_gates: Vec<QualityGateRef>,
    /// Input tables this transform reads from.
    pub input_tables: Vec<String>,
    /// Current status.
    pub status: ExecutableTableStatus,
    /// Execution history.
    pub history: Vec<ExecutionRecord>,
    /// Version history tracking code changes.
    #[serde(default)]
    pub versions: Vec<TransformVersion>,
    /// When this executable table was created.
    pub created_at: String,
    /// Last successful refresh.
    pub last_refresh: Option<String>,
    /// Next scheduled refresh.
    pub next_refresh: Option<String>,
    /// Estimated cost per execution (USD).
    pub estimated_cost_usd: f64,
    /// Total executions.
    pub total_executions: u64,
    /// Total cost (USD).
    pub total_cost_usd: f64,
    /// Whether this transform supports incremental execution.
    #[serde(default)]
    pub incremental: bool,
    /// Column to use as watermark for incremental processing.
    #[serde(default)]
    pub watermark_column: Option<String>,
    /// Last processed watermark value (ISO timestamp or row ID).
    #[serde(default)]
    pub last_watermark: Option<String>,
    /// Number of executions skipped (e.g. due to no upstream changes).
    #[serde(default)]
    pub executions_skipped: u64,
    /// Total cost saved by skipping executions (USD).
    #[serde(default)]
    pub cost_saved_usd: f64,
    /// Whether automatic refresh is enabled.
    #[serde(default)]
    pub auto_refresh: bool,
    /// Interval in seconds between automatic refreshes.
    #[serde(default)]
    pub refresh_interval_seconds: u64,
}

/// The transform logic that produces the table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTransform {
    /// Transform type: "sql", "rust", "notebook", "python".
    pub transform_type: String,
    /// Source code of the transform.
    pub source_code: String,
    /// Hash of the source code (for binary cache lookup).
    pub source_hash: String,
    /// S3 path to the compiled binary (for Rust/notebook transforms).
    pub binary_path: Option<String>,
    /// Binary size in bytes.
    pub binary_size: Option<u64>,
    /// Whether the binary is cached on S3.
    pub binary_cached: bool,
    /// Compiler version used.
    pub compiler_version: Option<String>,
    /// Target architecture.
    pub target_arch: Option<String>,
}

/// Reference to a quality gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityGateRef {
    pub gate_type: String, // "not_null", "unique", "range", "row_count", "custom_sql"
    pub column: Option<String>,
    pub threshold: Option<f64>,
    pub description: String,
}

/// Status of an executable table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutableTableStatus {
    pub state: String, // "active", "stale", "error", "refreshing", "disabled"
    pub health: String, // "healthy", "warning", "critical"
    pub last_error: Option<String>,
    pub staleness_hours: f64,
    pub data_freshness: String, // "fresh", "stale", "unknown"
}

/// A single execution record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub execution_id: String,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub duration_ms: u64,
    pub status: String, // "success", "failed", "timeout"
    pub rows_produced: Option<u64>,
    pub bytes_written: Option<u64>,
    pub cost_usd: f64,
    pub binary_cached: bool,
    pub compile_ms: u64,
    pub run_ms: u64,
    pub error: Option<String>,
    /// Where the binary executed: "local", "lambda", "spot", "edge"
    pub execution_location: String,
    /// Which code version was running when this execution happened.
    #[serde(default = "default_version")]
    pub version: u32,
}

/// Cost comparison between RustLake executable tables and traditional approaches.
#[derive(Debug, Clone, Serialize)]
pub struct CostComparison {
    pub rustlake: CostEstimate,
    pub databricks: CostEstimate,
    pub snowflake: CostEstimate,
    pub lambda: CostEstimate,
}

#[derive(Debug, Clone, Serialize)]
pub struct CostEstimate {
    pub platform: String,
    pub cost_per_execution_usd: f64,
    pub monthly_cost_usd: f64, // assuming hourly schedule
    pub cold_start_ms: u64,
    pub execution_ms: u64,
    pub always_on: bool,
    pub cluster_required: bool,
}

/// Estimate costs for an executable table across platforms.
pub fn estimate_costs(
    binary_size_kb: u64,
    execution_ms: u64,
    executions_per_day: u32,
) -> CostComparison {
    let monthly_runs = executions_per_day as f64 * 30.0;

    CostComparison {
        rustlake: CostEstimate {
            platform: "RustLake (cached binary)".into(),
            cost_per_execution_usd: 0.000001 * execution_ms as f64, // ~$0.001/sec compute
            monthly_cost_usd: 0.000001 * execution_ms as f64 * monthly_runs
                + 0.023 * (binary_size_kb as f64 / 1024.0 / 1024.0), // compute + S3 storage
            cold_start_ms: 2, // cached binary
            execution_ms,
            always_on: false,
            cluster_required: false,
        },
        databricks: CostEstimate {
            platform: "Databricks (jobs cluster)".into(),
            cost_per_execution_usd: 0.07 * (execution_ms as f64 / 1000.0 / 60.0).max(1.0), // min 1 min DBU
            monthly_cost_usd: 0.07 * monthly_runs, // $0.07/DBU-min minimum
            cold_start_ms: 45000,                   // 45s cluster start
            execution_ms: execution_ms + 5000,      // JVM overhead
            always_on: false,
            cluster_required: true,
        },
        snowflake: CostEstimate {
            platform: "Snowflake (X-Small warehouse)".into(),
            cost_per_execution_usd: 2.0 / 60.0, // $2/credit, min 1 min
            monthly_cost_usd: (2.0 / 60.0) * monthly_runs,
            cold_start_ms: 5000,
            execution_ms: execution_ms + 2000, // warehouse overhead
            always_on: false,
            cluster_required: true,
        },
        lambda: CostEstimate {
            platform: "AWS Lambda (RustLake binary)".into(),
            cost_per_execution_usd: 0.0000002
                + 0.0000166667 * (execution_ms as f64 / 1000.0) * 0.125, // 128MB
            monthly_cost_usd: (0.0000002
                + 0.0000166667 * (execution_ms as f64 / 1000.0) * 0.125)
                * monthly_runs,
            cold_start_ms: 100, // download 453KB binary
            execution_ms,
            always_on: false,
            cluster_required: false,
        },
    }
}

/// Generate Iceberg table properties for an executable table.
pub fn to_iceberg_properties(table: &ExecutableTable) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("rustlake.executable".into(), "true".into());
    props.insert(
        "rustlake.transform.type".into(),
        table.transform.transform_type.clone(),
    );
    props.insert(
        "rustlake.transform.source-hash".into(),
        table.transform.source_hash.clone(),
    );
    if let Some(ref bp) = table.transform.binary_path {
        props.insert("rustlake.transform.binary-path".into(), bp.clone());
    }
    if let Some(ref sched) = table.schedule {
        props.insert("rustlake.schedule".into(), sched.clone());
    }
    for (i, gate) in table.quality_gates.iter().enumerate() {
        props.insert(
            format!("rustlake.quality-gate.{}.type", i),
            gate.gate_type.clone(),
        );
        if let Some(ref col) = gate.column {
            props.insert(format!("rustlake.quality-gate.{}.column", i), col.clone());
        }
        props.insert(
            format!("rustlake.quality-gate.{}.description", i),
            gate.description.clone(),
        );
    }
    for (i, input) in table.input_tables.iter().enumerate() {
        props.insert(format!("rustlake.input-table.{}", i), input.clone());
    }
    props.insert(
        "rustlake.total-executions".into(),
        table.total_executions.to_string(),
    );
    props.insert(
        "rustlake.total-cost-usd".into(),
        format!("{:.6}", table.total_cost_usd),
    );
    props.insert(
        "rustlake.estimated-cost-per-run".into(),
        format!("{:.6}", table.estimated_cost_usd),
    );
    props
}

/// Parse executable table info from Iceberg properties.
pub fn from_iceberg_properties(
    props: &HashMap<String, String>,
    table_name: &str,
) -> Option<ExecutableTable> {
    if props.get("rustlake.executable")?.as_str() != "true" {
        return None;
    }

    let transform_type = props.get("rustlake.transform.type")?.clone();
    let source_hash = props
        .get("rustlake.transform.source-hash")
        .cloned()
        .unwrap_or_default();
    let binary_path = props.get("rustlake.transform.binary-path").cloned();

    Some(ExecutableTable {
        table_name: table_name.to_string(),
        table_location: String::new(),
        transform: TableTransform {
            transform_type,
            source_code: String::new(),
            source_hash,
            binary_path: binary_path.clone(),
            binary_size: None,
            binary_cached: binary_path.is_some(),
            compiler_version: None,
            target_arch: None,
        },
        schedule: props.get("rustlake.schedule").cloned(),
        quality_gates: Vec::new(),
        input_tables: Vec::new(),
        status: ExecutableTableStatus {
            state: "active".into(),
            health: "healthy".into(),
            last_error: None,
            staleness_hours: 0.0,
            data_freshness: "unknown".into(),
        },
        history: Vec::new(),
        versions: Vec::new(),
        created_at: chrono::Utc::now().to_rfc3339(),
        last_refresh: None,
        next_refresh: None,
        estimated_cost_usd: props
            .get("rustlake.estimated-cost-per-run")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0),
        total_executions: props
            .get("rustlake.total-executions")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        total_cost_usd: props
            .get("rustlake.total-cost-usd")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0),
        incremental: false,
        watermark_column: None,
        last_watermark: None,
        executions_skipped: 0,
        cost_saved_usd: 0.0,
        auto_refresh: false,
        refresh_interval_seconds: 0,
    })
}

// ── Code-Data Provenance (Binary Time Travel) ───────────────────

/// A snapshot of a transform at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformVersion {
    /// Version number (increments on each code change).
    pub version: u32,
    /// Source code at this version.
    pub source_code: String,
    /// Source hash for binary cache lookup.
    pub source_hash: String,
    /// When this version was created.
    pub created_at: String,
    /// Who created this version (user ID or "system").
    pub created_by: String,
    /// Change description.
    pub change_description: String,
    /// Binary size at this version.
    pub binary_size_bytes: Option<u64>,
    /// Which Iceberg snapshots were produced by this version.
    pub snapshot_ids: Vec<i64>,
}

/// Diff between two transform versions.
#[derive(Debug, Clone, Serialize)]
pub struct TransformDiff {
    pub from_version: u32,
    pub to_version: u32,
    pub from_hash: String,
    pub to_hash: String,
    pub lines_added: usize,
    pub lines_removed: usize,
    pub lines_changed: usize,
    pub diff_lines: Vec<DiffLine>,
    /// Whether the output changed significantly.
    pub output_regression: Option<RegressionResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffLine {
    pub line_number: usize,
    pub change_type: String, // "added", "removed", "unchanged"
    pub content: String,
}

/// Result of regression detection between two transform versions.
#[derive(Debug, Clone, Serialize)]
pub struct RegressionResult {
    pub has_regression: bool,
    pub severity: String, // "none", "minor", "major", "critical"
    pub metrics: Vec<RegressionMetric>,
    pub recommendation: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegressionMetric {
    pub metric_name: String,
    pub old_value: f64,
    pub new_value: f64,
    pub change_pct: f64,
    pub is_regression: bool,
}

/// Full provenance chain for a table.
#[derive(Debug, Clone, Serialize)]
pub struct ProvenanceChain {
    pub table_name: String,
    pub total_versions: usize,
    pub total_executions: u64,
    pub total_snapshots: usize,
    pub versions: Vec<TransformVersion>,
    pub timeline: Vec<ProvenanceEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProvenanceEvent {
    pub timestamp: String,
    pub event_type: String, // "code_change", "execution", "regression_detected", "rollback"
    pub version: u32,
    pub description: String,
    pub source_hash: String,
}

/// Compute a line-by-line diff between two source code versions.
pub fn diff_transforms(old_source: &str, new_source: &str) -> (Vec<DiffLine>, usize, usize, usize) {
    let old_lines: Vec<&str> = old_source.lines().collect();
    let new_lines: Vec<&str> = new_source.lines().collect();

    let mut diff = Vec::new();
    let mut added = 0usize;
    let mut removed = 0usize;
    let mut changed = 0usize;

    let max_len = old_lines.len().max(new_lines.len());

    for i in 0..max_len {
        let old_line = old_lines.get(i).copied();
        let new_line = new_lines.get(i).copied();

        match (old_line, new_line) {
            (Some(old), Some(new)) if old == new => {
                diff.push(DiffLine {
                    line_number: i + 1,
                    change_type: "unchanged".to_string(),
                    content: old.to_string(),
                });
            }
            (Some(old), Some(new)) => {
                diff.push(DiffLine {
                    line_number: i + 1,
                    change_type: "removed".to_string(),
                    content: old.to_string(),
                });
                diff.push(DiffLine {
                    line_number: i + 1,
                    change_type: "added".to_string(),
                    content: new.to_string(),
                });
                changed += 1;
            }
            (Some(old), None) => {
                diff.push(DiffLine {
                    line_number: i + 1,
                    change_type: "removed".to_string(),
                    content: old.to_string(),
                });
                removed += 1;
            }
            (None, Some(new)) => {
                diff.push(DiffLine {
                    line_number: i + 1,
                    change_type: "added".to_string(),
                    content: new.to_string(),
                });
                added += 1;
            }
            (None, None) => {}
        }
    }

    (diff, added, removed, changed)
}

/// Detect regressions between two execution outputs.
pub fn detect_regression(
    old_rows: Option<u64>,
    new_rows: Option<u64>,
    old_duration_ms: u64,
    new_duration_ms: u64,
    old_cost: f64,
    new_cost: f64,
) -> RegressionResult {
    let mut metrics = Vec::new();
    let mut has_regression = false;
    let mut max_severity = "none";

    // Row count regression
    if let (Some(old_r), Some(new_r)) = (old_rows, new_rows) {
        let old_f = old_r as f64;
        let new_f = new_r as f64;
        if old_f > 0.0 {
            let change_pct = ((new_f - old_f) / old_f) * 100.0;
            let is_reg = change_pct < -10.0; // >10% fewer rows = regression
            if is_reg {
                has_regression = true;
                max_severity = "major";
            }
            metrics.push(RegressionMetric {
                metric_name: "row_count".to_string(),
                old_value: old_f,
                new_value: new_f,
                change_pct,
                is_regression: is_reg,
            });
        }
    }

    // Duration regression
    if old_duration_ms > 0 {
        let change_pct = ((new_duration_ms as f64 - old_duration_ms as f64)
            / old_duration_ms as f64)
            * 100.0;
        let is_reg = change_pct > 100.0; // 2x slower = regression
        if is_reg {
            has_regression = true;
            if max_severity == "none" {
                max_severity = "minor";
            }
        }
        metrics.push(RegressionMetric {
            metric_name: "duration_ms".to_string(),
            old_value: old_duration_ms as f64,
            new_value: new_duration_ms as f64,
            change_pct,
            is_regression: is_reg,
        });
    }

    // Cost regression
    if old_cost > 0.0 {
        let change_pct = ((new_cost - old_cost) / old_cost) * 100.0;
        let is_reg = change_pct > 50.0; // 50% more expensive = regression
        if is_reg {
            has_regression = true;
            if max_severity == "none" {
                max_severity = "minor";
            }
        }
        metrics.push(RegressionMetric {
            metric_name: "cost_usd".to_string(),
            old_value: old_cost,
            new_value: new_cost,
            change_pct,
            is_regression: is_reg,
        });
    }

    // Zero rows = critical
    if new_rows == Some(0) && old_rows.map(|r| r > 0).unwrap_or(false) {
        has_regression = true;
        max_severity = "critical";
    }

    let recommendation = if !has_regression {
        "No regression detected. Safe to deploy.".to_string()
    } else {
        match max_severity {
            "critical" => "CRITICAL: Transform produces zero rows. Do NOT deploy. Investigate immediately.".to_string(),
            "major" => "MAJOR: Significant row count change detected. Review transform logic before deploying.".to_string(),
            "minor" => "MINOR: Performance regression detected. Consider optimizing before deploying.".to_string(),
            _ => "Review changes before deploying.".to_string(),
        }
    };

    RegressionResult {
        has_regression,
        severity: max_severity.to_string(),
        metrics,
        recommendation,
    }
}

/// FNV-1a hash for source code — fast, good distribution for cache keys.
pub fn hash_source(code: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in code.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}

fn default_version() -> u32 { 1 }

// ── Quality Gate Validation on Execute ──────────────────────────

/// Result of validating a single quality gate against data.
#[derive(Debug, Clone, Serialize)]
pub struct GateResult {
    pub gate_type: String,
    pub column: Option<String>,
    pub passed: bool,
    pub detail: String,
}

/// Validate quality gates against produced RecordBatches.
///
/// Maps each `QualityGateRef` to the appropriate `QualityCheck`, runs
/// `validate_batches`, and collects per-gate results.
pub fn validate_gates(gates: &[QualityGateRef], batches: &[RecordBatch]) -> Vec<GateResult> {
    use crate::quality_gates::{QualityCheck, QualityGate};

    let mut results = Vec::new();

    for gate_ref in gates {
        let check = match gate_ref.gate_type.as_str() {
            "not_null" => {
                if let Some(ref col) = gate_ref.column {
                    Some(QualityCheck::NotNull { column: col.clone() })
                } else {
                    results.push(GateResult {
                        gate_type: gate_ref.gate_type.clone(),
                        column: None,
                        passed: false,
                        detail: "not_null gate requires a column".to_string(),
                    });
                    continue;
                }
            }
            "unique" => {
                if let Some(ref col) = gate_ref.column {
                    Some(QualityCheck::Unique { column: col.clone() })
                } else {
                    results.push(GateResult {
                        gate_type: gate_ref.gate_type.clone(),
                        column: None,
                        passed: false,
                        detail: "unique gate requires a column".to_string(),
                    });
                    continue;
                }
            }
            "range" => {
                if let Some(ref col) = gate_ref.column {
                    let max = gate_ref.threshold.unwrap_or(f64::MAX);
                    Some(QualityCheck::Range { column: col.clone(), min: 0.0, max })
                } else {
                    results.push(GateResult {
                        gate_type: gate_ref.gate_type.clone(),
                        column: None,
                        passed: false,
                        detail: "range gate requires a column".to_string(),
                    });
                    continue;
                }
            }
            "row_count" => {
                let min = gate_ref.threshold.unwrap_or(1.0) as usize;
                Some(QualityCheck::RowCountMin { min })
            }
            "custom_sql" => {
                // Custom SQL needs a query engine context — skip with a note.
                results.push(GateResult {
                    gate_type: gate_ref.gate_type.clone(),
                    column: gate_ref.column.clone(),
                    passed: true,
                    detail: "custom_sql gate skipped (requires query context)".to_string(),
                });
                continue;
            }
            _ => {
                results.push(GateResult {
                    gate_type: gate_ref.gate_type.clone(),
                    column: gate_ref.column.clone(),
                    passed: true,
                    detail: format!("Unknown gate type '{}' — skipped", gate_ref.gate_type),
                });
                continue;
            }
        };

        if let Some(check) = check {
            let gate = QualityGate {
                id: format!("exec-gate-{}", gate_ref.gate_type),
                table: String::new(),
                checks: vec![check],
                enabled: true,
            };
            let validation = crate::quality_gates::validate_batches(&gate, batches);
            if validation.passed {
                results.push(GateResult {
                    gate_type: gate_ref.gate_type.clone(),
                    column: gate_ref.column.clone(),
                    passed: true,
                    detail: format!("{} — passed ({} rows checked)", gate_ref.description, validation.rows_checked),
                });
            } else {
                let failure_msgs: Vec<String> = validation.failures.iter()
                    .map(|f| f.message.clone())
                    .collect();
                results.push(GateResult {
                    gate_type: gate_ref.gate_type.clone(),
                    column: gate_ref.column.clone(),
                    passed: false,
                    detail: failure_msgs.join("; "),
                });
            }
        }
    }

    results
}

// ── Data-Level Regression Detection ─────────────────────────────

/// Detect data-level regressions by comparing old and new RecordBatch outputs.
///
/// Checks for:
/// 1. Schema drift — columns removed in the new output.
/// 2. NULL increase — per-column: old < 5% nulls but new > 20% nulls.
/// 3. Cardinality drop — per-column: distinct count drops > 50%.
pub fn detect_data_regression(
    old_batches: &[RecordBatch],
    new_batches: &[RecordBatch],
) -> Vec<RegressionMetric> {
    let mut metrics = Vec::new();

    if old_batches.is_empty() || new_batches.is_empty() {
        return metrics;
    }

    let old_schema = old_batches[0].schema();
    let new_schema = new_batches[0].schema();

    // 1. Schema drift — check for removed columns
    let old_fields: Vec<&str> = old_schema.fields().iter().map(|f| f.name().as_str()).collect();
    let new_fields: Vec<&str> = new_schema.fields().iter().map(|f| f.name().as_str()).collect();

    let removed: Vec<&&str> = old_fields.iter().filter(|f| !new_fields.contains(f)).collect();
    if !removed.is_empty() {
        metrics.push(RegressionMetric {
            metric_name: "schema_drift".to_string(),
            old_value: old_fields.len() as f64,
            new_value: new_fields.len() as f64,
            change_pct: -((removed.len() as f64 / old_fields.len() as f64) * 100.0),
            is_regression: true,
        });
    }

    let old_total_rows: usize = old_batches.iter().map(|b| b.num_rows()).sum();
    let new_total_rows: usize = new_batches.iter().map(|b| b.num_rows()).sum();

    // Only check columns present in both schemas
    for col_name in &old_fields {
        if !new_fields.contains(col_name) {
            continue;
        }

        // 2. NULL increase
        if old_total_rows > 0 && new_total_rows > 0 {
            let old_nulls = count_nulls_across_batches(old_batches, col_name);
            let new_nulls = count_nulls_across_batches(new_batches, col_name);

            let old_null_pct = old_nulls as f64 / old_total_rows as f64 * 100.0;
            let new_null_pct = new_nulls as f64 / new_total_rows as f64 * 100.0;

            if old_null_pct < 5.0 && new_null_pct > 20.0 {
                metrics.push(RegressionMetric {
                    metric_name: format!("null_increase.{}", col_name),
                    old_value: old_null_pct,
                    new_value: new_null_pct,
                    change_pct: new_null_pct - old_null_pct,
                    is_regression: true,
                });
            }
        }

        // 3. Cardinality drop
        let old_distinct = count_distinct_across_batches(old_batches, col_name);
        let new_distinct = count_distinct_across_batches(new_batches, col_name);

        if old_distinct > 0 {
            let drop_pct = ((old_distinct as f64 - new_distinct as f64) / old_distinct as f64) * 100.0;
            if drop_pct > 50.0 {
                metrics.push(RegressionMetric {
                    metric_name: format!("cardinality_drop.{}", col_name),
                    old_value: old_distinct as f64,
                    new_value: new_distinct as f64,
                    change_pct: -drop_pct,
                    is_regression: true,
                });
            }
        }
    }

    metrics
}

/// Count null values for a column across multiple RecordBatches.
fn count_nulls_across_batches(batches: &[RecordBatch], col: &str) -> usize {
    let mut nulls = 0;
    for batch in batches {
        if let Ok(idx) = batch.schema().index_of(col) {
            nulls += batch.column(idx).null_count();
        }
    }
    nulls
}

/// Count distinct non-null values for a column across multiple RecordBatches.
fn count_distinct_across_batches(batches: &[RecordBatch], col: &str) -> usize {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    for batch in batches {
        if let Ok(idx) = batch.schema().index_of(col) {
            let array = batch.column(idx);
            for i in 0..array.len() {
                if !array.is_null(i) {
                    let val = arrow::util::display::array_value_to_string(array, i)
                        .unwrap_or_default();
                    seen.insert(val);
                }
            }
        }
    }
    seen.len()
}

// ── A/B Testing ─────────────────────────────────────────────────

/// Result of an A/B test between two transform versions.
#[derive(Debug, Clone, Serialize)]
pub struct ABTestResult {
    pub table_name: String,
    pub version_a: u32,
    pub version_b: u32,
    pub version_a_metrics: ABVersionMetrics,
    pub version_b_metrics: ABVersionMetrics,
    pub comparison: ABComparison,
    pub winner: String,
    pub confidence: f64,
    pub recommendation: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ABVersionMetrics {
    pub version: u32,
    pub rows_produced: u64,
    pub duration_ms: u64,
    pub cost_usd: f64,
    pub schema_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ABComparison {
    pub row_count_diff: i64,
    pub row_count_pct: f64,
    pub schema_match: bool,
    pub columns_added: Vec<String>,
    pub columns_removed: Vec<String>,
    pub duration_diff_ms: i64,
    pub cost_diff_usd: f64,
    pub data_regressions: Vec<RegressionMetric>,
}

/// Compare outputs from two A/B test executions.
pub fn compare_ab_outputs(
    a_batches: &[RecordBatch],
    b_batches: &[RecordBatch],
    a_duration_ms: u64,
    b_duration_ms: u64,
    a_cost: f64,
    b_cost: f64,
) -> ABComparison {
    let a_rows: u64 = a_batches.iter().map(|b| b.num_rows() as u64).sum();
    let b_rows: u64 = b_batches.iter().map(|b| b.num_rows() as u64).sum();

    let a_cols: Vec<String> = if a_batches.is_empty() {
        Vec::new()
    } else {
        a_batches[0].schema().fields().iter().map(|f| f.name().clone()).collect()
    };
    let b_cols: Vec<String> = if b_batches.is_empty() {
        Vec::new()
    } else {
        b_batches[0].schema().fields().iter().map(|f| f.name().clone()).collect()
    };

    let columns_added: Vec<String> = b_cols.iter().filter(|c| !a_cols.contains(c)).cloned().collect();
    let columns_removed: Vec<String> = a_cols.iter().filter(|c| !b_cols.contains(c)).cloned().collect();

    let row_count_diff = b_rows as i64 - a_rows as i64;
    let row_count_pct = if a_rows > 0 {
        (row_count_diff as f64 / a_rows as f64) * 100.0
    } else {
        0.0
    };

    let data_regressions = detect_data_regression(a_batches, b_batches);

    ABComparison {
        row_count_diff,
        row_count_pct,
        schema_match: columns_added.is_empty() && columns_removed.is_empty(),
        columns_added,
        columns_removed,
        duration_diff_ms: b_duration_ms as i64 - a_duration_ms as i64,
        cost_diff_usd: b_cost - a_cost,
        data_regressions,
    }
}

// ── Data Contracts ──────────────────────────────────────────────

/// A data contract between a producer and consumer executable table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    pub id: String,
    pub producer_table: String,
    pub consumer_tables: Vec<String>,
    pub schema_checks: Vec<SchemaCheck>,
    pub freshness_sla_hours: Option<f64>,
    pub quality_gates: Vec<String>,
    pub status: String,
    pub last_validated: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaCheck {
    pub column: String,
    pub data_type: String,
    pub nullable: bool,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractValidationResult {
    pub contract_id: String,
    pub passed: bool,
    pub violations: Vec<ContractViolation>,
    pub validated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractViolation {
    pub check_type: String,
    pub column: String,
    pub expected: String,
    pub actual: String,
}

/// Validate a data contract against actual RecordBatch output.
pub fn validate_contract(
    contract: &DataContract,
    batches: &[RecordBatch],
) -> ContractValidationResult {
    let mut violations = Vec::new();

    if batches.is_empty() {
        return ContractValidationResult {
            contract_id: contract.id.clone(),
            passed: false,
            violations: vec![ContractViolation {
                check_type: "no_data".to_string(),
                column: "*".to_string(),
                expected: "at least 1 batch".to_string(),
                actual: "0 batches".to_string(),
            }],
            validated_at: chrono::Utc::now().to_rfc3339(),
        };
    }

    let schema = batches[0].schema();
    for check in &contract.schema_checks {
        match schema.field_with_name(&check.column) {
            Ok(field) => {
                // Check data type
                let actual_type = format!("{:?}", field.data_type());
                if !actual_type.to_lowercase().contains(&check.data_type.to_lowercase()) {
                    violations.push(ContractViolation {
                        check_type: "type_mismatch".to_string(),
                        column: check.column.clone(),
                        expected: check.data_type.clone(),
                        actual: actual_type,
                    });
                }
                // Check nullable
                if check.nullable != field.is_nullable() {
                    violations.push(ContractViolation {
                        check_type: "nullable_mismatch".to_string(),
                        column: check.column.clone(),
                        expected: format!("nullable={}", check.nullable),
                        actual: format!("nullable={}", field.is_nullable()),
                    });
                }
            }
            Err(_) => {
                if check.required {
                    violations.push(ContractViolation {
                        check_type: "missing_column".to_string(),
                        column: check.column.clone(),
                        expected: "column exists".to_string(),
                        actual: "column not found".to_string(),
                    });
                }
            }
        }
    }

    ContractValidationResult {
        contract_id: contract.id.clone(),
        passed: violations.is_empty(),
        violations,
        validated_at: chrono::Utc::now().to_rfc3339(),
    }
}

// ── Transform Marketplace ───────────────────────────────────────

/// A marketplace package — a shareable executable table definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplacePackage {
    pub id: String,
    pub name: String,
    pub description: String,
    pub author: String,
    pub version: String,
    pub table_definition: ExecutableTable,
    pub tags: Vec<String>,
    pub category: String,
    pub install_count: u64,
    pub published_at: String,
}

// ── Feature 2: Column-Level Lineage ────────────────────────────

/// A single column lineage entry mapping output to source.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnLineageEntry {
    pub output_column: String,
    pub source_table: Option<String>,
    pub source_column: Option<String>,
    pub transform_expression: String,
}

/// Parse SQL to extract column-level lineage.
/// Extracts SELECT expressions and maps aliases to source table.column references.
pub fn parse_sql_column_lineage(sql: &str, input_tables: &[String]) -> Vec<ColumnLineageEntry> {
    let mut entries = Vec::new();
    let sql_upper = sql.to_uppercase();

    // Find SELECT ... FROM boundary
    let select_pos = sql_upper.find("SELECT").unwrap_or(0) + 6;
    // Skip DISTINCT if present
    let after_select = sql_upper[select_pos..].trim_start();
    let offset = if after_select.starts_with("DISTINCT") {
        select_pos + sql_upper[select_pos..].find("DISTINCT").unwrap_or(0) + 8
    } else {
        select_pos
    };
    let from_pos = sql_upper.find(" FROM ").unwrap_or(sql.len());
    if offset >= from_pos {
        return entries;
    }

    let select_clause = &sql[offset..from_pos];

    // Determine default table from input_tables
    let default_table = input_tables.first().cloned();

    // Split on commas (respecting parentheses depth)
    let columns = split_select_columns(select_clause);

    for col_expr in columns {
        let col_expr = col_expr.trim();
        if col_expr.is_empty() || col_expr == "*" {
            if col_expr == "*" {
                entries.push(ColumnLineageEntry {
                    output_column: "*".to_string(),
                    source_table: default_table.clone(),
                    source_column: Some("*".to_string()),
                    transform_expression: "*".to_string(),
                });
            }
            continue;
        }

        // Check for alias: `expr AS alias` or `expr alias`
        let (expr, alias) = extract_alias(col_expr);

        // Determine output column name
        let output_column = alias.unwrap_or_else(|| {
            // Use the last identifier in the expression
            expr.split('.').last().unwrap_or(expr).trim().to_string()
        });

        // Parse source: look for table.column pattern
        let (source_table, source_column) = parse_source_reference(expr, input_tables, &default_table);

        entries.push(ColumnLineageEntry {
            output_column,
            source_table,
            source_column,
            transform_expression: expr.trim().to_string(),
        });
    }

    entries
}

/// Split SELECT column expressions respecting parentheses.
fn split_select_columns(clause: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, ch) in clause.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                result.push(&clause[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&clause[start..]);
    result
}

/// Extract alias from an expression like `expr AS alias`.
fn extract_alias(expr: &str) -> (&str, Option<String>) {
    let upper = expr.to_uppercase();
    // Look for " AS " (case insensitive)
    if let Some(pos) = upper.rfind(" AS ") {
        let alias = expr[pos + 4..].trim().trim_matches('"').trim_matches('`').to_string();
        return (&expr[..pos], Some(alias));
    }
    (expr, None)
}

/// Parse a source reference like `table.column` or just `column`.
fn parse_source_reference(expr: &str, input_tables: &[String], default_table: &Option<String>) -> (Option<String>, Option<String>) {
    let trimmed = expr.trim();

    // Check if it's a function call like SUM(x), COUNT(*), etc.
    let has_function = trimmed.contains('(');
    if has_function {
        // Extract column references inside function
        if let Some(start) = trimmed.find('(') {
            if let Some(end) = trimmed.rfind(')') {
                let inner = &trimmed[start + 1..end];
                if inner == "*" {
                    return (default_table.clone(), Some("*".to_string()));
                }
                // Recurse on inner expression
                let (tbl, col) = parse_source_reference(inner, input_tables, default_table);
                return (tbl, col);
            }
        }
        return (default_table.clone(), None);
    }

    // Check for table.column pattern
    if let Some(dot_pos) = trimmed.find('.') {
        let table_part = trimmed[..dot_pos].trim();
        let col_part = trimmed[dot_pos + 1..].trim();
        // Check if table_part matches any input table
        let matched_table = input_tables.iter().find(|t| {
            t.eq_ignore_ascii_case(table_part) || t.ends_with(&format!(".{}", table_part))
        });
        return (
            Some(matched_table.map(|t| t.clone()).unwrap_or_else(|| table_part.to_string())),
            Some(col_part.to_string()),
        );
    }

    // Plain column name
    (default_table.clone(), Some(trimmed.to_string()))
}

// ── Feature 1: Upstream Cascade Replay ─────────────────────────

/// Build a dependency DAG from executable tables' input_tables.
pub fn build_dependency_dag(tables: &[ExecutableTable]) -> HashMap<String, Vec<String>> {
    let mut dag: HashMap<String, Vec<String>> = HashMap::new();
    for t in tables {
        dag.entry(t.table_name.clone()).or_default();
        for input in &t.input_tables {
            dag.entry(input.clone()).or_default();
            dag.entry(t.table_name.clone())
                .or_default()
                .push(input.clone());
        }
    }
    dag
}

/// Topologically sort upstream dependencies of a target using Kahn's algorithm.
/// Returns execution order (roots first, target last).
pub fn topological_sort_upstream(dag: &HashMap<String, Vec<String>>, target: &str) -> Vec<String> {
    // Collect all upstream nodes via BFS
    let mut upstream: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    queue.push_back(target.to_string());
    upstream.insert(target.to_string());

    while let Some(node) = queue.pop_front() {
        if let Some(deps) = dag.get(&node) {
            for dep in deps {
                if upstream.insert(dep.clone()) {
                    queue.push_back(dep.clone());
                }
            }
        }
    }

    // Build sub-DAG with only upstream nodes
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut reverse_edges: HashMap<String, Vec<String>> = HashMap::new();
    for node in &upstream {
        in_degree.entry(node.clone()).or_insert(0);
        if let Some(deps) = dag.get(node) {
            for dep in deps {
                if upstream.contains(dep) {
                    reverse_edges.entry(dep.clone()).or_default().push(node.clone());
                    *in_degree.entry(node.clone()).or_insert(0) += 1;
                }
            }
        }
    }

    // Kahn's algorithm
    let mut result = Vec::new();
    let mut q: std::collections::VecDeque<String> = in_degree.iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(n, _)| n.clone())
        .collect();

    while let Some(node) = q.pop_front() {
        result.push(node.clone());
        if let Some(dependents) = reverse_edges.get(&node) {
            for dep in dependents {
                if let Some(deg) = in_degree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        q.push_back(dep.clone());
                    }
                }
            }
        }
    }

    result
}

/// Result of a cascade replay operation.
#[derive(Debug, Clone, Serialize)]
pub struct CascadeReplayResult {
    pub target: String,
    pub total_tables: usize,
    pub total_duration_ms: u64,
    pub results: Vec<CascadeNodeResult>,
    pub all_gates_passed: bool,
    pub all_contracts_valid: bool,
}

/// Result of executing a single node in a cascade replay.
#[derive(Debug, Clone, Serialize)]
pub struct CascadeNodeResult {
    pub table_name: String,
    pub version: u32,
    pub rows: u64,
    pub duration_ms: u64,
    pub gates_passed: bool,
    pub gate_results: Vec<GateResult>,
    pub contracts_validated: bool,
    pub status: String,
    pub error: Option<String>,
}

// ── Feature 4: Executable Pipelines ────────────────────────────

/// A named pipeline = ordered chain of executable tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutablePipeline {
    pub id: String,
    pub name: String,
    pub stages: Vec<PipelineStage>,
    pub status: String,
    pub last_run: Option<String>,
    pub total_runs: u64,
}

/// A single stage in an executable pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStage {
    pub table_name: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub gate_required: bool,
    #[serde(default)]
    pub contract_required: bool,
}

/// Result of running an executable pipeline.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineRunResult {
    pub pipeline_id: String,
    pub pipeline_name: String,
    pub status: String,
    pub total_duration_ms: u64,
    pub stages: Vec<PipelineStageResult>,
}

/// Result of a single pipeline stage execution.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineStageResult {
    pub table_name: String,
    pub status: String,
    pub rows: u64,
    pub duration_ms: u64,
    pub gate_results: Vec<GateResult>,
    pub gates_passed: bool,
    pub contract_valid: bool,
    pub error: Option<String>,
}

// ── Feature 5: Time-Travel Debugging ───────────────────────────

/// Result of a time-travel debug analysis.
#[derive(Debug, Clone, Serialize)]
pub struct DebugResult {
    pub table_name: String,
    pub bad_execution: Option<ExecutionSummary>,
    pub good_execution: Option<ExecutionSummary>,
    pub code_diff: Option<TransformDiff>,
    pub data_diff: DataDiffSummary,
    pub root_cause_lines: Vec<String>,
    pub upstream_changes: Vec<UpstreamChange>,
}

/// Summary of an execution for debug context.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionSummary {
    pub execution_id: String,
    pub version: u32,
    pub status: String,
    pub rows_produced: Option<u64>,
    pub duration_ms: u64,
    pub cost_usd: f64,
    pub started_at: String,
}

/// Summary of data differences between two executions.
#[derive(Debug, Clone, Serialize)]
pub struct DataDiffSummary {
    pub row_count_diff: i64,
    pub row_count_pct: f64,
    pub duration_diff_ms: i64,
    pub cost_diff_usd: f64,
    pub regressions: Vec<RegressionMetric>,
}

/// An upstream table change that may have caused the issue.
#[derive(Debug, Clone, Serialize)]
pub struct UpstreamChange {
    pub table_name: String,
    pub changed_at: Option<String>,
    pub version_before: Option<u32>,
    pub version_after: Option<u32>,
}

// ── Feature 7: Data Products + Compliance Audit ────────────────

/// A Data Product wrapping an executable table with SLA and ownership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataProduct {
    pub id: String,
    pub name: String,
    pub table_name: String,
    pub contract_id: Option<String>,
    pub sla_freshness_hours: f64,
    pub sla_quality_score: f64,
    pub owner: String,
    pub consumers: Vec<String>,
    pub certification: String,
    pub description: String,
}

/// Freshness status relative to SLA.
#[derive(Debug, Clone, Serialize)]
pub struct FreshnessStatus {
    pub sla_hours: f64,
    pub actual_hours: f64,
    pub within_sla: bool,
}

/// Complete compliance audit for a data product.
#[derive(Debug, Clone, Serialize)]
pub struct DataProductAudit {
    pub product: DataProduct,
    pub provenance_chain_length: usize,
    pub contract_validation: Option<ContractValidationResult>,
    pub gate_pass_rate: f64,
    pub freshness_status: FreshnessStatus,
    pub quality_score: f64,
    pub cost_summary: AuditCostSummary,
    pub upstream_chain: Vec<String>,
    pub certification_eligible: bool,
    pub compliance_issues: Vec<String>,
}

/// Cost summary for audit.
#[derive(Debug, Clone, Serialize)]
pub struct AuditCostSummary {
    pub total_cost_usd: f64,
    pub total_saved_usd: f64,
    pub total_executions: u64,
    pub total_skipped: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_comparison() {
        let costs = estimate_costs(453, 500, 24); // 453KB binary, 500ms exec, hourly
        assert!(costs.rustlake.monthly_cost_usd < costs.databricks.monthly_cost_usd);
        assert!(costs.lambda.monthly_cost_usd < costs.snowflake.monthly_cost_usd);
        assert!(costs.rustlake.cold_start_ms < costs.databricks.cold_start_ms);
    }

    #[test]
    fn test_iceberg_properties_roundtrip() {
        let table = ExecutableTable {
            table_name: "orders_daily".into(),
            table_location: "s3://warehouse/orders_daily".into(),
            transform: TableTransform {
                transform_type: "sql".into(),
                source_code: "SELECT * FROM raw_orders".into(),
                source_hash: "abc123".into(),
                binary_path: Some("rustlake-functions/bin-abc123".into()),
                binary_size: Some(453000),
                binary_cached: true,
                compiler_version: Some("1.89.0".into()),
                target_arch: Some("aarch64".into()),
            },
            schedule: Some("0 * * * *".into()),
            quality_gates: vec![QualityGateRef {
                gate_type: "not_null".into(),
                column: Some("order_id".into()),
                threshold: None,
                description: "order_id must not be null".into(),
            }],
            input_tables: vec!["raw_orders".into()],
            status: ExecutableTableStatus {
                state: "active".into(),
                health: "healthy".into(),
                last_error: None,
                staleness_hours: 0.5,
                data_freshness: "fresh".into(),
            },
            history: Vec::new(),
            versions: Vec::new(),
            created_at: "2026-03-21T00:00:00Z".into(),
            last_refresh: Some("2026-03-21T12:00:00Z".into()),
            next_refresh: Some("2026-03-21T13:00:00Z".into()),
            estimated_cost_usd: 0.001,
            total_executions: 720,
            total_cost_usd: 0.72,
            incremental: false,
            watermark_column: None,
            last_watermark: None,
            executions_skipped: 0,
            cost_saved_usd: 0.0,
            auto_refresh: false,
            refresh_interval_seconds: 0,
        };

        let props = to_iceberg_properties(&table);
        assert_eq!(props.get("rustlake.executable").unwrap(), "true");
        assert_eq!(props.get("rustlake.transform.type").unwrap(), "sql");
        assert_eq!(props.get("rustlake.schedule").unwrap(), "0 * * * *");

        let restored = from_iceberg_properties(&props, "orders_daily").unwrap();
        assert_eq!(restored.table_name, "orders_daily");
        assert_eq!(restored.total_executions, 720);
    }

    #[test]
    fn test_diff_transforms() {
        let old = "SELECT a, b\nFROM orders\nWHERE status = 'active'";
        let new = "SELECT a, b, c\nFROM orders\nWHERE status = 'active'\nGROUP BY a";
        let (diff, added, removed, changed) = diff_transforms(old, new);
        assert!(changed >= 1); // first line changed
        assert!(added >= 1); // GROUP BY added
        assert!(!diff.is_empty());
    }

    #[test]
    fn test_regression_detection_critical() {
        let result = detect_regression(Some(1000), Some(0), 500, 2, 0.001, 0.0);
        assert!(result.has_regression);
        assert_eq!(result.severity, "critical");
    }

    #[test]
    fn test_regression_detection_none() {
        let result = detect_regression(Some(1000), Some(1050), 500, 480, 0.001, 0.001);
        assert!(!result.has_regression);
        assert_eq!(result.severity, "none");
    }
}
