//! Data quality validation gates for pre-commit checks on Arrow RecordBatches.
//!
//! Quality gates run before Iceberg commits to ensure data integrity. Each gate
//! contains a set of checks (not-null, uniqueness, range, row count, custom SQL)
//! that are evaluated against incoming record batches.
//!
//! # Example
//!
//! ```rust,ignore
//! use rustlake_api::quality_gates::*;
//!
//! let gate = QualityGate {
//!     id: "orders-gate".to_string(),
//!     table: "orders".to_string(),
//!     checks: vec![
//!         QualityCheck::NotNull { column: "order_id".to_string() },
//!         QualityCheck::RowCountMin { min: 1 },
//!     ],
//!     enabled: true,
//! };
//!
//! let result = validate_batch(&gate, &batch);
//! assert!(result.passed);
//! ```

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// A quality gate that groups checks for a specific table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityGate {
    /// Unique gate identifier.
    pub id: String,
    /// The table this gate applies to.
    pub table: String,
    /// Ordered list of checks to run.
    pub checks: Vec<QualityCheck>,
    /// Whether this gate is active.
    pub enabled: bool,
}

/// A single data quality check.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QualityCheck {
    /// Verify that the specified column contains no null values.
    NotNull {
        /// Column name to check.
        column: String,
    },
    /// Verify that all values in the column are unique (no duplicates).
    Unique {
        /// Column name to check.
        column: String,
    },
    /// Verify that numeric column values fall within `[min, max]`.
    Range {
        /// Column name to check.
        column: String,
        /// Minimum allowed value (inclusive).
        min: f64,
        /// Maximum allowed value (inclusive).
        max: f64,
    },
    /// Verify the batch has at least `min` rows.
    RowCountMin {
        /// Minimum number of rows required.
        min: usize,
    },
    /// Placeholder for a custom SQL validation query.
    CustomSql {
        /// The SQL query to execute for validation.
        sql: String,
        /// Human-readable description of what this check validates.
        description: String,
    },
}

impl QualityCheck {
    /// Return a short human-readable name for the check type.
    pub fn check_name(&self) -> &'static str {
        match self {
            QualityCheck::NotNull { .. } => "not_null",
            QualityCheck::Unique { .. } => "unique",
            QualityCheck::Range { .. } => "range",
            QualityCheck::RowCountMin { .. } => "row_count_min",
            QualityCheck::CustomSql { .. } => "custom_sql",
        }
    }
}

impl std::fmt::Display for QualityCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QualityCheck::NotNull { column } => write!(f, "NotNull({})", column),
            QualityCheck::Unique { column } => write!(f, "Unique({})", column),
            QualityCheck::Range { column, min, max } => {
                write!(f, "Range({} in [{}, {}])", column, min, max)
            }
            QualityCheck::RowCountMin { min } => write!(f, "RowCountMin({})", min),
            QualityCheck::CustomSql { description, .. } => {
                write!(f, "CustomSql({})", description)
            }
        }
    }
}

/// Severity of a validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// The check failed but data can still be committed (soft gate).
    Warning,
    /// The check failed and the commit should be blocked (hard gate).
    Error,
}

/// A single validation failure with context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationFailure {
    /// The check that failed.
    pub check: String,
    /// Human-readable failure message.
    pub message: String,
    /// Whether this failure should block the commit.
    pub severity: Severity,
}

/// Result of running all checks in a quality gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether all checks passed.
    pub passed: bool,
    /// List of failures (empty if `passed` is true).
    pub failures: Vec<ValidationFailure>,
    /// ISO 8601 timestamp of when validation was performed.
    pub checked_at: String,
    /// Total number of checks that were evaluated.
    pub checks_run: usize,
    /// Total number of rows validated.
    pub rows_checked: usize,
}

// ---------------------------------------------------------------------------
// Validation logic
// ---------------------------------------------------------------------------

/// Validate a single `RecordBatch` against all checks in a quality gate.
///
/// If the gate is disabled, returns a passing result with zero checks run.
pub fn validate_batch(gate: &QualityGate, batch: &RecordBatch) -> ValidationResult {
    if !gate.enabled {
        return ValidationResult {
            passed: true,
            failures: Vec::new(),
            checked_at: Utc::now().to_rfc3339(),
            checks_run: 0,
            rows_checked: batch.num_rows(),
        };
    }

    let mut failures = Vec::new();

    for check in &gate.checks {
        match check {
            QualityCheck::NotNull { column } => {
                check_not_null(batch, column, &mut failures);
            }
            QualityCheck::Unique { column } => {
                check_unique(batch, column, &mut failures);
            }
            QualityCheck::Range { column, min, max } => {
                check_range(batch, column, *min, *max, &mut failures);
            }
            QualityCheck::RowCountMin { min } => {
                check_row_count_min(batch, *min, &mut failures);
            }
            QualityCheck::CustomSql { sql, description } => {
                // CustomSql requires a query engine context which is not available here.
                // Log a warning and skip.
                tracing::debug!(
                    sql = %sql,
                    description = %description,
                    "Skipping CustomSql check — requires query engine context"
                );
            }
        }
    }

    ValidationResult {
        passed: failures.is_empty(),
        failures,
        checked_at: Utc::now().to_rfc3339(),
        checks_run: gate.checks.len(),
        rows_checked: batch.num_rows(),
    }
}

/// Validate multiple `RecordBatch`es against all checks in a quality gate.
///
/// Each batch is validated independently and failures are aggregated.
/// The `rows_checked` field reports the total across all batches.
pub fn validate_batches(gate: &QualityGate, batches: &[RecordBatch]) -> ValidationResult {
    if !gate.enabled || batches.is_empty() {
        return ValidationResult {
            passed: true,
            failures: Vec::new(),
            checked_at: Utc::now().to_rfc3339(),
            checks_run: 0,
            rows_checked: batches.iter().map(|b| b.num_rows()).sum(),
        };
    }

    let mut all_failures = Vec::new();
    let mut total_rows = 0usize;

    // For RowCountMin, we check the total row count across all batches.
    let total_row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    for (batch_idx, batch) in batches.iter().enumerate() {
        total_rows += batch.num_rows();

        for check in &gate.checks {
            match check {
                QualityCheck::NotNull { column } => {
                    let before = all_failures.len();
                    check_not_null(batch, column, &mut all_failures);
                    // Annotate failures with batch index for multi-batch context.
                    for failure in all_failures.iter_mut().skip(before) {
                        failure.message = format!("[batch {}] {}", batch_idx, failure.message);
                    }
                }
                QualityCheck::Unique { column } => {
                    let before = all_failures.len();
                    check_unique(batch, column, &mut all_failures);
                    for failure in all_failures.iter_mut().skip(before) {
                        failure.message = format!("[batch {}] {}", batch_idx, failure.message);
                    }
                }
                QualityCheck::Range { column, min, max } => {
                    let before = all_failures.len();
                    check_range(batch, column, *min, *max, &mut all_failures);
                    for failure in all_failures.iter_mut().skip(before) {
                        failure.message = format!("[batch {}] {}", batch_idx, failure.message);
                    }
                }
                // RowCountMin is checked once after the loop, not per-batch.
                QualityCheck::RowCountMin { .. } => {}
                QualityCheck::CustomSql { .. } => {}
            }
        }
    }

    // Check RowCountMin against total row count.
    for check in &gate.checks {
        if let QualityCheck::RowCountMin { min } = check {
            if total_row_count < *min {
                all_failures.push(ValidationFailure {
                    check: check.to_string(),
                    message: format!(
                        "Total row count {} is below minimum {} (across {} batches)",
                        total_row_count,
                        min,
                        batches.len()
                    ),
                    severity: Severity::Error,
                });
            }
        }
    }

    ValidationResult {
        passed: all_failures.is_empty(),
        failures: all_failures,
        checked_at: Utc::now().to_rfc3339(),
        checks_run: gate.checks.len(),
        rows_checked: total_rows,
    }
}

// ---------------------------------------------------------------------------
// Individual check implementations
// ---------------------------------------------------------------------------

/// Check that the named column contains no null values.
fn check_not_null(batch: &RecordBatch, column: &str, failures: &mut Vec<ValidationFailure>) {
    let schema = batch.schema();
    let col_idx = match schema.index_of(column) {
        Ok(idx) => idx,
        Err(_) => {
            failures.push(ValidationFailure {
                check: format!("NotNull({})", column),
                message: format!("Column '{}' not found in batch schema", column),
                severity: Severity::Error,
            });
            return;
        }
    };

    let array = batch.column(col_idx);
    let null_count = array.null_count();
    if null_count > 0 {
        failures.push(ValidationFailure {
            check: format!("NotNull({})", column),
            message: format!(
                "Column '{}' contains {} null value(s) out of {} rows",
                column,
                null_count,
                array.len()
            ),
            severity: Severity::Error,
        });
    }
}

/// Check that all values in the named column are unique.
///
/// Uses a `HashSet` of string representations for simplicity. This works for
/// primitive types; complex nested types may need a more sophisticated approach.
fn check_unique(batch: &RecordBatch, column: &str, failures: &mut Vec<ValidationFailure>) {
    let schema = batch.schema();
    let col_idx = match schema.index_of(column) {
        Ok(idx) => idx,
        Err(_) => {
            failures.push(ValidationFailure {
                check: format!("Unique({})", column),
                message: format!("Column '{}' not found in batch schema", column),
                severity: Severity::Error,
            });
            return;
        }
    };

    let array = batch.column(col_idx);
    let total = array.len();
    if total == 0 {
        return; // Empty batch is trivially unique.
    }

    // Count distinct values by converting to string representations.
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = 0usize;

    let data_type = array.data_type().clone();
    match data_type {
        DataType::Utf8 => {
            let str_array = array.as_string::<i32>();
            for i in 0..total {
                if !array.is_null(i) {
                    let val = str_array.value(i);
                    if !seen.insert(val.to_string()) {
                        duplicates += 1;
                    }
                }
            }
        }
        DataType::LargeUtf8 => {
            let str_array = array.as_string::<i64>();
            for i in 0..total {
                if !array.is_null(i) {
                    let val = str_array.value(i);
                    if !seen.insert(val.to_string()) {
                        duplicates += 1;
                    }
                }
            }
        }
        DataType::Int32 => {
            let int_array = array.as_primitive::<arrow::datatypes::Int32Type>();
            for i in 0..total {
                if !array.is_null(i) {
                    if !seen.insert(int_array.value(i).to_string()) {
                        duplicates += 1;
                    }
                }
            }
        }
        DataType::Int64 => {
            let int_array = array.as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..total {
                if !array.is_null(i) {
                    if !seen.insert(int_array.value(i).to_string()) {
                        duplicates += 1;
                    }
                }
            }
        }
        DataType::Float32 => {
            let float_array = array.as_primitive::<arrow::datatypes::Float32Type>();
            for i in 0..total {
                if !array.is_null(i) {
                    // Use bits for exact float comparison.
                    if !seen.insert(float_array.value(i).to_bits().to_string()) {
                        duplicates += 1;
                    }
                }
            }
        }
        DataType::Float64 => {
            let float_array = array.as_primitive::<arrow::datatypes::Float64Type>();
            for i in 0..total {
                if !array.is_null(i) {
                    if !seen.insert(float_array.value(i).to_bits().to_string()) {
                        duplicates += 1;
                    }
                }
            }
        }
        _ => {
            // Fallback: use Debug representation (less efficient but works for any type).
            for i in 0..total {
                if !array.is_null(i) {
                    let repr = format!("{:?}", arrow::util::display::array_value_to_string(array, i));
                    if !seen.insert(repr) {
                        duplicates += 1;
                    }
                }
            }
        }
    }

    if duplicates > 0 {
        let distinct = seen.len();
        let non_null = total - array.null_count();
        failures.push(ValidationFailure {
            check: format!("Unique({})", column),
            message: format!(
                "Column '{}' has {} duplicate(s): {} distinct values out of {} non-null rows",
                column, duplicates, distinct, non_null
            ),
            severity: Severity::Error,
        });
    }
}

/// Check that numeric column values fall within `[min, max]`.
fn check_range(
    batch: &RecordBatch,
    column: &str,
    min: f64,
    max: f64,
    failures: &mut Vec<ValidationFailure>,
) {
    let schema = batch.schema();
    let col_idx = match schema.index_of(column) {
        Ok(idx) => idx,
        Err(_) => {
            failures.push(ValidationFailure {
                check: format!("Range({} in [{}, {}])", column, min, max),
                message: format!("Column '{}' not found in batch schema", column),
                severity: Severity::Error,
            });
            return;
        }
    };

    let array = batch.column(col_idx);
    if array.len() == 0 {
        return;
    }

    let data_type = array.data_type().clone();
    let (actual_min, actual_max) = match data_type {
        DataType::Int8 => {
            let a = array.as_primitive::<arrow::datatypes::Int8Type>();
            compute_min_max_primitive(a)
        }
        DataType::Int16 => {
            let a = array.as_primitive::<arrow::datatypes::Int16Type>();
            compute_min_max_primitive(a)
        }
        DataType::Int32 => {
            let a = array.as_primitive::<arrow::datatypes::Int32Type>();
            compute_min_max_primitive(a)
        }
        DataType::Int64 => {
            let a = array.as_primitive::<arrow::datatypes::Int64Type>();
            compute_min_max_primitive(a)
        }
        DataType::UInt8 => {
            let a = array.as_primitive::<arrow::datatypes::UInt8Type>();
            compute_min_max_primitive(a)
        }
        DataType::UInt16 => {
            let a = array.as_primitive::<arrow::datatypes::UInt16Type>();
            compute_min_max_primitive(a)
        }
        DataType::UInt32 => {
            let a = array.as_primitive::<arrow::datatypes::UInt32Type>();
            compute_min_max_primitive(a)
        }
        DataType::UInt64 => {
            let a = array.as_primitive::<arrow::datatypes::UInt64Type>();
            compute_min_max_primitive(a)
        }
        DataType::Float32 => {
            let a = array.as_primitive::<arrow::datatypes::Float32Type>();
            compute_min_max_primitive(a)
        }
        DataType::Float64 => {
            let a = array.as_primitive::<arrow::datatypes::Float64Type>();
            compute_min_max_primitive(a)
        }
        _ => {
            failures.push(ValidationFailure {
                check: format!("Range({} in [{}, {}])", column, min, max),
                message: format!(
                    "Column '{}' has non-numeric type {:?} — range check not applicable",
                    column, data_type
                ),
                severity: Severity::Warning,
            });
            return;
        }
    };

    let (actual_min, actual_max) = match (actual_min, actual_max) {
        (Some(lo), Some(hi)) => (lo, hi),
        _ => return, // All nulls — nothing to check.
    };

    if actual_min < min || actual_max > max {
        failures.push(ValidationFailure {
            check: format!("Range({} in [{}, {}])", column, min, max),
            message: format!(
                "Column '{}' values [{:.6}, {:.6}] exceed allowed range [{}, {}]",
                column, actual_min, actual_max, min, max
            ),
            severity: Severity::Error,
        });
    }
}

/// Compute min and max for a primitive array, skipping nulls.
/// Helper trait for converting Arrow native types to f64.
trait AsF64 {
    fn as_f64(self) -> f64;
}
macro_rules! impl_as_f64 {
    ($($t:ty),*) => { $(impl AsF64 for $t { fn as_f64(self) -> f64 { self as f64 } })* };
}
impl_as_f64!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64);

fn compute_min_max_primitive<T>(array: &arrow::array::PrimitiveArray<T>) -> (Option<f64>, Option<f64>)
where
    T: arrow::datatypes::ArrowPrimitiveType,
    T::Native: AsF64 + Copy,
{
    let mut min_val: Option<f64> = None;
    let mut max_val: Option<f64> = None;

    for i in 0..array.len() {
        if !array.is_null(i) {
            let v = array.value(i).as_f64();
            min_val = Some(min_val.map_or(v, |m: f64| m.min(v)));
            max_val = Some(max_val.map_or(v, |m: f64| m.max(v)));
        }
    }

    (min_val, max_val)
}

/// Check that the batch has at least `min` rows.
fn check_row_count_min(batch: &RecordBatch, min: usize, failures: &mut Vec<ValidationFailure>) {
    let count = batch.num_rows();
    if count < min {
        failures.push(ValidationFailure {
            check: format!("RowCountMin({})", min),
            message: format!("Batch has {} rows, minimum required is {}", count, min),
            severity: Severity::Error,
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                    None,
                    Some("eve"),
                ])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_not_null_pass() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::NotNull {
                column: "id".to_string(),
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(result.passed);
        assert!(result.failures.is_empty());
    }

    #[test]
    fn test_not_null_fail() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::NotNull {
                column: "name".to_string(),
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(!result.passed);
        assert_eq!(result.failures.len(), 1);
        assert!(result.failures[0].message.contains("1 null"));
    }

    #[test]
    fn test_not_null_missing_column() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::NotNull {
                column: "nonexistent".to_string(),
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(!result.passed);
        assert!(result.failures[0].message.contains("not found"));
    }

    #[test]
    fn test_unique_pass() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::Unique {
                column: "id".to_string(),
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(result.passed);
    }

    #[test]
    fn test_unique_fail() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 2, 3, 3]))],
        )
        .unwrap();

        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::Unique {
                column: "val".to_string(),
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(!result.passed);
        assert!(result.failures[0].message.contains("duplicate"));
    }

    #[test]
    fn test_range_pass() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::Range {
                column: "price".to_string(),
                min: 0.0,
                max: 100.0,
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(result.passed);
    }

    #[test]
    fn test_range_fail() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::Range {
                column: "price".to_string(),
                min: 15.0,
                max: 45.0,
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(!result.passed);
        assert!(result.failures[0].message.contains("exceed"));
    }

    #[test]
    fn test_row_count_min_pass() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::RowCountMin { min: 3 }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(result.passed);
    }

    #[test]
    fn test_row_count_min_fail() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::RowCountMin { min: 10 }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        assert!(!result.passed);
        assert!(result.failures[0].message.contains("5 rows"));
    }

    #[test]
    fn test_disabled_gate_passes() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::RowCountMin { min: 1000 }],
            enabled: false,
        };
        let result = validate_batch(&gate, &batch);
        assert!(result.passed);
        assert_eq!(result.checks_run, 0);
    }

    #[test]
    fn test_multiple_checks() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![
                QualityCheck::NotNull {
                    column: "id".to_string(),
                },
                QualityCheck::NotNull {
                    column: "name".to_string(),
                },
                QualityCheck::Range {
                    column: "price".to_string(),
                    min: 0.0,
                    max: 100.0,
                },
                QualityCheck::RowCountMin { min: 3 },
            ],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        // name has nulls, so it should fail.
        assert!(!result.passed);
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.checks_run, 4);
        assert_eq!(result.rows_checked, 5);
    }

    #[test]
    fn test_validate_batches_aggregates() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![
                QualityCheck::NotNull {
                    column: "id".to_string(),
                },
                QualityCheck::RowCountMin { min: 8 },
            ],
            enabled: true,
        };
        // Two batches of 5 rows each = 10 total, passes RowCountMin(8).
        let result = validate_batches(&gate, &[batch.clone(), batch]);
        assert!(result.passed);
        assert_eq!(result.rows_checked, 10);
    }

    #[test]
    fn test_validate_batches_row_count_fail() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::RowCountMin { min: 100 }],
            enabled: true,
        };
        let result = validate_batches(&gate, &[batch]);
        assert!(!result.passed);
        assert!(result.failures[0].message.contains("5"));
    }

    #[test]
    fn test_custom_sql_skipped() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::CustomSql {
                sql: "SELECT COUNT(*) > 0 FROM test_table".to_string(),
                description: "Table is non-empty".to_string(),
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        // CustomSql is skipped, so it should pass.
        assert!(result.passed);
    }

    #[test]
    fn test_quality_check_display() {
        assert_eq!(
            QualityCheck::NotNull {
                column: "id".to_string()
            }
            .to_string(),
            "NotNull(id)"
        );
        assert_eq!(
            QualityCheck::Range {
                column: "price".to_string(),
                min: 0.0,
                max: 100.0
            }
            .to_string(),
            "Range(price in [0, 100])"
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        let gate = QualityGate {
            id: "gate-1".to_string(),
            table: "orders".to_string(),
            checks: vec![
                QualityCheck::NotNull {
                    column: "order_id".to_string(),
                },
                QualityCheck::Range {
                    column: "amount".to_string(),
                    min: 0.0,
                    max: 1_000_000.0,
                },
            ],
            enabled: true,
        };
        let json = serde_json::to_string(&gate).unwrap();
        let deserialized: QualityGate = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, gate.id);
        assert_eq!(deserialized.checks.len(), 2);
    }

    #[test]
    fn test_range_non_numeric_column() {
        let batch = make_test_batch();
        let gate = QualityGate {
            id: "test".to_string(),
            table: "test_table".to_string(),
            checks: vec![QualityCheck::Range {
                column: "name".to_string(),
                min: 0.0,
                max: 100.0,
            }],
            enabled: true,
        };
        let result = validate_batch(&gate, &batch);
        // Non-numeric column gets a warning, not an error.
        assert!(!result.passed);
        assert_eq!(result.failures[0].severity, Severity::Warning);
    }
}
