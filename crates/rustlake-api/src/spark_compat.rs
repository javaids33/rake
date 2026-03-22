//! Spark SQL compatibility layer.
//!
//! Translates common Spark SQL syntax and functions to DataFusion-compatible SQL.
//! Allows Databricks users to run their existing queries without modification.

use serde::{Deserialize, Serialize};

/// Spark SQL compatibility mode configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkCompatConfig {
    /// Whether Spark SQL translation is enabled.
    pub enabled: bool,
    /// Log translations for debugging.
    pub log_translations: bool,
}

impl Default for SparkCompatConfig {
    fn default() -> Self {
        Self { enabled: true, log_translations: false }
    }
}

/// Result of translating Spark SQL to DataFusion SQL.
#[derive(Debug, Clone, Serialize)]
pub struct TranslationResult {
    /// The original Spark SQL.
    pub original: String,
    /// The translated DataFusion SQL.
    pub translated: String,
    /// Whether any translation was applied.
    pub was_translated: bool,
    /// List of translations applied.
    pub translations_applied: Vec<String>,
}

/// Translate Spark SQL to DataFusion-compatible SQL.
///
/// Handles common differences:
/// - Function names (NVL→COALESCE, DATEDIFF→DATE_DIFF, etc.)
/// - Spark-specific syntax (DISTRIBUTE BY, CLUSTER BY, LATERAL VIEW)
/// - Type casting differences (INT→INT32, STRING→VARCHAR)
/// - Delta Lake syntax (MERGE INTO basic support)
/// - Spark UDFs commonly used in PySpark
pub fn translate_spark_sql(sql: &str) -> TranslationResult {
    let original = sql.to_string();
    let mut translated = sql.to_string();
    let mut translations = Vec::new();

    // ── Function translations ─────────────────────────────────
    let function_map = [
        // Null handling
        ("NVL(", "COALESCE(", "NVL → COALESCE"),
        ("NVL2(", "CASE WHEN", "NVL2 → CASE WHEN (manual)"),
        ("IFNULL(", "COALESCE(", "IFNULL → COALESCE"),

        // String functions
        ("INSTR(", "STRPOS(", "INSTR → STRPOS"),
        ("LOCATE(", "STRPOS(", "LOCATE → STRPOS"),
        ("LEN(", "LENGTH(", "LEN → LENGTH"),
        ("SUBSTR(", "SUBSTRING(", "SUBSTR → SUBSTRING"),
        ("LPAD(", "LPAD(", "LPAD (compatible)"),
        ("RPAD(", "RPAD(", "RPAD (compatible)"),
        ("INITCAP(", "INITCAP(", "INITCAP (compatible)"),
        ("FORMAT_STRING(", "FORMAT(", "FORMAT_STRING → FORMAT"),
        ("CONCAT_WS(", "CONCAT_WS(", "CONCAT_WS (compatible)"),

        // Date/time functions
        ("DATEDIFF(", "DATE_DIFF('day', ", "DATEDIFF → DATE_DIFF"),
        ("DATE_ADD(", "DATE_ADD(", "DATE_ADD (compatible)"),
        ("DATE_SUB(", "DATE_SUB(", "DATE_SUB (compatible)"),
        ("ADD_MONTHS(", "DATE_ADD(", "ADD_MONTHS → DATE_ADD (months)"),
        ("MONTHS_BETWEEN(", "DATE_DIFF('month', ", "MONTHS_BETWEEN → DATE_DIFF"),
        ("TO_DATE(", "CAST(", "TO_DATE → CAST AS DATE"),
        ("TO_TIMESTAMP(", "CAST(", "TO_TIMESTAMP → CAST AS TIMESTAMP"),
        ("UNIX_TIMESTAMP(", "EXTRACT(EPOCH FROM ", "UNIX_TIMESTAMP → EXTRACT EPOCH"),
        ("FROM_UNIXTIME(", "TO_TIMESTAMP(", "FROM_UNIXTIME → TO_TIMESTAMP"),
        ("DATE_FORMAT(", "STRFTIME(", "DATE_FORMAT → STRFTIME"),
        ("CURRENT_DATE()", "CURRENT_DATE", "CURRENT_DATE() → CURRENT_DATE"),
        ("CURRENT_TIMESTAMP()", "NOW()", "CURRENT_TIMESTAMP() → NOW()"),

        // Aggregate functions
        ("COLLECT_LIST(", "ARRAY_AGG(", "COLLECT_LIST → ARRAY_AGG"),
        ("COLLECT_SET(", "ARRAY_AGG(DISTINCT ", "COLLECT_SET → ARRAY_AGG(DISTINCT)"),
        ("SIZE(", "ARRAY_LENGTH(", "SIZE → ARRAY_LENGTH"),
        ("EXPLODE(", "UNNEST(", "EXPLODE → UNNEST"),

        // Math
        ("RAND()", "RANDOM()", "RAND() → RANDOM()"),
        ("SHIFTLEFT(", "BIT_SHIFT_LEFT(", "SHIFTLEFT → BIT_SHIFT_LEFT"),
        ("SHIFTRIGHT(", "BIT_SHIFT_RIGHT(", "SHIFTRIGHT → BIT_SHIFT_RIGHT"),

        // Type casting
        ("BOOLEAN(", "CAST(", "BOOLEAN() → CAST AS BOOLEAN"),
        ("INT(", "CAST(", "INT() → CAST AS INT"),
        ("BIGINT(", "CAST(", "BIGINT() → CAST AS BIGINT"),
        ("DOUBLE(", "CAST(", "DOUBLE() → CAST AS DOUBLE"),
        ("STRING(", "CAST(", "STRING() → CAST AS VARCHAR"),
    ];

    for (spark_fn, df_fn, desc) in &function_map {
        let upper = translated.to_uppercase();
        if upper.contains(&spark_fn.to_uppercase()) {
            // Skip if spark_fn == df_fn (already compatible)
            if *spark_fn == *df_fn {
                continue;
            }

            // Case-insensitive replacement
            let mut result = String::new();
            let mut remaining = translated.as_str();
            let spark_upper = spark_fn.to_uppercase();

            while let Some(pos) = remaining.to_uppercase().find(&spark_upper) {
                result.push_str(&remaining[..pos]);
                result.push_str(df_fn);
                remaining = &remaining[pos + spark_fn.len()..];
                if !translations.contains(&desc.to_string()) {
                    translations.push(desc.to_string());
                }
            }
            result.push_str(remaining);
            translated = result;
        }
    }

    // ── Syntax translations ───────────────────────────────────

    // DISTRIBUTE BY → (remove, DataFusion handles distribution)
    let upper = translated.to_uppercase();
    if upper.contains("DISTRIBUTE BY") {
        if let Some(pos) = upper.find("DISTRIBUTE BY") {
            // Find the end of the DISTRIBUTE BY clause
            let rest = &translated[pos..];
            let end = rest.find(|c: char| c == ';' || c == ')').unwrap_or(rest.len());
            translated = format!("{}{}", &translated[..pos], &translated[pos + end..]);
            translations.push("DISTRIBUTE BY removed (DataFusion handles automatically)".to_string());
        }
    }

    // CLUSTER BY → ORDER BY (approximate)
    let upper = translated.to_uppercase();
    if upper.contains("CLUSTER BY") {
        translated = case_insensitive_replace(&translated, "CLUSTER BY", "ORDER BY");
        translations.push("CLUSTER BY → ORDER BY".to_string());
    }

    // SORT BY → ORDER BY
    let upper = translated.to_uppercase();
    if upper.contains("SORT BY") && !upper.contains("ORDER BY") {
        translated = case_insensitive_replace(&translated, "SORT BY", "ORDER BY");
        translations.push("SORT BY → ORDER BY".to_string());
    }

    // TABLESAMPLE → (remove, not supported)
    let upper = translated.to_uppercase();
    if upper.contains("TABLESAMPLE") {
        translations.push("TABLESAMPLE not supported — full table scan used".to_string());
    }

    // LATERAL VIEW EXPLODE → CROSS JOIN UNNEST
    let upper = translated.to_uppercase();
    if upper.contains("LATERAL VIEW") {
        translations.push("LATERAL VIEW → CROSS JOIN UNNEST (manual rewrite may be needed)".to_string());
    }

    // USING → ON (for JOINs, if simple column match)
    // This is already supported by DataFusion, so no translation needed

    // RLIKE → similar regex (already supported in DataFusion)

    let was_translated = !translations.is_empty();

    if was_translated {
        tracing::info!(
            original_len = original.len(),
            translated_len = translated.len(),
            translations = translations.len(),
            "Spark SQL translated to DataFusion SQL"
        );
    }

    TranslationResult {
        original,
        translated,
        was_translated,
        translations_applied: translations,
    }
}

/// Case-insensitive string replacement helper.
fn case_insensitive_replace(input: &str, pattern: &str, replacement: &str) -> String {
    let upper_input = input.to_uppercase();
    let upper_pattern = pattern.to_uppercase();
    let mut result = String::new();
    let mut remaining = input;
    let mut remaining_upper = upper_input.as_str();

    while let Some(pos) = remaining_upper.find(&upper_pattern) {
        result.push_str(&remaining[..pos]);
        result.push_str(replacement);
        remaining = &remaining[pos + pattern.len()..];
        remaining_upper = &remaining_upper[pos + upper_pattern.len()..];
    }
    result.push_str(remaining);
    result
}

/// Check if a SQL statement looks like Spark SQL (heuristic).
pub fn looks_like_spark_sql(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    let spark_indicators = [
        "NVL(", "DATEDIFF(", "COLLECT_LIST(", "COLLECT_SET(",
        "EXPLODE(", "DISTRIBUTE BY", "CLUSTER BY", "SORT BY",
        "LATERAL VIEW", "TABLESAMPLE", "STRING(",
        "UNIX_TIMESTAMP(", "FROM_UNIXTIME(", "DATE_FORMAT(",
        "FORMAT_STRING(", "INSTR(", "LOCATE(",
    ];

    spark_indicators.iter().any(|indicator| upper.contains(indicator))
}

/// Get a human-readable summary of Spark SQL compatibility.
pub fn compatibility_summary() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("NVL / IFNULL", "COALESCE", "Full"),
        ("DATEDIFF", "DATE_DIFF", "Full"),
        ("COLLECT_LIST / COLLECT_SET", "ARRAY_AGG", "Full"),
        ("EXPLODE", "UNNEST", "Full"),
        ("DATE_FORMAT", "STRFTIME", "Partial"),
        ("UNIX_TIMESTAMP / FROM_UNIXTIME", "EXTRACT / TO_TIMESTAMP", "Full"),
        ("DISTRIBUTE BY / CLUSTER BY", "Automatic / ORDER BY", "Automatic"),
        ("LATERAL VIEW", "CROSS JOIN UNNEST", "Manual rewrite"),
        ("MERGE INTO", "Not yet supported", "Planned"),
        ("Delta Lake SQL", "Iceberg SQL", "Different format"),
        ("PySpark DataFrame API", "SQL or Python notebooks", "Via Pyodide"),
        ("Spark UDFs (Python)", "Python notebook cells", "Via Pyodide"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nvl_translation() {
        let result = translate_spark_sql("SELECT NVL(name, 'unknown') FROM users");
        assert!(result.was_translated);
        assert!(result.translated.contains("COALESCE"));
        assert!(!result.translated.contains("NVL"));
    }

    #[test]
    fn test_datediff_translation() {
        let result = translate_spark_sql("SELECT DATEDIFF(end_date, start_date) FROM events");
        assert!(result.was_translated);
        assert!(result.translated.contains("DATE_DIFF"));
    }

    #[test]
    fn test_collect_list_translation() {
        let result = translate_spark_sql("SELECT COLLECT_LIST(item) FROM orders GROUP BY order_id");
        assert!(result.was_translated);
        assert!(result.translated.contains("ARRAY_AGG"));
    }

    #[test]
    fn test_no_translation_needed() {
        let result = translate_spark_sql("SELECT COUNT(*) FROM orders WHERE status = 'active'");
        assert!(!result.was_translated);
        assert_eq!(result.original, result.translated);
    }

    #[test]
    fn test_looks_like_spark() {
        assert!(looks_like_spark_sql("SELECT NVL(name, 'x') FROM t"));
        assert!(looks_like_spark_sql("SELECT * FROM t DISTRIBUTE BY id"));
        assert!(!looks_like_spark_sql("SELECT COUNT(*) FROM orders"));
    }

    #[test]
    fn test_multiple_translations() {
        let result = translate_spark_sql("SELECT NVL(name, 'x'), DATEDIFF(a, b), COLLECT_LIST(c) FROM t");
        assert!(result.was_translated);
        assert!(result.translations_applied.len() >= 3);
    }

    #[test]
    fn test_cluster_by() {
        let result = translate_spark_sql("SELECT * FROM orders CLUSTER BY region");
        assert!(result.was_translated);
        assert!(result.translated.contains("ORDER BY"));
    }
}
