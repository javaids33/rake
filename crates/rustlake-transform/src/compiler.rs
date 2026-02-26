//! SQL compiler — resolves ref() and source() macros in model SQL.

use std::collections::HashMap;

use rustlake_core::{Result, RustLakeError};

use crate::model::Model;

/// Compiles model SQL by resolving macros and building dependency info.
pub struct SqlCompiler {
    /// Map of model name → compiled SQL.
    models: HashMap<String, Model>,
}

impl SqlCompiler {
    /// Create a new compiler with the given models.
    pub fn new(models: Vec<Model>) -> Self {
        let map = models.into_iter().map(|m| (m.name.clone(), m)).collect();
        Self { models: map }
    }

    /// Compile a model's SQL, resolving all `ref('model_name')` and
    /// `source('source_name', 'table_name')` macros.
    ///
    /// Supports both bare macros (`ref('name')`, `source('src', 'table')`)
    /// and Jinja-style wrappers (`{{ ref('name') }}`, `{{ source('src', 'table') }}`).
    pub fn compile(&self, model_name: &str) -> Result<String> {
        let model = self
            .models
            .get(model_name)
            .ok_or_else(|| RustLakeError::Engine(format!("Model '{}' not found", model_name)))?;

        let mut sql = model.sql.clone();

        // Strip Jinja-style {{ }} wrappers so the core resolution logic works uniformly.
        sql = Self::strip_jinja_wrappers(&sql);

        // Resolve ref() macros — replace ref('name') with the actual table name
        while let Some(start) = sql.find("ref('") {
            let after_ref = start + 5; // skip ref('
            let end = sql[after_ref..].find("')").ok_or_else(|| {
                RustLakeError::Engine(format!(
                    "Unterminated ref() macro in model '{}'",
                    model_name
                ))
            })? + after_ref;

            let ref_name = &sql[after_ref..end];

            // Verify the referenced model exists
            if !self.models.contains_key(ref_name) {
                return Err(RustLakeError::Engine(format!(
                    "Model '{}' references unknown model '{}'",
                    model_name, ref_name
                )));
            }

            // Replace ref('name') with the table name
            let replacement = ref_name.to_string();
            sql.replace_range(start..end + 2, &replacement);
        }

        // Resolve source() macros — replace source('src', 'table') with src.table
        while let Some(start) = sql.find("source('") {
            let after_source = start + 8; // skip source('
            let first_end = sql[after_source..].find("'").ok_or_else(|| {
                RustLakeError::Engine(format!(
                    "Unterminated source() macro in model '{}'",
                    model_name
                ))
            })? + after_source;

            let source_name = sql[after_source..first_end].to_string();

            // Skip past the closing quote to find the second argument: ', 'table_name')
            // first_end points to the closing ' of source name.
            // We need to find the next ' after that (opening quote of table name).
            let after_first_quote = first_end + 1; // skip past the closing '
            let table_start = sql[after_first_quote..].find("'").ok_or_else(|| {
                RustLakeError::Engine("Missing table name in source() macro".into())
            })? + after_first_quote
                + 1; // +1 to skip past the opening '

            let table_end = sql[table_start..]
                .find("'")
                .ok_or_else(|| RustLakeError::Engine("Unterminated source() macro".into()))?
                + table_start;

            let table_name = sql[table_start..table_end].to_string();

            // Find the closing )
            let close_paren = sql[table_end..]
                .find(')')
                .ok_or_else(|| RustLakeError::Engine("Missing closing paren in source()".into()))?
                + table_end;

            let replacement = format!("{}.{}", source_name, table_name);
            sql.replace_range(start..close_paren + 1, &replacement);
        }

        Ok(sql)
    }

    /// Compile a model's SQL with a custom source mapping function.
    ///
    /// After resolving ref/source macros, replaces `source_name.table_name`
    /// references with the value returned by `source_mapper`.
    pub fn compile_with_source_map<F>(&self, model_name: &str, source_mapper: F) -> Result<String>
    where
        F: Fn(&str, &str) -> Option<String>,
    {
        let model = self
            .models
            .get(model_name)
            .ok_or_else(|| RustLakeError::Engine(format!("Model '{}' not found", model_name)))?;

        let mut sql = model.sql.clone();

        // Strip Jinja-style {{ }} wrappers
        sql = Self::strip_jinja_wrappers(&sql);

        // Resolve source() macros with custom mapping
        while let Some(start) = sql.find("source('") {
            let after_source = start + 8;
            let first_end = sql[after_source..].find("'").ok_or_else(|| {
                RustLakeError::Engine(format!(
                    "Unterminated source() macro in model '{}'",
                    model_name
                ))
            })? + after_source;

            let source_name = sql[after_source..first_end].to_string();

            // Skip past the closing quote to find the second argument
            let after_first_quote = first_end + 1;
            let table_start = sql[after_first_quote..].find("'").ok_or_else(|| {
                RustLakeError::Engine("Missing table name in source() macro".into())
            })? + after_first_quote
                + 1;

            let table_end = sql[table_start..]
                .find("'")
                .ok_or_else(|| RustLakeError::Engine("Unterminated source() macro".into()))?
                + table_start;

            let table_name = sql[table_start..table_end].to_string();

            let close_paren = sql[table_end..]
                .find(')')
                .ok_or_else(|| RustLakeError::Engine("Missing closing paren in source()".into()))?
                + table_end;

            let replacement = source_mapper(&source_name, &table_name)
                .unwrap_or_else(|| format!("{}.{}", source_name, table_name));
            sql.replace_range(start..close_paren + 1, &replacement);
        }

        // Resolve ref() macros — replace ref('name') with the table name.
        // For ref models, we recursively compile them so subqueries resolve properly.
        while let Some(start) = sql.find("ref('") {
            let after_ref = start + 5;
            let end = sql[after_ref..].find("')").ok_or_else(|| {
                RustLakeError::Engine(format!(
                    "Unterminated ref() macro in model '{}'",
                    model_name
                ))
            })? + after_ref;

            let ref_name = &sql[after_ref..end];

            if !self.models.contains_key(ref_name) {
                return Err(RustLakeError::Engine(format!(
                    "Model '{}' references unknown model '{}'",
                    model_name, ref_name
                )));
            }

            let replacement = ref_name.to_string();
            sql.replace_range(start..end + 2, &replacement);
        }

        Ok(sql)
    }

    /// Strip Jinja-style `{{ ... }}` wrappers, leaving only the inner expression.
    fn strip_jinja_wrappers(sql: &str) -> String {
        let mut result = sql.to_string();
        // Replace {{ expr }} with expr, trimming inner whitespace
        while let Some(open) = result.find("{{") {
            if let Some(rel_close) = result[open..].find("}}") {
                let close = open + rel_close;
                let inner = result[open + 2..close].trim().to_string();
                result.replace_range(open..close + 2, &inner);
            } else {
                break;
            }
        }
        result
    }

    /// Extract all ref() dependencies from a model.
    pub fn dependencies(&self, model_name: &str) -> Result<Vec<String>> {
        let model = self
            .models
            .get(model_name)
            .ok_or_else(|| RustLakeError::Engine(format!("Model '{}' not found", model_name)))?;

        let mut deps = Vec::new();
        let sql = &model.sql;
        let mut search_from = 0;

        while let Some(start) = sql[search_from..].find("ref('") {
            let abs_start = search_from + start + 5;
            if let Some(end) = sql[abs_start..].find("')") {
                let ref_name = &sql[abs_start..abs_start + end];
                if !deps.contains(&ref_name.to_string()) {
                    deps.push(ref_name.to_string());
                }
                search_from = abs_start + end + 2;
            } else {
                break;
            }
        }

        Ok(deps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Model, ModelConfig};

    #[test]
    fn test_compile_ref() {
        let models = vec![
            Model {
                name: "stg_orders".to_string(),
                sql: "SELECT * FROM raw.orders".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
            Model {
                name: "fct_orders".to_string(),
                sql: "SELECT * FROM ref('stg_orders') WHERE status = 'completed'".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
        ];

        let compiler = SqlCompiler::new(models);
        let compiled = compiler.compile("fct_orders").unwrap();
        assert_eq!(
            compiled,
            "SELECT * FROM stg_orders WHERE status = 'completed'"
        );
    }

    #[test]
    fn test_dependencies() {
        let models = vec![
            Model {
                name: "a".to_string(),
                sql: "SELECT 1".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
            Model {
                name: "b".to_string(),
                sql: "SELECT * FROM ref('a') JOIN ref('a')".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
        ];

        let compiler = SqlCompiler::new(models);
        let deps = compiler.dependencies("b").unwrap();
        assert_eq!(deps, vec!["a".to_string()]);
    }

    #[test]
    fn test_compile_jinja_ref() {
        let models = vec![
            Model {
                name: "stg_orders".to_string(),
                sql: "SELECT * FROM raw.orders".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
            Model {
                name: "fct_orders".to_string(),
                sql: "SELECT * FROM {{ ref('stg_orders') }} WHERE status = 'completed'".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
        ];

        let compiler = SqlCompiler::new(models);
        let compiled = compiler.compile("fct_orders").unwrap();
        assert_eq!(
            compiled,
            "SELECT * FROM stg_orders WHERE status = 'completed'"
        );
    }

    #[test]
    fn test_compile_jinja_source() {
        let models = vec![Model {
            name: "stg_orders".to_string(),
            sql: "SELECT * FROM {{ source('raw', 'orders') }}".to_string(),
            config: ModelConfig::default(),
            description: String::new(),
            columns: vec![],
        }];

        let compiler = SqlCompiler::new(models);
        let compiled = compiler.compile("stg_orders").unwrap();
        assert_eq!(compiled, "SELECT * FROM raw.orders");
    }

    #[test]
    fn test_compile_with_source_map() {
        let models = vec![
            Model {
                name: "stg_orders".to_string(),
                sql: "SELECT * FROM {{ source('raw', 'orders') }}".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
            Model {
                name: "fct_orders".to_string(),
                sql: "SELECT * FROM {{ ref('stg_orders') }} WHERE status = 'completed'".to_string(),
                config: ModelConfig::default(),
                description: String::new(),
                columns: vec![],
            },
        ];

        let compiler = SqlCompiler::new(models);
        let compiled = compiler
            .compile_with_source_map("stg_orders", |src, table| {
                if src == "raw" && table == "orders" {
                    Some("'sample-data/orders.csv'".to_string())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(compiled, "SELECT * FROM 'sample-data/orders.csv'");
    }
}
