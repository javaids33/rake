use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::ast::Statement;
use rustlake_core::{Result, RustLakeError};

/// Classification of a SQL query to determine which engine should handle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// Analytical queries (SELECT with aggregations, joins, etc.)
    Olap,
    /// Streaming queries (CREATE MATERIALIZED VIEW, continuous queries)
    Streaming,
    /// Interactive / point queries (simple lookups, LIMIT-bound)
    Interactive,
    /// Machine learning operations (PREDICT, vector search)
    MachineLearning,
    /// DDL statements (CREATE TABLE, DROP, ALTER)
    Ddl,
    /// DML statements (INSERT, UPDATE, DELETE)
    Dml,
    /// Utility statements (SHOW, DESCRIBE, EXPLAIN)
    Utility,
}

impl std::fmt::Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Olap => write!(f, "OLAP"),
            Self::Streaming => write!(f, "Streaming"),
            Self::Interactive => write!(f, "Interactive"),
            Self::MachineLearning => write!(f, "ML"),
            Self::Ddl => write!(f, "DDL"),
            Self::Dml => write!(f, "DML"),
            Self::Utility => write!(f, "Utility"),
        }
    }
}

/// Classifies SQL queries to route them to the appropriate execution engine.
pub struct QueryClassifier;

impl QueryClassifier {
    /// Classify a SQL string into a QueryType.
    pub fn classify(sql: &str) -> Result<QueryType> {
        let statements = DFParser::parse_sql(sql)
            .map_err(|e| RustLakeError::Query(format!("Failed to parse SQL: {}", e)))?;

        let stmt = statements
            .into_iter()
            .next()
            .ok_or_else(|| RustLakeError::Query("Empty SQL statement".into()))?;

        // Convert DFStatement to sqlparser Statement for matching
        match &stmt {
            datafusion::sql::parser::Statement::Statement(s) => Self::classify_statement(s),
            datafusion::sql::parser::Statement::CreateExternalTable(_) => Ok(QueryType::Ddl),
            _ => Ok(QueryType::Utility),
        }
    }

    fn classify_statement(stmt: &Statement) -> Result<QueryType> {
        match stmt {
            // DDL
            Statement::CreateTable { .. }
            | Statement::CreateView { .. }
            | Statement::CreateIndex { .. }
            | Statement::AlterTable { .. }
            | Statement::Drop { .. } => Ok(QueryType::Ddl),

            // DML
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                Ok(QueryType::Dml)
            }

            // Utility
            Statement::ShowTables { .. }
            | Statement::ShowColumns { .. }
            | Statement::Explain { .. } => Ok(QueryType::Utility),

            // SELECT — further classify based on query characteristics
            Statement::Query(query) => Self::classify_query(query),

            _ => Ok(QueryType::Olap),
        }
    }

    fn classify_query(query: &datafusion::sql::sqlparser::ast::Query) -> Result<QueryType> {
        let sql_str = query.to_string().to_uppercase();

        // Heuristic: streaming queries reference streams or materialized views
        if sql_str.contains("STREAM") || sql_str.contains("EMIT") {
            return Ok(QueryType::Streaming);
        }

        // Heuristic: ML queries use model functions
        if sql_str.contains("PREDICT")
            || sql_str.contains("EMBEDDING")
            || sql_str.contains("VECTOR_SEARCH")
        {
            return Ok(QueryType::MachineLearning);
        }

        // Heuristic: OLAP queries have GROUP BY or JOIN
        let has_group_by = sql_str.contains("GROUP BY");
        let has_join = sql_str.contains("JOIN");

        if has_group_by || has_join {
            return Ok(QueryType::Olap);
        }

        // Interactive: simple lookups, LIMIT-bound, no aggregation/join
        if query.limit_clause.is_some() || (!has_group_by && !has_join) {
            return Ok(QueryType::Interactive);
        }

        // Default: OLAP
        Ok(QueryType::Olap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_select() {
        let qt = QueryClassifier::classify("SELECT 1 + 1 AS result").unwrap();
        assert_eq!(qt, QueryType::Interactive);
    }

    #[test]
    fn test_classify_aggregate() {
        let qt = QueryClassifier::classify("SELECT region, SUM(sales) FROM orders GROUP BY region")
            .unwrap();
        assert_eq!(qt, QueryType::Olap);
    }

    #[test]
    fn test_classify_ddl() {
        let qt = QueryClassifier::classify("CREATE TABLE test (id INT)").unwrap();
        assert_eq!(qt, QueryType::Ddl);
    }

    #[test]
    fn test_classify_insert() {
        let qt = QueryClassifier::classify("INSERT INTO test VALUES (1)").unwrap();
        assert_eq!(qt, QueryType::Dml);
    }
}
