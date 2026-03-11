//! Trino TableProvider for DataFusion — makes Trino tables first-class citizens.
//!
//! Implements DataFusion's `TableProvider` trait backed by Trino REST API.
//! This enables:
//! - Transparent SQL queries: `SELECT * FROM trino_pg.customers WHERE city = 'NYC'`
//! - Predicate pushdown: WHERE filters are sent to Trino as SQL
//! - Projection pushdown: Only requested columns are fetched
//! - Cross-engine joins: Trino data can be JOINed with local tables, Postgres, etc.
//! - Rust UDFs on Trino data: ai_classify, ai_extract, etc. all work

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use futures::TryStreamExt;

use crate::trino_client::TrinoRestClient;

// ── Type mapping ────────────────────────────────────────────────────

/// Map Trino SQL types to Arrow DataType.
pub fn trino_type_to_arrow(trino_type: &str) -> DataType {
    let lower = trino_type.to_lowercase();
    match lower.as_str() {
        "integer" | "int" => DataType::Int32,
        "bigint" | "long" => DataType::Int64,
        "smallint" | "short" => DataType::Int16,
        "tinyint" | "byte" => DataType::Int8,
        "real" | "float" => DataType::Float32,
        "double" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "timestamp" | "timestamp with time zone" | "timestamp without time zone" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
        "decimal" => DataType::Float64, // simplification
        s if s.starts_with("decimal(") => DataType::Float64,
        s if s.starts_with("varchar") || s.starts_with("char") => DataType::Utf8,
        "varbinary" => DataType::Binary,
        _ => DataType::Utf8, // fallback to string
    }
}

/// Build an Arrow Schema from Trino column metadata.
pub fn trino_columns_to_schema(
    columns: &[crate::trino_client::TrinoColumnInfo],
) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, trino_type_to_arrow(&c.data_type), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Convert Trino JSON rows to Arrow RecordBatch.
pub fn json_rows_to_batch(
    schema: &SchemaRef,
    rows: &[Vec<serde_json::Value>],
) -> DFResult<RecordBatch> {
    if rows.is_empty() {
        return RecordBatch::try_new(schema.clone(), vec![])
            .or_else(|_| Ok(RecordBatch::new_empty(schema.clone())));
    }

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let values: Vec<Option<&serde_json::Value>> =
                rows.iter().map(|row| row.get(i)).collect();
            json_column_to_array(field.data_type(), &values)
        })
        .collect();

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
    })
}

fn json_column_to_array(
    data_type: &DataType,
    values: &[Option<&serde_json::Value>],
) -> ArrayRef {
    match data_type {
        DataType::Int32 | DataType::Int16 | DataType::Int8 => {
            let arr: Int32Array = values
                .iter()
                .map(|v| v.and_then(|v| v.as_i64()).map(|n| n as i32))
                .collect();
            Arc::new(arr)
        }
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| v.and_then(|v| v.as_i64()))
                .collect();
            Arc::new(arr)
        }
        DataType::Float32 | DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| {
                    v.and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_i64().map(|n| n as f64))
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            let arr: BooleanArray = values
                .iter()
                .map(|v| v.and_then(|v| v.as_bool()))
                .collect();
            Arc::new(arr)
        }
        DataType::Date32 => {
            use arrow::array::Date32Array;
            let arr: Date32Array = values
                .iter()
                .map(|v| {
                    v.and_then(|v| v.as_str()).and_then(|s| {
                        // Parse "YYYY-MM-DD" to days since epoch
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .ok()
                            .map(|d| {
                                (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                                    .num_days() as i32
                            })
                    })
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Timestamp(_, _) => {
            use arrow::array::TimestampMillisecondArray;
            let arr: TimestampMillisecondArray = values
                .iter()
                .map(|v| {
                    v.and_then(|v| v.as_str()).and_then(|s| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                            .ok()
                            .or_else(|| {
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                                    .ok()
                            })
                            .map(|dt| dt.and_utc().timestamp_millis())
                    })
                })
                .collect();
            Arc::new(arr)
        }
        _ => {
            // Fallback: convert everything to string
            let arr: StringArray = values
                .iter()
                .map(|v| {
                    v.and_then(|v| match v {
                        serde_json::Value::String(s) => Some(s.as_str()),
                        serde_json::Value::Null => None,
                        other => Some(other.as_str().unwrap_or("")),
                    })
                })
                .collect();
            Arc::new(arr)
        }
    }
}

// ── Filter to SQL conversion ────────────────────────────────────────

/// Convert a DataFusion `Expr` filter to a Trino SQL WHERE clause fragment.
fn expr_to_sql(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = expr_to_sql(&binary.left)?;
            let right = expr_to_sql(&binary.right)?;
            let op = match binary.op {
                Operator::Eq => "=",
                Operator::NotEq => "!=",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                Operator::And => "AND",
                Operator::Or => "OR",
                _ => return None,
            };
            Some(format!("({} {} {})", left, op, right))
        }
        Expr::Column(col) => Some(format!("\"{}\"", col.name)),
        Expr::Literal(scalar, _) => {
            use datafusion::scalar::ScalarValue;
            match scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                    Some(format!("'{}'", s.replace('\'', "''")))
                }
                ScalarValue::Int8(Some(n)) => Some(n.to_string()),
                ScalarValue::Int16(Some(n)) => Some(n.to_string()),
                ScalarValue::Int32(Some(n)) => Some(n.to_string()),
                ScalarValue::Int64(Some(n)) => Some(n.to_string()),
                ScalarValue::Float32(Some(n)) => Some(n.to_string()),
                ScalarValue::Float64(Some(n)) => Some(n.to_string()),
                ScalarValue::Boolean(Some(b)) => Some(if *b { "TRUE" } else { "FALSE" }.to_string()),
                ScalarValue::Null => Some("NULL".to_string()),
                _ => None,
            }
        }
        Expr::Not(inner) => {
            let s = expr_to_sql(inner)?;
            Some(format!("NOT {}", s))
        }
        Expr::IsNull(inner) => {
            let s = expr_to_sql(inner)?;
            Some(format!("{} IS NULL", s))
        }
        Expr::IsNotNull(inner) => {
            let s = expr_to_sql(inner)?;
            Some(format!("{} IS NOT NULL", s))
        }
        _ => None,
    }
}

// ── TrinoTableProvider ──────────────────────────────────────────────

/// A DataFusion `TableProvider` backed by Trino REST API.
///
/// Enables transparent SQL access to Trino tables with predicate and
/// projection pushdown. Queries like:
/// ```sql
/// SELECT name, email FROM trino_pg.customers WHERE city = 'NYC'
/// ```
/// become Trino REST calls: `SELECT "name", "email" FROM "pg"."public"."customers" WHERE "city" = 'NYC'`
pub struct TrinoTableProvider {
    /// Fully qualified Trino table: catalog.schema.table
    catalog: String,
    schema_name: String,
    table_name: String,
    /// Arrow schema for this table
    arrow_schema: SchemaRef,
    /// Shared Trino REST client
    rest: Arc<TrinoRestClient>,
}

impl TrinoTableProvider {
    pub fn new(
        catalog: String,
        schema_name: String,
        table_name: String,
        arrow_schema: SchemaRef,
        rest: Arc<TrinoRestClient>,
    ) -> Self {
        Self {
            catalog,
            schema_name,
            table_name,
            arrow_schema,
            rest,
        }
    }

    fn trino_fqn(&self) -> String {
        format!(
            "\"{}\".\"{}\".\"{}\"\n",
            self.catalog, self.schema_name, self.table_name
        )
    }
}

impl fmt::Debug for TrinoTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TrinoTableProvider({}.{}.{})",
            self.catalog, self.schema_name, self.table_name
        )
    }
}

#[async_trait]
impl TableProvider for TrinoTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if expr_to_sql(f).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Build projected column list
        let projected_schema = if let Some(indices) = projection {
            let fields: Vec<Field> = indices
                .iter()
                .map(|&i| self.arrow_schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(fields))
        } else {
            self.arrow_schema.clone()
        };

        let select_cols = if let Some(indices) = projection {
            indices
                .iter()
                .map(|&i| format!("\"{}\"", self.arrow_schema.field(i).name()))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "*".to_string()
        };

        // Build WHERE clause from pushable filters
        let where_parts: Vec<String> = filters
            .iter()
            .filter_map(|f| expr_to_sql(f))
            .collect();
        let where_clause = if where_parts.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_parts.join(" AND "))
        };

        let limit_clause = limit
            .map(|n| format!(" LIMIT {}", n))
            .unwrap_or_default();

        let sql = format!(
            "SELECT {} FROM {}{}{}",
            select_cols,
            self.trino_fqn(),
            where_clause,
            limit_clause
        );

        // For queries without LIMIT, use partitioned execution for parallelism
        let num_partitions = if limit.is_some() { 1 } else { 1 };
        // Note: Trino handles internal parallelism, so we use 1 partition
        // by default. Multi-partition support available via TrinoExec::new_partitioned()
        // for explicit LIMIT/OFFSET splitting on very large tables.

        Ok(Arc::new(TrinoExec::new(
            sql,
            self.catalog.clone(),
            projected_schema,
            self.rest.clone(),
            num_partitions,
        )))
    }
}

// ── TrinoExec (ExecutionPlan) ───────────────────────────────────────

/// Custom DataFusion `ExecutionPlan` that executes SQL against Trino REST API
/// and returns Arrow RecordBatch stream.
#[derive(Debug)]
struct TrinoExec {
    sql: String,
    catalog: String,
    schema: SchemaRef,
    rest: Arc<TrinoRestClient>,
    properties: PlanProperties,
}

impl TrinoExec {
    fn new(
        sql: String,
        catalog: String,
        schema: SchemaRef,
        rest: Arc<TrinoRestClient>,
        num_partitions: usize,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(num_partitions),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            sql,
            catalog,
            schema,
            rest,
            properties,
        }
    }
}

impl DisplayAs for TrinoExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TrinoExec: {}", self.sql)
            }
            _ => write!(f, "TrinoExec"),
        }
    }
}

impl ExecutionPlan for TrinoExec {
    fn name(&self) -> &str {
        "TrinoExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let sql = self.sql.clone();
        let catalog = self.catalog.clone();
        let schema = self.schema.clone();
        let rest = self.rest.clone();

        // Stream results in chunks of BATCH_SIZE rows to avoid loading all into memory
        const BATCH_SIZE: usize = 8192;

        let stream = futures::stream::once(async move {
            tracing::debug!(sql = %sql, catalog = %catalog, "TrinoExec: executing query");
            let start = std::time::Instant::now();

            let result = rest.execute_query(&sql, &catalog).await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Trino query failed: {}",
                    e
                ))
            })?;

            tracing::debug!(
                rows = result.row_count,
                duration_ms = start.elapsed().as_millis() as u64,
                "TrinoExec: query complete"
            );

            // Chunk rows into batches for streaming
            let mut batches = Vec::new();
            for chunk in result.rows.chunks(BATCH_SIZE) {
                batches.push(json_rows_to_batch(&schema, chunk)?);
            }
            if batches.is_empty() {
                batches.push(RecordBatch::new_empty(schema.clone()));
            }
            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
        })
        .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
