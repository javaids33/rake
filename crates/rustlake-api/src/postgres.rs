//! Postgres connector — connects to external Postgres databases, discovers tables,
//! and converts query results to Arrow RecordBatches for registration in DataFusion.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rust_decimal::Decimal;
use tokio_postgres::{types::Type, Column, NoTls, Row};

/// Connection parameters for a Postgres database.
#[derive(Clone)]
pub struct PgConnParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

impl PgConnParams {
    fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.database, self.username, self.password
        )
    }
}

/// Connect to Postgres and discover all public user tables.
pub async fn connect_and_discover(params: &PgConnParams) -> Result<Vec<String>, String> {
    let (client, connection) = tokio_postgres::connect(&params.connection_string(), NoTls)
        .await
        .map_err(|e| format!("Failed to connect to Postgres: {}", e))?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "Postgres connection error");
        }
    });

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW') \
             ORDER BY table_name",
            &[],
        )
        .await
        .map_err(|e| format!("Failed to discover tables: {}", e))?;

    let tables: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    Ok(tables)
}

/// Fetch all rows from a Postgres table and convert to an Arrow RecordBatch.
pub async fn fetch_table_as_arrow(
    params: &PgConnParams,
    table_name: &str,
) -> Result<RecordBatch, String> {
    let (client, connection) = tokio_postgres::connect(&params.connection_string(), NoTls)
        .await
        .map_err(|e| format!("Failed to connect to Postgres: {}", e))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "Postgres connection error");
        }
    });

    // Simple identifier validation to prevent SQL injection
    if !table_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        return Err(format!("Invalid table name: {}", table_name));
    }

    let query = format!("SELECT * FROM \"{}\"", table_name);
    let rows = client
        .query(&query, &[])
        .await
        .map_err(|e| format!("Failed to query table '{}': {}", table_name, e))?;

    if rows.is_empty() {
        // Still need to get the schema from an empty result
        let stmt = client
            .prepare(&query)
            .await
            .map_err(|e| format!("Failed to prepare query: {}", e))?;
        let schema = pg_columns_to_schema(stmt.columns());
        return Ok(RecordBatch::new_empty(Arc::new(schema)));
    }

    pg_rows_to_record_batch(&rows, rows[0].columns())
}

/// Map Postgres column types to an Arrow Schema.
fn pg_columns_to_schema(columns: &[Column]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = pg_type_to_arrow(col.type_());
            Field::new(col.name(), dt, true)
        })
        .collect();
    Schema::new(fields)
}

/// Convert Postgres type to Arrow DataType.
fn pg_type_to_arrow(pg_type: &Type) -> DataType {
    match *pg_type {
        Type::BOOL => DataType::Boolean,
        Type::INT2 | Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => DataType::Float64,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        _ => DataType::Utf8, // TEXT, VARCHAR, and everything else
    }
}

/// Convert a slice of Postgres rows to an Arrow RecordBatch.
fn pg_rows_to_record_batch(rows: &[Row], columns: &[Column]) -> Result<RecordBatch, String> {
    let schema = Arc::new(pg_columns_to_schema(columns));
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let array: ArrayRef = match *col.type_() {
            Type::BOOL => {
                let values: Vec<Option<bool>> =
                    rows.iter().map(|r| r.get::<_, Option<bool>>(i)).collect();
                Arc::new(BooleanArray::from(values))
            }
            Type::INT2 | Type::INT4 => {
                let values: Vec<Option<i32>> =
                    rows.iter().map(|r| r.get::<_, Option<i32>>(i)).collect();
                Arc::new(Int32Array::from(values))
            }
            Type::INT8 => {
                let values: Vec<Option<i64>> =
                    rows.iter().map(|r| r.get::<_, Option<i64>>(i)).collect();
                Arc::new(Int64Array::from(values))
            }
            Type::FLOAT4 | Type::FLOAT8 => {
                let values: Vec<Option<f64>> =
                    rows.iter().map(|r| r.get::<_, Option<f64>>(i)).collect();
                Arc::new(Float64Array::from(values))
            }
            Type::NUMERIC => {
                // Use rust_decimal::Decimal which has proper FromSql for NUMERIC
                let mut builder = Float64Array::builder(rows.len());
                for row in rows {
                    let val: Option<Decimal> = row.try_get(i).ok().flatten();
                    match val.and_then(|d| d.to_string().parse::<f64>().ok()) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            Type::DATE => {
                let mut builder = Date32Array::builder(rows.len());
                for row in rows {
                    let val: Option<chrono::NaiveDate> = row.get(i);
                    match val {
                        Some(d) => {
                            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let days = (d - epoch).num_days() as i32;
                            builder.append_value(days);
                        }
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                let mut builder = TimestampMicrosecondArray::builder(rows.len());
                for row in rows {
                    let val: Option<chrono::NaiveDateTime> = row.get(i);
                    match val {
                        Some(ts) => {
                            builder.append_value(ts.and_utc().timestamp_micros());
                        }
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Fallback: read as text
                let mut builder = StringBuilder::new();
                for row in rows {
                    let val: Option<String> = row
                        .try_get::<_, Option<String>>(i)
                        .unwrap_or(None);
                    match val {
                        Some(s) => builder.append_value(&s),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
}
