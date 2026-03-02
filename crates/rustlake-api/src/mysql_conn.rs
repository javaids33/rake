//! MySQL connector — connects to external MySQL databases, discovers tables,
//! and converts query results to Arrow RecordBatches for registration in DataFusion.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, Pool, Row, Value};

/// Connection parameters for a MySQL database.
#[derive(Clone)]
pub struct MysqlConnParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

/// Connect to MySQL and discover all user tables in the target database.
pub async fn connect_and_discover(params: &MysqlConnParams) -> Result<Vec<String>, String> {
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname(&params.host)
        .tcp_port(params.port)
        .db_name(Some(&params.database))
        .user(Some(&params.username))
        .pass(Some(&params.password))
        .into();

    let pool = Pool::new(opts);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| format!("Failed to connect to MySQL: {}", e))?;

    let tables: Vec<String> = conn
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
        )
        .await
        .map_err(|e| format!("Failed to discover tables: {}", e))?;

    drop(conn);
    pool.disconnect()
        .await
        .map_err(|e| format!("Failed to disconnect: {}", e))?;

    Ok(tables)
}

/// Fetch all rows from a MySQL table and convert to an Arrow RecordBatch.
pub async fn fetch_table_as_arrow(
    params: &MysqlConnParams,
    table_name: &str,
) -> Result<RecordBatch, String> {
    // Simple identifier validation to prevent SQL injection
    if !table_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        return Err(format!("Invalid table name: {}", table_name));
    }

    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname(&params.host)
        .tcp_port(params.port)
        .db_name(Some(&params.database))
        .user(Some(&params.username))
        .pass(Some(&params.password))
        .into();

    let pool = Pool::new(opts);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| format!("Failed to connect to MySQL: {}", e))?;

    // First get column metadata
    let col_query = format!(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name = '{}' ORDER BY ordinal_position",
        table_name
    );
    let col_rows: Vec<(String, String, String)> = conn
        .query(col_query)
        .await
        .map_err(|e| format!("Failed to get column info: {}", e))?;

    if col_rows.is_empty() {
        return Err(format!("Table '{}' not found or has no columns", table_name));
    }

    let fields: Vec<Field> = col_rows
        .iter()
        .map(|(name, dtype, _nullable)| {
            // Mark all columns as nullable to handle type conversion edge cases
            Field::new(name, mysql_type_to_arrow(dtype), true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Fetch all rows
    let query = format!("SELECT * FROM `{}`", table_name);
    let rows: Vec<Row> = conn
        .query(&query)
        .await
        .map_err(|e| format!("Failed to query table '{}': {}", table_name, e))?;

    drop(conn);
    pool.disconnect().await.ok();

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Build Arrow arrays from rows
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_rows.len());

    for (col_idx, (_, dtype, _)) in col_rows.iter().enumerate() {
        let arrow_type = mysql_type_to_arrow(dtype);
        let array: ArrayRef = match arrow_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| r.get::<Value, _>(col_idx).and_then(value_to_bool))
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|r| r.get::<Value, _>(col_idx).and_then(value_to_i32))
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| r.get::<Value, _>(col_idx).and_then(value_to_i64))
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let mut builder = Float64Array::builder(rows.len());
                for row in &rows {
                    match row.get::<Value, _>(col_idx).and_then(value_to_f64) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Date32 => {
                let mut builder = Date32Array::builder(rows.len());
                for row in &rows {
                    match row.get::<Value, _>(col_idx) {
                        Some(Value::Date(y, m, d, _, _, _, _)) => {
                            if let Some(date) =
                                chrono::NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32)
                            {
                                let epoch =
                                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                builder.append_value((date - epoch).num_days() as i32);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let mut builder = TimestampMicrosecondArray::builder(rows.len());
                for row in &rows {
                    match row.get::<Value, _>(col_idx) {
                        Some(Value::Date(y, m, d, h, mi, s, us)) => {
                            if let Some(dt) = chrono::NaiveDate::from_ymd_opt(
                                y as i32, m as u32, d as u32,
                            )
                            .and_then(|date| {
                                date.and_hms_micro_opt(h as u32, mi as u32, s as u32, us)
                            }) {
                                builder.append_value(dt.and_utc().timestamp_micros());
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Fallback: read as text
                let mut builder = StringBuilder::new();
                for row in &rows {
                    match row.get::<Value, _>(col_idx) {
                        Some(Value::NULL) | None => builder.append_null(),
                        Some(v) => builder.append_value(value_to_string(&v)),
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

/// Map MySQL data type string to Arrow DataType.
fn mysql_type_to_arrow(dtype: &str) -> DataType {
    match dtype.to_lowercase().as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" | "double" | "decimal" | "numeric" => DataType::Float64,
        "bit" | "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => DataType::Utf8,
    }
}

fn value_to_bool(v: Value) -> Option<bool> {
    match v {
        Value::Int(i) => Some(i != 0),
        Value::UInt(u) => Some(u != 0),
        Value::Bytes(ref b) => std::str::from_utf8(b).ok()?.parse::<i64>().ok().map(|i| i != 0),
        Value::NULL => None,
        _ => None,
    }
}

fn value_to_i32(v: Value) -> Option<i32> {
    match v {
        Value::Int(i) => Some(i as i32),
        Value::UInt(u) => Some(u as i32),
        Value::Bytes(ref b) => std::str::from_utf8(b).ok()?.parse::<i32>().ok(),
        Value::NULL => None,
        _ => None,
    }
}

fn value_to_i64(v: Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(i),
        Value::UInt(u) => Some(u as i64),
        Value::Bytes(ref b) => std::str::from_utf8(b).ok()?.parse::<i64>().ok(),
        Value::NULL => None,
        _ => None,
    }
}

fn value_to_f64(v: Value) -> Option<f64> {
    match v {
        Value::Float(f) => Some(f as f64),
        Value::Double(d) => Some(d),
        Value::Int(i) => Some(i as f64),
        Value::UInt(u) => Some(u as f64),
        Value::Bytes(ref b) => std::str::from_utf8(b).ok()?.parse::<f64>().ok(),
        Value::NULL => None,
        _ => None,
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::NULL => String::new(),
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Date(y, m, d, h, mi, s, _us) => {
            format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mi, s)
        }
        Value::Time(neg, d, h, mi, s, _us) => {
            let sign = if *neg { "-" } else { "" };
            format!("{}{} {:02}:{:02}:{:02}", sign, d, h, mi, s)
        }
    }
}
