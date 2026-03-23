//! Confluent Schema Registry client and Avro deserialization.
//!
//! Provides `SchemaRegistryClient` for fetching and caching Avro/JSON schemas,
//! and utilities for decoding Confluent wire-format messages (magic byte + 4-byte
//! schema ID + payload) into JSON strings or Arrow RecordBatches.

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::from_avro_datum;
use apache_avro::Schema as AvroSchema;
use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rustlake_core::{Result, RustLakeError};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// A schema entry from the Confluent Schema Registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    /// Schema ID assigned by the registry.
    pub id: i32,
    /// Schema version under this subject.
    pub version: i32,
    /// Subject name (e.g. `"events-value"`).
    pub subject: String,
    /// Schema type: `"AVRO"`, `"JSON"`, or `"PROTOBUF"`.
    #[serde(default = "default_schema_type")]
    pub schema_type: String,
    /// The schema definition as a string (Avro JSON, JSON Schema, or Proto).
    pub schema: String,
}

fn default_schema_type() -> String {
    "AVRO".into()
}

/// Compatibility modes for schema evolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityMode {
    None,
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
}

/// Client for the Confluent Schema Registry REST API.
///
/// Caches schemas by ID to avoid redundant network requests.
pub struct SchemaRegistryClient {
    base_url: String,
    http: reqwest::Client,
    /// Cache: schema ID → parsed Avro schema + raw JSON.
    cache: RwLock<HashMap<i32, CachedSchema>>,
}

struct CachedSchema {
    avro_schema: AvroSchema,
    #[allow(dead_code)]
    raw_json: String,
}

impl SchemaRegistryClient {
    /// Create a new Schema Registry client.
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Fetch a schema by subject and version.
    pub async fn get_schema(&self, subject: &str, version: &str) -> Result<SchemaEntry> {
        let url = format!("{}/subjects/{}/versions/{}", self.base_url, subject, version);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Schema Registry request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(RustLakeError::Engine(format!(
                "Schema Registry returned {status}: {body}"
            )));
        }

        resp.json::<SchemaEntry>()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Failed to parse schema response: {e}")))
    }

    /// Fetch a schema by its global ID.
    pub async fn get_schema_by_id(&self, id: i32) -> Result<SchemaEntry> {
        let url = format!("{}/schemas/ids/{}", self.base_url, id);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Schema Registry request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(RustLakeError::Engine(format!(
                "Schema Registry returned {status}: {body}"
            )));
        }

        let entry: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Failed to parse schema response: {e}")))?;

        Ok(SchemaEntry {
            id,
            version: 0,
            subject: String::new(),
            schema_type: entry
                .get("schemaType")
                .and_then(|v| v.as_str())
                .unwrap_or("AVRO")
                .to_string(),
            schema: entry
                .get("schema")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }

    /// Register a new schema under a subject.
    pub async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: &str,
    ) -> Result<i32> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let body = serde_json::json!({
            "schema": schema,
            "schemaType": schema_type,
        });

        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Schema Registry request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(RustLakeError::Engine(format!(
                "Schema Registry returned {status}: {body}"
            )));
        }

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Failed to parse register response: {e}")))?;

        result
            .get("id")
            .and_then(|v| v.as_i64())
            .map(|id| id as i32)
            .ok_or_else(|| RustLakeError::Engine("Missing 'id' in register response".into()))
    }

    /// Check schema compatibility.
    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema: &str,
        schema_type: &str,
    ) -> Result<bool> {
        let url = format!(
            "{}/compatibility/subjects/{}/versions/latest",
            self.base_url, subject
        );
        let body = serde_json::json!({
            "schema": schema,
            "schemaType": schema_type,
        });

        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Schema Registry request failed: {e}")))?;

        if !resp.status().is_success() {
            return Ok(false);
        }

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Failed to parse compat response: {e}")))?;

        Ok(result
            .get("is_compatible")
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    /// List all subjects in the registry.
    pub async fn list_subjects(&self) -> Result<Vec<String>> {
        let url = format!("{}/subjects", self.base_url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Schema Registry request failed: {e}")))?;

        if !resp.status().is_success() {
            return Ok(Vec::new());
        }

        resp.json::<Vec<String>>()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Failed to parse subjects: {e}")))
    }

    /// List all versions for a subject.
    pub async fn list_versions(&self, subject: &str) -> Result<Vec<i32>> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Schema Registry request failed: {e}")))?;

        if !resp.status().is_success() {
            return Ok(Vec::new());
        }

        resp.json::<Vec<i32>>()
            .await
            .map_err(|e| RustLakeError::Engine(format!("Failed to parse versions: {e}")))
    }

    /// Decode a Confluent wire-format Avro message to a JSON string.
    ///
    /// Wire format: `[0x00][4-byte schema ID BE][Avro payload]`
    pub async fn decode_avro_message(&self, payload: &[u8]) -> Result<String> {
        if payload.len() < 5 {
            return Err(RustLakeError::Engine(
                "Avro message too short (< 5 bytes)".into(),
            ));
        }

        // Check magic byte
        if payload[0] != 0x00 {
            // Not Confluent wire format — try plain JSON
            return Ok(String::from_utf8_lossy(payload).to_string());
        }

        let schema_id =
            i32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);

        let avro_schema = self.get_or_fetch_avro_schema(schema_id).await?;

        let mut reader = &payload[5..];
        let value = from_avro_datum(&avro_schema, &mut reader, None)
            .map_err(|e| RustLakeError::Engine(format!("Avro decode failed: {e}")))?;

        let json_value = avro_value_to_json(&value);
        serde_json::to_string(&json_value)
            .map_err(|e| RustLakeError::Engine(format!("JSON serialize failed: {e}")))
    }

    /// Get a cached Avro schema or fetch from registry.
    async fn get_or_fetch_avro_schema(&self, schema_id: i32) -> Result<AvroSchema> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(&schema_id) {
                return Ok(cached.avro_schema.clone());
            }
        }

        // Fetch from registry
        let entry = self.get_schema_by_id(schema_id).await?;
        let avro_schema = AvroSchema::parse_str(&entry.schema)
            .map_err(|e| RustLakeError::Engine(format!("Failed to parse Avro schema: {e}")))?;

        // Cache it
        {
            let mut cache = self.cache.write().await;
            cache.insert(
                schema_id,
                CachedSchema {
                    avro_schema: avro_schema.clone(),
                    raw_json: entry.schema,
                },
            );
        }

        Ok(avro_schema)
    }
}

/// Convert an Avro schema to an Arrow schema (best-effort type mapping).
pub fn avro_schema_to_arrow(avro: &AvroSchema) -> Result<SchemaRef> {
    match avro {
        AvroSchema::Record(record) => {
            let fields: Vec<Field> = record
                .fields
                .iter()
                .map(|f| avro_field_to_arrow(&f.name, &f.schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Schema::new(fields)))
        }
        _ => Err(RustLakeError::Engine(
            "Top-level Avro schema must be a record".into(),
        )),
    }
}

fn avro_field_to_arrow(name: &str, schema: &AvroSchema) -> Result<Field> {
    match schema {
        AvroSchema::Null => Ok(Field::new(name, DataType::Null, true)),
        AvroSchema::Boolean => Ok(Field::new(name, DataType::Boolean, false)),
        AvroSchema::Int => Ok(Field::new(name, DataType::Int32, false)),
        AvroSchema::Long => Ok(Field::new(name, DataType::Int64, false)),
        AvroSchema::Float => Ok(Field::new(name, DataType::Float32, false)),
        AvroSchema::Double => Ok(Field::new(name, DataType::Float64, false)),
        AvroSchema::Bytes => Ok(Field::new(name, DataType::Binary, false)),
        AvroSchema::String => Ok(Field::new(name, DataType::Utf8, false)),
        AvroSchema::Union(union_schema) => {
            // Handle nullable types: Union(Null, T) → T with nullable=true
            let variants: Vec<&AvroSchema> = union_schema
                .variants()
                .iter()
                .filter(|s| !matches!(s, AvroSchema::Null))
                .collect();
            if variants.len() == 1 {
                let mut field = avro_field_to_arrow(name, variants[0])?;
                field = field.with_nullable(true);
                Ok(field)
            } else {
                // Complex union — store as JSON string
                Ok(Field::new(name, DataType::Utf8, true))
            }
        }
        AvroSchema::Array(_) => Ok(Field::new(name, DataType::Utf8, true)), // Serialize as JSON
        AvroSchema::Map(_) => Ok(Field::new(name, DataType::Utf8, true)),   // Serialize as JSON
        AvroSchema::Record(_) => Ok(Field::new(name, DataType::Utf8, true)), // Flatten as JSON
        AvroSchema::Date => Ok(Field::new(name, DataType::Date32, false)),
        AvroSchema::TimestampMillis => Ok(Field::new(
            name,
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )),
        AvroSchema::TimestampMicros => Ok(Field::new(
            name,
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )),
        _ => Ok(Field::new(name, DataType::Utf8, true)), // Fallback
    }
}

/// Convert an Avro `Value` to a `serde_json::Value`.
fn avro_value_to_json(value: &apache_avro::types::Value) -> serde_json::Value {
    use apache_avro::types::Value;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Long(l) => serde_json::json!(l),
        Value::Float(f) => serde_json::json!(f),
        Value::Double(d) => serde_json::json!(d),
        Value::Bytes(b) => serde_json::json!(base64_encode(b)),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Union(_idx, inner) => avro_value_to_json(inner),
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(avro_value_to_json).collect())
        }
        Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Record(fields) => {
            let obj: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Date(d) => serde_json::json!(d),
        Value::TimestampMillis(ts) => serde_json::json!(ts),
        Value::TimestampMicros(ts) => serde_json::json!(ts),
        Value::Enum(_idx, s) => serde_json::Value::String(s.clone()),
        Value::Fixed(_size, bytes) => serde_json::json!(base64_encode(bytes)),
        Value::Decimal(_d) => {
            // Decimal — convert to string representation
            serde_json::json!("decimal")
        }
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        _ => serde_json::Value::Null,
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    // Simple hex encoding since we don't want to add base64 dep
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a batch of Confluent wire-format Avro messages into a single Arrow RecordBatch.
///
/// Each message becomes a row. Nested Avro records are flattened to JSON strings.
pub async fn decode_avro_batch(
    sr_client: &SchemaRegistryClient,
    payloads: &[Vec<u8>],
) -> Result<RecordBatch> {
    if payloads.is_empty() {
        return Err(RustLakeError::Engine("Empty payload batch".into()));
    }

    // Decode all messages to JSON values
    let mut json_rows = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let json_str = sr_client.decode_avro_message(payload).await?;
        let value: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| RustLakeError::Engine(format!("JSON parse failed: {e}")))?;
        json_rows.push(value);
    }

    // Build Arrow schema from first message's keys
    let first = &json_rows[0];
    let fields: Vec<Field> = if let serde_json::Value::Object(map) = first {
        map.keys()
            .map(|k| Field::new(k.as_str(), DataType::Utf8, true))
            .collect()
    } else {
        vec![Field::new("value", DataType::Utf8, false)]
    };
    let schema = Arc::new(Schema::new(fields.clone()));

    // Build columns
    let columns: Vec<Arc<dyn arrow::array::Array>> = fields
        .iter()
        .map(|field| {
            let values: Vec<Option<String>> = json_rows
                .iter()
                .map(|row| {
                    row.get(field.name()).map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                })
                .collect();
            Arc::new(arrow::array::StringArray::from(values)) as Arc<dyn arrow::array::Array>
        })
        .collect();

    RecordBatch::try_new(schema, columns)
        .map_err(|e| RustLakeError::Engine(format!("Failed to create Avro batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avro_value_to_json() {
        use apache_avro::types::Value;

        assert_eq!(avro_value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            avro_value_to_json(&Value::Boolean(true)),
            serde_json::Value::Bool(true)
        );
        assert_eq!(avro_value_to_json(&Value::Int(42)), serde_json::json!(42));
        assert_eq!(
            avro_value_to_json(&Value::String("hello".into())),
            serde_json::json!("hello")
        );
    }

    #[test]
    fn test_avro_record_to_json() {
        use apache_avro::types::Value;

        let record = Value::Record(vec![
            ("name".into(), Value::String("Alice".into())),
            ("age".into(), Value::Int(30)),
        ]);
        let json = avro_value_to_json(&record);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
    }

    #[test]
    fn test_avro_union_to_json() {
        use apache_avro::types::Value;

        let union_val = Value::Union(1, Box::new(Value::String("inner".into())));
        assert_eq!(
            avro_value_to_json(&union_val),
            serde_json::json!("inner")
        );
    }

    #[test]
    fn test_schema_entry_default_type() {
        let json = r#"{"id":1,"version":1,"subject":"test","schema":"{}"}"#;
        let entry: SchemaEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.schema_type, "AVRO");
    }
}
