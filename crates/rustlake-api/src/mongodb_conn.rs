//! MongoDB connector — connects to external MongoDB databases, discovers collections,
//! and converts documents to Arrow RecordBatches for registration in DataFusion.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use mongodb::bson::{Bson, Document};
use mongodb::options::ClientOptions;
use mongodb::Client;

/// Connection parameters for a MongoDB database.
#[derive(Clone)]
pub struct MongoConnParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

impl MongoConnParams {
    fn connection_string(&self) -> String {
        format!(
            "mongodb://{}:{}@{}:{}/{}?authSource=admin",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

/// Connect to MongoDB and discover all user collections (excluding system collections).
pub async fn connect_and_discover(params: &MongoConnParams) -> Result<Vec<String>, String> {
    let client_options = ClientOptions::parse(&params.connection_string())
        .await
        .map_err(|e| format!("Failed to parse MongoDB connection string: {}", e))?;

    let client = Client::with_options(client_options)
        .map_err(|e| format!("Failed to create MongoDB client: {}", e))?;

    let db = client.database(&params.database);

    let collections = db
        .list_collection_names(None)
        .await
        .map_err(|e| format!("Failed to list collections: {}", e))?;

    // Filter out system collections
    let user_collections: Vec<String> = collections
        .into_iter()
        .filter(|c| !c.starts_with("system."))
        .collect();

    Ok(user_collections)
}

/// Fetch all documents from a MongoDB collection and convert to an Arrow RecordBatch.
///
/// Schema is inferred by sampling the first batch of documents. Fields are discovered
/// from all sampled documents (union of all keys). MongoDB's flexible schema means
/// some documents may not have all fields — those are represented as nulls.
pub async fn fetch_collection_as_arrow(
    params: &MongoConnParams,
    collection_name: &str,
) -> Result<RecordBatch, String> {
    let client_options = ClientOptions::parse(&params.connection_string())
        .await
        .map_err(|e| format!("Failed to parse MongoDB connection string: {}", e))?;

    let client = Client::with_options(client_options)
        .map_err(|e| format!("Failed to create MongoDB client: {}", e))?;

    let db = client.database(&params.database);
    let collection = db.collection::<Document>(collection_name);

    // Fetch all documents (up to 100k to avoid memory issues)
    use futures::TryStreamExt;
    let mut cursor = collection
        .find(None, None)
        .await
        .map_err(|e| format!("Failed to query collection '{}': {}", collection_name, e))?;

    let mut docs: Vec<Document> = Vec::new();
    while let Some(doc) = cursor
        .try_next()
        .await
        .map_err(|e| format!("Failed to read document: {}", e))?
    {
        docs.push(doc);
        if docs.len() >= 100_000 {
            break;
        }
    }

    if docs.is_empty() {
        let schema = Schema::new(vec![Field::new("_id", DataType::Utf8, false)]);
        return Ok(RecordBatch::new_empty(Arc::new(schema)));
    }

    // Discover schema from all documents (union of keys, excluding _id)
    let mut field_types: Vec<(String, DataType)> = Vec::new();
    let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

    for doc in &docs {
        for (key, value) in doc {
            if key == "_id" {
                continue;
            }
            if !seen_keys.contains(key) {
                seen_keys.insert(key.clone());
                field_types.push((key.clone(), bson_to_arrow_type(value)));
            }
        }
    }

    // Sort fields alphabetically for deterministic schema
    field_types.sort_by(|a, b| a.0.cmp(&b.0));

    let fields: Vec<Field> = field_types
        .iter()
        .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Build Arrow arrays
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(field_types.len());

    for (field_name, arrow_type) in &field_types {
        let array: ArrayRef = match arrow_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = docs
                    .iter()
                    .map(|d| d.get(field_name).and_then(bson_to_bool))
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = docs
                    .iter()
                    .map(|d| d.get(field_name).and_then(bson_to_i64))
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let mut builder = Float64Array::builder(docs.len());
                for doc in &docs {
                    match doc.get(field_name).and_then(bson_to_f64) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let mut builder = TimestampMicrosecondArray::builder(docs.len());
                for doc in &docs {
                    match doc.get(field_name) {
                        Some(Bson::DateTime(dt)) => {
                            builder.append_value(dt.timestamp_millis() * 1000);
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Fallback: stringify
                let mut builder = StringBuilder::new();
                for doc in &docs {
                    match doc.get(field_name) {
                        Some(v) => builder.append_value(bson_to_string(v)),
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

/// Map a BSON value to an Arrow DataType.
fn bson_to_arrow_type(value: &Bson) -> DataType {
    match value {
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Int32(_) | Bson::Int64(_) => DataType::Int64,
        Bson::Double(_) => DataType::Float64,
        Bson::DateTime(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => DataType::Utf8,
    }
}

fn bson_to_bool(v: &Bson) -> Option<bool> {
    match v {
        Bson::Boolean(b) => Some(*b),
        Bson::Null => None,
        _ => None,
    }
}

fn bson_to_i64(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(d) => Some(*d as i64),
        Bson::Null => None,
        _ => None,
    }
}

fn bson_to_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(d) => Some(*d),
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Null => None,
        _ => None,
    }
}

fn bson_to_string(v: &Bson) -> String {
    match v {
        Bson::String(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(d) => d.to_string(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Null => String::new(),
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::DateTime(dt) => dt.try_to_rfc3339_string().unwrap_or_default(),
        Bson::Document(doc) => serde_json::to_string(doc).unwrap_or_default(),
        Bson::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
        other => format!("{:?}", other),
    }
}
