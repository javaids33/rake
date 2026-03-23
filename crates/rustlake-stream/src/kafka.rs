//! Kafka consumer/producer using `rdkafka`.
//!
//! `KafkaSource` implements [`StreamSource`] — consumes messages from a Kafka topic
//! and emits Arrow `RecordBatch` batches. `KafkaSink` implements [`StreamSink`] —
//! produces Arrow rows as JSON messages to a Kafka topic.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::TopicPartitionList;
use rustlake_core::{Result, RustLakeError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::schema_registry::SchemaRegistryClient;
use crate::StreamSource;

// ── Configuration ────────────────────────────────────────────────────

/// Kafka connection and consumer/producer configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Comma-separated broker addresses (e.g. `"localhost:9092"`).
    pub brokers: String,
    /// Topic to consume from or produce to.
    pub topic: String,
    /// Consumer group ID.
    #[serde(default = "default_group_id")]
    pub group_id: String,
    /// Offset reset policy: `"earliest"` or `"latest"`.
    #[serde(default = "default_offset_reset")]
    pub offset_reset: String,
    /// Number of messages to batch into a single RecordBatch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum time in ms to wait for a batch to fill before flushing.
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
    /// SASL security protocol (e.g. `"SASL_SSL"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_protocol: Option<String>,
    /// SASL mechanism (e.g. `"PLAIN"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_mechanism: Option<String>,
    /// SASL username.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_username: Option<String>,
    /// SASL password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_password: Option<String>,
    /// Optional Schema Registry URL for Avro/JSON Schema deserialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry_url: Option<String>,
    /// Message format: `"json"` (default), `"avro"`, or `"string"`.
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_group_id() -> String {
    "rustlake-consumer".into()
}
fn default_offset_reset() -> String {
    "earliest".into()
}
fn default_batch_size() -> usize {
    1000
}
fn default_batch_timeout_ms() -> u64 {
    5000
}
fn default_format() -> String {
    "json".into()
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".into(),
            topic: "events".into(),
            group_id: default_group_id(),
            offset_reset: default_offset_reset(),
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            schema_registry_url: None,
            format: default_format(),
        }
    }
}

// ── KafkaSource ──────────────────────────────────────────────────────

/// Kafka consumer that implements [`StreamSource`].
///
/// Consumes messages from a single Kafka topic and emits Arrow `RecordBatch`es.
/// Messages are deserialized based on the configured format (JSON, Avro, or raw string).
pub struct KafkaSource {
    name: String,
    config: KafkaConfig,
    schema: SchemaRef,
    running: Arc<AtomicBool>,
    consumer: Arc<StreamConsumer>,
    schema_registry: Option<Arc<SchemaRegistryClient>>,
}

impl KafkaSource {
    /// Create a new Kafka consumer source.
    ///
    /// Connects to the broker(s) and subscribes to the configured topic.
    /// If `schema_registry_url` is set and format is `"avro"`, initializes
    /// the schema registry client for Avro deserialization.
    pub fn new(name: &str, config: KafkaConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("auto.offset.reset", &config.offset_reset)
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "30000")
            .set("max.poll.interval.ms", "300000");

        if let Some(ref proto) = config.security_protocol {
            client_config.set("security.protocol", proto);
        }
        if let Some(ref mechanism) = config.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref user) = config.sasl_username {
            client_config.set("sasl.username", user);
        }
        if let Some(ref pass) = config.sasl_password {
            client_config.set("sasl.password", pass);
        }

        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| RustLakeError::Engine(format!("Failed to create Kafka consumer: {e}")))?;

        consumer
            .subscribe(&[&config.topic])
            .map_err(|e| RustLakeError::Engine(format!("Failed to subscribe to topic '{}': {e}", config.topic)))?;

        // Raw message schema: offset, key, value, topic, partition, timestamp
        let schema = Arc::new(Schema::new(vec![
            Field::new("offset", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("topic", DataType::Utf8, false),
            Field::new("partition", DataType::Int64, false),
        ]));

        let schema_registry = if let Some(ref url) = config.schema_registry_url {
            Some(Arc::new(SchemaRegistryClient::new(url)))
        } else {
            None
        };

        Ok(Self {
            name: name.to_string(),
            config,
            schema,
            running: Arc::new(AtomicBool::new(false)),
            consumer: Arc::new(consumer),
            schema_registry,
        })
    }

    /// Get current consumer lag across all assigned partitions.
    pub fn fetch_consumer_lag(&self) -> Result<u64> {
        let assignment = self.consumer.assignment()
            .map_err(|e| RustLakeError::Engine(format!("Failed to get assignment: {e}")))?;

        let mut total_lag: u64 = 0;
        for elem in assignment.elements() {
            let (lo, hi) = self.consumer
                .fetch_watermarks(elem.topic(), elem.partition(), Duration::from_secs(5))
                .map_err(|e| RustLakeError::Engine(format!("Failed to fetch watermarks: {e}")))?;
            let committed = self.consumer
                .committed_offsets(
                    {
                        let mut tpl = TopicPartitionList::new();
                        tpl.add_partition(elem.topic(), elem.partition());
                        tpl
                    },
                    Duration::from_secs(5),
                )
                .ok()
                .and_then(|c| {
                    c.elements().first().and_then(|e| {
                        match e.offset() {
                            rdkafka::Offset::Offset(o) => Some(o),
                            _ => None,
                        }
                    })
                })
                .unwrap_or(lo);
            if hi > committed {
                total_lag += (hi - committed) as u64;
            }
        }
        Ok(total_lag)
    }

    /// Commit current consumer offsets synchronously.
    pub fn commit_offsets(&self) -> Result<()> {
        self.consumer
            .commit_consumer_state(CommitMode::Sync)
            .map_err(|e| RustLakeError::Engine(format!("Failed to commit offsets: {e}")))?;
        Ok(())
    }

    /// Check if this consumer is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Signal the consumer loop to stop.
    pub fn signal_stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl StreamSource for KafkaSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<mpsc::Receiver<Result<RecordBatch>>> {
        let (tx, rx) = mpsc::channel(32);
        let schema = self.schema.clone();
        let consumer = self.consumer.clone();
        let running = self.running.clone();
        let batch_size = self.config.batch_size;
        let batch_timeout = Duration::from_millis(self.config.batch_timeout_ms);
        let format = self.config.format.clone();
        let sr_client = self.schema_registry.clone();

        running.store(true, Ordering::SeqCst);

        tokio::spawn(async move {
            tracing::info!(
                topic = consumer.subscription().map(|s| s.elements().iter().map(|e| e.topic().to_string()).collect::<Vec<_>>().join(",")).unwrap_or_default(),
                batch_size = batch_size,
                format = %format,
                "Kafka consumer loop started"
            );

            while running.load(Ordering::SeqCst) {
                // Collect a micro-batch
                let mut offsets = Vec::with_capacity(batch_size);
                let mut timestamps = Vec::with_capacity(batch_size);
                let mut keys = Vec::with_capacity(batch_size);
                let mut values = Vec::with_capacity(batch_size);
                let mut topics = Vec::with_capacity(batch_size);
                let mut partitions = Vec::with_capacity(batch_size);

                let batch_deadline = tokio::time::Instant::now() + batch_timeout;
                let mut count = 0;

                while count < batch_size {
                    let remaining = batch_deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() && count > 0 {
                        break;
                    }

                    let poll_timeout = if count == 0 {
                        // First message: wait longer
                        Duration::from_secs(1)
                    } else {
                        remaining.min(Duration::from_millis(100))
                    };

                    match tokio::time::timeout(poll_timeout, consumer.recv()).await {
                        Ok(Ok(msg)) => {
                            offsets.push(msg.offset());
                            timestamps.push(
                                msg.timestamp()
                                    .to_millis()
                                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                            );
                            keys.push(
                                msg.key()
                                    .map(|k| String::from_utf8_lossy(k).to_string()),
                            );

                            // Decode value based on format
                            let decoded_value = match msg.payload() {
                                Some(payload) => {
                                    if format == "avro" {
                                        if let Some(ref sr) = sr_client {
                                            match sr.decode_avro_message(payload).await {
                                                Ok(json_str) => Some(json_str),
                                                Err(e) => {
                                                    tracing::warn!(error = %e, "Avro decode failed, storing raw");
                                                    Some(String::from_utf8_lossy(payload).to_string())
                                                }
                                            }
                                        } else {
                                            Some(String::from_utf8_lossy(payload).to_string())
                                        }
                                    } else {
                                        Some(String::from_utf8_lossy(payload).to_string())
                                    }
                                }
                                None => None,
                            };

                            values.push(decoded_value);
                            topics.push(
                                msg.topic().to_string(),
                            );
                            partitions.push(msg.partition() as i64);
                            count += 1;
                        }
                        Ok(Err(e)) => {
                            tracing::error!(error = %e, "Kafka consumer recv error");
                            if !running.load(Ordering::SeqCst) {
                                break;
                            }
                        }
                        Err(_) => {
                            // Timeout — flush what we have
                            if count > 0 {
                                break;
                            }
                            // No messages yet, keep polling
                            if !running.load(Ordering::SeqCst) {
                                break;
                            }
                            continue;
                        }
                    }
                }

                if count == 0 {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    continue;
                }

                // Build RecordBatch
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(offsets)),
                        Arc::new(
                            TimestampMillisecondArray::from(timestamps)
                                .with_timezone("UTC"),
                        ),
                        Arc::new(StringArray::from(
                            keys.iter()
                                .map(|k| k.as_deref())
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(StringArray::from(
                            values.iter()
                                .map(|v| v.as_deref())
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(StringArray::from(topics)),
                        Arc::new(Int64Array::from(partitions)),
                    ],
                )
                .map_err(|e| RustLakeError::Engine(format!("Failed to create batch: {e}")));

                // Commit offsets after building batch
                if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                    tracing::warn!(error = %e, "Offset commit failed");
                }

                if tx.send(batch).await.is_err() {
                    tracing::info!("Kafka consumer channel closed");
                    break;
                }
            }

            tracing::info!("Kafka consumer loop stopped");
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        // Final commit
        let _ = self.consumer.commit_consumer_state(CommitMode::Sync);
        Ok(())
    }

    async fn lag(&self) -> Result<Option<u64>> {
        match self.fetch_consumer_lag() {
            Ok(lag) => Ok(Some(lag)),
            Err(e) => {
                tracing::debug!(error = %e, "Failed to fetch consumer lag");
                Ok(None)
            }
        }
    }
}

// ── KafkaSink ────────────────────────────────────────────────────────

/// Kafka producer that serializes Arrow rows as JSON messages.
pub struct KafkaSink {
    name: String,
    topic: String,
    producer: FutureProducer,
    buffer: tokio::sync::Mutex<Vec<String>>,
    flush_size: usize,
}

impl KafkaSink {
    /// Create a new Kafka producer sink.
    pub fn new(name: &str, config: &KafkaConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "30000")
            .set("acks", "all")
            .set("enable.idempotence", "true");

        if let Some(ref proto) = config.security_protocol {
            client_config.set("security.protocol", proto);
        }
        if let Some(ref mechanism) = config.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref user) = config.sasl_username {
            client_config.set("sasl.username", user);
        }
        if let Some(ref pass) = config.sasl_password {
            client_config.set("sasl.password", pass);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| RustLakeError::Engine(format!("Failed to create Kafka producer: {e}")))?;

        Ok(Self {
            name: name.to_string(),
            topic: config.topic.clone(),
            producer,
            buffer: tokio::sync::Mutex::new(Vec::new()),
            flush_size: config.batch_size,
        })
    }
}

#[async_trait]
impl crate::StreamSink for KafkaSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn write(&self, batch: RecordBatch) -> Result<()> {
        // Serialize each row to JSON
        let mut buf = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(&batch)
            .map_err(|e| RustLakeError::Engine(format!("Arrow JSON write failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| RustLakeError::Engine(format!("Arrow JSON finish failed: {e}")))?;

        let json_str = String::from_utf8_lossy(&buf);
        let mut buffer = self.buffer.lock().await;
        for line in json_str.lines() {
            if !line.trim().is_empty() {
                buffer.push(line.to_string());
            }
        }

        if buffer.len() >= self.flush_size {
            let messages: Vec<String> = buffer.drain(..).collect();
            drop(buffer);
            self.produce_messages(&messages).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut buffer = self.buffer.lock().await;
        if buffer.is_empty() {
            return Ok(());
        }
        let messages: Vec<String> = buffer.drain(..).collect();
        drop(buffer);
        self.produce_messages(&messages).await
    }

    async fn commit(&self) -> Result<()> {
        self.flush().await
    }
}

impl KafkaSink {
    async fn produce_messages(&self, messages: &[String]) -> Result<()> {
        let mut futures = Vec::with_capacity(messages.len());
        for msg in messages {
            let record = FutureRecord::to(&self.topic)
                .payload(msg.as_bytes())
                .key("");
            futures.push(self.producer.send(record, Duration::from_secs(5)));
        }
        for fut in futures {
            fut.await
                .map_err(|(e, _)| RustLakeError::Engine(format!("Kafka produce failed: {e}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config_defaults() {
        let cfg = KafkaConfig::default();
        assert_eq!(cfg.brokers, "localhost:9092");
        assert_eq!(cfg.group_id, "rustlake-consumer");
        assert_eq!(cfg.offset_reset, "earliest");
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.format, "json");
    }

    #[test]
    fn test_kafka_config_serde() {
        let json = r#"{"brokers":"kafka:29092","topic":"orders","group_id":"test"}"#;
        let cfg: KafkaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.brokers, "kafka:29092");
        assert_eq!(cfg.topic, "orders");
        assert_eq!(cfg.group_id, "test");
        assert_eq!(cfg.offset_reset, "earliest"); // default
        assert_eq!(cfg.format, "json"); // default
    }

    #[test]
    fn test_kafka_config_with_auth() {
        let json = r#"{
            "brokers": "broker:9093",
            "topic": "secure-topic",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "user",
            "sasl_password": "pass"
        }"#;
        let cfg: KafkaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.security_protocol.as_deref(), Some("SASL_SSL"));
        assert_eq!(cfg.sasl_mechanism.as_deref(), Some("PLAIN"));
    }
}
