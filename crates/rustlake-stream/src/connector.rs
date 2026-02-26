//! Built-in stream connectors.
//!
//! Production connectors (Kafka, MongoDB CDC, Postgres) require
//! their respective client crates. This module provides a simulated
//! connector for development and testing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use chrono::Utc;
use rand::Rng;
use rustlake_core::{Result, RustLakeError};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{StreamEvent, StreamSource};

// ── Constants for realistic e-commerce data ──────────────────────────

const EVENT_TYPES: &[&str] = &["page_view", "add_to_cart", "purchase", "search"];

const PAGES: &[&str] = &[
    "/home",
    "/products",
    "/products/electronics",
    "/products/clothing",
    "/products/shoes",
    "/products/home-garden",
    "/cart",
    "/checkout",
    "/search",
    "/account",
    "/deals",
    "/products/accessories",
];

const SEARCH_TERMS: &[&str] = &[
    "red shoes",
    "wireless headphones",
    "laptop stand",
    "running shoes",
    "coffee maker",
    "yoga mat",
    "backpack",
    "winter jacket",
    "mechanical keyboard",
    "desk lamp",
    "water bottle",
    "phone case",
];

const PRODUCT_NAMES: &[&str] = &[
    "UltraBoost Running Shoe",
    "Noise-Cancel Headphones Pro",
    "Ergonomic Laptop Stand",
    "Premium Yoga Mat",
    "Stainless Travel Mug",
    "Wireless Keyboard MX",
    "LED Desk Lamp",
    "Canvas Backpack",
    "Winter Parka",
    "Phone Case Ultra",
];

const CATEGORIES: &[&str] = &[
    "electronics",
    "clothing",
    "shoes",
    "home-garden",
    "accessories",
    "sports",
    "kitchen",
    "office",
];

/// A simulated stream source that generates synthetic events for testing.
pub struct SimulatedSource {
    name: String,
    schema: SchemaRef,
    rate_per_sec: u64,
    running: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
}

impl SimulatedSource {
    /// Create a new simulated source with the given event rate.
    pub fn new(name: &str, rate_per_sec: u64) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, true),
        ]));

        Self {
            name: name.to_string(),
            schema,
            rate_per_sec,
            running: Arc::new(AtomicBool::new(false)),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Generate a burst of realistic e-commerce `StreamEvent`s.
    ///
    /// Returns `count` events with randomized event types, customer IDs,
    /// session IDs, product references, and timestamps.
    pub fn generate_events(count: usize) -> Vec<StreamEvent> {
        let mut rng = rand::thread_rng();
        let mut events = Vec::with_capacity(count);
        let base_time = Utc::now();

        for i in 0..count {
            let event_type_idx = rng.gen_range(0..EVENT_TYPES.len());
            let event_type = EVENT_TYPES[event_type_idx].to_string();
            let customer_id = rng.gen_range(1000..9999_u32);
            let session_id = Uuid::new_v4().to_string();

            // Spread timestamps across the burst window (up to 1 second jitter)
            let jitter_ms = rng.gen_range(0..1000_i64);
            let timestamp = base_time + chrono::Duration::milliseconds((i as i64) * 10 + jitter_ms);

            let (product_id, page, properties) = match event_type.as_str() {
                "page_view" => {
                    let page = PAGES[rng.gen_range(0..PAGES.len())].to_string();
                    let referrer = if rng.gen_bool(0.6) {
                        "google.com"
                    } else if rng.gen_bool(0.5) {
                        "direct"
                    } else {
                        "social"
                    };
                    let props = serde_json::json!({
                        "referrer": referrer,
                        "device": if rng.gen_bool(0.6) { "mobile" } else { "desktop" },
                        "duration_ms": rng.gen_range(500..30000),
                    });
                    (None, page, props)
                }
                "add_to_cart" => {
                    let pid = rng.gen_range(100..999_u32);
                    let product_name = PRODUCT_NAMES[rng.gen_range(0..PRODUCT_NAMES.len())];
                    let category = CATEGORIES[rng.gen_range(0..CATEGORIES.len())];
                    let price = (rng.gen_range(999..19999_u32) as f64) / 100.0;
                    let quantity = rng.gen_range(1..5_u32);
                    let props = serde_json::json!({
                        "product_name": product_name,
                        "category": category,
                        "price": price,
                        "quantity": quantity,
                        "currency": "USD",
                    });
                    (Some(pid), "/cart".to_string(), props)
                }
                "purchase" => {
                    let pid = rng.gen_range(100..999_u32);
                    let items = rng.gen_range(1..6_u32);
                    let total = (rng.gen_range(1999..49999_u32) as f64) / 100.0;
                    let payment = if rng.gen_bool(0.6) {
                        "credit_card"
                    } else if rng.gen_bool(0.5) {
                        "paypal"
                    } else {
                        "apple_pay"
                    };
                    let props = serde_json::json!({
                        "order_id": format!("ORD-{}", rng.gen_range(100000..999999_u32)),
                        "total": total,
                        "items_count": items,
                        "payment_method": payment,
                        "currency": "USD",
                        "shipping": if rng.gen_bool(0.4) { "express" } else { "standard" },
                    });
                    (Some(pid), "/checkout".to_string(), props)
                }
                "search" => {
                    let term = SEARCH_TERMS[rng.gen_range(0..SEARCH_TERMS.len())];
                    let results = rng.gen_range(0..250_u32);
                    let props = serde_json::json!({
                        "query": term,
                        "results_count": results,
                        "filters": {
                            "category": CATEGORIES[rng.gen_range(0..CATEGORIES.len())],
                            "price_max": rng.gen_range(50..500_u32),
                        },
                    });
                    (None, "/search".to_string(), props)
                }
                _ => (None, "/home".to_string(), serde_json::json!({})),
            };

            events.push(StreamEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type,
                customer_id,
                session_id,
                product_id,
                page,
                timestamp,
                properties,
            });
        }

        events
    }
}

#[async_trait]
impl StreamSource for SimulatedSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<mpsc::Receiver<Result<RecordBatch>>> {
        let (tx, rx) = mpsc::channel(32);
        let schema = self.schema.clone();
        let rate = self.rate_per_sec;
        let running = self.running.clone();
        let counter = self.counter.clone();

        running.store(true, Ordering::SeqCst);

        tokio::spawn(async move {
            let batch_size = rate.max(1) as usize;
            let interval = tokio::time::Duration::from_secs(1);

            while running.load(Ordering::SeqCst) {
                let start_id = counter.fetch_add(batch_size as u64, Ordering::SeqCst);
                let now = chrono::Utc::now().timestamp_millis();

                let event_types = ["page_view", "add_to_cart", "purchase", "search"];

                let ids: Vec<i64> = (0..batch_size)
                    .map(|i| start_id as i64 + i as i64)
                    .collect();
                let timestamps: Vec<i64> = (0..batch_size).map(|_| now).collect();
                let types: Vec<&str> = (0..batch_size)
                    .map(|i| event_types[i % event_types.len()])
                    .collect();
                let payloads: Vec<String> = (0..batch_size)
                    .map(|i| format!("{{\"item_id\": {}}}", start_id as usize + i))
                    .collect();

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC")),
                        Arc::new(StringArray::from(types)),
                        Arc::new(StringArray::from(
                            payloads.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                        )),
                    ],
                )
                .map_err(|e| RustLakeError::Engine(format!("Failed to create batch: {}", e)));

                if tx.send(batch).await.is_err() {
                    break;
                }

                tokio::time::sleep(interval).await;
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn lag(&self) -> Result<Option<u64>> {
        Ok(Some(0)) // Simulated source has no lag
    }
}
