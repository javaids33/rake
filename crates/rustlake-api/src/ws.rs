use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use chrono::Utc;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use rustlake_router::{QueryClassifier, QueryType};

use crate::routes;
use crate::state::{AppState, QueryHistoryEntry};

// ── Client -> Server messages ────────────────────────────────────────

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ClientMsg {
    #[serde(rename = "query")]
    Query {
        query_id: String,
        sql: String,
        #[serde(default = "default_auto")]
        engine: String,
    },
    #[serde(rename = "cancel")]
    Cancel { query_id: String },
    #[serde(rename = "ping")]
    Ping,
}

fn default_auto() -> String {
    "auto".to_string()
}

// ── Server -> Client messages ────────────────────────────────────────

#[derive(Serialize)]
#[serde(tag = "type")]
enum ServerMsg {
    #[serde(rename = "query_start")]
    QueryStart {
        query_id: String,
        engine: String,
        query_type: String,
    },
    #[serde(rename = "query_rows")]
    QueryRows {
        query_id: String,
        columns: Vec<String>,
        rows: Vec<serde_json::Value>,
        chunk_index: u32,
    },
    #[serde(rename = "query_complete")]
    QueryComplete {
        query_id: String,
        row_count: usize,
        duration_ms: u128,
        parse_ms: u128,
        exec_ms: u128,
        engine: String,
        query_type: String,
    },
    #[serde(rename = "query_error")]
    QueryError {
        query_id: String,
        error: String,
        engine: String,
    },
    #[serde(rename = "query_cancelled")]
    QueryCancelled { query_id: String },
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "error")]
    Error { message: String },
}

const CHUNK_SIZE: usize = 100;
const MAX_INFLIGHT: usize = 3;

type Sender = Arc<Mutex<SplitSink<WebSocket, Message>>>;

/// Axum handler that upgrades HTTP to WebSocket.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_connection(socket, state))
}

async fn send_msg(tx: &Sender, msg: &ServerMsg) {
    if let Ok(json) = serde_json::to_string(msg) {
        let _ = tx.lock().await.send(Message::Text(json.into())).await;
    }
}

async fn handle_connection(socket: WebSocket, state: Arc<AppState>) {
    let (sink, mut stream) = socket.split();
    let tx: Sender = Arc::new(Mutex::new(sink));

    // Track in-flight queries for cancellation
    let inflight: Arc<Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    while let Some(Ok(msg)) = stream.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };

        let client_msg: ClientMsg = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(e) => {
                send_msg(
                    &tx,
                    &ServerMsg::Error {
                        message: format!("Invalid message: {}", e),
                    },
                )
                .await;
                continue;
            }
        };

        match client_msg {
            ClientMsg::Ping => {
                send_msg(&tx, &ServerMsg::Pong).await;
            }
            ClientMsg::Cancel { query_id } => {
                let mut map = inflight.lock().await;
                if let Some(cancel_tx) = map.remove(&query_id) {
                    let _ = cancel_tx.send(true);
                    send_msg(&tx, &ServerMsg::QueryCancelled { query_id }).await;
                }
            }
            ClientMsg::Query {
                query_id,
                sql,
                engine,
            } => {
                // Check concurrency limit
                {
                    let map = inflight.lock().await;
                    if map.len() >= MAX_INFLIGHT {
                        send_msg(
                            &tx,
                            &ServerMsg::QueryError {
                                query_id,
                                error:
                                    "Too many concurrent queries (max 3). Wait for one to finish."
                                        .into(),
                                engine: String::new(),
                            },
                        )
                        .await;
                        continue;
                    }
                }

                // Create cancellation channel
                let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
                inflight.lock().await.insert(query_id.clone(), cancel_tx);

                let tx = tx.clone();
                let state = state.clone();
                let inflight = inflight.clone();

                // Spawn query execution as a separate task
                let qid = query_id.clone();
                tokio::spawn(async move {
                    execute_query(tx, state, qid.clone(), sql, engine, cancel_rx).await;
                    inflight.lock().await.remove(&qid);
                });
            }
        }
    }
}

async fn execute_query(
    tx: Sender,
    state: Arc<AppState>,
    query_id: String,
    sql: String,
    engine_choice: String,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
) {
    let start = Instant::now();
    let uuid = Uuid::parse_str(&query_id).unwrap_or_else(|_| Uuid::new_v4());

    tracing::info!(sql = %sql, query_id = %query_id, engine = %engine_choice, "WS query received");

    // Classify
    let parse_start = Instant::now();
    let classification = QueryClassifier::classify_with_engine(&sql)
        .unwrap_or(rustlake_router::ClassificationResult {
            query_type: QueryType::Olap,
            engine: rustlake_router::EngineTarget::Either,
        });
    let query_type = classification.query_type;
    let parse_ms = parse_start.elapsed().as_millis();

    // Read-only check
    if matches!(query_type, QueryType::Ddl | QueryType::Dml) {
        let read_only = state.read_only_tables.read().await;
        if !read_only.is_empty() {
            let sql_upper = sql.trim().to_uppercase();
            if let Some(table) = routes::extract_target_table(&sql_upper) {
                let table_lower = table.to_lowercase();
                if read_only
                    .iter()
                    .any(|ro| table_lower == ro.to_lowercase())
                {
                    send_msg(
                        &tx,
                        &ServerMsg::QueryError {
                            query_id,
                            error: format!("Table '{}' is read-only", table),
                            engine: String::new(),
                        },
                    )
                    .await;
                    return;
                }
            }
        }
    }

    let engine_name = routes::determine_engine(&state, &engine_choice, &classification.engine);

    // Send query_start
    send_msg(
        &tx,
        &ServerMsg::QueryStart {
            query_id: query_id.clone(),
            engine: engine_name.to_string(),
            query_type: query_type.to_string(),
        },
    )
    .await;

    // Execute with cancellation support
    let exec_start = Instant::now();
    let result = tokio::select! {
        res = async {
            match engine_name {
                "DuckDB" => routes::execute_via_duckdb(&state, &sql).await,
                "Polars" => routes::execute_via_polars(&state, &sql).await,
                _ => {
                    let ctx = state.ctx.read().await;
                    ctx.sql(&sql).await.map_err(|e| e.to_string())
                }
            }
        } => res,
        _ = async {
            while !*cancel_rx.borrow() {
                if cancel_rx.changed().await.is_err() {
                    // Sender dropped — stop waiting
                    std::future::pending::<()>().await;
                }
            }
        } => {
            tracing::info!(query_id = %query_id, "Query cancelled via WebSocket");
            return; // Cancellation message already sent by the Cancel handler
        }
    };
    let exec_ms = exec_start.elapsed().as_millis();

    // Handle engine failure with DataFusion fallback
    let (batches, final_engine, final_exec_ms) = match result {
        Ok(b) => (b, engine_name, exec_ms),
        Err(e) if engine_name != "DataFusion" => {
            tracing::warn!(engine = engine_name, error = %e, "WS: engine failed, falling back to DataFusion");
            let fb_start = Instant::now();
            let ctx = state.ctx.read().await;
            match ctx.sql(&sql).await {
                Ok(b) => (b, "DataFusion", fb_start.elapsed().as_millis()),
                Err(fb_err) => {
                    let duration_ms = start.elapsed().as_millis();
                    state
                        .record_query(QueryHistoryEntry {
                            query_id: uuid,
                            sql,
                            query_type: query_type.to_string(),
                            row_count: 0,
                            duration_ms,
                            timestamp: Utc::now(),
                            status: "error".into(),
                            error: Some(fb_err.to_string()),
                            engine: "DataFusion".into(),
                            s3_bytes_scanned: 0,
                            s3_requests: 0,
                            estimated_cost_usd: 0.0,
                            snapshot_context: std::collections::HashMap::new(),
                        })
                        .await;
                    send_msg(
                        &tx,
                        &ServerMsg::QueryError {
                            query_id,
                            error: fb_err.to_string(),
                            engine: "DataFusion".into(),
                        },
                    )
                    .await;
                    return;
                }
            }
        }
        Err(e) => {
            let duration_ms = start.elapsed().as_millis();
            state
                .record_query(QueryHistoryEntry {
                    query_id: uuid,
                    sql,
                    query_type: query_type.to_string(),
                    row_count: 0,
                    duration_ms,
                    timestamp: Utc::now(),
                    status: "error".into(),
                    error: Some(e.to_string()),
                    engine: engine_name.into(),
                    s3_bytes_scanned: 0,
                    s3_requests: 0,
                    estimated_cost_usd: 0.0,
                    snapshot_context: std::collections::HashMap::new(),
                })
                .await;
            send_msg(
                &tx,
                &ServerMsg::QueryError {
                    query_id,
                    error: e.to_string(),
                    engine: engine_name.into(),
                },
            )
            .await;
            return;
        }
    };

    // Serialize and stream rows in chunks
    let columns: Vec<String> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();

    let all_rows = match routes::batches_to_json(&batches) {
        Ok(r) => r,
        Err(e) => {
            send_msg(
                &tx,
                &ServerMsg::QueryError {
                    query_id,
                    error: format!("Serialization failed: {}", e),
                    engine: final_engine.into(),
                },
            )
            .await;
            return;
        }
    };

    let row_count = all_rows.len();

    // Stream chunks
    for (i, chunk) in all_rows.chunks(CHUNK_SIZE).enumerate() {
        // Check cancellation between chunks
        if *cancel_rx.borrow() {
            return;
        }
        send_msg(
            &tx,
            &ServerMsg::QueryRows {
                query_id: query_id.clone(),
                columns: if i == 0 { columns.clone() } else { vec![] },
                rows: chunk.to_vec(),
                chunk_index: i as u32,
            },
        )
        .await;
    }

    let duration_ms = start.elapsed().as_millis();

    // Record history
    state
        .query_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state
        .record_query(QueryHistoryEntry {
            query_id: uuid,
            sql,
            query_type: query_type.to_string(),
            row_count,
            duration_ms,
            timestamp: Utc::now(),
            status: "success".into(),
            error: None,
            engine: final_engine.into(),
            s3_bytes_scanned: 0,
            s3_requests: 0,
            estimated_cost_usd: 0.0,
            snapshot_context: std::collections::HashMap::new(),
        })
        .await;

    // Send completion
    send_msg(
        &tx,
        &ServerMsg::QueryComplete {
            query_id,
            row_count,
            duration_ms,
            parse_ms,
            exec_ms: final_exec_ms,
            engine: final_engine.to_string(),
            query_type: query_type.to_string(),
        },
    )
    .await;

    tracing::info!(
        query_id = %uuid,
        engine = final_engine,
        row_count,
        exec_ms = final_exec_ms,
        duration_ms,
        "WS query complete"
    );
}
