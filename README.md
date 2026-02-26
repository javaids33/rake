<h1 align="center">RustLake</h1>
<p align="center"><strong>The open-source Databricks alternative, built entirely in Rust.</strong></p>

<p align="center">
  <img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License">
  <img src="https://img.shields.io/badge/rust-1.75%2B-orange" alt="Rust Version">
  <img src="https://img.shields.io/badge/crates-13-yellow" alt="Crate Count">
  <img src="https://img.shields.io/badge/TPC--H-22%2F22%20pass-brightgreen" alt="TPC-H">
  <img src="https://img.shields.io/badge/build-passing-brightgreen" alt="Build Status">
</p>

---

## Mission

Most data platforms today require teams to stitch together several JVM-based services — a cluster scheduler, a query engine, a streaming layer, a transform framework, a vector database — each with its own deployment, cold-start penalty, and serialization boundary. RustLake is a hypothesis: what if you didn't have to? By building every subsystem in Rust on a shared Apache Arrow memory model, we can test whether a single-binary platform that cold-starts in 100ms and passes data between components with zero serialization can meet the same needs that currently require an entire distributed stack. This isn't about replacing Databricks for every use case — it's about exploring whether the complexity tax most teams pay is actually necessary, and giving teams a lighter, open-source option to find out for themselves.

## Cold Start Benchmark

Measured on an Apple M-series machine, release build (`cargo build --release`). Time from process launch to each subsystem accepting requests:

| Milestone | Time |
|-----------|------|
| `/health` responds | 24ms |
| First SQL query executes | 56ms |
| Vector index loaded (20 docs, 128 dims) | 79ms |
| Scheduled jobs loaded (4 jobs from disk) | 101ms |
| **Fully operational** | **101ms** |

All subsystems — SQL engine, vector search, scheduler, streaming, transforms, REST API — running and serving requests in ~100ms from a cold process start. For comparison, a typical Spark driver takes 15-30 seconds to initialize.

## Architecture

```
                            ┌──────────────────────────────────────────┐
                            │              Web Dashboard               │
                            │  SQL Editor  Catalog  Transforms  Jobs   │
                            │  Streaming   Vector   Benchmarks  ...    │
                            └────────────────────┬─────────────────────┘
                                                 │ HTTP :3001
                            ┌────────────────────▼─────────────────────┐
                            │          rustlake-api  (Axum :3000)      │
                            │  REST API · CORS · Query History · Auth  │
                            └──┬─────┬──────┬──────┬──────┬────────┬───┘
                               │     │      │      │      │        │
              ┌────────────────▼─┐ ┌─▼──────▼─┐ ┌─▼──────▼──┐ ┌───▼──────────┐
              │  rustlake-router │ │  engine   │ │  stream    │ │  transform   │
              │                  │ │(DataFusion│ │ Kafka/CDC  │ │  dbt-compat  │
              │  SQL AST →       │ │  51)      │ │ ingestion  │ │  ref/source  │
              │  workload type → │ │           │ │            │ │  lineage DAG │
              │  engine dispatch │ │  Arrow    │ │  Arrow     │ │              │
              └────────┬─────────┘ │  native   │ │  native    │ └──────────────┘
                       │           └─────┬─────┘ └──────┬─────┘
                       │                 │              │
              ┌────────▼──┐        ┌─────▼─────┐  ┌────▼──────┐  ┌────────────┐
              │  vector   │        │  format   │  │ scheduler │  │  flight    │
              │  Lance +  │        │  Iceberg  │  │  DAG-based│  │ Arrow      │
              │  cosine   │        │  Delta    │  │  cron +   │  │ Flight SQL │
              │  HNSW     │        │  Parquet  │  │  events   │  │ gRPC       │
              └─────┬─────┘        └─────┬─────┘  └───────────┘  └────────────┘
                    │                    │
              ┌─────▼────────────────────▼─────┐
              │           rustlake-catalog      │
              │  Iceberg REST Catalog spec      │
              │  In-memory · SQLite · Postgres  │
              └──────────────┬─────────────────┘
                             │
              ┌──────────────▼─────────────────┐
              │          rustlake-storage       │
              │  S3 · GCS · ADLS · MinIO · Local│
              │  object_store abstraction       │
              └──────────────┬─────────────────┘
                             │
              ┌──────────────▼─────────────────┐
              │          rustlake-core          │
              │  Arrow utilities · Error types  │
              │  Config · Tracing · Traits      │
              └────────────────────────────────┘
```

**Data flows as Arrow `RecordBatch` arrays at every boundary** — storage to engine, engine to API, API to Flight clients. No serialization tax.

## Features

- **SQL Analytics** — Full SQL via DataFusion 51 on Arrow 57. TPC-H 22/22 at SF0.1 (600K rows). JOINs, CTEs, window functions, subqueries.
- **Streaming Ingestion** — Kafka, MongoDB CDC, Postgres CDC connectors. Pipeline CRUD, backpressure, DLQ, checkpoint management.
- **Vector Search** — Cosine similarity on 128-dim embeddings. Brute-force, IVF-PQ, HNSW index support. Pre-loaded product catalog.
- **dbt-Compatible Transforms** — `ref()` and `source()` macro resolution, dependency DAGs, topological execution, column-level lineage.
- **Scheduler** — Time-based (CRON/Quartz/intervals), event-based (file arrival), continuous triggers. 9 job types. Job clusters with concurrency limits.
- **Web Dashboard** — 14-page Next.js UI: SQL editor with Monaco + autocomplete, data catalog, streaming monitor, vector search, transform editor, benchmarks, scheduler, and more.
- **Arrow Flight SQL** — gRPC transport for BI tool connectivity (Tableau, Superset, DBeaver).
- **Multi-Source Queries** — Upload CSV/Parquet/JSON, connect Postgres/MySQL/MongoDB, and JOIN across sources in a single SQL query.
- **Python Bindings** — PyO3-powered `RustLakeSession` with zero-copy Arrow FFI to Polars and PyArrow.

## Local Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Rust | 1.75+ | Backend (13 crates) |
| Node.js | 18+ | Frontend (Next.js) |
| Docker | 20+ | Optional — Postgres, MinIO, etc. |

### 1. Clone and build

```bash
git clone https://github.com/rustlake/rustlake.git
cd rustlake

# Build the Rust backend
cargo build

# Install frontend dependencies
cd web && npm install && cd ..
```

### 2. Start the API server

```bash
cargo run --bin rustlake-api
# Server starts at http://127.0.0.1:3000
# Includes: sample data, 20 pre-indexed vector docs, 6 transform models
```

### 3. Start the web UI

```bash
cd web
npm run dev
# Dashboard opens at http://localhost:3001
```

### 4. (Optional) Start external databases

```bash
# Postgres, MySQL, MongoDB, MinIO — all seeded with demo data
docker compose up -d

# Connect to Postgres from the UI:
#   Host: localhost, Port: 5433, DB: rustlake_demo
#   User: rustlake, Password: rustlake
```

### 5. Verify it works

```bash
# Health check
curl http://127.0.0.1:3000/health

# Run a query
curl -X POST http://127.0.0.1:3000/api/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT 1 + 1 AS result"}'

# Vector search
curl -X POST http://127.0.0.1:3000/api/v1/vector/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "wireless headphones", "k": 5}'
```

## Crate Map

```
crates/
  rustlake-core/          Shared types, Arrow utilities, config, error handling
  rustlake-storage/       object_store abstraction (S3/GCS/ADLS/local)
  rustlake-catalog/       Iceberg REST Catalog spec, namespace/table CRUD
  rustlake-format/        Table format layer — Iceberg, Delta, Lance, Parquet
  rustlake-engine/        DataFusion integration — SQL execution, table providers
  rustlake-stream/        Kafka/CDC ingestion, materialized views, backpressure
  rustlake-vector/        Vector similarity search, embeddings, KNN
  rustlake-router/        SQL AST classification → multi-engine dispatch
  rustlake-scheduler/     DAG orchestration, cron scheduling, job clusters
  rustlake-flight/        Arrow Flight SQL server/client for distributed transport
  rustlake-transform/     dbt-compatible SQL compilation, ref/source, lineage
  rustlake-python/        PyO3 bindings — DataFrame API, notebook integration
  rustlake-api/           Axum HTTP server, REST API, web dashboard proxy
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/api/v1/sql` | Execute SQL, returns Arrow-serialized JSON |
| `GET` | `/api/v1/tables` | List registered tables |
| `POST` | `/api/v1/tables/register` | Register CSV/Parquet/JSON as a table |
| `DELETE` | `/api/v1/tables/{name}` | Deregister a table |
| `GET` | `/api/v1/tables/{name}/schema` | Column names, types, nullability |
| `GET` | `/api/v1/tables/{name}/preview` | First 100 rows |
| `GET` | `/api/v1/tables/{name}/stats` | Row count, min/max/null stats |
| `PUT` | `/api/v1/tables/{name}/description` | Update table/column descriptions |
| `GET` | `/api/v1/query/history` | Query audit log with timing |
| `GET` | `/api/v1/system/info` | Version, uptime, engine info |
| `GET` | `/api/v1/transforms` | List dbt-compatible transform models |
| `POST` | `/api/v1/transforms` | Create a new transform |
| `POST` | `/api/v1/transforms/{name}/run` | Compile and execute a transform |
| `GET` | `/api/v1/lineage` | DAG lineage graph (nodes + edges) |
| `POST` | `/api/v1/vector/search` | Semantic similarity search |
| `GET` | `/api/v1/vector/status` | Vector index stats |
| `POST` | `/api/v1/stream/ingest` | Trigger event ingestion |
| `GET` | `/api/v1/stream/status` | Pipeline metrics |
| `GET` | `/api/v1/stream/events` | Recent events from buffer |
| `GET` | `/api/v1/schedules` | List scheduled jobs |
| `POST` | `/api/v1/schedules` | Create a scheduled job |
| `POST` | `/api/v1/schedules/{id}/run` | Trigger immediate execution |
| `GET` | `/api/v1/clusters` | List job clusters |
| `GET` | `/api/v1/connections` | List database connections |
| `POST` | `/api/v1/connections` | Add external database |
| `POST` | `/api/v1/upload` | Upload CSV/Parquet/JSON file |
| `GET` | `/api/v1/streaming/pipelines` | List streaming pipelines |
| `GET` | `/api/v1/flight/info` | Arrow Flight capabilities |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Query Engine | DataFusion 51 |
| Columnar Format | Apache Arrow 57 / Parquet 57 |
| HTTP Server | Axum 0.8 |
| Async Runtime | Tokio (full features) |
| RPC | Arrow Flight via tonic 0.14 |
| Frontend | Next.js 16, React 19, Monaco Editor, SWR, Tailwind CSS |
| Databases | Postgres 16, MySQL 8, MongoDB 7 (via Docker) |
| Object Storage | MinIO / S3 / GCS / ADLS via `object_store` |
| Error Handling | thiserror 2 (libraries), anyhow 1 (binaries) |
| Python | PyO3 0.23, maturin |

## Contributing

1. Read `CLAUDE.md` for architecture details and crate boundaries
2. One PR per feature/fix — small, focused, reviewable
3. Run before pushing:
   ```bash
   cargo clippy -- -D warnings && cargo fmt --check && cargo test
   ```
4. Every PR includes: tests, doc comments on new public API, changelog entry
5. Performance-sensitive changes need `criterion` benchmarks with before/after numbers

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
