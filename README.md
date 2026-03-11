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

## Compile & Run Locally

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Rust | 1.75+ | Backend (13 crates) |
| Bun | 1.0+ | Frontend (Vite + React). Node.js 18+ also works (`npm` instead of `bun`) |
| Docker | 20+ | Optional — Postgres, MySQL, MongoDB, MinIO, Trino |

### 1. Clone and build

```bash
git clone https://github.com/rustlake/rustlake.git
cd rustlake

# Build the entire workspace (all 13 crates)
cargo build

# Or build only the API server binary
cargo build --bin rustlake-api

# Release build (optimized, ~100ms cold start)
cargo build --release
```

#### Feature flags

The API server has optional features enabled by default. To compile with a minimal set:

```bash
# Default features: duckdb, postgres, mysql, sqlite, polars
cargo build --bin rustlake-api

# Disable DuckDB and Polars (faster compile, fewer dependencies)
cargo build --bin rustlake-api --no-default-features --features postgres,mysql,sqlite

# Everything off (DataFusion-only, smallest binary)
cargo build --bin rustlake-api --no-default-features
```

| Feature | Default | Description |
|---------|---------|-------------|
| `duckdb` | yes | DuckDB OLAP accelerator (hybrid engine) |
| `polars` | yes | Polars engine backend |
| `postgres` | yes | Live Postgres federation via `datafusion-table-providers` |
| `mysql` | yes | Live MySQL federation via `datafusion-table-providers` |
| `sqlite` | yes | SQLite federation (bundled) |
| `clickhouse` | no | ClickHouse federation |
| `flight-sql-client` | no | Flight SQL client for remote engines |

### 2. Start the API server

```bash
# Development (debug build)
cargo run --bin rustlake-api
# Server starts at http://127.0.0.1:3000
# Includes: sample data, 20 pre-indexed vector docs, 6 transform models

# Release
cargo run --release --bin rustlake-api
```

#### Environment variables

```bash
# Enable/disable engines
RUSTLAKE_DUCKDB__ENABLED=true       # Enable DuckDB hybrid engine
RUSTLAKE_POLARS__ENABLED=true       # Enable Polars engine

# Arrow Flight gRPC (default: off)
RUSTLAKE_FLIGHT__ENABLED=true       # Start Flight SQL on :50051

# Cluster mode (default: standalone)
RUSTLAKE_CLUSTER__NODE_ROLE=coordinator  # or "worker" or "standalone"

# Auto-bootstrap external databases on startup
RUSTLAKE_AUTO_BOOTSTRAP=true

# Postgres connection (registered as pg.* tables)
RUSTLAKE_PG_HOST=localhost
RUSTLAKE_PG_PORT=5433
RUSTLAKE_PG_DB=rustlake_demo
RUSTLAKE_PG_USER=rustlake
RUSTLAKE_PG_PASSWORD=rustlake

# MySQL connection (registered as mysql.* tables)
RUSTLAKE_MYSQL_HOST=localhost
RUSTLAKE_MYSQL_PORT=3307
RUSTLAKE_MYSQL_DB=rustlake_demo
RUSTLAKE_MYSQL_USER=rustlake
RUSTLAKE_MYSQL_PASSWORD=rustlake

# MongoDB connection (registered as mongo.* tables)
RUSTLAKE_MONGO_HOST=localhost
RUSTLAKE_MONGO_PORT=27018
RUSTLAKE_MONGO_DB=rustlake_demo
RUSTLAKE_MONGO_USER=rustlake
RUSTLAKE_MONGO_PASSWORD=rustlake

# Logging
RUST_LOG=info                        # or debug, trace
```

### 3. Install and start the web UI

```bash
cd web
bun install        # or: npm install
bun run dev        # or: npm run dev
# Dashboard opens at http://localhost:3001
```

The UI is a Vite + React + TypeScript app with Tailwind CSS and Monaco Editor. It proxies API requests to the backend at `:3000`.

### 4. (Optional) Start Docker services

```bash
# Core databases — Postgres, MySQL, MongoDB, MinIO (all seeded with demo data)
docker compose up -d

# Add streaming services (Kafka, RabbitMQ, NATS, MQTT)
docker compose --profile streaming up -d

# Add analytics (ClickHouse, Cassandra)
docker compose --profile analytics up -d

# Add search (Elasticsearch, Redis)
docker compose --profile search up -d

# Start everything
docker compose --profile streaming --profile analytics --profile search up -d
```

#### Docker service credentials

| Service | Host | Port | User | Password | Database |
|---------|------|------|------|----------|----------|
| Postgres | localhost | 5433 | rustlake | rustlake | rustlake_demo |
| MySQL | localhost | 3307 | rustlake | rustlake | rustlake_demo |
| MongoDB | localhost | 27018 | rustlake | rustlake | rustlake_demo |
| MinIO | localhost | 9000 (API) / 9001 (Console) | rustlake | rustlake123 | — |
| ClickHouse | localhost | 8123 (HTTP) / 9100 (TCP) | rustlake | rustlake | rustlake_demo |
| Kafka | localhost | 9092 | — | — | — |
| Redis | localhost | 6379 | — | — | — |
| Elasticsearch | localhost | 9200 | — | — | — |
| RabbitMQ | localhost | 5672 (AMQP) / 15672 (UI) | rustlake | rustlake | — |

#### (Optional) Add Trino for federated queries

Trino can query across Postgres, MySQL, and its built-in TPC-H generator in a single SQL statement:

```bash
# Start Trino on the same Docker network
docker run -d \
  --name rustlake-trino \
  --network rake_default \
  -p 8080:8080 \
  -v ./docker/trino/catalog:/etc/trino/catalog \
  trinodb/trino:latest

# Trino UI: http://localhost:8080
# Query via CLI:
docker exec rustlake-trino trino --execute "SHOW CATALOGS"
# Returns: mysql, postgresql, system, tpch

# Cross-catalog query example:
docker exec rustlake-trino trino --execute \
  "SELECT pg.name, my.name
   FROM postgresql.public.customers pg
   JOIN mysql.rustlake_demo.customers my ON pg.customer_id = my.customer_id
   LIMIT 5"
```

Trino catalog configs are in `docker/trino/catalog/` (postgresql.properties, mysql.properties, tpch.properties).

### 5. Verify everything works

```bash
# Health check
curl http://127.0.0.1:3000/health

# Run a SQL query
curl -X POST http://127.0.0.1:3000/api/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT 1 + 1 AS result"}'

# Query with engine selection (auto | datafusion | duckdb | polars)
curl -X POST http://127.0.0.1:3000/api/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT count(*) FROM pg.tpch_orders", "engine": "duckdb"}'

# Vector search
curl -X POST http://127.0.0.1:3000/api/v1/vector/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "wireless headphones", "k": 5}'

# List registered tables
curl http://127.0.0.1:3000/api/v1/tables

# List engines and their status
curl http://127.0.0.1:3000/api/v1/engines

# System metrics
curl http://127.0.0.1:3000/api/v1/system/resources
```

### 6. Run tests

```bash
# All tests
cargo test

# Single crate
cargo test -p rustlake-engine

# With logging output
RUST_LOG=debug cargo test -- --nocapture

# Lint and format checks (run before committing)
cargo clippy -- -D warnings
cargo fmt --check
```

### 7. CLI usage

```bash
# Run SQL from the command line
cargo run --bin rustlake -- query "SELECT count(*) FROM 'sample-data/sales.csv'"

# Start the API server via CLI
cargo run --bin rustlake -- serve
cargo run --bin rustlake -- serve --port 8080 --flight  # with Flight SQL

# List registered tables
cargo run --bin rustlake -- tables list

# Register a file as a table
cargo run --bin rustlake -- tables register --path /path/to/data.parquet --name my_table
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
| Frontend | Vite 6, React 18, Monaco Editor, TanStack Query, Tailwind CSS |
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
