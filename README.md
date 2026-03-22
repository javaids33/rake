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

Most data platforms require teams to stitch together JVM-based services — a cluster scheduler, a query engine, a streaming layer, a transform framework — each with its own deployment, cold-start penalty, and serialization boundary. RustLake eliminates that entire stack. By building every subsystem in Rust on a shared Apache Arrow memory model, RustLake delivers a single-binary platform that cold-starts in 100ms, passes data between components with zero serialization, and introduces architectural innovations that no existing platform can match.

## What Makes RustLake Different

### 1. Data Models — Self-Maintaining Data Organisms

Traditional Iceberg tables are passive — they store data and wait for external systems to update them. RustLake tables are **active** — they store the compiled transform binary alongside the data it produces, on S3. Each table is a versioned, diffable, auditable unit with quality gates, lineage tracking, and self-healing capabilities.

```
s3://warehouse/daily_revenue/
  data/2026-03-21.parquet           ← data files (Parquet, Snappy)
  metadata/v5.metadata.json         ← Iceberg v2 metadata + transform reference
  binary/bin-abc123                 ← ~470KB compiled transform binary
  binary/manifest-abc123.json       ← source code hash, schedule, quality gates
```

**What this enables:**
- Tables refresh themselves on schedule — skip when upstream unchanged, incremental for small deltas, full rebuild for large changes
- Quality gates (not-null, unique, range, row count) validate every execution — gate failure auto-rolls back to the last known-good version
- Column-level lineage traces every output column back to its source columns and transform expressions
- Data and code are versioned together in Iceberg metadata — rollback both atomically (git semantics: rollback = new version with old code)
- Compliance audit assembles full provenance chain, gate history, SLA freshness, and quality score in seconds

**The Data Organism pattern:**
```
Raw Source
  → Ingestion Table (self-refreshing, CDC-aware)
    → Feature Table (compiled Rust, sub-ms execution)
      → Aggregation Table (quality-gated, self-healing)
        → Data Product (SLA-bound, certified, auditable)
```

Each node breathes (auto-refresh), heals (auto-rollback on gate failure), grows (version history), and reports (compliance audit).

**Infrastructure requirements for full capability:**

| Capability | What You Need | Without It |
|---|---|---|
| **SQL transforms** | RustLake only (DataFusion in-process) | N/A — always works |
| **Rust compiled transforms** | `rustc` installed on the host | SQL-only transforms still work |
| **S3/Iceberg persistence** | MinIO or S3-compatible storage | Data stays in-memory only |
| **Self-healing auto-rollback** | Quality gates defined + scheduler running | Manual rollback via API |
| **Cascade replay** | Multiple tables with `input_tables` DAG | Single-table execution only |
| **Compliance audit** | Data product created with SLA config | Individual table provenance still works |

**Future: serverless execution.** The compiled binary (~470KB) is self-contained and could run on AWS Lambda, Cloudflare Workers, or edge nodes. This would require a thin dispatcher service that:
1. Watches Iceberg metadata for schedule triggers or upstream changes
2. Pulls the binary from S3 (`binary/bin-{hash}`)
3. Invokes Lambda with the binary as the payload
4. Writes output Parquet + Iceberg metadata back to S3
5. Validates quality gates and triggers auto-rollback if needed

This dispatcher doesn't exist yet — today, RustLake's built-in scheduler handles execution. The binary format is Lambda-ready; the orchestration layer is the gap.

### 2. Four-Language Notebooks with Server-Side Compilation

RustLake is the only platform where SQL, Python, Rust, and Spark SQL run in the same notebook:

| Language | Execution | Cold Start | Best For |
|----------|-----------|------------|----------|
| **SQL** | DataFusion (in-process) | 0ms | Queries, joins, aggregations |
| **Rust** | Compiled by `rustc`, binary cached | 2ms (cached) / 300ms (cold) | Algorithms, ML inference, custom logic |
| **Python** | Pyodide WASM (browser-side) | 0ms (loaded) | Pandas, matplotlib, data science |
| **Spark SQL** | Auto-translated to DataFusion | 0ms | Migration from Databricks (zero rewrite) |

Rust cells compile once, then the binary is cached locally AND on S3. Re-execution takes 2ms — competitive with SQL. No other platform offers native compiled language execution in notebooks.

### 3. Browser-Side WASM Compute (Zero Server Round-Trips)

RustLake loads compute engines directly into the browser via WebAssembly:

| Engine | What It Does | No Other Platform Offers This |
|--------|-------------|------------------------------|
| **Pyodide** | Full Python + pandas + numpy + matplotlib in the browser | Databricks/Snowflake need server-side kernels |
| **DuckDB-WASM** | Offline SQL analytics on local/S3 Parquet files | Query data when the server is down |
| **SQLite-WASM** | Persistent local storage for notebooks and settings | Work survives browser clears |
| **Arrow-WASM** | Zero-copy data transfer between WASM engines | 10x faster than JSON between engines |

This means RustLake works at full capability in three modes: connected (server-side power), degraded (server down, WASM available), and offline (airplane mode, sensitive data stays local).

### 4. Spark SQL Auto-Translation

Databricks users paste their existing Spark SQL and it runs without modification:

```sql
-- User writes Spark SQL:
SELECT NVL(name, 'unknown'), DATEDIFF(end_date, start_date),
       COLLECT_LIST(item) FROM orders CLUSTER BY region

-- RustLake auto-translates to DataFusion SQL:
SELECT COALESCE(name, 'unknown'), DATE_DIFF('day', end_date, start_date),
       ARRAY_AGG(item) FROM orders ORDER BY region
```

30+ Spark functions translated automatically. Zero migration effort for SQL workloads.

### 5. Multi-Engine Cost-Based Routing

Every query is automatically routed to the fastest engine:

```
SQL → AST Classification → Cost Model → Profile → Route
  → DataFusion: federated pushdown (Postgres/MySQL/MongoDB)
  → DuckDB: heavy OLAP (GROUP BY, JOINs on S3 Parquet)
  → Polars: DataFrame analytics
  → WASM: browser-side (offline/privacy)
```

The profiler learns from execution history — if DuckDB was 2x faster on similar queries, it boosts DuckDB's score by 30%.

### 6. Neo4j Graph Database Integration

First-class graph database support with force-directed visualization:
- Cypher queries execute via HTTP REST API
- Results convert to Arrow RecordBatch for SQL Editor display
- Graph results auto-render as interactive Canvas 2D visualizations with drag, zoom, pan
- Schema discovery: labels, relationship types, property keys

### 7. Real-Time Workflow Visualization

The Workflow Viz page shows live memory distribution across engines, query pipeline flow, job execution, and incoming load — updating every 2 seconds. No other single-binary platform offers this level of operational visibility.

### 8. Apache Iceberg REST Catalog

RustLake implements the full Apache Iceberg REST Catalog spec. Trino, Spark, Flink, and PyIceberg can use RustLake as their catalog server — making it the single point of truth for table metadata across the entire data stack.

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

### Query & Analytics
- **SQL Analytics** — Full SQL via DataFusion 51 on Arrow 57. TPC-H 22/22 at SF0.1 (600K rows). JOINs, CTEs, window functions, subqueries.
- **Multi-Engine Execution** — DataFusion (primary), DuckDB (OLAP accelerator), Polars (DataFrame engine). Cost-based routing automatically picks the fastest engine.
- **Multi-Source Queries** — Upload CSV/Parquet/JSON, connect Postgres/MySQL/MongoDB/Neo4j, and JOIN across sources in a single SQL query.
- **Time Travel** — `SELECT * FROM table VERSION AS OF <snapshot_id>` and `FOR SYSTEM_TIME AS OF '<timestamp>'` for historical queries on Iceberg tables.

### Notebooks & WASM
- **Interactive Notebooks** — SQL, Python, Markdown, and Rust cells with Monaco editor. Results render inline as tables, charts, and matplotlib plots.
- **Pyodide WASM** — Full Python runtime (pandas, numpy, matplotlib, scipy) executes in the browser. SQL cell results automatically become DataFrames (`_result_1`, `_result_2`).
- **Browser-Side Compute** — No server round-trips needed for Python data science. Works offline, zero compute cost, instant startup.
- **Graph Visualization** — Force-directed Canvas 2D renderer for Neo4j Cypher results with drag, zoom, pan, and tooltips.

### Data Engineering
- **CDC Pipelines** — MongoDB Change Stream, Postgres CDC with snapshot + streaming phases. Parquet files to S3, Iceberg v2 metadata, real-time SSE updates.
- **Iceberg Table Format** — Multi-snapshot metadata, schema evolution, partition evolution, table compaction, snapshot expiry, orphan file cleanup.
- **Iceberg REST Catalog** — Apache spec-compliant REST catalog at `/v1/`. Trino, Spark, Flink, PyIceberg can use RustLake as their catalog server.
- **Data Quality Gates** — NotNull, Unique, Range, RowCount, CustomSQL checks that validate data before Iceberg commit.

### Platform
- **Streaming Ingestion** — Kafka, MongoDB CDC, Postgres CDC connectors. Pipeline CRUD, backpressure, DLQ, checkpoint management.
- **Vector Search** — Cosine similarity on 128-dim embeddings. Brute-force, IVF-PQ, HNSW index support.
- **dbt-Compatible Transforms** — `ref()` and `source()` macro resolution, dependency DAGs, topological execution, column-level lineage.
- **Scheduler** — Time-based (CRON/Quartz/intervals), event-based (file arrival), continuous triggers. Job clusters with concurrency limits.
- **Arrow Flight SQL** — gRPC transport for BI tool connectivity (Tableau, Superset, DBeaver).
- **JWT RBAC** — Token-based authentication with row-level and column-level security policies.
- **Neo4j Connector** — HTTP REST API connector for graph database queries. Cypher execution with Arrow conversion and graph visualization.

## Web Dashboard

RustLake ships with a 19-page React dashboard built on Vite, Tailwind CSS, and Monaco Editor.

| Page | Path | Description |
|------|------|-------------|
| **Home** | `/` | Platform overview — search, quick actions, recent queries, connections |
| **Catalog** | `/catalog` | 32+ tables with 7 tabs: Schema, Preview, Statistics, Lineage, History, Maintenance, Metadata |
| **SQL Editor** | `/sql` | Multi-tab Monaco editor with catalog sidebar, engine selector, cost estimation, compare all engines |
| **Notebooks** | `/notebooks` | Interactive notebook with SQL/Python/Markdown/Rust cells, WASM engine status panel |
| **Query History** | `/history` | Full audit log with duration, engine, status, and query replay |
| **Data Sources** | `/sources` | Connection management — Postgres, MySQL, MongoDB, Trino, Neo4j, S3/MinIO |
| **Streaming / CDC** | `/streaming` | CDC pipeline creation, monitoring, live event stream, S3 sink configuration |
| **Transforms** | `/transforms` | dbt-compatible SQL models with dependency DAG visualization |
| **Data Quality** | `/quality` | Quality checks, rules, table health scores |
| **Migration** | `/migration` | Trino-to-RustLake migration with side-by-side query comparison |
| **Vector Search** | `/vector` | Semantic search interface with similarity scores |
| **Jobs & Pipelines** | `/scheduler` | Scheduled jobs, execution history, DAG visualization |
| **Engine Metrics** | `/metrics` | System gauges (CPU, memory, disk), query performance, engine configuration |
| **Workflow Viz** | `/workflow` | Real-time memory distribution, query pipeline flow, active job monitor |
| **Benchmarks** | `/benchmarks` | TPC-H benchmark runner with multi-engine comparison |
| **Settings** | `/settings` | System info, query router config, WASM engines, data providers, Flight/cluster config |
| **Data Models** | `/data-models` | Versioned transforms — 6-tab detail view (Versions, Diff, History, Compare, Contracts, Lineage), quality gates, cascade replay |
| **Data Products** | `/data-products` | Compliance audit dashboard — freshness SLA, quality score, provenance chain, certification status |
| **About** | `/about` | Platform version, architecture, credits |

## WASM Engines (Browser-Side Compute)

RustLake uses WebAssembly to run compute engines directly in the browser — a capability no other data platform offers.

| Engine | Size | Status | What It Does |
|--------|------|--------|-------------|
| **Pyodide** | ~10 MB | Available | Full Python runtime — pandas, numpy, matplotlib, scipy, scikit-learn |
| **DuckDB-WASM** | ~8 MB | Available | Offline SQL analytics — query Parquet files without any server |
| **SQLite-WASM** | ~1 MB | Planned | Local persistence — notebooks, settings survive offline |
| **Arrow-WASM** | ~2 MB | Planned | Zero-copy data exchange between WASM engines |

**Why this matters:** Databricks, Snowflake, and BigQuery require server round-trips for every computation. RustLake with WASM engines can do Python data science, SQL analytics, and DataFrame manipulation entirely in the browser tab — works offline, zero latency, zero compute cost.

## Benchmarks

### Cold Start: 100ms vs 45 seconds

| Platform | Cold Start | What's Happening |
|----------|-----------|-----------------|
| **RustLake** | **101ms** | Single binary loads, DuckDB state restored, 11 jobs + 8 connections ready |
| Databricks | 45,000ms | JVM boots, Spark driver initializes, cluster manager allocates resources |
| Snowflake | 5,000ms | Virtual warehouse resumes from suspended state |
| Jupyter | 8,000ms | Python kernel starts, imports loaded |

### Multi-Language Notebook Execution

Measured on Apple M2, 8 cores, 16GB RAM:

| Task | SQL | Rust (cold) | Rust (cached) | Speedup (cached vs cold) |
|------|-----|-------------|---------------|--------------------------|
| Statistics (1000 values) | **12ms** | 373ms | **2ms** | 186x |
| String processing | **6ms** | 340ms | **2ms** | 170x |
| 2-stage ETL pipeline | **3ms** | 614ms | **4ms** | 153x |
| Fibonacci (30 terms) | N/A | 489ms | **2ms** | 244x |

Rust binary caching delivers **2ms execution** on re-runs — faster than most SQL queries. The binary persists to S3 for cross-node sharing.

### Executable Table Execution Model

RustLake compiles Rust transforms to ~470KB native binaries, cached by content hash (FNV-1a). SQL transforms execute through DataFusion in-process with no compilation step.

| Metric | SQL Transform | Rust Transform (cold) | Rust Transform (cached) |
|--------|--------------|----------------------|------------------------|
| Execution time | 3-50ms | 300-600ms (includes compilation) | 2-7ms |
| Binary size | N/A | ~470KB | ~470KB (from cache) |
| Cold start | 0ms | ~300ms (rustc) | 0ms (binary loaded from disk/S3) |
| Cluster required | No | No | No |

The tradeoff: Rust transforms have a ~300ms first-compile penalty, offset by instant cache hits. SQL transforms have no compilation overhead. Both produce Iceberg v2 tables on S3.

### Multi-Engine Query Routing

The cost-based profiler routes each query to the optimal engine:

```
TPC-H Q1 (aggregation):  DataFusion 45ms  │  DuckDB 28ms  │  Winner: DuckDB
TPC-H Q5 (5-way join):   DataFusion 120ms │  DuckDB 95ms  │  Winner: DuckDB
Point lookup:             DataFusion 1ms   │  DuckDB 5ms   │  Winner: DataFusion
Cross-source join:        DataFusion 80ms  │  DuckDB N/A   │  Winner: DataFusion (federated)
```

The profiler learns from history — after 50+ queries, routing accuracy reaches 90%+.

## Streaming & CDC Pipeline

### MongoDB CDC → Iceberg End-to-End

```
MongoDB Change Stream
  → Resume token captured BEFORE snapshot (gap-free guarantee)
  → Phase 1: Snapshot existing documents (1000 docs/batch)
  → Phase 2: Parquet files written to S3 (Snappy compression, bloom filters)
  → Phase 3: Iceberg v2 metadata (schema, snapshot, manifest list)
  → Phase 4: Table registered in DataFusion (queryable from SQL Editor)
  → Phase 5: CDC change stream resumes from token
  → Ongoing: New events → Parquet → S3 → SSE to UI in real-time
```

The pipeline writes spec-compliant Iceberg v2 tables that are readable by Trino, Spark, Flink, and any Iceberg-compatible engine. Quality gates validate each batch before commit.

## Iceberg-Native Data Catalog

### 7-Tab Table Explorer

Every table in the catalog has rich metadata:

| Tab | What It Shows |
|-----|--------------|
| **Schema** | Column names, types, nullability with fill-rate progress bars |
| **Preview** | First 100 rows with inline data display |
| **Statistics** | Min/max/null counts per column, row count, size |
| **Lineage** | DAG graph showing upstream/downstream table dependencies |
| **History** | Iceberg snapshot timeline with operation type, row deltas, "Query at snapshot" button |
| **Maintenance** | File count, avg size, fragmentation score, compact/expire/orphan cleanup buttons |
| **Metadata** | Table format, engine, quick actions (Open in SQL Editor, Copy SELECT, Count Rows) |

### Time Travel

Query historical data by snapshot ID or timestamp:

```sql
-- By snapshot ID
SELECT * FROM orders VERSION AS OF 1711036800000

-- By timestamp
SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-03-21 12:00:00'
```

### Schema & Partition Evolution

```bash
# Add a column
curl -X POST /api/v1/tables/orders/schema/evolve \
  -d '{"changes": [{"type": "add", "name": "region", "data_type": "string", "nullable": true}]}'

# Change partition strategy (new data uses new layout, existing data unchanged)
curl -X POST /api/v1/tables/orders/partitions/evolve \
  -d '{"fields": [{"source_id": 1, "field_id": 1000, "name": "order_date_month", "transform": "month"}]}'
```

## Notebook → ETL Pipeline

Notebooks convert to scheduled ETL jobs with one click:

```
Interactive Notebook
  ↓ "Deploy as ETL Job"
Execution Planner (DAG analysis)
  ├── Stage 0: [SQL Cell 1, SQL Cell 2]  ← parallelizable (independent)
  ├── Stage 1: [Python Cell 3]           ← depends on Stage 0
  └── Stage 2: [Rust Cell 4]            ← depends on Stage 1
  ↓ Optimizations applied automatically
  ├── Skip markdown cells (documentation only)
  ├── Merge consecutive SQL into batch (save 50ms/merge)
  ├── Parallelize independent cells (save 100ms/pair)
  └── Cache Rust binary on S3 (save 300ms on re-runs)
  ↓ Schedule
  Cron job: "0 * * * *" (hourly)
```

### Notebook Execution API

```bash
# Get execution plan (no execution)
curl -X POST /api/v1/notebooks/plan -d '{...}'
# → stages, parallelization opportunities, optimization suggestions

# Execute all cells server-side
curl -X POST /api/v1/notebooks/execute -d '{...}'
# → per-cell results with timing, total duration, DAG order

# Schedule as recurring ETL job
curl -X POST /api/v1/notebooks/schedule \
  -d '{"notebook_id": "nb1", "schedule": "0 * * * *"}'
```

## Security & Governance

### JWT RBAC with Row/Column Security

```
Token → Claims (user, roles) → Permission Check → Row Filter → Column Mask → Results

Row-level: Auto-inject WHERE clauses per table policy
  SELECT * FROM orders → SELECT * FROM orders WHERE region = '{user.region}'

Column-level: Strip restricted columns before serialization
  SELECT * FROM employees → salary column masked for non-HR roles
```

### Data Quality Gates

Pre-commit validation on every Iceberg write:

| Check | What It Validates |
|-------|------------------|
| NotNull | Column has no null values |
| Unique | Column has no duplicates |
| Range | Numeric column within min/max bounds |
| RowCount | Batch has minimum number of rows |
| CustomSQL | Arbitrary SQL assertion passes |

Failed quality gates prevent the Iceberg snapshot from committing — bad data never reaches the table.

## Regulatory Compliance Automation

7 composable features that together enable provenance-backed compliance auditing:

| Feature | What It Does | API |
|---------|-------------|-----|
| **Cascade Replay** | Re-executes entire upstream DAG in topological order, validates gates at every node | `POST /api/v1/executable-tables/{name}/cascade-replay` |
| **Column Lineage** | Parses SQL to trace output columns back to source columns + transform expressions | `GET /api/v1/executable-tables/{name}/column-lineage` |
| **Materialized Views** | Auto-refresh on configurable interval with full version tracking | `auto_refresh` + `refresh_interval_seconds` fields |
| **Executable Pipelines** | Named chain of tables — execution stops on gate/contract failure | `POST /api/v1/executable-pipelines/{id}/run` |
| **Time-Travel Debug** | Compare bad vs good executions — code diff, data diff, upstream changes | `POST /api/v1/executable-tables/{name}/debug` |
| **Cost-Aware Scheduling** | Skip unchanged, incremental for small deltas, full for large changes | Built into scheduler tick |
| **Self-Healing** | Auto-rollback to last known-good version on quality gate failure | Built into scheduler tick |
| **Data Products** | SLA-bound, certified datasets with provenance chain and compliance audit | `GET /api/v1/data-products/{name}/audit` |

**The compliance audit demo:** A regulator asks "prove the risk score for customer X on March 15th was computed correctly." RustLake assembles: time-travel snapshot → code version → cascade replay → A/B verification → gate results → contract validation → full audit JSON. Seconds, not weeks.

## Competitive Positioning

### vs Databricks

| Capability | Databricks | RustLake |
|-----------|-----------|----------|
| Cold start | 30-120s (cluster) | **100ms** (single binary) |
| Languages | SQL, Python, Scala, R | **SQL, Rust, Python, Spark SQL** |
| Notebook → ETL | Schedule notebook as job | **Same + DAG optimization + binary caching** |
| Offline capability | None | **Full WASM (Pyodide + DuckDB in browser)** |
| Table format | Delta Lake | **Iceberg v2 (open standard)** |
| Self-maintaining tables | No | **Yes (versioned, self-healing, auditable)** |
| Compliance audit | Manual (weeks) | **Automated (seconds)** |
| Graph databases | No | **Neo4j integration with visualization** |

### vs Snowflake

| Capability | Snowflake | RustLake |
|-----------|-----------|----------|
| Deployment | Cloud-only SaaS | **Single binary, anywhere** |
| SQL engine | Proprietary | **DataFusion (open source)** |
| Python | Snowpark (server-side) | **Pyodide WASM (browser-side, free)** |
| Compiled transforms | No | **Rust cells with binary caching** |
| Open table format | Iceberg (read-only) | **Iceberg v2 (full read/write + REST catalog)** |
| Cost model | Credit-based, opaque | **Transparent per-query cost tracking** |

### vs Apache Spark + Jupyter

| Capability | Spark + Jupyter | RustLake |
|-----------|----------------|----------|
| Setup time | Hours (install Spark, configure YARN/K8s) | **Minutes (cargo build, single binary)** |
| Memory model | JVM heap + off-heap + Arrow FFI | **Native Arrow throughout, zero serialization** |
| Multi-engine | Spark only | **DataFusion + DuckDB + Polars + WASM** |
| Table maintenance | Manual scripts | **Built-in compaction, expiry, orphan cleanup** |
| REST catalog | Separate service (Hive Metastore) | **Built-in, spec-compliant** |
| Graph support | GraphX (deprecated) | **Neo4j + force-directed visualization** |

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

## Deployment

### Single Node (Development / Small Team)

```bash
# Build release binary (~100ms cold start)
cargo build --release --bin rustlake-api

# Run with all engines enabled
RUSTLAKE_SECRET_KEY=your-secret-key \
RUSTLAKE_DUCKDB__ENABLED=true \
RUSTLAKE_POLARS__ENABLED=true \
./target/release/rustlake-api
```

One binary, one process. Serves the REST API, executes SQL across 3 engines, runs CDC pipelines, schedules jobs.

### Docker Compose (Recommended)

```bash
# Start RustLake + all data sources
docker compose up -d

# This starts:
#   rustlake-api    → :3000 (HTTP API)
#   rustlake-web    → :3001 (Dashboard)
#   postgres        → :5433 (12 TPC-H tables)
#   mysql           → :3307 (9 tables)
#   mongodb         → :27018 (11 collections)
#   minio           → :9000 (S3-compatible storage)
```

### Kubernetes (Production)

RustLake ships with Helm charts and K8s manifests for production deployment.

```bash
# Deploy with Helm
helm install rustlake deploy/k8s/helm/rustlake/ \
  --set coordinator.replicas=1 \
  --set worker.replicas=3 \
  --set worker.autoscaling.enabled=true \
  --set worker.autoscaling.maxReplicas=20

# Or raw manifests
kubectl apply -f deploy/k8s/base/
```

## Distributed Compute

RustLake supports distributed query execution across multiple nodes using Arrow Flight gRPC.

### Architecture

```
                 ┌──────────────────────┐
                 │     Coordinator      │
                 │  Query planning      │
                 │  Cost-based routing  │
                 │  Worker registry     │
                 └──┬──────┬──────┬─────┘
                    │      │      │
          ┌─────────▼┐ ┌──▼──────▼┐ ┌──────────┐
          │ Worker 1  │ │ Worker 2  │ │ Worker N  │
          │ DataFusion│ │ DataFusion│ │ DataFusion│
          │ + DuckDB  │ │ + DuckDB  │ │ + DuckDB  │
          │ Flight RPC│ │ Flight RPC│ │ Flight RPC│
          └───────────┘ └───────────┘ └──────────┘
```

### Enable Distributed Mode

```bash
# Start coordinator
RUSTLAKE_CLUSTER__NODE_ROLE=coordinator \
RUSTLAKE_FLIGHT__ENABLED=true \
RUSTLAKE_FLIGHT__HOST=0.0.0.0 \
RUSTLAKE_FLIGHT__PORT=50051 \
./target/release/rustlake-api

# Start workers (on different machines/containers)
RUSTLAKE_CLUSTER__NODE_ROLE=worker \
RUSTLAKE_CLUSTER__COORDINATOR_HOST=coordinator-host \
RUSTLAKE_CLUSTER__COORDINATOR_PORT=50051 \
RUSTLAKE_FLIGHT__ENABLED=true \
./target/release/rustlake-api
```

### Distribution Strategies

The `DistributedPlanner` analyzes each query and selects from 5 strategies:

| Strategy | When Used | Example |
|----------|-----------|---------|
| **Local** | Small tables, point lookups | `SELECT * FROM dim_country WHERE id = 5` |
| **SingleWorker** | Medium tables, single-partition | `SELECT COUNT(*) FROM orders` |
| **RangePartition** | Large tables, partition-aligned | `SELECT * FROM events WHERE date BETWEEN ...` |
| **ScatterGather** | Hash joins across partitions | `SELECT * FROM orders JOIN customers ON ...` |
| **PartialAggregate** | GROUP BY with pre-aggregation | `SELECT region, SUM(amount) FROM sales GROUP BY region` |

### Seeing Multiple Engines Work

The SQL Editor shows which engine executed each query with color-coded badges:

- **DF** (amber) — DataFusion: default engine, federated pushdown to source DBs
- **DK** (emerald) — DuckDB: OLAP accelerator for heavy aggregations and S3 scans
- **PL** (cyan) — Polars: DataFrame engine for in-memory analytics
- **WA** (violet) — WASM: browser-side Python/SQL execution

Use "Compare All" in the toolbar to run the same query on all engines and see which is fastest.

The **Workflow Viz** page (`/workflow`) shows real-time memory distribution across engines, active query pipelines, and job execution status.

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
| `GET` | `/api/v1/tables/{name}/snapshots` | Iceberg snapshot history |
| `GET` | `/api/v1/tables/{name}/schemas` | Schema version history |
| `POST` | `/api/v1/tables/{name}/schema/evolve` | Schema evolution (add/drop/rename columns) |
| `POST` | `/api/v1/tables/{name}/maintenance/compact` | Merge small Parquet files |
| `POST` | `/api/v1/tables/{name}/maintenance/expire-snapshots` | Remove old snapshots |
| `GET` | `/api/v1/tables/{name}/maintenance/status` | Fragmentation score, recommendations |
| `GET` | `/v1/config` | Iceberg REST Catalog config |
| `GET` | `/v1/namespaces` | List Iceberg namespaces |
| `GET` | `/v1/namespaces/{ns}/tables` | List tables in namespace |
| `GET` | `/v1/namespaces/{ns}/tables/{table}` | Load table metadata |
| `POST` | `/api/v1/neo4j/connect` | Connect to Neo4j instance |
| `POST` | `/api/v1/neo4j/cypher` | Execute Cypher query |
| `POST` | `/api/v1/neo4j/graph` | Cypher with graph visualization data |
| `GET` | `/api/v1/neo4j/schema` | Discover labels, relationship types |
| `POST` | `/api/v1/sql/profile` | Cost-based multi-engine profiling (no execution) |
| `POST` | `/api/v1/sql/estimate` | Query cost estimation |
| `GET/POST` | `/api/v1/executable-tables` | List/create data models (executable tables) |
| `PUT` | `/api/v1/executable-tables/{name}` | Update transform code (creates new version) |
| `POST` | `/api/v1/executable-tables/{name}/execute-version` | Execute a specific version |
| `POST` | `/api/v1/executable-tables/{name}/rollback` | Rollback to a previous version |
| `GET` | `/api/v1/executable-tables/{name}/versions` | Version history with diffs |
| `GET` | `/api/v1/executable-tables/{name}/column-lineage` | Column-level lineage |
| `POST` | `/api/v1/executable-tables/{name}/cascade-replay` | Re-execute entire upstream DAG |
| `POST` | `/api/v1/executable-tables/{name}/debug` | Time-travel debug (bad vs good) |
| `POST` | `/api/v1/executable-tables/{name}/ab-test` | Compare two versions side-by-side |
| `GET/POST` | `/api/v1/executable-pipelines` | List/create executable pipelines |
| `POST` | `/api/v1/executable-pipelines/{id}/run` | Run a pipeline (stops on failure) |
| `GET/POST` | `/api/v1/data-products` | List/create data products |
| `GET` | `/api/v1/data-products/{name}/audit` | Full compliance audit |
| `POST` | `/api/v1/notebooks/execute` | Run all notebook cells server-side |
| `POST` | `/api/v1/notebooks/plan` | Get DAG execution plan with optimizations |
| `POST` | `/api/v1/notebooks/schedule` | Deploy notebook as scheduled ETL job |
| `GET` | `/api/v1/notebooks/jobs` | List notebook-based scheduled jobs |
| `POST` | `/api/v1/notebook/execute-rust` | Compile and run Rust code (with binary cache) |
| `GET` | `/api/v1/spark/compat` | Spark SQL compatibility matrix |
| `POST` | `/api/v1/spark/translate` | Translate Spark SQL to DataFusion SQL |

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
