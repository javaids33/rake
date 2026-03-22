# Executable Lakehouse: Git for Data Transforms

*Code-Data Provenance with Binary Time Travel on Apache Iceberg*

**RustLake Technical Whitepaper | March 2026**

---

## Abstract

Modern data platforms treat code and data as separate concerns. ETL logic lives in notebooks, orchestrators, or CI/CD pipelines, while the data it produces sits in tables with no link back to the code version that created it. RustLake introduces **executable tables** -- Iceberg v2 tables that store compiled transform binaries alongside the data they produce, using an Iceberg-native S3 layout: `data/` for Parquet output, `metadata/` for Iceberg v2 snapshots, and `binary/` for compiled native executables with manifests. Each transform version is content-hashed (FNV-1a) and mapped to a native binary on S3. When the transform executes, it writes Parquet data files, incremental Iceberg v2 metadata (appending snapshots, never overwriting), and links each Iceberg snapshot to the code version that produced it -- creating an unbroken provenance chain from every data file back to the exact source code. Quality gates (not-null, unique, range, row count) are validated against actual output data on every execution, and automatic regression detection fires after every run, comparing row counts, duration, and output data against the previous execution. Versions are immutable and append-only, following git semantics: rollback creates a new version with old code (like `git revert`), and any historical version can be executed without changing HEAD, with results persisted to S3 under versioned paths (like `git stash && git checkout v1 && run`). The content-addressable binary caching layer eliminates recompilation entirely -- a cache hit means zero compilation overhead, loading a ~470KB native binary directly from disk or S3. RustLake also provides regulatory compliance automation: upstream cascade replay, living data lineage with column-level tracking, versioned materialized views, executable pipelines with stop-on-failure semantics, time-travel debugging, cost-aware scheduling, and data products with compliance audit -- enabling organizations to answer regulatory inquiries with full provenance in seconds. Beyond compliance, executable tables form the foundation for **self-maintaining data organisms** -- DAGs of tables that adaptively refresh, self-heal on quality failure, and compose into ML feature stores, embedding pipelines, and distribution-monitored model inputs without additional infrastructure. All metadata is stored as standard Iceberg v2 table properties, making executable tables discoverable and queryable by Trino, Spark, Flink, and any Iceberg-compatible engine.

---

## 1. The Problem: Data Pipelines Are Black Boxes

Every data team has hit this wall: a downstream dashboard breaks, and the investigation begins. Which version of the ETL produced the bad data? When did the transform logic change? Who approved it? Can we roll back without reprocessing everything from scratch?

Today's platforms offer no good answers:

**No code-data provenance.** Databricks notebooks live in a workspace, Spark jobs live in a JAR, Airflow DAGs live in a Git repo -- but the tables they produce carry no reference to which code version created them. Lineage tools track table-to-table dependencies, not code-to-data relationships.

**Silent schema breakage.** When an upstream transform adds a column or changes a type, downstream consumers discover the problem at query time. There is no pre-deployment validation that catches regressions before they reach production.

**Rollback requires full reprocessing.** To undo a bad transform, teams must identify the last known good code version, find the corresponding source data, and rerun the entire pipeline. On large datasets, this takes hours.

**Cluster cold starts slow iteration.** Traditional platforms require cluster provisioning or warehouse resume before any transform can execute. For transforms that complete in milliseconds, the infrastructure overhead dominates total wall-clock time.

**Minimum billing increments.** Many platforms bill in one-minute increments. Sub-second transforms are billed the same as transforms that use the full minute.

---

## 2. The Solution: Executable Tables

An executable table is a standard Apache Iceberg v2 table augmented with four additional capabilities:

1. **Transform code** -- the SQL, Rust, or Python logic that produces the table's data
2. **Compiled binary** -- a native executable cached on S3, keyed by source code hash
3. **Version history** -- an append-only log of every code change, with diffs and authorship
4. **Execution history** -- a record of every run, with duration, row counts, and regression analysis

The core data structure is `ExecutableTable`, defined in `crates/rustlake-api/src/executable_table.rs`:

```rust
pub struct ExecutableTable {
    pub table_name: String,
    pub table_location: String,
    pub transform: TableTransform,
    pub schedule: Option<String>,       // cron expression
    pub quality_gates: Vec<QualityGateRef>,
    pub input_tables: Vec<String>,
    pub status: ExecutableTableStatus,
    pub history: Vec<ExecutionRecord>,
    pub versions: Vec<TransformVersion>,
    pub total_executions: u64,
    pub total_cost_usd: f64,
    // ...
}
```

The `TableTransform` carries the source code, its FNV-1a content hash, the S3 path to the compiled binary, binary size, and compilation metadata (compiler version, target architecture):

```rust
pub struct TableTransform {
    pub transform_type: String,    // "sql", "rust", "python"
    pub source_code: String,
    pub source_hash: String,       // FNV-1a: "cf6d36987343c72a"
    pub binary_path: Option<String>,
    pub binary_size: Option<u64>,
    pub binary_cached: bool,
    pub compiler_version: Option<String>,
    pub target_arch: Option<String>,
}
```

Transforms can be scheduled via cron, triggered by events, or executed on demand. SQL transforms run through DataFusion. Rust transforms compile to native binaries. Python transforms are planned for a future release.

---

## 3. Technology Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Arrow | v57 | Zero-copy columnar memory format; all inter-component data exchange uses `RecordBatch` |
| DataFusion | v51 | SQL parser, optimizer (30+ rules), execution engine for SQL transforms |
| Apache Iceberg | v2 | Table format with snapshot isolation, schema evolution, time travel |
| Apache Parquet | (via arrow v57) | Columnar storage with Snappy compression, bloom filters, column statistics |
| Rust / rustc | 2021 edition | Compiled transforms produce native binaries (~453KB typical) |
| S3 / MinIO | - | Object storage for data files, Iceberg metadata, and compiled binaries |
| FNV-1a | - | Content-addressable binary storage: `hash(source_code)` produces a 64-bit hex key |
| object_store | v0.12 | Unified S3/MinIO/local filesystem access with path-style request support |

All components communicate via Arrow `RecordBatch` -- no serialization between crates. Data hits disk only as Parquet files on S3. This is a single-binary platform: `cargo install rustlake` gets the full system with sub-500ms cold start.

---

## 4. The Binary Lifecycle

The journey from source code to queryable Iceberg table proceeds through six stages:

```
Source Code (v1)
    |  FNV-1a hash
Content Hash: "cf6d36987343c72a"
    |  rustc --edition 2021 (cold: up to 30s, cached: 0ms)
Compiled Binary: ~470KB native aarch64
    |  Cache locally
.rustlake-cache/rust-bins/bin-cf6d36987343c72a
    |  Execute (2ms from cache)
Results: Arrow RecordBatch
    |  Write all artifacts to versioned Iceberg-native S3 layout
```

### S3 Layout: Iceberg-Native with Binary Extension

Each executable table version produces a self-contained directory on S3 that follows Iceberg conventions. The `binary/` directory is a RustLake extension that sits alongside standard Iceberg directories without interfering -- Iceberg readers (Trino, Spark, Flink, DuckDB) only scan `metadata/` and follow manifest pointers to `data/`, ignoring any sibling directories.

```
s3://bucket/executable_tables/{table_name}/
└── v{N}/                           # Per-version isolation
    ├── data/                        # Standard Iceberg data directory
    │   └── 2026-03-22/
    │       └── batch-130607-0001.parquet   (Snappy compressed)
    ├── metadata/                    # Standard Iceberg metadata
    │   ├── v1.metadata.json         # Initial snapshot
    │   ├── v2.metadata.json         # Incremental (appended, never overwritten)
    │   ├── snap-{id}-manifest-list.json
    │   └── snap-{id2}-manifest-list.json
    └── binary/                      # RustLake extension (invisible to Iceberg)
        ├── bin-{hash}               # Compiled native binary (~470 KB)
        └── manifest-{hash}.json     # Build metadata + Iceberg-style properties
```

**Why this layout is Iceberg-safe:** The Iceberg v2 spec defines table discovery via the `metadata/` directory. Our scanning logic (`find_latest_metadata`) only lists files under `{prefix}/metadata/` matching `v*.metadata.json`. The `binary/` directory is invisible to all Iceberg readers because they never list the table root -- they follow manifest pointers from metadata to data files. This has been verified against Trino, Spark, and DuckDB Iceberg connectors.

**Binary manifest.** Each binary is accompanied by a JSON manifest containing build metadata and Iceberg-style properties:

```json
{
    "format-version": 1,
    "type": "rustlake-executable-binary",
    "table-name": "customer_segments",
    "version": 4,
    "source-hash": "68db9ca38506b3f9",
    "binary-path": "executable_tables/customer_segments/v4/binary/bin-68db9ca38506b3f9",
    "binary-size": 481240,
    "compiled-at": "2026-03-22T14:19:27Z",
    "compiler": "rustc",
    "target": "aarch64",
    "os": "macos",
    "properties": {
        "rustlake.executable": "true",
        "rustlake.transform.type": "rust",
        "rustlake.transform.source-hash": "68db9ca38506b3f9",
        "rustlake.function.cacheable": "true",
        "rustlake.function.binary-format": "native"
    }
}
```

**Incremental Iceberg metadata.** Each execution appends a new snapshot to the existing metadata chain. The first execution creates `v1.metadata.json`; subsequent executions write `v2.metadata.json`, `v3.metadata.json`, etc. Each metadata file contains all prior snapshots plus the new one. This preserves full snapshot history -- the core purpose of the Iceberg format -- whereas earlier implementations overwrote `v1.metadata.json` on every execution, destroying history.

**Per-version isolation.** Each code version writes to its own S3 prefix (`v1/`, `v2/`, etc.). This means v1 and v2 data coexist. Time-travel to an old version doesn't destroy current data. Rolling back to v1 re-executes and writes to `v1/`, while v3's data remains untouched in `v3/`.

**Hashing.** The FNV-1a hash function produces a 16-character hex string from source code. The implementation in `executable_table.rs` uses the standard FNV-1a offset basis (`0xcbf29ce484222325`) and prime (`0x100000001b3`):

```rust
pub fn hash_source(code: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in code.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}
```

**Compilation.** The `execute_rust()` function in `crates/rustlake-api/src/rust_executor.rs` compiles user code via `rustc --edition 2021` with a 30-second timeout. Code without a `fn main()` is automatically wrapped. Common `std` imports (`HashMap`, `HashSet`, `BTreeMap`, `VecDeque`, `io`) are injected.

**Caching.** The local binary cache (`BINARY_CACHE`) holds up to 100 entries. When full, the oldest 20% by last-used time are evicted. The cache key is the FNV-1a hash of the source code, so identical code always hits the same binary.

**S3 persistence.** After local caching, the binary is uploaded to S3 asynchronously via `tokio::spawn` -- compilation does not block on the upload. Each binary is accompanied by a JSON manifest containing the source hash, source code, binary path, binary size, compilation timestamp, compiler version, target architecture, and Iceberg-style properties.

**S3 restore.** On cache miss, the executor checks S3 before recompiling. The `download_binary_from_s3()` function fetches the binary, writes it to the local cache directory, sets executable permissions (Unix), and registers it in the in-memory cache. This turns a cold compile into a sub-second S3 download.

**Execution.** Cached binaries execute with a 10-second timeout. Output is captured from stdout/stderr and truncated at 64KB. The `RustExecutionResult` tracks `compile_ms` and `run_ms` separately, enabling precise timing attribution.

---

## 5. Version Control: Git Semantics for Data

Each code change creates a new `TransformVersion`:

```rust
pub struct TransformVersion {
    pub version: u32,
    pub source_code: String,
    pub source_hash: String,
    pub created_at: String,
    pub created_by: String,
    pub change_description: String,
    pub binary_size_bytes: Option<u64>,
    pub snapshot_ids: Vec<i64>,    // Iceberg snapshots produced by this version
}
```

**Append-only history.** Versions are immutable once created. Version numbers increment monotonically. The `versions` vector on `ExecutableTable` is the complete history.

**Diff algorithm.** The `diff_transforms()` function performs line-by-line comparison between two source code versions, classifying each line as `added`, `removed`, or `unchanged`. Changed lines (same position, different content) emit both a `removed` and `added` entry. The function returns counts of added, removed, and changed lines alongside the full diff.

**Rollback as revert.** Rolling back to version N creates version N+1 with the source code from version N. The old version remains in history. This matches `git revert` semantics -- history is never rewritten.

**Execute-version.** Any historical version can be executed without changing HEAD. The system looks up the version's `source_hash`, finds (or compiles) the corresponding binary, executes it, writes the results to S3 under the version's own path (`v{N}/data/`), generates incremental Iceberg metadata, validates quality gates against the output, runs auto-regression detection, and records the result in execution history. HEAD remains unchanged. This is analogous to `git stash && git checkout v1 && run && git checkout main && git stash pop` -- except the output is persisted, validated, and regression-checked.

---

## 6. Provenance Chain: Who Changed What, When, Why

The `ProvenanceChain` struct assembles the complete history of a table:

```rust
pub struct ProvenanceChain {
    pub table_name: String,
    pub total_versions: usize,
    pub total_executions: u64,
    pub total_snapshots: usize,
    pub versions: Vec<TransformVersion>,
    pub timeline: Vec<ProvenanceEvent>,
}
```

Every significant action produces a `ProvenanceEvent` with one of four types:

| Event Type | Trigger | Data Recorded |
|------------|---------|---------------|
| `code_change` | Transform source modified | New version number, source hash, author, description |
| `execution` | Transform runs (scheduled or manual) | Version, duration, rows, bytes, binary cached flag |
| `rollback` | Revert to historical version | Source and target version numbers |
| `regression_detected` | Automatic comparison flags anomaly | Severity, metrics, recommendation |

Each `ExecutionRecord` captures fine-grained metrics:

- `execution_id` -- unique identifier
- `version` -- which code version was running
- `duration_ms`, `compile_ms`, `run_ms` -- timing breakdown
- `rows_produced`, `bytes_written` -- output metrics
- `binary_cached` -- whether compilation was skipped
- `execution_location` -- where the binary ran: `local`, `lambda`, `spot`, or `edge`

This chain answers questions that are unanswerable on other platforms: "Which code version produced the data in snapshot 47?" "How many rows did version 3 produce across its 12 executions?" "When did the regression start?"

---

## 7. Regression Detection

Regression detection operates at two levels: **metric-level** (row count, duration) and **data-level** (schema drift, null increase, cardinality drop). Both fire automatically after every execution.

### Metric-Level Regression (Auto-fires after every execution)

The `detect_regression()` function compares consecutive executions across dimensions:

| Metric | Threshold | Severity |
|--------|-----------|----------|
| Row count drops >10% | `change_pct < -10.0` | **major** |
| Duration increases >100% (2x slower) | `change_pct > 100.0` | **minor** |
| Row count drops to zero (was >0) | `new_rows == 0 && old_rows > 0` | **critical** |

This runs automatically after every execution in both `execute_executable_table` and `execute_version` handlers. Results are included in the API response and logged via `tracing::warn` when regressions are detected. The provenance timeline inserts `regression_detected` events between consecutive successful executions, making regressions visible in the timeline without manual comparison.

### Data-Level Regression (Content-aware comparison)

The `detect_data_regression()` function compares the actual Arrow `RecordBatch` outputs of two executions:

| Check | Condition | Regression? |
|-------|-----------|-------------|
| **Schema drift** | Columns present in old output but missing from new | Yes -- columns removed |
| **NULL increase** | Column had <5% nulls, now has >20% nulls | Yes -- data quality degraded |
| **Cardinality drop** | Distinct value count drops >50% | Yes -- data collapsed |

This catches regressions that metric-level detection misses. A transform returning the same row count but with all values set to NULL passes row count checks but fails the NULL increase check.

### Recommendation Engine

- **none** -- "No regression detected. Safe to deploy."
- **minor** -- "MINOR: Performance regression detected. Consider optimizing before deploying."
- **major** -- "MAJOR: Significant row count change detected. Review transform logic before deploying."
- **critical** -- "CRITICAL: Transform produces zero rows. Do NOT deploy. Investigate immediately."

Regression results are attached to the `TransformDiff` between versions, so a code review shows both the line-level changes and their measured impact on output. They are also returned in the execution API response, enabling CI/CD gates that block deployment on regression.

---

## 8. Design Rationale: Native Binary Execution

We chose to compile Rust transforms to native binaries rather than interpreting them or running them on managed clusters. This section explains the engineering reasoning and acknowledges the tradeoffs.

### Why Native Binaries

**Binary size.** A typical compiled Rust transform produces a ~470KB statically linked binary. This is the entire executable -- no runtime dependencies, no classpath, no virtual environment. By comparison, a JVM application requires the JRE (typically 150-300MB), and a Python application requires a virtualenv with its dependency tree (often 50-500MB depending on packages). The small binary size enables fast S3 upload/download and efficient caching.

**Deterministic execution.** The same binary produces the same behavior on every invocation. There is no dependency resolution at runtime, no package version conflicts, no interpreter version mismatches. The FNV-1a hash of the source code uniquely identifies the binary -- if the hash matches, the behavior matches.

**Content-addressable caching.** Each binary is stored under its source hash: `bin-{hash}`. When a transform is submitted for execution, the system hashes the source code and checks the cache hierarchy (in-memory -> local disk -> S3). A cache hit means zero compilation -- the binary is loaded and executed directly. Cache invalidation is automatic: any change to source code produces a different hash, which misses the cache and triggers recompilation. Unchanged code always hits.

**Single binary deployment.** RustLake ships as a single binary (`cargo install rustlake`). There is no cluster to provision, no package manager to run, no virtualenv to create. The compiled transform binaries are similarly self-contained -- copy the binary to any compatible machine and it runs.

**Predictable resource usage.** Native binaries have bounded memory usage with no garbage collection pauses. Memory allocation is explicit and deterministic. This matters for data transforms where consistent latency and predictable resource consumption are important for scheduling and capacity planning.

### Tradeoffs

**Compilation latency on first run.** The first execution of a new or modified Rust transform incurs up to ~30 seconds of compilation time via `rustc`. Subsequent executions of the same source code are instant (cache hit). This tradeoff is acceptable for transforms that run repeatedly (scheduled, event-driven) but adds friction for one-off exploratory work. SQL transforms avoid this entirely -- they execute through DataFusion with no compilation step.

**Platform-specific binaries.** Native binaries are compiled for a specific architecture and OS (e.g., `aarch64-macos`, `x86_64-linux`). A binary compiled on macOS ARM will not run on Linux x86. Cross-compilation is possible but not yet implemented. In practice, production deployments target a single platform.

**Language restriction for compiled transforms.** Only Rust source code compiles to native binaries. SQL transforms are available to all users and execute through DataFusion without compilation. Python transforms are planned for a future release. The Rust requirement for compiled transforms limits the audience to teams with Rust experience, though the SQL path covers the majority of transform use cases.

**Cache storage.** The local binary cache holds up to 100 entries. The S3 binary store grows without bound. For teams with thousands of distinct transforms, S3 storage costs for binaries are non-trivial, though at ~470KB per binary, 10,000 transforms would consume approximately 4.5GB.

---

## 9. Iceberg Integration

All executable table metadata is stored as Iceberg v2 table properties using the `rustlake.*` namespace. The `to_iceberg_properties()` function generates these properties:

| Property | Example Value | Description |
|----------|--------------|-------------|
| `rustlake.executable` | `true` | Marks the table as an executable table |
| `rustlake.transform.type` | `sql`, `rust`, `python` | Transform language |
| `rustlake.transform.source-hash` | `cf6d36987343c72a` | FNV-1a hash of current source code |
| `rustlake.transform.binary-path` | `rustlake-functions/bin-cf6d...` | S3 path to compiled binary |
| `rustlake.schedule` | `0 * * * *` | Cron expression for scheduled execution |
| `rustlake.quality-gate.{i}.type` | `not_null` | Quality gate type |
| `rustlake.quality-gate.{i}.column` | `order_id` | Target column |
| `rustlake.quality-gate.{i}.description` | `order_id must not be null` | Human-readable description |
| `rustlake.input-table.{i}` | `raw_orders` | Input dependency |
| `rustlake.total-executions` | `720` | Lifetime execution count |
| `rustlake.estimated-cost-per-run` | `0.001000` | Per-execution estimate |

The `from_iceberg_properties()` function reconstructs an `ExecutableTable` from these properties, enabling any Iceberg-compatible engine to discover executable tables by scanning for `rustlake.executable = "true"`.

Each execution creates a new Iceberg snapshot via incremental append. The system first calls `find_latest_metadata()` to locate the current metadata version, then `load_table_state()` to parse it, and finally `finalize_iceberg_incremental()` to write a new metadata file (e.g., `v2.metadata.json`) containing all prior snapshots plus the new one. If no prior metadata exists, `finalize_iceberg()` creates `v1.metadata.json` with the first snapshot. This preserves the full snapshot chain that Iceberg's time-travel feature depends on.

The `TransformVersion.snapshot_ids` field links code versions to the Iceberg snapshots they produced. After each execution writes data and metadata to S3, the returned snapshot ID is pushed onto the matching version's `snapshot_ids` vector. This answers the question "which Iceberg snapshots did version N produce?" -- a query that is impossible on any other platform.

Data files are written via the Parquet sink (`crates/rustlake-api/src/parquet_sink.rs`) to versioned paths: `s3://{bucket}/executable_tables/{table}/v{N}/data/{date}/batch-{timestamp}-{seq}.parquet`. Per-version isolation means v1 and v3 data coexist on S3 without overwriting.

---

## 10. Quality Gates

Quality gates are declarative checks that are validated against actual output data on every execution. Each gate is a `QualityGateRef`:

```rust
pub struct QualityGateRef {
    pub gate_type: String,    // "not_null", "unique", "range", "row_count", "custom_sql"
    pub column: Option<String>,
    pub threshold: Option<f64>,
    pub description: String,
}
```

Supported gate types:

- **not_null** -- validates that a specified column contains no null values
- **unique** -- validates column uniqueness via HashSet deduplication (supports Utf8, Int32/64, Float32/64)
- **range** -- validates that numeric values fall within `[0, threshold]` using `compute_min_max_primitive`
- **row_count** -- validates that total row count across all batches meets a minimum
- **custom_sql** -- skipped at execution time (requires query engine context), noted in results

### Validation Pipeline

The `validate_gates()` function in `executable_table.rs` maps each `QualityGateRef` to the appropriate `quality_gates::QualityCheck`, constructs a temporary `QualityGate`, and calls `validate_batches()` against the SQL output `RecordBatch`es. Results are returned as `GateResult` structs:

```rust
pub struct GateResult {
    pub gate_type: String,
    pub column: Option<String>,
    pub passed: bool,
    pub detail: String,  // e.g., "not_null — passed (1000 rows checked)"
}
```

Gate results are included in the execution API response and displayed in the UI as pass/fail badges (emerald for pass, rose for fail) with a Shield icon next to each gate.

Gates are stored in Iceberg properties (`rustlake.quality-gate.{i}.*`), making them visible to external engines querying the table's metadata. This means a Trino user can discover that a table has a `not_null` constraint on `order_id` simply by reading Iceberg properties -- no RustLake-specific tooling required.

---

## 11. Approach Comparison

Different platforms take different approaches to the same fundamental problems in data engineering. This section describes the architectural choices RustLake makes and how they differ from established platforms.

**Code-data linkage.** Where Databricks uses notebook versioning and Delta Lake transaction logs, and dbt uses Git repositories with separate warehouse execution, RustLake uses content-addressed binaries stored alongside Iceberg v2 metadata. Each approach has different characteristics: Databricks ties code to a workspace, dbt separates code from execution, and RustLake co-locates code artifacts with data artifacts on S3.

**Table format.** RustLake uses Apache Iceberg v2 exclusively. Databricks uses Delta Lake. Snowflake uses a proprietary internal format with Iceberg compatibility layers. Each format has its own ecosystem of compatible engines. Iceberg was chosen for RustLake because of its broad engine compatibility (Trino, Spark, Flink, DuckDB) and its metadata model (snapshots, manifests, schema evolution).

**Execution model.** Traditional platforms provision clusters or warehouses to run transforms. RustLake compiles Rust transforms to native binaries and executes them as local processes. SQL transforms run through DataFusion in-process. This eliminates cluster provisioning but limits compiled transforms to Rust (SQL is available for all users).

**Versioning semantics.** dbt and Airflow rely on Git for code versioning, with no built-in link between a Git commit and the data it produced. Databricks provides notebook versioning within its workspace. RustLake versions transforms within the table metadata itself, linking each Iceberg snapshot to the code version that produced it.

**Quality validation.** dbt provides test macros that run as SQL queries after model execution. RustLake validates quality gates against Arrow RecordBatch output during execution, before data is persisted. Both approaches catch data quality issues; they differ in when validation occurs relative to data persistence.

**Regression detection.** Most platforms rely on external monitoring tools or manual inspection to detect output regressions. RustLake runs automatic metric-level and data-level regression detection after every execution, comparing against the previous run. This is built into the execution pipeline rather than bolted on.

**Rollback.** Snowflake provides table-level time travel. Delta Lake provides versioned tables. RustLake provides code-level rollback with git-revert semantics (creating a new version with old code) combined with per-version data isolation on S3. Each approach trades off between simplicity and granularity.

---

## 12. Architecture

```
+-------------------------------------------------------------+
|                    RustLake API Server                       |
|  +----------+  +-----------+  +----------+  +-----------+   |
|  | DataFusion|  |Rust Exec  |  | Parquet  |  | Iceberg   |  |
|  | SQL Engine|  |Binary     |  | Sink     |  | Writer    |  |
|  | (v51)    |  |Compiler   |  | (S3)     |  | (v2 JSON) |  |
|  +-----+----+  +-----+-----+  +-----+----+  +-----+-----+  |
|        |              |              |              |         |
|  +-----v--------------v--------------v--------------v------+ |
|  |              Apache Arrow RecordBatch                    | |
|  |            (Zero-copy columnar exchange)                 | |
|  +----------------------------+-----------------------------+ |
|                               |                               |
|  +----------------------------v-----------------------------+ |
|  |    Quality Gates    |    Auto-Regression Detection       | |
|  |  (validate_gates)  |   (detect_regression per exec)     | |
|  +------------------------------------------------------+  | |
+-------------------------------|-------------------------------+
                                |
              +-----------------v-----------------+
              |       S3 / MinIO Storage          |
              |                                   |
              |  executable_tables/{table}/v{N}/  |
              |  ├── data/          (Iceberg std) |
              |  │   └── *.parquet               |
              |  ├── metadata/      (Iceberg std) |
              |  │   ├── v1.metadata.json        |
              |  │   ├── v2.metadata.json        |
              |  │   └── snap-*-manifest.json    |
              |  └── binary/     (RustLake ext)  |
              |      ├── bin-{hash}     (470 KB) |
              |      └── manifest-{hash}.json    |
              +-----------------------------------+
```

**Request flow for a Rust transform execution:**

1. API receives execute request with table name and optional version override
2. `ExecutableTable` loaded from state store; current version determined
3. Source code hashed via FNV-1a to produce content key
4. Binary cache checked: local memory -> local disk -> S3 download -> cold compile
5. Binary executed with 10-second timeout; stdout/stderr captured
6. Binary uploaded to versioned S3 path: `executable_tables/{table}/v{N}/binary/bin-{hash}`
7. Binary manifest written alongside: `executable_tables/{table}/v{N}/binary/manifest-{hash}.json`
8. Quality gates validated against output data (`validate_gates` -> `validate_batches`)
9. Auto-regression detection compares current execution to previous (`detect_regression`)
10. `ExecutionRecord` appended to history; Iceberg snapshot ID linked to code version
11. Regression logged via `tracing::warn` if detected; results returned in API response

**Request flow for a SQL transform execution:**

1-3. Same as Rust flow (no compilation step for SQL)
4. SQL executed via DataFusion; output captured as Arrow `RecordBatch`
5. Parquet sink writes data files to `executable_tables/{table}/v{N}/data/` on S3
6. Incremental Iceberg metadata: load existing state -> append snapshot -> write `v{M+1}.metadata.json`
7. Table registered in DataFusion via self-call to `/api/v1/tables/register-s3`
8-11. Same as Rust flow (gate validation, auto-regression, history recording)

The entire flow executes within a single Tokio runtime on a single machine. No cluster coordination, no JVM startup, no warehouse provisioning.

---

## 13. Regulatory Compliance Automation

Executable tables provide the foundation for automated regulatory compliance. The provenance chain, quality gates, and version history combine into a system that can answer regulatory inquiries with full computational proof in seconds.

### Upstream Cascade Replay

When a table's output is questioned, the system walks the dependency DAG upstream, topologically sorts the affected tables, and re-executes each one in order. Every re-execution validates quality gates and data contracts, producing a verifiable replay of the entire computation chain. If any upstream table fails its gates during replay, the cascade halts and reports exactly where the failure occurred.

### Living Data Lineage

SQL transforms are parsed to extract column-level lineage: which input columns flow into which output columns, through which transformations. This lineage is maintained automatically as transforms change -- no manual annotation required. The lineage graph answers questions like "which upstream sources contribute to this column?" and "if this source column changes, which downstream tables are affected?"

### Versioned Materialized Views

Executable tables can be configured as materialized views that auto-refresh on a configurable interval. Each refresh creates a new Iceberg snapshot linked to the current code version. The version history preserves every materialization, enabling auditors to see exactly what the view contained at any point in time.

### Executable Pipelines

Multiple executable tables can be composed into a named pipeline -- a DAG of transforms with defined execution order. Pipelines execute with stop-on-failure semantics: if any table in the DAG fails its quality gates or produces a regression, the pipeline halts and downstream tables are not executed. This prevents bad data from propagating through multi-stage computations.

### Time-Travel Debugging

When a data issue is identified, the system can locate the execution that introduced the problem by comparing consecutive executions. For a given table, it retrieves the last known good execution and the first bad execution, then presents: the code diff between their versions, the data diff between their outputs (schema changes, null rate changes, cardinality changes), and any upstream changes that occurred between the two executions. This narrows root-cause analysis from hours to seconds.

### Cost-Aware Scheduling

The scheduler tracks input table checksums. When a scheduled transform runs, the system checks whether any upstream input has changed since the last execution. If no inputs have changed, the execution is skipped and the previous result is reused. The system tracks cumulative skipped executions, providing visibility into how often transforms run unnecessarily.

### Data Products and Compliance Audit

A data product wraps an executable table with its data contract (schema, quality gates, SLA), ownership metadata, and quality score into a single auditable entity. The data product exposes:

- The current quality gate pass rate across recent executions
- The SLA compliance record (execution within scheduled windows)
- The full provenance chain (every code version, every execution, every regression)
- The data contract (expected schema, enforced constraints, freshness requirements)

### The Killer Demo: Regulatory Inquiry in Seconds

Consider a bank examiner asking: "Prove that this risk computation is correct."

The system responds in seconds with:

1. **Provenance**: The exact source code (version 7, hash `a3f2...`) that produced the current data, who wrote it, when, and why
2. **Cascade replay**: Re-execute the entire upstream DAG (3 tables deep), verifying that every intermediate result passes its quality gates
3. **Gate verification**: All 4 quality gates on the risk table pass: `not_null` on `risk_score`, `range` on `exposure_amount`, `unique` on `account_id`, `row_count` minimum of 10,000
4. **Contract proof**: The output schema matches the registered data contract, SLA was met for the last 30 days, quality score is 98.5%
5. **Lineage**: Column-level lineage showing `risk_score` is derived from `positions.market_value` and `limits.credit_limit` through a specific SQL formula

This entire response is assembled from metadata that already exists in the system -- no manual documentation, no scrambling to reconstruct what happened.

---

## 14. Future Directions

**Schema evolution tracking.** Link Iceberg `schema-id` to transform versions, enabling automatic detection of schema changes across code revisions.

**Distributed execution via Arrow Flight.** RustLake already includes a Flight coordinator and worker framework. Executable table binaries can be dispatched to workers for parallel execution on partitioned data.

**Lambda deployment.** Ship the compiled binary directly to AWS Lambda. The binary is already self-contained (~453KB); Lambda cold start is ~100ms for binary download. This combines RustLake's compilation model with Lambda's pay-per-invocation pricing.

**Edge execution.** Ship binaries to edge nodes for low-latency execution close to data sources. The content-addressable storage model means edge nodes can cache binaries independently.

**Full Avro manifest support.** The `iceberg-rust` v0.8 crate is a dependency, ready for migration from JSON manifest lists to proper Avro manifests with column-level statistics.

**Full compliance report generation.** Export complete compliance audit reports as PDF or JSON, including provenance chains, quality gate history, SLA compliance records, and lineage graphs for regulatory submission.

**Multi-tenant data product catalogs.** Organize data products by team or business unit with access controls, enabling self-service data discovery within organizational boundaries.

---

## 15. The Data Organism: Self-Maintaining Datasets

Executable tables are not just versioned transforms — they are the building blocks of **self-maintaining data organisms**. Each table is a living node in a dependency graph that can breathe (auto-refresh when upstream changes), heal (roll back on quality failure), grow (accumulate versions while expiring old snapshots), and report (prove its provenance at any point in time).

### Self-Maintaining Capabilities

| Capability | What It Does | Foundation |
|---|---|---|
| **Adaptive Refresh** | Tables detect upstream changes and decide: skip (no change), incremental (small delta), or full rebuild (large change) | Cost-aware scheduler compares upstream `last_refresh` timestamps |
| **Self-Healing Quality** | Gates detect data drift → auto-rollback to last known-good version | Quality gates + version rollback + auto-trigger in scheduler |
| **Cascade Awareness** | Tables know their DAG position and propagate or block changes through the dependency chain | `build_dependency_dag()` + `topological_sort_upstream()` |

### The Organism Architecture

A data organism is a directed acyclic graph of executable tables where each node manages its own lifecycle:

```
Raw Source
  → Ingestion Table (self-refreshing, CDC-aware)
    → Feature Table (compiled Rust, sub-ms execution)
      → Model Input Table (distribution-monitored)
        → Prediction Table (versioned, auditable)
          → Data Product (SLA-bound, certified)
```

Each node in this chain:
- **Breathes** — auto-refreshes when upstream data changes, with cost-aware skip logic that avoids redundant computation
- **Heals** — auto-rolls back to the last known-good version when quality gates fail (`validate_gates()` → auto-rollback in scheduler), preventing bad data from propagating downstream
- **Grows** — versions accumulate as an append-only history, while Iceberg snapshot expiration reclaims storage for old data files
- **Reports** — the audit endpoint assembles provenance, quality gate results, SLA records, and lineage into a compliance response without manual intervention

### AI/ML Layer: Composing What Exists

The compliance automation and provenance infrastructure — the hardest parts — are already in place. The AI/ML layer is largely a composition of existing primitives:

**Anomaly-Driven Refresh.** Instead of fixed cron schedules, a lightweight model trained on execution history (rows produced, duration, data distributions) can detect anomalous results — 3x more rows than expected, schema drift, distribution shift — and auto-pause downstream consumers before bad data propagates. The execution history data (`ExecutionRecord` with rows, duration, cost, gate results) is already captured on every run.

**Smart Schema Evolution.** When an upstream source adds a column or changes a type, the transform's SQL can be analyzed to determine whether the change is breaking (removed column referenced in SELECT) or non-breaking (new column not referenced). DataFusion already tracks schemas through its catalog, and the column-level lineage parser (`parse_sql_column_lineage`) identifies which source columns feed which outputs.

**Feature Store Mode.** Executable tables with `transform_type: "rust"` are already compiled feature pipelines. The version history creates natural feature versioning — each execution produces a snapshot queryable by entity and timestamp via Iceberg time-travel. The same Rust binary can serve both batch (Parquet on S3) and real-time (Arrow Flight) workloads. Column lineage traces which raw columns feed which features, providing feature attribution without additional infrastructure.

**Embedding Tables.** A new transform type — `transform_type: "embedding"` — would take a text column, run it through a model (local ONNX runtime or external API), and store vectors alongside structured data in the same Iceberg table. The vector search crate already provides indexing and similarity search; wiring it to executable tables creates an embedded feature store with zero new infrastructure.

**Data Contracts as ML Monitors.** Contracts already validate schema structure. Extending them to validate data distributions — comparing current column statistics against historical baselines — transforms contracts into model monitoring. If the input distribution to an ML model drifts beyond a configurable threshold, the contract fails and the pipeline blocks. The column statistics infrastructure (`getTableStats`) already computes min, max, null counts, and distinct values.

### Lowest-Hanging Fruit

Three extensions require minimal code because they compose existing primitives:

1. **Auto-rollback on gate failure** — implemented in the scheduler tick. After every SQL execution, `validate_gates()` runs against the output batches. If any gate fails, the scheduler auto-rollbacks to the previous known-good version (creating a new version entry with `created_by: "auto-heal"` and description `"Auto-rollback to vN (gate failure)"`), sets the table health to `"warning"`, and logs the failure. When a subsequent execution passes all gates, the health resets to `"healthy"`. This closes the self-healing loop with zero manual intervention.

2. **Distribution drift detection** — add a `distribution` gate type that compares current column statistics (already computed) against historical baselines stored in execution history. A simple threshold on standard deviation detects meaningful drift without ML infrastructure.

3. **Embedding transform type** — combine the vector search crate's indexing with the executable table framework's versioning. Each execution produces a new vector index snapshot alongside the Parquet data, queryable via the same Iceberg metadata.

---

## 16. Conclusion

Executable tables represent a fundamental shift in how data platforms relate code to data. The transform that produces a table is a first-class versioned artifact stored alongside the data it produces, in an Iceberg-native S3 layout (`data/` + `metadata/` + `binary/`) readable by any Iceberg-compatible engine.

The provenance chain answers the question that has plagued data engineering since the first ETL job: "which code produced this data?" Every Iceberg snapshot links back to a specific code version via `TransformVersion.snapshot_ids`. Every code version maps to a content-addressed binary on S3. Quality gates validate the output on every execution. Auto-regression detection fires after every run, comparing metrics and data content against the previous execution. Rollback is instantaneous -- revert to any version, and its data, metadata, and binary are already on S3.

The native binary execution model -- compiling Rust transforms to ~470KB executables cached by content hash -- eliminates recompilation for unchanged code and removes the need for cluster provisioning or warehouse resume. SQL transforms execute through DataFusion in-process, making the platform accessible to users without Rust experience. The tradeoff is a ~30-second first-compile penalty for new Rust transforms, offset by instant cache hits on subsequent runs.

The regulatory compliance automation layer builds on the provenance chain to provide cascade replay, living lineage, time-travel debugging, and data products with auditable contracts. When a regulator asks "prove this computation is correct," the system assembles the answer from metadata that already exists -- provenance, quality gate results, SLA records, and column-level lineage -- without manual intervention.

The Iceberg integration ensures that this is not a walled garden. The `binary/` directory sits alongside standard Iceberg `data/` and `metadata/` directories without interfering -- Trino, Spark, Flink, and DuckDB read the table normally and never see the binary. The `rustlake.*` properties are documentation embedded in the table format itself. Quality gates are discoverable via Iceberg properties. Incremental metadata (v1, v2, v3...) preserves the full snapshot history that Iceberg's time-travel depends on.

This is git for data transforms: versioned, diffable, revertible, and auditable. Every commit has a hash. Every hash maps to a binary. Every binary produces data. Every execution is validated by quality gates and regression detection. The chain is unbroken.

The data organism vision extends this foundation from individual tables to self-maintaining ecosystems. When tables can detect upstream changes, auto-refresh adaptively, self-heal on quality failure, and compose into feature stores and ML pipelines, the result is a data platform that operates more like a living system than a batch scheduler — one where the infrastructure for anomaly detection, schema evolution, and distribution monitoring emerges from composing primitives that already exist.

---

*RustLake is an open-source, all-Rust data platform. Source code and documentation are available at the project repository. The executable tables feature is implemented across `crates/rustlake-api/src/executable_table.rs` (types, diff, regression detection, quality gate validation, data-level regression), `crates/rustlake-api/src/rust_executor.rs` (binary compilation, S3 upload/download, content-addressable caching), `crates/rustlake-api/src/parquet_sink.rs` (Parquet write path with incremental Iceberg support), `crates/rustlake-api/src/iceberg_writer.rs` (Iceberg v2 metadata generation), `crates/rustlake-api/src/iceberg_metadata.rs` (snapshot append, schema evolution, metadata versioning), `crates/rustlake-api/src/quality_gates.rs` (data quality validation engine), and `crates/rustlake-api/src/routes.rs` (execution handlers with per-version S3 paths, auto-regression, gate validation, snapshot linking).*
