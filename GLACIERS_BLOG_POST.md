# Glaciers: What If Iceberg Tables Could Maintain Themselves?

*We built self-maintaining Iceberg tables that version code and data together, auto-rollback on quality failure, and compose into autonomous data flows. Here's the architecture, what works today, and where this goes.*

---

## The Problem Everyone Accepts

Every data team runs the same loop: write a transform, schedule it in Airflow, add monitoring, build a runbook for when it breaks, debug failures by searching through logs, manually rollback when data goes bad.

The table — the thing you actually care about — is passive. It sits on S3 and waits for some external system to update it. The code that produces it lives in a completely separate repository. The schedule lives in a third system. The quality checks live in a fourth. When something breaks, you're hunting across four systems to figure out what happened.

**What if the table itself knew how to maintain itself?**

---

## Introducing Glaciers

A glacier is a standard Apache Iceberg v2 table with three additions:

```
s3://warehouse/risk_scores/
  data/                          ← Parquet files (standard Iceberg)
  metadata/v3.metadata.json      ← Iceberg v2 metadata (standard)
  binary/bin-aff76043            ← ~470KB compiled transform binary
  binary/manifest-aff76043.json  ← source code, schedule, quality gates
```

The `binary/` directory sits alongside standard Iceberg `data/` and `metadata/` directories. Trino, Spark, Flink, and DuckDB read the table normally — they never see the binary. The glacier IS a standard Iceberg table. It just happens to also know how to refresh itself.

The name comes from the relationship: **glaciers produce icebergs**. A glacier produces Iceberg tables.

---

## What a Glacier Does

Each glacier is a self-contained unit: transform code + output data + metadata. Here's what that enables:

### 1. Versioned Transforms (Git for Data)

Every code change creates a new version. Versions are immutable, append-only, content-hashed. You can diff any two versions, rollback to any previous version (which creates a new version, like `git revert`), and execute any historical version without changing HEAD.

![Glaciers page showing 5 glaciers with version counts, execution counts, and health indicators](screenshots/glaciers-list.png)
*The Glaciers page in RustLake. Each glacier shows its transform type (SQL/Rust), version count, execution count, and health status. The re-derive button (↻) triggers cascade replay of the entire upstream DAG.*

### 2. Quality Gates That Heal

Every execution validates quality gates — not-null checks, uniqueness, row counts, range bounds. If a gate fails, the glacier auto-rolls back to the last known-good version. No manual intervention. No PagerDuty alert. The table heals itself.

```
Scheduler tick:
  1. Execute transform SQL
  2. validate_gates() against output RecordBatches
  3. Any gate fails?
     → Auto-rollback to previous version
     → Set health to "warning"
     → Log: "AUTO-ROLLBACK triggered (self-healing)"
  4. All gates pass?
     → Update last_refresh
     → Reset health to "healthy"
```

This is implemented in ~25 lines of Rust in the scheduler. The quality gate engine (`quality_gates.rs`) validates against actual Arrow `RecordBatch` data — not samples, not metadata, the real output.

**Validated claim:** We created a glacier with `not_null`, `unique`, and `row_count` gates. On cascade replay, the root glacier executed 5 rows with all 3 gates passing:

```
raw_events: success | 5 rows
  gates: not_null(user_id):PASS, not_null(event):PASS, row_count:PASS
```

### 3. Column-Level Lineage

Glaciers parse their SQL transforms to extract column-level lineage — which output columns come from which source columns, through which expressions.

![Column lineage for risk_scores showing 5 output columns traced back to user_metrics](screenshots/lineage.png)
*Column lineage for the `risk_scores` glacier. Each output column traces back to its source table and column with the exact transform expression. `risk_level` is derived from a CASE expression on `user_metrics.total_value`. `avg_value` uses `ROUND(total_value / NULLIF(event_count, 0), 2)`.*

**Validated claim:** The lineage API correctly traces `risk_level` ← `CASE WHEN total_value < 0 THEN 'high' WHEN total_value < 100 THEN 'medium' ELSE 'low' END` from `user_metrics`.

### 4. Cascade Replay (No Orchestrator Needed)

Glaciers know their upstream dependencies via `input_tables`. When you trigger cascade replay on a glacier, it:

1. Builds the dependency DAG from all glaciers' `input_tables`
2. Topologically sorts to find the execution order
3. Executes each glacier in order
4. Validates quality gates at every node
5. Returns a per-node report

```
POST /api/v1/executable-tables/risk_scores/cascade-replay

Response:
  Target: risk_scores
  Tables in chain: 3
  Total duration: 10ms
  All gates passed: true

  raw_events     → success | 5 rows | 3 gates passed
  user_metrics   → executed
  risk_scores    → executed
```

The "orchestration" IS the dependency graph encoded in each glacier's metadata. No Airflow. No dbt. No external scheduler.

### 5. Compliance Audit in Seconds

A data product wraps a glacier with SLA requirements (freshness, quality score) and ownership metadata. The audit endpoint composes everything into a single compliance response:

![Compliance audit dashboard showing Risk Score Product with freshness, quality, gate rate, provenance chain](screenshots/audit.png)
*The compliance audit for "Risk Score Product." Freshness: 999h (SLA violation — table hasn't been refreshed yet). Quality: 50%. Gate rate: 100%. Provenance chain: `raw_events → user_metrics → risk_scores`. Consumers: compliance, trading-desk, regulators.*

**What the audit assembles:**
1. **Provenance chain** — the full DAG from root sources to this table
2. **Quality gate history** — pass rate across all executions
3. **Freshness check** — last refresh vs SLA requirement
4. **Contract validation** — schema matches registered contract
5. **Cost summary** — total compute cost + savings from skip optimization
6. **Certification eligibility** — freshness OK + quality OK + contracts OK

A regulator asks "prove this risk score was computed correctly." The system answers in one API call.

---

## The Architecture

```
s3://team-a/
  glacier: raw_events        ← SQL: SELECT from CDC source
    Quality gates: not_null(user_id), not_null(event), row_count
    Schedule: */5 * * * *

  glacier: user_metrics      ← SQL: aggregate raw_events by user
    Input tables: [raw_events]
    Quality gates: not_null(user_id), unique(user_id), row_count
    Schedule: */10 * * * *

  glacier: risk_scores       ← SQL: CASE scoring on user_metrics
    Input tables: [user_metrics]
    Quality gates: not_null(risk_level), not_null(user_id), row_count
    Schedule: */15 * * * *
    Data Product: "Risk Score Product" (SLA: 1h freshness, 99% quality)
```

Each glacier is independently versioned, independently gated, independently auditable. The dependency graph is declared, not configured in an external tool.

### What's on S3

Every glacier produces standard Iceberg v2 output:

```
s3://warehouse/raw_events/
  data/
    2026-03-22-part-0.parquet     ← 5 events, Snappy compressed
  metadata/
    v1.metadata.json              ← Iceberg v2 snapshot
    manifest-list-snap-123.json   ← manifest list
  binary/
    bin-aff76043                  ← compiled transform (~470KB)
    manifest-aff76043.json        ← source hash, gates, schedule
```

Any Iceberg-compatible engine reads the `data/` and `metadata/` directories normally. The `binary/` directory is invisible to external tools — it's a convention, not a spec extension.

### The Self-Healing Loop

```
┌─────────────────────────────────────────────────────────┐
│                    Scheduler Tick                        │
│                                                         │
│  1. Check upstream: has raw_events refreshed since      │
│     user_metrics last ran?                              │
│     → No change: SKIP (track cost saved)                │
│     → Changed: proceed                                  │
│                                                         │
│  2. Execute transform SQL via DataFusion                │
│     → Produces Arrow RecordBatches                      │
│                                                         │
│  3. Validate quality gates against output                │
│     → not_null(user_id): check column for nulls         │
│     → unique(user_id): check for duplicates             │
│     → row_count: verify rows > 0                        │
│                                                         │
│  4a. All gates pass:                                    │
│      → Write Parquet to S3                              │
│      → Update Iceberg metadata                          │
│      → Set health = "healthy"                           │
│                                                         │
│  4b. Any gate fails:                                    │
│      → AUTO-ROLLBACK to previous version                │
│      → Create new version: "Auto-rollback to vN"        │
│      → Set health = "warning"                           │
│      → Bad data never reaches consumers                 │
└─────────────────────────────────────────────────────────┘
```

---

## What Works Today (Verified)

Every claim in this post was validated against a running RustLake instance with 3 glaciers in a dependency chain.

| Capability | Status | Evidence |
|---|---|---|
| **Versioned transforms** | Working | v1/v2 with diff, rollback, HEAD tracking |
| **Quality gates** | Working | not_null, unique, row_count validated against real RecordBatches |
| **Self-healing auto-rollback** | Working | Gate failure → auto-rollback in scheduler tick |
| **Cascade replay** | Working | 3-glacier DAG executed in topological order, 10ms total |
| **Column lineage** | Working | SQL parsed → 5 columns traced through CASE expressions and aggregations |
| **Cost-aware scheduling** | Working | Skip when upstream unchanged, track savings |
| **Compliance audit** | Working | Provenance chain, gate history, SLA check, certification in one API call |
| **Iceberg v2 output** | Working | Standard Parquet + metadata, readable by Trino/Spark/Flink |
| **195 unit tests** | Passing | Quality gates, regression detection, Iceberg metadata, cost model, profiler |

---

## What's Not Built Yet (Honest)

| Capability | Gap | What It Needs |
|---|---|---|
| **Cross-bucket glaciers** | Today: same-process only | S3 event notifications or Iceberg metadata polling |
| **Decentralized scheduler** | Today: one central scheduler | Each glacier runs its own refresh loop |
| **Lambda execution** | Binary is Lambda-ready (~470KB) | Thin dispatcher: watch metadata → pull binary → invoke Lambda → write output |
| **Contract negotiation** | Today: manual `input_tables` declaration | Upstream publishes schema contract, downstream auto-validates |
| **Distribution drift gates** | Today: structural checks only | Compare column stats against historical baselines |

The foundation — versioning, gates, lineage, provenance, self-healing — is complete. The gaps are in decentralization and serverless execution.

---

## The Vision: Data as Microservices

What we're building toward is **data as microservices**. Each glacier is:

- **Self-contained**: binary + data + metadata in one S3 prefix
- **Discoverable**: via Iceberg catalog (any engine can find it)
- **Self-healing**: gates + auto-rollback, no human intervention
- **Independently deployable**: just needs an S3 bucket
- **Composable**: declare `input_tables` to latch onto upstream glaciers

The end state:

```
Team A's bucket (s3://team-a/)
  glacier: raw_events        ← CDC from Kafka
  glacier: clean_events      ← depends on raw_events
  glacier: event_features    ← depends on clean_events

Team B's bucket (s3://team-b/)
  glacier: user_scores       ← depends on team-a/event_features
  glacier: risk_model_input  ← depends on user_scores
  glacier: risk_predictions  ← depends on risk_model_input

Team C latches on:
  glacier: compliance_report ← depends on team-b/risk_predictions
```

No shared infrastructure. No monolithic Airflow. A new team "latches on" by declaring their upstream dependencies. The glacier discovers the upstream schema via Iceberg metadata, validates compatibility via contracts, and starts refreshing.

Development reduces to three artifacts:
1. **Binary** — the compiled transform logic (~470KB)
2. **Data** — Parquet files on S3
3. **Metadata** — Iceberg v2 JSON describing both

No Docker images. No Kubernetes manifests. No Airflow DAGs. No dbt project files. The glacier IS the deployment unit.

---

## Try It

```bash
# Option 1: Install from crates.io
cargo install rustlake-api
rustlake serve

# Option 2: Build from source
git clone https://github.com/rustlake/rustlake.git
cd rustlake
cargo build && rustlake serve

# Create a glacier
curl -X POST http://localhost:3000/api/v1/executable-tables \
  -H 'Content-Type: application/json' \
  -d '{
    "table_name": "my_glacier",
    "transform": {
      "transform_type": "sql",
      "source_code": "SELECT 1 as id, now() as ts"
    },
    "quality_gates": [
      {"gate_type": "not_null", "column": "id"}
    ]
  }'

# Execute it
curl -X POST http://localhost:3000/api/v1/executable-tables/my_glacier/execute-version \
  -d '{"version": 1}'

# See lineage
curl http://localhost:3000/api/v1/executable-tables/my_glacier/column-lineage
```

RustLake is open source, written entirely in Rust, cold-starts in 100ms, and runs on a single binary. The Glaciers feature is built on DataFusion 51, Apache Arrow 57, and Iceberg v2.

---

*RustLake is an all-Rust, single-binary data platform. Glaciers are one of 20+ features including multi-engine query routing, CDC pipelines, browser-side WASM compute, and an Apache Iceberg REST Catalog. See the [full README](README.md) for the complete feature set.*

*The technical whitepaper with full architecture details is available at [EXECUTABLE_TABLES_WHITEPAPER.md](EXECUTABLE_TABLES_WHITEPAPER.md).*
