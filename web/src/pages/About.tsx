import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Button } from '../components/ui/Button'
import { cn } from '../lib/utils'
import {
  Info, Cpu, Database, Layers, GitBranch, Boxes, ArrowRight,
  Code2, Gauge, Zap, Shield, Terminal, Globe, Radio,
  Server, Copy, ExternalLink, Search, Clock, BarChart3,
  Plane, Activity, FileText, CheckCircle2, Route,
  HardDrive, Network, BookOpen, Workflow, Settings,
  Upload, Table2, Eye, Trash2, Play, PenTool, Milestone,
  RefreshCw, Loader2, Rocket,
} from 'lucide-react'
import toast from 'react-hot-toast'
import { getBootstrapStatus, runBootstrap } from '../api/client'
import type { BootstrapStatus } from '../types'

// ── Static data ─────────────────────────────────────────────────

const DOCKER_SERVICES = [
  { category: 'Core', services: [
    { name: 'RustLake API', port: '3000', conn: 'http://localhost:3000', color: 'text-amber-400' },
    { name: 'Arrow Flight gRPC', port: '50051', conn: 'grpc://localhost:50051 (optional — RUSTLAKE_FLIGHT__ENABLED=true)', color: 'text-sky-400' },
    { name: 'Web UI', port: '3001', conn: 'http://localhost:3001', color: 'text-amber-300' },
    { name: 'PostgreSQL', port: '5433', conn: 'postgresql://rustlake:rustlake@localhost:5433/rustlake_demo', color: 'text-blue-400' },
    { name: 'MySQL', port: '3307', conn: 'mysql://rustlake:rustlake@localhost:3307/rustlake_demo', color: 'text-cyan-400' },
    { name: 'MongoDB', port: '27018', conn: 'mongodb://rustlake:rustlake@localhost:27018/rustlake_demo', color: 'text-emerald-400' },
    { name: 'MinIO (S3)', port: '9000 / 9001', conn: 'http://localhost:9000 (API) · http://localhost:9001 (Console)', user: 'rustlake', pass: 'rustlake123', color: 'text-yellow-400' },
  ]},
  { category: 'Search', profile: 'search', services: [
    { name: 'Redis', port: '6379', conn: 'redis://localhost:6379', color: 'text-rose-400' },
    { name: 'Elasticsearch', port: '9200', conn: 'http://localhost:9200', color: 'text-orange-400' },
  ]},
  { category: 'Analytics', profile: 'analytics', services: [
    { name: 'ClickHouse', port: '8123 / 9100', conn: 'http://localhost:8123 (HTTP) · localhost:9100 (TCP)', user: 'rustlake', pass: 'rustlake', color: 'text-violet-400' },
    { name: 'Cassandra', port: '9042', conn: 'localhost:9042', color: 'text-indigo-400' },
  ]},
  { category: 'Streaming', profile: 'streaming', services: [
    { name: 'Kafka', port: '9092', conn: 'localhost:9092', color: 'text-sky-400' },
    { name: 'RabbitMQ', port: '5672 / 15672', conn: 'amqp://rustlake:rustlake@localhost:5672 · http://localhost:15672 (UI)', user: 'rustlake', pass: 'rustlake', color: 'text-pink-400' },
    { name: 'NATS', port: '4222 / 8222', conn: 'nats://localhost:4222 · http://localhost:8222 (monitor)', color: 'text-lime-400' },
    { name: 'Mosquitto', port: '1883 / 9883', conn: 'mqtt://localhost:1883 · ws://localhost:9883 (WebSocket)', color: 'text-teal-400' },
  ]},
]

const DOCKER_COMMANDS = [
  { label: 'Core only', cmd: 'docker compose up -d' },
  { label: '+ Search', cmd: 'docker compose --profile search up -d' },
  { label: '+ Analytics', cmd: 'docker compose --profile analytics up -d' },
  { label: '+ Streaming', cmd: 'docker compose --profile streaming up -d' },
  { label: 'Everything', cmd: 'docker compose --profile search --profile analytics --profile streaming up -d' },
]

const CRATES = [
  { name: 'rustlake-core', desc: 'Arrow RecordBatch utilities, RustLakeError enum (thiserror), config parsing (TOML + env), tracing/metrics setup, common traits', color: 'text-amber-400' },
  { name: 'rustlake-storage', desc: 'object_store wrapper with connection pooling, credential rotation, retry with backoff, multi-region failover', color: 'text-amber-300' },
  { name: 'rustlake-catalog', desc: 'Iceberg REST Catalog spec — in-memory, SQLite, Postgres backends. Table discovery, schema evolution, namespace management', color: 'text-blue-400' },
  { name: 'rustlake-format', desc: 'Unified TableFormat trait over iceberg-rust, delta-rs, lance. Compaction, snapshots, partition pruning, time travel', color: 'text-emerald-400' },
  { name: 'rustlake-engine', desc: 'DataFusion integration — TableProvider registration, custom optimizer rules, AI UDFs (ai_classify, ai_extract, ai_gen), memory pools', color: 'text-violet-400' },
  { name: 'rustlake-stream', desc: 'Kafka consumer (rdkafka), MongoDB CDC, Postgres logical replication. Materializes into Iceberg via format crate', color: 'text-cyan-400' },
  { name: 'rustlake-vector', desc: 'LanceDB vector search (IVF-PQ, HNSW). Embeddings via OpenAI/Ollama/ONNX. UDFs: vector_search(), cosine_similarity(), embed()', color: 'text-rose-400' },
  { name: 'rustlake-router', desc: 'SQL AST analysis → workload classification → engine dispatch. 5 routing paths, adaptive latency tracking', color: 'text-yellow-400' },
  { name: 'rustlake-scheduler', desc: 'DAG-based workflow orchestration. Cron scheduling, job clusters, retry policies, timeout management', color: 'text-orange-400' },
  { name: 'rustlake-flight', desc: 'Arrow Flight RPC server (FlightService trait) + client. do_get, get_flight_info, actions. tonic 0.14 gRPC binding', color: 'text-sky-400' },
  { name: 'rustlake-transform', desc: 'dbt-compatible SQL compilation. ref()/source() resolution, Jinja-like templates, dependency DAGs, column-level lineage', color: 'text-indigo-400' },
  { name: 'rustlake-python', desc: 'PyO3 bindings — RustLakeSession, DataFrame, Table. pip install via maturin. Zero-copy Polars/PyArrow via Arrow FFI', color: 'text-lime-400' },
  { name: 'rustlake-api', desc: 'Axum 0.8 HTTP server — 40+ REST endpoints. Dual HTTP + gRPC mode when Flight enabled. JSON API + Arrow Flight data plane', color: 'text-teal-400' },
]

const PRINCIPLES = [
  { icon: Zap, title: 'Arrow-Native', desc: 'All inter-crate data exchange uses Arrow RecordBatch — zero serialization between components. Core structural advantage over JVM platforms.', color: 'text-amber-400' },
  { icon: Cpu, title: 'DataFusion Kernel', desc: 'SQL parsing, 30+ optimizer rules, vectorized execution flow through DataFusion. Used by InfluxDB 3.0, GlareDB, GreptimeDB. Extend via traits, never fork.', color: 'text-cyan-400' },
  { icon: Boxes, title: 'Composable', desc: 'Each crate is independently useful. Embed rustlake-engine alone without the API server or streaming layer. The platform is the composition.', color: 'text-violet-400' },
  { icon: Gauge, title: 'Instant Startup', desc: 'No JVM, no class loading, no 30-second startup. Cold start target: < 500ms. Single binary via cargo install.', color: 'text-emerald-400' },
  { icon: Shield, title: 'No Unsafe', desc: 'Zero unsafe code across the entire codebase. The only exception is the PyO3 FFI boundary in rustlake-python.', color: 'text-rose-400' },
  { icon: Code2, title: 'Python First-Class', desc: 'PyO3 bindings expose the full DataFrame and SQL API. Zero-copy interop with Polars and PyArrow via Arrow FFI. Jupyter _repr_html_().', color: 'text-blue-400' },
]

const DESIGN_DECISIONS = [
  { title: 'DataFusion as kernel', desc: 'Production-tested SQL parser, 30+ optimizer rules, vectorized columnar execution, Substrait plan support. 20+ production systems validate this foundation.', color: 'text-amber-400' },
  { title: 'Iceberg-first', desc: 'De facto universal table format. Databricks acquired Tabular, Snowflake open-sourced Polaris, AWS launched S3 Tables with native Iceberg. Delta read-compat via delta-rs.', color: 'text-blue-400' },
  { title: 'Lance + Iceberg dual-format', desc: 'Iceberg+Parquet for columnar analytics. Lance for random access + vector ops (100x faster). Both on same S3. Router dispatches to optimal format per query.', color: 'text-rose-400' },
  { title: 'Single-node first', desc: 'DataFusion handles TPC-H at 100GB+ on 8 cores. Build single-node, prove correctness, then add Flight distribution. Premature distribution kills database startups.', color: 'text-emerald-400' },
  { title: 'Iceberg REST Catalog as interop', desc: 'HTTP/OpenAPI protocol that Trino, Spark, Flink, DuckDB all speak. Any external tool queries RustLake tables via standard catalog clients — no custom connectors.', color: 'text-cyan-400' },
]

const ROUTING_RULES = [
  { pattern: 'Full table scans, aggregations, joins', engine: 'DataFusion OLAP', color: 'text-blue-400', bgColor: 'bg-blue-400' },
  { pattern: 'WHERE pk = ? point lookups on Lance', engine: 'LanceDB Direct (sub-ms)', color: 'text-rose-400', bgColor: 'bg-rose-400' },
  { pattern: 'INSERT INTO ... FROM kafka_topic()', engine: 'Streaming Engine', color: 'text-cyan-400', bgColor: 'bg-cyan-400' },
  { pattern: 'SELECT vector_search(...)', engine: 'Vector Engine (Lance + HNSW)', color: 'text-violet-400', bgColor: 'bg-violet-400' },
  { pattern: 'Multi-engine federated queries', engine: 'Arrow Flight Exchange', color: 'text-amber-400', bgColor: 'bg-amber-400' },
]

const ROADMAP = [
  { phase: '1', title: 'Rust Lakehouse CLI', status: 'done' as const, desc: 'Single-binary Iceberg/Delta reader on S3 with SQL via DataFusion', scope: 'core, storage, catalog, format, engine, cli' },
  { phase: '2', title: 'Streaming + CDC', status: 'done' as const, desc: 'Kafka ingestion, MongoDB CDC, Postgres logical replication into Iceberg', scope: 'stream, format write path' },
  { phase: '3', title: 'AI/Vector + Lance', status: 'done' as const, desc: 'Dual-format lakehouse — Iceberg for analytics, Lance for AI. Embeddings, vector search, AI UDFs', scope: 'vector, router, python bindings' },
  { phase: '4', title: 'Distributed Execution', status: 'partial' as const, desc: 'Arrow Flight gRPC server implemented (do_get, get_flight_info, actions). Dual HTTP+gRPC startup. Flight SQL, coordinator/worker, K8s operator remaining.', scope: 'flight (~30% complete)' },
  { phase: '5', title: 'Platform UX', status: 'current' as const, desc: 'Full-surface UI exposing all 13 crates. 11 pages, 40+ API endpoints, 109 data source connectors, 8 job types', scope: 'web/ frontend, rustlake-api routes' },
]

const API_GROUPS = [
  { group: 'SQL Execution', color: 'text-amber-400', endpoints: [
    { method: 'POST', path: '/api/v1/sql', desc: 'Execute SQL, return JSON rows + metadata' },
    { method: 'POST', path: '/api/v1/sql/explain', desc: 'EXPLAIN plan — logical + physical plan + node tree' },
  ]},
  { group: 'Tables & Catalog', color: 'text-cyan-400', endpoints: [
    { method: 'GET', path: '/api/v1/tables', desc: 'List all registered tables' },
    { method: 'POST', path: '/api/v1/tables/register', desc: 'Register file path as named table' },
    { method: 'DELETE', path: '/api/v1/tables/{name}', desc: 'Deregister a table' },
    { method: 'GET', path: '/api/v1/tables/{name}/schema', desc: 'Column names, types, nullability' },
    { method: 'GET', path: '/api/v1/tables/{name}/preview', desc: 'First 50 rows as JSON' },
    { method: 'GET', path: '/api/v1/tables/{name}/stats', desc: 'Row count, min/max, null counts, distinct counts' },
    { method: 'GET/PUT', path: '/api/v1/tables/{name}/description', desc: 'Table + column descriptions' },
  ]},
  { group: 'Query History', color: 'text-violet-400', endpoints: [
    { method: 'GET', path: '/api/v1/query/history', desc: 'Recent queries with duration, status, type, row count' },
  ]},
  { group: 'Transforms & Lineage', color: 'text-indigo-400', endpoints: [
    { method: 'GET/POST', path: '/api/v1/transforms', desc: 'List or create dbt-compatible SQL transforms' },
    { method: 'POST', path: '/api/v1/transforms/{name}/run', desc: 'Execute transform with ref() resolution' },
    { method: 'DELETE', path: '/api/v1/transforms/{name}', desc: 'Delete a transform' },
    { method: 'GET', path: '/api/v1/lineage', desc: 'Full lineage DAG (nodes + edges)' },
  ]},
  { group: 'Vector / AI', color: 'text-rose-400', endpoints: [
    { method: 'POST', path: '/api/v1/vector/search', desc: 'Semantic similarity search over indexed documents' },
    { method: 'POST', path: '/api/v1/vector/index', desc: 'Index documents with embeddings' },
    { method: 'GET', path: '/api/v1/vector/status', desc: 'Index size, dimensions, model info' },
  ]},
  { group: 'Streaming & CDC', color: 'text-emerald-400', endpoints: [
    { method: 'POST', path: '/api/v1/stream/ingest', desc: 'Generate simulated stream events' },
    { method: 'GET', path: '/api/v1/stream/status', desc: 'Metrics: events/s, bytes, latency, buffer' },
    { method: 'GET', path: '/api/v1/stream/events', desc: 'Recent stream events from buffer' },
    { method: 'GET/POST', path: '/api/v1/streaming/pipelines', desc: 'List or create streaming pipelines (Kafka/CDC)' },
    { method: 'DELETE', path: '/api/v1/streaming/pipelines/{id}', desc: 'Delete a pipeline' },
  ]},
  { group: 'Scheduler & Jobs', color: 'text-orange-400', endpoints: [
    { method: 'GET/POST', path: '/api/v1/schedules', desc: 'List or create scheduled jobs (8 types)' },
    { method: 'GET/PUT/DELETE', path: '/api/v1/schedules/{id}', desc: 'Get, update, or delete a job' },
    { method: 'POST', path: '/api/v1/schedules/{id}/run', desc: 'Trigger immediate job execution' },
    { method: 'GET', path: '/api/v1/schedules/runs', desc: 'Job execution history' },
    { method: 'GET', path: '/api/v1/schedules/dag', desc: 'Scheduler dependency DAG' },
    { method: 'GET', path: '/api/v1/clusters', desc: 'Job clusters (default, high-memory, gpu)' },
  ]},
  { group: 'Connections & Upload', color: 'text-blue-400', endpoints: [
    { method: 'GET/POST', path: '/api/v1/connections', desc: 'List or create Postgres connections' },
    { method: 'DELETE', path: '/api/v1/connections/{id}', desc: 'Delete a connection' },
    { method: 'POST', path: '/api/v1/connections/{id}/register/{table}', desc: 'Register external table into DataFusion' },
    { method: 'POST', path: '/api/v1/upload', desc: 'Upload CSV/Parquet/JSON (multipart)' },
    { method: 'GET/POST', path: '/api/v1/storage/s3', desc: 'S3/MinIO storage configs' },
  ]},
  { group: 'Data Quality', color: 'text-sky-400', endpoints: [
    { method: 'GET', path: '/api/v1/quality/checks', desc: 'Run quality checks across all tables' },
    { method: 'GET/POST', path: '/api/v1/quality/rules', desc: 'List or create alert rules' },
    { method: 'DELETE', path: '/api/v1/quality/rules/{id}', desc: 'Delete a quality rule' },
  ]},
  { group: 'dbt Integration', color: 'text-lime-400', endpoints: [
    { method: 'POST', path: '/api/v1/dbt/upload', desc: 'Upload dbt project (models + sources)' },
    { method: 'GET', path: '/api/v1/dbt/project', desc: 'Project metadata' },
    { method: 'GET', path: '/api/v1/dbt/models', desc: 'List models and sources' },
    { method: 'POST', path: '/api/v1/dbt/run/{name}', desc: 'Run a single model' },
    { method: 'POST', path: '/api/v1/dbt/run-all', desc: 'Run all models in dependency order' },
  ]},
  { group: 'Flight & System', color: 'text-zinc-400', endpoints: [
    { method: 'GET', path: '/health', desc: 'Health check (version, engine status)' },
    { method: 'GET', path: '/api/v1/system/info', desc: 'Platform version, uptime, query count' },
    { method: 'GET', path: '/api/v1/system/resources', desc: 'CPU, memory, engine config, Flight status' },
    { method: 'GET', path: '/api/v1/flight/info', desc: 'Flight capabilities, clients, queries served' },
    { method: 'GET', path: '/api/v1/flight/status', desc: 'Flight running state, connections, port' },
    { method: 'GET/POST', path: '/api/v1/feedback', desc: 'Chat feedback messages' },
  ]},
]

const E2E_FEATURES = [
  'Upload CSV/Parquet/JSON files, auto-register as tables, query immediately in SQL Editor',
  'Connect to Postgres, discover tables, register into DataFusion, run cross-source SQL joins',
  'Multi-tab SQL editor with Monaco autocomplete (tables, columns with types, SQL keywords)',
  '5 chart types on query results (bar, line, scatter, pie, area) + CSV export + clipboard copy',
  'EXPLAIN plan visualization with logical/physical plan tree and node details',
  'Saved queries with localStorage persistence and one-click replay from history',
  'Transform CRUD with SQL formatter, compiled preview, ref() resolution, lineage DAG',
  'dbt project import — upload models + sources, run individually or in dependency order',
  'Scheduler CRUD with 8 job types, 18 templates (ETL, MV, compaction, snapshots, dbt)',
  'Streaming pipeline CRUD with Kafka/CDC source config, transform SQL, sink table',
  'Full data catalog with schema, stats, null distribution, cardinality, freshness metadata',
  'Vector search with configurable embedding pipeline (IVF-PQ, HNSW, Brute Force indexes)',
  'Per-table data quality checks with configurable alert rules and aggregate health dashboard',
  '109 data source connectors across 7 categories with configuration modals',
  'Arrow Flight gRPC server for high-performance data transport (enable with env var)',
  'Engine metrics dashboard with latency distribution, success rates, streaming throughput',
]

// ── Component ─────────────────────────────────────────────────

export function About() {
  const navigate = useNavigate()
  const [bootstrap, setBootstrap] = useState<BootstrapStatus | null>(null)
  const [bootstrapping, setBootstrapping] = useState(false)

  const fetchStatus = useCallback(() => {
    getBootstrapStatus().then(setBootstrap).catch(() => {})
  }, [])

  useEffect(() => { fetchStatus() }, [fetchStatus])

  const handleBootstrap = async () => {
    setBootstrapping(true)
    try {
      await runBootstrap()
      toast.success('Bootstrap complete')
      fetchStatus()
    } catch (e) {
      toast.error('Bootstrap failed')
    } finally {
      setBootstrapping(false)
    }
  }

  return (
    <div className="flex flex-col h-full animate-fade-in">
      {/* Header */}
      <div className="px-6 py-4 border-b border-white/[0.04]">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
            <Info className="w-4.5 h-4.5 text-amber-400" />
          </div>
          <div>
            <h1 className="text-base font-display font-bold text-zinc-100">About RustLake</h1>
            <p className="text-2xs text-zinc-500">Architecture, scope, API reference, and platform capabilities</p>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-6">
        <div className="max-w-5xl mx-auto space-y-8">
          {/* Hero */}
          <div className="relative overflow-hidden rounded-2xl border border-amber-400/[0.08] p-8 bg-gradient-to-br from-navy-950 via-navy-900 to-navy-850">
            <div className="absolute top-0 right-0 w-80 h-80 bg-amber-400/5 rounded-full blur-3xl" />
            <div className="absolute bottom-0 left-20 w-60 h-60 bg-cyan-400/5 rounded-full blur-3xl" />
            <div className="relative">
              <div className="flex items-center gap-5 mb-4">
                <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-amber-400 to-amber-600 flex items-center justify-center shadow-2xl shadow-amber-500/20">
                  <span className="text-navy-950 font-mono font-bold text-3xl">R</span>
                </div>
                <div>
                  <h1 className="text-2xl font-display font-bold text-zinc-100 tracking-tight">RustLake</h1>
                  <p className="text-sm text-zinc-400 mt-0.5">All-Rust composable data platform — a Databricks alternative</p>
                </div>
              </div>
              <p className="text-xs text-zinc-500 leading-relaxed max-w-2xl">
                Unified runtime for batch analytics, stream processing, AI/vector workloads, and data transformation
                over open table formats on object storage. Built on Apache Arrow, DataFusion, and Iceberg.
                13-crate Cargo workspace, single binary, instant startup.
              </p>
              <div className="flex items-center gap-2 mt-4">
                <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20">Apache Arrow 57</Badge>
                <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20">DataFusion 51</Badge>
                <Badge className="bg-blue-400/10 text-blue-400 border-blue-400/20">Apache Iceberg</Badge>
                <Badge className="bg-rose-400/10 text-rose-400 border-rose-400/20">Lance</Badge>
                <Badge className="bg-sky-400/10 text-sky-400 border-sky-400/20">Arrow Flight</Badge>
              </div>
            </div>
          </div>

          {/* Design Principles */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
              <Shield className="w-4 h-4 text-zinc-500" /> Design Principles
            </h2>
            <div className="grid grid-cols-3 gap-3">
              {PRINCIPLES.map(p => (
                <Card key={p.title} padding="sm" hover>
                  <p.icon className={cn('w-5 h-5 mb-2', p.color)} />
                  <h3 className="text-xs font-display font-semibold text-zinc-200">{p.title}</h3>
                  <p className="text-2xs text-zinc-500 mt-1 leading-relaxed">{p.desc}</p>
                </Card>
              ))}
            </div>
          </div>

          {/* Key Design Decisions */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
              <BookOpen className="w-4 h-4 text-zinc-500" /> Key Design Decisions
            </h2>
            <div className="space-y-2">
              {DESIGN_DECISIONS.map(d => (
                <div key={d.title} className="p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                  <h3 className={cn('text-xs font-display font-semibold mb-1', d.color)}>{d.title}</h3>
                  <p className="text-2xs text-zinc-500 leading-relaxed">{d.desc}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Query Router Architecture */}
          <Card>
            <h3 className="text-sm font-display font-semibold text-zinc-200 mb-2 flex items-center gap-2">
              <Route className="w-4 h-4 text-amber-400" /> Query Router — The "Query Garage"
            </h3>
            <p className="text-2xs text-zinc-500 mb-3 leading-relaxed">
              Inspects incoming SQL AST using DataFusion's parser, classifies workload type,
              and routes to the optimal execution engine. Tracks per-engine latency for adaptive routing.
            </p>
            <div className="space-y-1.5">
              {ROUTING_RULES.map((r, i) => (
                <div key={i} className="flex items-center gap-3 p-2.5 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                  <span className={cn('w-1.5 h-1.5 rounded-full flex-shrink-0', r.bgColor)} />
                  <span className="text-2xs text-zinc-400 flex-1">{r.pattern}</span>
                  <ArrowRight className="w-3 h-3 text-zinc-700" />
                  <Badge className={cn('bg-white/[0.04] border-white/[0.06]', r.color)}>{r.engine}</Badge>
                </div>
              ))}
            </div>
          </Card>

          {/* Data Exchange Rules */}
          <Card>
            <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <Workflow className="w-4 h-4 text-cyan-400" /> Data Exchange Rules
            </h3>
            <div className="grid grid-cols-2 gap-3">
              {[
                { label: 'Between crates', value: 'Arrow RecordBatch — no exceptions', color: 'text-amber-400' },
                { label: 'To/from storage', value: 'Parquet files (read/write), Arrow IPC (spill)', color: 'text-emerald-400' },
                { label: 'To/from network', value: 'Arrow Flight (high perf), JSON over HTTP REST', color: 'text-sky-400' },
                { label: 'To/from Python', value: 'Zero-copy Arrow arrays via PyArrow FFI', color: 'text-violet-400' },
              ].map(r => (
                <div key={r.label} className="p-2.5 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                  <p className={cn('text-2xs font-semibold mb-0.5', r.color)}>{r.label}</p>
                  <p className="text-2xs text-zinc-500">{r.value}</p>
                </div>
              ))}
            </div>
          </Card>

          {/* Platform Features — Frontend */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-1 flex items-center gap-2">
              <Eye className="w-4 h-4 text-zinc-500" /> Frontend Features (11 UI Pages)
            </h2>
            <p className="text-2xs text-zinc-500 mb-4">What you can do from the browser dashboard.</p>
            <div className="grid grid-cols-2 gap-3">
              {[
                { icon: Code2, title: 'SQL Editor', color: 'text-amber-400', items: [
                  'Multi-tab editor (up to 5 tabs, double-click rename)',
                  'Monaco autocomplete — table names, columns with types, SQL keywords',
                  'Saved queries with localStorage persistence',
                  'EXPLAIN plan visualization (logical + physical tree)',
                  '5 chart types: bar, line, scatter, pie, area',
                  'CSV export + clipboard copy on result sets',
                ]},
                { icon: Database, title: 'Data Catalog', color: 'text-cyan-400', items: [
                  'Browse all registered tables with format badges',
                  'Schema viewer with column types and nullability',
                  'Inline statistics: row count, min/max, null counts',
                  'Rich metadata: cardinality, freshness, format, source',
                  'Table/column descriptions (user-editable)',
                  'Type distribution chart across all tables',
                ]},
                { icon: Upload, title: 'Data Sources', color: 'text-blue-400', items: [
                  '109 connectors across 7 categories (RDBMS, Analytics, NoSQL, Storage, Formats, Streaming, SaaS)',
                  'File upload: CSV, Parquet, JSON with auto-register',
                  'Postgres connector: connect, discover tables, register',
                  'Quick path templates (Local, S3, MinIO, Parquet)',
                  'S3/MinIO storage configuration',
                ]},
                { icon: BarChart3, title: 'Query History', color: 'text-violet-400', items: [
                  'Full execution log with SQL, duration, status, rows',
                  'Query type filter tabs (OLAP, Interactive, etc.)',
                  'Summary stats: success/fail rates, avg duration',
                  'One-click replay to re-execute any past query',
                ]},
                { icon: Activity, title: 'Streaming', color: 'text-emerald-400', items: [
                  'Events tab with real-time event stream',
                  'Connectors tab: Kafka, CDC, Postgres cards',
                  'Pipeline CRUD: create with source, transform SQL, sink',
                  'Buffer utilization gauge (Tokio mpsc bounded channel)',
                ]},
                { icon: Search, title: 'Vector / AI', color: 'text-rose-400', items: [
                  'Semantic search over indexed documents',
                  'Index info: IVF-PQ, HNSW, Brute Force types',
                  'Lance Format tab: Lance vs Parquet comparison',
                  'Embedding pipeline configuration',
                ]},
                { icon: FileText, title: 'Transforms', color: 'text-indigo-400', items: [
                  'Model browser with SQL viewer',
                  'Create transforms with ref()/source() macros',
                  'SQL formatter + compiled preview',
                  'Lineage DAG visualization',
                  'dbt project import (upload models + sources)',
                ]},
                { icon: Clock, title: 'Scheduler / Jobs', color: 'text-orange-400', items: [
                  '8 job types: ETL, MV, SQL, dbt, Pipeline, Compaction, Quality, Snapshot',
                  '18 pre-built templates for common workflows',
                  'ETL pipeline creation with source/sink/write-mode',
                  'Materialized view creation with refresh schedules',
                  'Cron presets + visual cron helper',
                  'Dependency DAG visualization',
                ]},
                { icon: CheckCircle2, title: 'Data Quality', color: 'text-sky-400', items: [
                  'Per-table health checks (null distribution, row counts)',
                  'Configurable alert rules per table',
                  'Aggregate health dashboard: healthy / warning / critical',
                ]},
                { icon: Gauge, title: 'Engine Metrics', color: 'text-yellow-400', items: [
                  'Machine resources: CPU cores, memory, Tokio workers',
                  'Latency distribution + query type breakdown charts',
                  'Success vs failure rates, top 10 slowest queries',
                  'Streaming throughput, scheduler stats, storage breakdown',
                  'Live Flight server status + engine health grid',
                ]},
                { icon: Settings, title: 'Settings', color: 'text-zinc-400', items: [
                  'System info: version, uptime, Arrow/DataFusion versions',
                  'Architecture stack: engine, format, storage, server',
                  'Query router rules: 5 routing paths with AST classification',
                  'Flight/Cluster: real-time gRPC status, cluster topology',
                ]},
                { icon: Info, title: 'About', color: 'text-amber-400', items: [
                  'Architecture overview, design philosophy, crate details',
                  'API reference, roadmap, dependency versions',
                  'Docker credentials for all local services',
                ]},
              ].map(cat => (
                <Card key={cat.title} padding="sm">
                  <div className="flex items-center gap-2 mb-2">
                    <cat.icon className={cn('w-4 h-4', cat.color)} />
                    <h3 className="text-xs font-display font-semibold text-zinc-200">{cat.title}</h3>
                  </div>
                  <ul className="space-y-1">
                    {cat.items.map(item => (
                      <li key={item} className="text-2xs text-zinc-500 flex items-start gap-1.5">
                        <span className={cn('mt-1 w-1 h-1 rounded-full flex-shrink-0', cat.color.replace('text-', 'bg-'))} />
                        {item}
                      </li>
                    ))}
                  </ul>
                </Card>
              ))}
            </div>
          </div>

          {/* Backend API Reference */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-1 flex items-center gap-2">
              <Server className="w-4 h-4 text-zinc-500" /> Backend API Reference (Axum :3000)
            </h2>
            <p className="text-2xs text-zinc-500 mb-4">All REST endpoints served by rustlake-api. JSON request/response. Arrow Flight on :50051 when enabled.</p>
            <div className="space-y-3">
              {API_GROUPS.map(g => (
                <Card key={g.group} padding="sm">
                  <h3 className={cn('text-xs font-display font-semibold mb-2', g.color)}>{g.group}</h3>
                  <div className="space-y-1">
                    {g.endpoints.map(ep => (
                      <div key={ep.path} className="flex items-center gap-2 text-2xs">
                        <Badge className="bg-white/[0.04] text-zinc-500 border-white/[0.06] text-[10px] font-mono px-1.5 py-0 min-w-[52px] text-center">{ep.method}</Badge>
                        <code className="font-mono text-zinc-300 flex-shrink-0">{ep.path}</code>
                        <span className="text-zinc-600 ml-auto flex-shrink-0">{ep.desc}</span>
                      </div>
                    ))}
                  </div>
                </Card>
              ))}
            </div>
          </div>

          {/* Working End-to-End */}
          <Card>
            <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <CheckCircle2 className="w-4 h-4 text-emerald-400" /> Working End-to-End
            </h3>
            <div className="grid grid-cols-2 gap-x-6 gap-y-1">
              {E2E_FEATURES.map(f => (
                <div key={f} className="flex items-start gap-2 py-1">
                  <span className="mt-1 w-1.5 h-1.5 rounded-full bg-emerald-400 flex-shrink-0" />
                  <span className="text-2xs text-zinc-400 leading-relaxed">{f}</span>
                </div>
              ))}
            </div>
          </Card>

          {/* 13 Crate Workspace */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
              <Layers className="w-4 h-4 text-zinc-500" /> 13 Crate Workspace
            </h2>
            <div className="space-y-1">
              {CRATES.map(c => (
                <div key={c.name} className="flex items-start gap-3 px-3 py-2 rounded-lg hover:bg-white/[0.02] transition-colors">
                  <span className={cn('text-xs font-mono font-semibold w-44 flex-shrink-0 pt-0.5', c.color)}>{c.name}</span>
                  <ArrowRight className="w-3 h-3 text-zinc-700 mt-1 flex-shrink-0" />
                  <span className="text-2xs text-zinc-500 leading-relaxed">{c.desc}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Dependency Flow */}
          <Card>
            <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <GitBranch className="w-4 h-4 text-violet-400" /> Dependency Flow
            </h3>
            <pre className="text-2xs font-mono text-zinc-400 leading-relaxed overflow-x-auto">{`core ──→ storage ──→ catalog ──→ format ──→ engine ──→ api/cli
                                    │
core ──→ stream  (format + engine for streaming ingestion)
core ──→ vector  (format + engine, lance for AI workloads)
core ──→ router  (SQL AST analysis → engine dispatch)
core ──→ flight  (engine + Arrow Flight RPC distribution)
core ──→ scheduler (DAG orchestration across engines)
core ──→ transform (SQL compilation, ref resolution, lineage)
core ──→ python  (PyO3 bridge to engine/stream/vector)`}</pre>
          </Card>

          {/* Roadmap */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
              <Milestone className="w-4 h-4 text-zinc-500" /> Roadmap
            </h2>
            <div className="space-y-2">
              {ROADMAP.map(r => (
                <div key={r.phase} className="flex items-start gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                  <div className={cn(
                    'w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 font-mono text-sm font-bold',
                    r.status === 'done' ? 'bg-emerald-400/10 text-emerald-400 border border-emerald-400/20' :
                    r.status === 'current' ? 'bg-amber-400/10 text-amber-400 border border-amber-400/20' :
                    'bg-blue-400/10 text-blue-400 border border-blue-400/20'
                  )}>
                    {r.phase}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-display font-semibold text-zinc-200">{r.title}</span>
                      <Badge className={cn(
                        r.status === 'done' ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' :
                        r.status === 'current' ? 'bg-amber-400/10 text-amber-400 border-amber-400/20' :
                        'bg-blue-400/10 text-blue-400 border-blue-400/20'
                      )}>
                        {r.status === 'done' ? 'Complete' : r.status === 'current' ? 'In Progress' : 'Partial'}
                      </Badge>
                    </div>
                    <p className="text-2xs text-zinc-500 mt-0.5 leading-relaxed">{r.desc}</p>
                    <p className="text-2xs text-zinc-600 mt-0.5 font-mono">Scope: {r.scope}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Configuration */}
          <Card>
            <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <Settings className="w-4 h-4 text-zinc-400" /> Configuration
            </h3>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-2xs font-semibold text-zinc-300 mb-2">TOML config files</p>
                <div className="space-y-1 text-2xs text-zinc-500">
                  <p><code className="text-zinc-400 font-mono">~/.rustlake/config.toml</code> — user-level</p>
                  <p><code className="text-zinc-400 font-mono">/etc/rustlake/config.toml</code> — system-level</p>
                  <p className="mt-2 text-zinc-600">Sections: storage, engine, api, stream, vector, flight</p>
                </div>
              </div>
              <div>
                <p className="text-2xs font-semibold text-zinc-300 mb-2">Environment variable overrides</p>
                <div className="space-y-1 text-2xs font-mono text-zinc-500">
                  <p><code className="text-zinc-400">RUSTLAKE_STORAGE__S3__REGION=us-east-1</code></p>
                  <p><code className="text-zinc-400">RUSTLAKE_FLIGHT__ENABLED=true</code></p>
                  <p><code className="text-zinc-400">RUSTLAKE_FLIGHT__PORT=50051</code></p>
                  <p className="font-sans text-zinc-600 mt-2">Double-underscore nesting. Secrets via env vars only.</p>
                </div>
              </div>
            </div>
          </Card>

          {/* Key Versions */}
          <Card>
            <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <Database className="w-4 h-4 text-blue-400" /> Pinned Dependency Versions
            </h3>
            <p className="text-2xs text-zinc-600 mb-3">Arrow, Parquet, and DataFusion must move together in a single PR. Never drift individually.</p>
            <div className="grid grid-cols-4 gap-x-6 gap-y-1 text-xs">
              {[
                ['arrow', '57'], ['parquet', '57'], ['datafusion', '51'], ['object_store', '0.12'],
                ['iceberg', '0.5'], ['deltalake', '0.24'], ['lance', '0.22'], ['lance-index', '0.22'],
                ['tokio', '1 (full)'], ['axum', '0.8'], ['tonic', '0.14'], ['prost', '0.13'],
                ['serde', '1'], ['tracing', '0.1'], ['pyo3', '0.23'], ['thiserror', '2'],
              ].map(([name, ver]) => (
                <div key={name} className="flex justify-between py-1.5 border-b border-white/[0.03]">
                  <span className="font-mono text-zinc-400">{name}</span>
                  <span className="font-mono text-zinc-200">{ver}</span>
                </div>
              ))}
            </div>
          </Card>

          {/* Quick Start Playground */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
              <Rocket className="w-4 h-4 text-amber-400" /> Quick Start Playground
            </h2>

            {/* Bootstrap Status Panel */}
            <Card padding="md" className="mb-4">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h3 className="text-xs font-display font-semibold text-zinc-200">Bootstrap Status</h3>
                  <p className="text-2xs text-zinc-500 mt-0.5">Auto-connected services and demo data from Docker Compose</p>
                </div>
                <Button
                  size="sm"
                  variant="secondary"
                  onClick={handleBootstrap}
                  disabled={bootstrapping}
                  className="text-2xs"
                >
                  {bootstrapping ? <Loader2 className="w-3 h-3 animate-spin mr-1" /> : <RefreshCw className="w-3 h-3 mr-1" />}
                  {bootstrapping ? 'Bootstrapping...' : 'Re-bootstrap'}
                </Button>
              </div>

              {bootstrap ? (
                <div className="space-y-3">
                  {/* Service status row */}
                  <div className="grid grid-cols-2 gap-3">
                    <div className="p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                      <div className="flex items-center gap-2 mb-1">
                        <div className={cn('w-2 h-2 rounded-full', bootstrap.postgres.available ? 'bg-emerald-400' : 'bg-zinc-600')} />
                        <span className="text-xs font-semibold text-zinc-200">Postgres</span>
                        <Badge className={cn('text-[10px]', bootstrap.postgres.available ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' : 'bg-zinc-800 text-zinc-500 border-zinc-700')}>
                          {bootstrap.postgres.available ? 'Connected' : 'Offline'}
                        </Badge>
                      </div>
                      {bootstrap.postgres.available && (
                        <p className="text-2xs text-zinc-500">{bootstrap.postgres.tables.length} tables discovered</p>
                      )}
                      {bootstrap.postgres.error && (
                        <p className="text-2xs text-zinc-600">{bootstrap.postgres.error}</p>
                      )}
                    </div>
                    <div className="p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                      <div className="flex items-center gap-2 mb-1">
                        <div className={cn('w-2 h-2 rounded-full', bootstrap.minio.available ? 'bg-emerald-400' : 'bg-zinc-600')} />
                        <span className="text-xs font-semibold text-zinc-200">MinIO S3</span>
                        <Badge className={cn('text-[10px]', bootstrap.minio.available ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' : 'bg-zinc-800 text-zinc-500 border-zinc-700')}>
                          {bootstrap.minio.available ? 'Configured' : 'Offline'}
                        </Badge>
                      </div>
                      {bootstrap.minio.error && (
                        <p className="text-2xs text-zinc-600">{bootstrap.minio.error}</p>
                      )}
                    </div>
                  </div>

                  {/* Demo data counts */}
                  <div className="grid grid-cols-4 gap-3">
                    {[
                      { label: 'Tables', value: bootstrap.registered_tables.length, icon: Table2 },
                      { label: 'Jobs', value: bootstrap.demo_jobs, icon: Clock },
                      { label: 'Pipelines', value: bootstrap.demo_pipelines, icon: Activity },
                      { label: 'Transforms', value: bootstrap.demo_transforms, icon: GitBranch },
                    ].map(item => (
                      <div key={item.label} className="p-2.5 rounded-lg bg-white/[0.02] border border-white/[0.04] text-center">
                        <item.icon className="w-3.5 h-3.5 text-zinc-500 mx-auto mb-1" />
                        <p className="text-sm font-mono font-bold text-zinc-200">{item.value}</p>
                        <p className="text-2xs text-zinc-500">{item.label}</p>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="flex items-center justify-center py-6">
                  <Loader2 className="w-4 h-4 animate-spin text-zinc-500" />
                  <span className="text-2xs text-zinc-500 ml-2">Loading status...</span>
                </div>
              )}
            </Card>

            {/* Validation Checklist */}
            <div className="grid grid-cols-3 gap-3">
              {[
                { title: 'SQL Engine', desc: 'Run a cross-table query', icon: Terminal, path: '/sql', color: 'text-amber-400' },
                { title: 'Streaming', desc: 'View live events pipeline', icon: Radio, path: '/streaming', color: 'text-cyan-400' },
                { title: 'Scheduler', desc: 'Run a demo job', icon: Clock, path: '/scheduler', color: 'text-blue-400' },
                { title: 'Transforms', desc: 'Compile a model', icon: GitBranch, path: '/transforms', color: 'text-emerald-400' },
                { title: 'Vector Search', desc: 'Search products by text', icon: Search, path: '/vector', color: 'text-rose-400' },
                { title: 'Data Catalog', desc: 'Browse table metadata', icon: Database, path: '/catalog', color: 'text-violet-400' },
              ].map(item => (
                <Card
                  key={item.title}
                  padding="sm"
                  hover
                  onClick={() => navigate(item.path)}
                  className="cursor-pointer group"
                >
                  <div className="flex items-center gap-2 mb-1.5">
                    <item.icon className={cn('w-4 h-4', item.color)} />
                    <h3 className="text-xs font-display font-semibold text-zinc-200">{item.title}</h3>
                  </div>
                  <p className="text-2xs text-zinc-500">{item.desc}</p>
                  <div className="flex items-center gap-1 mt-2 text-2xs text-zinc-600 group-hover:text-amber-400 transition-colors">
                    <Play className="w-3 h-3" /> Try it
                  </div>
                </Card>
              ))}
            </div>
          </div>

          {/* Local Docker Credentials */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
              <Terminal className="w-4 h-4 text-emerald-400" /> Local Development — Docker Credentials
            </h2>
            <p className="text-xs text-zinc-500 mb-4">
              Connection details for all services when running <code className="text-amber-400/80 bg-amber-400/5 px-1.5 py-0.5 rounded font-mono">docker compose up</code> locally.
            </p>

            {/* Docker commands */}
            <Card className="mb-4">
              <h3 className="text-xs font-display font-semibold text-zinc-300 mb-3 flex items-center gap-2">
                <Terminal className="w-3.5 h-3.5 text-zinc-500" /> Docker Compose Commands
              </h3>
              <div className="space-y-1.5">
                {DOCKER_COMMANDS.map(c => (
                  <div key={c.label} className="flex items-center gap-3 group">
                    <span className="text-2xs text-zinc-500 w-24 flex-shrink-0">{c.label}</span>
                    <code className="flex-1 text-2xs font-mono text-zinc-300 bg-white/[0.03] px-3 py-1.5 rounded-md border border-white/[0.04]">
                      {c.cmd}
                    </code>
                    <button
                      onClick={() => { navigator.clipboard.writeText(c.cmd); toast.success('Copied') }}
                      className="p-1 rounded text-zinc-600 hover:text-zinc-300 opacity-0 group-hover:opacity-100 transition-all"
                    >
                      <Copy className="w-3 h-3" />
                    </button>
                  </div>
                ))}
              </div>
            </Card>

            {/* Service credentials by category */}
            <div className="space-y-3">
              {DOCKER_SERVICES.map(cat => (
                <Card key={cat.category} padding="sm">
                  <div className="flex items-center gap-2 mb-3">
                    <h3 className="text-xs font-display font-semibold text-zinc-200">{cat.category}</h3>
                    {cat.profile && (
                      <Badge className="bg-white/[0.04] text-zinc-500 border-white/[0.06] text-[10px]">
                        --profile {cat.profile}
                      </Badge>
                    )}
                  </div>
                  <div className="space-y-2">
                    {cat.services.map(svc => (
                      <div key={svc.name} className="p-2.5 rounded-lg bg-white/[0.02] border border-white/[0.04] group">
                        <div className="flex items-center justify-between mb-1.5">
                          <div className="flex items-center gap-2">
                            <span className={cn('text-xs font-semibold font-mono', svc.color)}>{svc.name}</span>
                            <Badge className="bg-white/[0.04] text-zinc-500 border-white/[0.06] text-[10px]">
                              :{svc.port}
                            </Badge>
                          </div>
                          <button
                            onClick={() => { navigator.clipboard.writeText(svc.conn); toast.success(`Copied ${svc.name} connection string`) }}
                            className="flex items-center gap-1 px-2 py-0.5 rounded text-2xs text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.04] opacity-0 group-hover:opacity-100 transition-all"
                          >
                            <Copy className="w-3 h-3" /> Copy
                          </button>
                        </div>
                        <code className="text-2xs font-mono text-zinc-400 break-all leading-relaxed">{svc.conn}</code>
                        {(svc as any).user && (
                          <div className="flex items-center gap-3 mt-1.5 text-2xs">
                            <span className="text-zinc-600">user: <span className="text-zinc-400 font-mono">{(svc as any).user}</span></span>
                            <span className="text-zinc-600">pass: <span className="text-zinc-400 font-mono">{(svc as any).pass}</span></span>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </Card>
              ))}
            </div>

            {/* Default credentials note */}
            <div className="mt-4 p-3 rounded-lg bg-amber-400/5 border border-amber-400/10">
              <p className="text-2xs text-amber-400/80 leading-relaxed">
                <strong>Default credentials</strong> — Most services use <code className="font-mono bg-amber-400/10 px-1 rounded">rustlake / rustlake</code> except MinIO which uses <code className="font-mono bg-amber-400/10 px-1 rounded">rustlake / rustlake123</code> and MySQL root which uses <code className="font-mono bg-amber-400/10 px-1 rounded">rootpass</code>. Database name is <code className="font-mono bg-amber-400/10 px-1 rounded">rustlake_demo</code> across all services. Never use these credentials in production.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
