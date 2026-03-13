export interface SqlResponse {
  query_id: string
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  query_type: string
  duration_ms: number
  parse_ms?: number
  exec_ms?: number
  engine: string
}

export interface SqlRequest {
  sql: string
  engine?: string
}

export interface ColumnSchema {
  name: string
  data_type: string
  nullable: boolean
}

export interface TableInfo {
  name: string
  columns?: ColumnSchema[]
}

export interface TableSchemaResponse {
  table: string
  columns: ColumnSchema[]
}

export interface TableStatsResponse {
  table: string
  row_count: number
  column_count: number
  columns: ColumnStat[]
}

export interface ColumnStat {
  name: string
  data_type: string
  min?: unknown
  max?: unknown
  null_count: number
  distinct_count?: number
}

export interface TablePreviewResponse {
  table: string
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
}

export interface QueryHistoryEntry {
  query_id: string
  sql: string
  query_type: string
  row_count: number
  duration_ms: number
  timestamp: string
  status: string
  error?: string
  engine?: string
}

export interface HealthResponse {
  status: string
  version: string
  engine: string
}

export interface SystemInfoResponse {
  platform: string
  version: string
  uptime_seconds: number
  query_count: number
  registered_tables: number
  arrow_version: string
  datafusion_version: string
}

export interface FlightInfoResponse {
  protocol: string
  host: string
  port: number
  status: string
  max_message_size: number
  capabilities: string[]
  arrow_version: string
  active_clients: number
  queries_served: number
  supported_clients: string[]
}

export interface FlightStatusResponse {
  enabled: boolean
  running: boolean
  host: string
  port: number
  active_connections: number
  queries_served: number
}

export interface UserTransform {
  name: string
  sql: string
  depends_on: string[]
  materialization: string
  description: string
  created_at: string
}

export interface TransformRunResponse {
  transform: string
  compiled_sql: string
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  duration_ms: number
}

export interface LineageNode {
  id: string
  label: string
  type: string
}

export interface LineageEdge {
  source: string
  target: string
}

export interface LineageResponse {
  nodes: LineageNode[]
  edges: LineageEdge[]
}

export interface VectorSearchResult {
  id: string
  text: string
  metadata?: Record<string, unknown>
  similarity_score: number
}

export interface VectorSearchResponse {
  query: string
  results: VectorSearchResult[]
  result_count: number
  duration_ms: number
}

export interface VectorStatusResponse {
  status: string
  document_count: number
  dimensions: number
  model: string
  index_type: string
}

export interface StreamEvent {
  event_type: string
  user_id: string
  timestamp: string
  data: Record<string, unknown>
}

export interface StreamingMetrics {
  events_ingested: number
  bytes_ingested: number
  events_per_sec: number
  avg_latency_ms: number
  active_streams: number
  uptime_seconds: number
}

export interface StreamStatusResponse {
  status: string
  metrics: StreamingMetrics
  buffer_size: number
}

export interface StreamingPipeline {
  id: string
  name: string
  source_type: string
  source_config: Record<string, unknown>
  transform_sql?: string
  sink_table: string
  status: string
  events_processed: number
  created_at: string
}

export interface ScheduledJob {
  id: string
  name: string
  job_type: string
  cron: string
  target: string
  enabled: boolean
  last_run?: string
  next_run?: string
  engine?: string
  trigger_type: string
  event_config?: Record<string, unknown>
  cluster?: string
  timeout_seconds?: number
  retries: number
  tags: string[]
}

export interface JobRun {
  id: string
  job_id: string
  job_name: string
  status: string
  started_at: string
  completed_at?: string
  duration_ms?: number
  result?: string
  error?: string
}

export interface ConnectionEntry {
  id: string
  name: string
  conn_type: string
  host: string
  port: number
  database: string
  username: string
  status: string
  tables: string[]
  created_at: string
  mode: 'federated' | 'snapshot'
  sync_status?: 'syncing' | 'ready' | 'error'
  sync_error?: string
  auth_method?: 'scram' | 'aws_iam' | 'connection_string'
  connection_string?: string
  aws_access_key?: string
  aws_secret_key?: string
  aws_session_token?: string
  aws_region?: string
}

export interface ProviderInfo {
  name: string
  enabled: boolean
  connections: number
  tables: number
}

export interface S3Config {
  name: string
  endpoint: string
  access_key: string
  bucket: string
  region: string
  status: string
  created_at: string
  sync_status?: 'syncing' | 'ready' | 'error' | 'configured'
  sync_error?: string
  tables?: string[]
}

export interface ClusterInfo {
  clusters: Array<{
    id: string
    name: string
    status: string
    workers: number
    cpu_cores: number
    memory_gb: number
    created_at: string
  }>
}

export interface SystemResourcesResponse {
  cpu_cores: number
  total_memory_bytes: number
  engine_memory_limit: number | null
  batch_size: number
  target_partitions: number
  tokio_workers: number
  distributed_mode: boolean
  flight_status: string
  node_role: string
}

export interface PlanNode {
  id: number
  operator: string
  detail: string
  estimated_rows?: number
  parent?: number
  depth: number
}

export interface ExplainResponse {
  sql: string
  logical_plan: string
  physical_plan: string
  nodes: PlanNode[]
}

export interface ColumnNullInfo {
  name: string
  data_type: string
  null_count: number
  total_rows: number
  null_pct: number
}

export interface TableQualityCheck {
  table: string
  row_count: number
  column_count: number
  null_percentages: ColumnNullInfo[]
  health: string
  issues: string[]
  checked_at: string
}

export interface QualityChecksResponse {
  checks: TableQualityCheck[]
  healthy_count: number
  warning_count: number
  critical_count: number
  total_tables: number
}

export interface QualityRule {
  id: string
  table_name: string
  rule_type: string
  threshold: number
  enabled: boolean
  created_at: string
}

export interface DagNode {
  id: string
  name: string
  job_type: string
  status: string
  cron: string
  enabled: boolean
  last_run?: string
}

export interface DagEdge {
  from: string
  to: string
  label?: string
}

export interface SchedulerDagResponse {
  nodes: DagNode[]
  edges: DagEdge[]
}

export interface DbtModel {
  name: string
  sql: string
  depends_on: string[]
  materialization: string
  description: string
  schema_name?: string
  tags: string[]
}

export interface DbtSource {
  name: string
  schema_name: string
  tables: string[]
}

export interface DbtProject {
  name: string
  version: string
  models: DbtModel[]
  sources: DbtSource[]
  uploaded_at: string
}

export interface DbtRunResponse {
  model: string
  status: string
  compiled_sql: string
  row_count: number
  duration_ms: number
  error?: string
}

export interface DbtRunAllResponse {
  results: DbtRunResponse[]
  total_duration_ms: number
  success_count: number
  failure_count: number
}

export type ChartType = 'bar' | 'line' | 'scatter' | 'pie' | 'area'

export interface EditorTab {
  id: string
  name: string
  sql: string
}

// ── System Metrics ──────────────────────────────────────────────
export interface SystemMetricsResponse {
  cpu_usage_percent: number
  memory_used_bytes: number
  memory_total_bytes: number
  memory_usage_percent: number
  disk_used_bytes: number
  disk_total_bytes: number
  disk_usage_percent: number
  load_avg_1m: number
  load_avg_5m: number
  active_queries: number
  total_queries: number
  queries_per_second: number
  uptime_seconds: number
}

// ── Query Cost Estimation ───────────────────────────────────────
export interface QueryEstimateResponse {
  sql: string
  estimated_rows: number
  estimated_bytes: number
  estimated_scan_size: string
  partitions: number
  cost_rating: 'low' | 'medium' | 'high'
  tables_referenced: string[]
  notes: string[]
}

// ── Connection Test ─────────────────────────────────────────────
export interface ConnectionTestRequest {
  conn_type: string
  host: string
  port?: number
  database?: string
  username?: string
  password?: string
  auth_method?: string
  connection_string?: string
  aws_access_key?: string
  aws_secret_key?: string
  aws_session_token?: string
  aws_region?: string
}

export interface ConnectionTestResponse {
  success: boolean
  message: string
  latency_ms?: number
  server_version?: string
  tables_found?: number
  validation_level: string
  checks: ConnectionCheck[]
}

export interface ConnectionCheck {
  name: string
  passed: boolean
  detail: string
}

// ── Benchmarks ──────────────────────────────────────────────────
export interface BenchmarkQuery {
  id: string
  name: string
  description: string
  sql: string
  category: string
}

export interface BenchmarkResult {
  query_id: string
  query_name: string
  duration_ms: number
  row_count: number
  status: string
  error?: string
  timestamp: string
  engine?: string
}

export interface BenchmarkQueriesResponse {
  queries: BenchmarkQuery[]
  scale_factor: string
  tables: Record<string, number>
}

export interface BenchmarkRunResponse {
  query_id: string
  query_name: string
  sql: string
  duration_ms: number
  row_count: number
  columns: string[]
  rows: Record<string, unknown>[]
  status: string
  engine?: string
}

// ── Bootstrap ───────────────────────────────────────────────────
export interface ServiceStatus {
  available: boolean
  tables: string[]
  error?: string
}

export interface BootstrapStatus {
  postgres: ServiceStatus
  mysql: ServiceStatus
  mongodb: ServiceStatus
  minio: ServiceStatus
  demo_jobs: number
  demo_pipelines: number
  demo_transforms: number
  registered_tables: string[]
}

// ── Alert / SLA Configuration ───────────────────────────────────
export interface AlertRule {
  id: string
  name: string
  type: 'freshness' | 'query_duration' | 'error_rate' | 'row_count' | 'custom'
  target: string
  condition: string
  threshold: number
  channel: string
  enabled: boolean
}

// ── Engine Info ──────────────────────────────────────────────────
export interface EngineInfo {
  name: string
  version: string
  status: string
  default: boolean
  description: string
}

export interface EnginesResponse {
  engines: EngineInfo[]
}

export interface BenchmarkCompareResponse {
  query_id: string
  query_name: string
  datafusion: { duration_ms: number; row_count: number; status: string; error?: string }
  duckdb: { duration_ms: number; row_count: number; status: string; error?: string }
  polars?: { duration_ms: number; row_count: number; status: string; error?: string }
  speedup: number
  winner: string
}
