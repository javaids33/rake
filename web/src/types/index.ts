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
  execution_mode?: string
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
  sync_status?: 'syncing' | 'ready' | 'error' | 'cached'
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
  sync_status?: 'syncing' | 'ready' | 'error' | 'configured' | 'cached'
  sync_error?: string
  tables?: string[]
  /** Table type info: table_name → type (e.g. "MATERIALIZED_VIEW", "VIEW") */
  table_types?: Record<string, string>
  /** Table format info: table_name → format (e.g. "iceberg", "delta", "parquet") */
  table_formats?: Record<string, string>
  /** Format breakdown: format → count */
  format_counts?: Record<string, number>
  /** Scan progress fields */
  scan_progress?: string
  scan_detail?: string
  scan_scanned?: number
  scan_total?: number
  scan_found?: number
  scan_elapsed_ms?: number
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

// ── Iceberg Metadata ──────────────────────────────────────────────
export interface IcebergSnapshot {
  snapshot_id: number
  parent_snapshot_id: number | null
  timestamp_ms: number
  operation: string
  summary: Record<string, string>
  manifest_list_path: string
  data_files_count: number
}

export interface IcebergSnapshotResponse {
  table: string
  current_snapshot_id: number | null
  snapshot_count: number
  snapshots: IcebergSnapshot[]
}

export interface IcebergDataFile {
  file_path: string
  file_size: number
  row_count: number
}

export interface IcebergSchemaField {
  id: number
  name: string
  required: boolean
  type: string
}

export interface IcebergSchemaVersion {
  schema_id: number
  fields: IcebergSchemaField[]
}

export interface IcebergSchemasResponse {
  table: string
  current_schema_id: number
  schemas: IcebergSchemaVersion[]
}

export interface IcebergPartitionField {
  source_id: number
  field_id: number
  name: string
  transform: string
}

export interface IcebergPartitionSpec {
  spec_id: number
  fields: IcebergPartitionField[]
}

export interface MaintenanceStatus {
  total_files: number
  avg_file_size_bytes: number
  small_file_count: number
  fragmentation_score: number
  snapshot_count: number
  oldest_snapshot_ms: number | null
  recommendations: string[]
}

// ── Notebooks ────────────────────────────────────────────────────────
export interface NotebookDocument {
  id: string
  name: string
  cells: NotebookCell[]
  createdAt: string
  updatedAt?: string
}

export interface NotebookCell {
  id: string
  type: 'sql' | 'python' | 'markdown' | 'rust'
  source: string
  output: CellOutput | null
  status: 'idle' | 'running' | 'success' | 'error'
  executionOrder: number | null
}

export interface CellOutput {
  type: 'table' | 'text' | 'image' | 'error'
  data: unknown
}

// ── Graph Database ───────────────────────────────────────────────
export interface GraphNode {
  id: string
  label: string
  group: string
  properties: Record<string, string>
  size: number
}

export interface GraphEdge {
  source: string
  target: string
  label: string
  properties: Record<string, string>
}

export interface GraphData {
  nodes: GraphNode[]
  edges: GraphEdge[]
  node_count: number
  edge_count: number
}

export interface Neo4jConnectResponse {
  status: string
  node_count: number
  relationship_count: number
  labels: string[]
  relationship_types: string[]
}

export interface CypherResponse {
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  graph: GraphData
  node_count: number
  relationship_count: number
}

// ── Cost Tracking ────────────────────────────────────────────────
export interface CostRecord {
  timestamp: string
  table: string
  engine: string
  bytes_scanned: number
  requests: number
  cost_usd: number
}

export interface CostSummary {
  total_cost_usd: number
  total_bytes_scanned: number
  total_requests: number
  by_engine: Record<string, { cost_usd: number; queries: number }>
  by_day: Array<{ date: string; cost_usd: number; queries: number }>
}

// ── Materialized Views ──────────────────────────────────────────
export interface MaterializedView {
  name: string
  sql: string
  refresh_interval: string
  last_refresh: string | null
  next_refresh: string | null
  row_count: number
  status: 'active' | 'refreshing' | 'error'
}

// ── Data Contracts ──────────────────────────────────────────────
export interface DataContract {
  id: string
  producer_table: string
  consumer_tables: string[]
  schema_checks: SchemaCheck[]
  freshness_sla_hours: number | null
  quality_gates: string[]
  status: 'passing' | 'failing' | 'unknown'
  last_validated: string | null
  created_at: string
}

// ── Notebook ETL ─────────────────────────────────────────────────
export interface NotebookJob {
  job_id: string
  notebook_id: string
  notebook_name: string
  schedule: string
  enabled: boolean
  last_run: string | null
  last_status: string | null
  last_duration_ms: number | null
  created_at: string
  optimization_level: string
  tags: string[]
}

export interface ExecutionPlan {
  stages: ExecutionStage[]
  total_cells: number
  parallelizable_cells: number
  estimated_duration_ms: number
  optimizations: Optimization[]
}

export interface ExecutionStage {
  stage_id: number
  cell_ids: string[]
  cell_types: string[]
  can_parallelize: boolean
  estimated_ms: number
}

export interface Optimization {
  optimization_type: string
  description: string
  cells_affected: string[]
  estimated_speedup_ms: number
}

// ── Executable Lakehouse ─────────────────────────────────────────
export interface ExecutableTable {
  table_name: string
  table_location: string
  transform: {
    transform_type: string
    source_code: string
    source_hash: string
    binary_path: string | null
    binary_size: number | null
    binary_cached: boolean
  }
  schedule: string | null
  quality_gates: Array<{ gate_type: string; column: string | null; description: string }>
  input_tables: string[]
  status: { state: string; health: string; staleness_hours: number; data_freshness: string }
  history: Array<{ execution_id: string; duration_ms: number; status: string; rows_produced: number | null; bytes_written?: number | null; files_written?: number | null; cost_usd: number; binary_cached: boolean; version?: number }>
  created_at: string
  last_refresh: string | null
  next_refresh: string | null
  estimated_cost_usd: number
  total_executions: number
  total_cost_usd: number
  executions_skipped: number
  cost_saved_usd: number
  versions?: TransformVersion[]
  incremental?: boolean
  watermark_column?: string | null
  last_watermark?: string | null
}

// ── Quality Gate Results ─────────────────────────────────────────
export interface GateResult {
  gate_type: string
  column: string | null
  passed: boolean
  detail: string
}

// ── Code-Data Provenance ─────────────────────────────────────────
export interface TransformVersion {
  version: number
  source_code: string
  source_hash: string
  created_at: string
  created_by: string
  change_description: string
  binary_size_bytes: number | null
  snapshot_ids: number[]
}

export interface DiffLine {
  line_number: number
  change_type: 'added' | 'removed' | 'unchanged'
  content: string
}

export interface RegressionMetric {
  metric_name: string
  old_value: number
  new_value: number
  change_pct: number
  is_regression: boolean
}

export interface RegressionResult {
  has_regression: boolean
  severity: 'none' | 'minor' | 'major' | 'critical'
  metrics: RegressionMetric[]
  recommendation: string
}

export interface ProvenanceEvent {
  timestamp: string
  event_type: 'code_change' | 'execution' | 'regression_detected' | 'rollback'
  version: number
  description: string
  source_hash: string
  duration_ms?: number
  rows_produced?: number | null
  cost_usd?: number
  binary_cached?: boolean
}

export interface ProvenanceChain {
  table_name: string
  total_versions: number
  total_executions: number
  total_snapshots: number
  total_cost_usd: number
  current_hash: string
  timeline: ProvenanceEvent[]
}

export interface IcebergProperties {
  table: string
  properties: Record<string, string>
  format_version: number
  compatible_engines: string[]
}

export interface CostComparison {
  rustlake: CostEstimate
  databricks: CostEstimate
  snowflake: CostEstimate
  lambda: CostEstimate
}

export interface CostEstimate {
  platform: string
  cost_per_execution_usd: number
  monthly_cost_usd: number
  cold_start_ms: number
  execution_ms: number
  always_on: boolean
  cluster_required: boolean
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

// ── A/B Testing ─────────────────────────────────────────────────
export interface ABTestResult {
  table_name: string
  version_a: number
  version_b: number
  version_a_metrics: { version: number; rows_produced: number; duration_ms: number; cost_usd: number; schema_columns: string[] }
  version_b_metrics: { version: number; rows_produced: number; duration_ms: number; cost_usd: number; schema_columns: string[] }
  comparison: {
    row_count_diff: number
    row_count_pct: number
    schema_match: boolean
    columns_added: string[]
    columns_removed: string[]
    duration_diff_ms: number
    cost_diff_usd: number
    data_regressions: RegressionMetric[]
  }
  winner: 'version_a' | 'version_b' | 'tie'
  confidence: number
  recommendation: string
}

// ── Data Contracts (updated) ────────────────────────────────────
export interface SchemaCheck {
  column: string
  data_type: string
  nullable: boolean
  required: boolean
}

export interface ContractViolation {
  check_type: string
  column: string
  expected: string
  actual: string
}

export interface ContractValidationResult {
  contract_id: string
  passed: boolean
  violations: ContractViolation[]
  validated_at: string
}

// ── Transform Marketplace ───────────────────────────────────────
export interface MarketplacePackage {
  id: string
  name: string
  description: string
  author: string
  version: string
  tags: string[]
  category: string
  install_count: number
  published_at: string
  table_definition: ExecutableTable
}

// ── Column Lineage ──────────────────────────────────────────────
export interface ColumnLineageEntry {
  output_column: string
  source_table: string | null
  source_column: string | null
  transform_expression: string
}

export interface ColumnLineageResponse {
  table: string
  transform_type: string
  lineage: ColumnLineageEntry[]
  input_tables: string[]
}

// ── Cascade Replay ──────────────────────────────────────────────
export interface CascadeNodeResult {
  table_name: string
  version: number
  rows: number
  duration_ms: number
  gates_passed: boolean
  gate_results: GateResult[]
  contracts_validated: boolean
  status: string
  error: string | null
}

export interface CascadeReplayResult {
  target: string
  total_tables: number
  total_duration_ms: number
  results: CascadeNodeResult[]
  all_gates_passed: boolean
  all_contracts_valid: boolean
}

// ── Executable Pipelines ────────────────────────────────────────
export interface PipelineStage {
  table_name: string
  depends_on: string[]
  gate_required: boolean
  contract_required: boolean
}

export interface ExecutablePipeline {
  id: string
  name: string
  stages: PipelineStage[]
  status: string
  last_run: string | null
  total_runs: number
}

export interface PipelineStageResult {
  table_name: string
  status: string
  rows: number
  duration_ms: number
  gate_results: GateResult[]
  gates_passed: boolean
  contract_valid: boolean
  error: string | null
}

export interface PipelineRunResult {
  pipeline_id: string
  pipeline_name: string
  status: string
  total_duration_ms: number
  stages: PipelineStageResult[]
}

// ── Time-Travel Debugging ───────────────────────────────────────
export interface ExecutionSummary {
  execution_id: string
  version: number
  status: string
  rows_produced: number | null
  duration_ms: number
  cost_usd: number
  started_at: string
}

export interface DataDiffSummary {
  row_count_diff: number
  row_count_pct: number
  duration_diff_ms: number
  cost_diff_usd: number
  regressions: RegressionMetric[]
}

export interface UpstreamChange {
  table_name: string
  changed_at: string | null
  version_before: number | null
  version_after: number | null
}

export interface DebugResult {
  table_name: string
  bad_execution: ExecutionSummary | null
  good_execution: ExecutionSummary | null
  code_diff: { from_version: number; to_version: number; lines_added: number; lines_removed: number; lines_changed: number; diff_lines: DiffLine[] } | null
  data_diff: DataDiffSummary
  root_cause_lines: string[]
  upstream_changes: UpstreamChange[]
}

// ── Data Products ───────────────────────────────────────────────
export interface DataProduct {
  id: string
  name: string
  table_name: string
  contract_id: string | null
  sla_freshness_hours: number
  sla_quality_score: number
  owner: string
  consumers: string[]
  certification: string
  description: string
}

export interface FreshnessStatus {
  sla_hours: number
  actual_hours: number
  within_sla: boolean
}

export interface AuditCostSummary {
  total_cost_usd: number
  total_saved_usd: number
  total_executions: number
  total_skipped: number
}

export interface DataProductAudit {
  product: DataProduct
  provenance_chain_length: number
  contract_validation: ContractValidationResult | null
  gate_pass_rate: number
  freshness_status: FreshnessStatus
  quality_score: number
  cost_summary: AuditCostSummary
  upstream_chain: string[]
  certification_eligible: boolean
  compliance_issues: string[]
}
