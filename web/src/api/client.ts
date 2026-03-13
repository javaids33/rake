import type {
  SqlResponse, SqlRequest, TableInfo, TableSchemaResponse, TableStatsResponse,
  TablePreviewResponse, QueryHistoryEntry, HealthResponse, SystemInfoResponse,
  SystemResourcesResponse, FlightInfoResponse, FlightStatusResponse, UserTransform, TransformRunResponse,
  LineageResponse, VectorSearchResponse, VectorStatusResponse, StreamStatusResponse,
  StreamingPipeline, ScheduledJob, JobRun, ConnectionEntry, S3Config,
  ClusterInfo, StreamEvent, ExplainResponse, QualityChecksResponse, QualityRule,
  SchedulerDagResponse, DbtProject, DbtModel, DbtSource, DbtRunResponse, DbtRunAllResponse,
  SystemMetricsResponse, QueryEstimateResponse, ConnectionTestRequest, ConnectionTestResponse,
  BootstrapStatus, BenchmarkQueriesResponse, BenchmarkRunResponse, BenchmarkResult,
  EnginesResponse, BenchmarkCompareResponse,
} from '../types'

const BASE = ''

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(body.error || `HTTP ${res.status}`)
  }
  return res.json()
}

// Health & System
export const getHealth = () => request<HealthResponse>('/health')
export const getSystemInfo = () => request<SystemInfoResponse>('/api/v1/system/info')
export const getFlightInfo = () => request<FlightInfoResponse>('/api/v1/flight/info')
export const getFlightStatus = () => request<FlightStatusResponse>('/api/v1/flight/status')
export const getSystemResources = () => request<SystemResourcesResponse>('/api/v1/system/resources')

// SQL Execution
export const executeSql = (sql: string, engine = 'auto') =>
  request<SqlResponse>('/api/v1/sql', { method: 'POST', body: JSON.stringify({ sql, engine } as SqlRequest) })

// Tables
export const getTables = () => request<{ tables: TableInfo[] }>('/api/v1/tables')
export const getTableSchema = (name: string) => request<TableSchemaResponse>(`/api/v1/tables/${name}/schema`)
export const getTableStats = (name: string) => request<TableStatsResponse>(`/api/v1/tables/${name}/stats`)
export const getTablePreview = (name: string) => request<TablePreviewResponse>(`/api/v1/tables/${name}/preview`)
export const registerTable = (path: string, name?: string, format?: string) =>
  request<{ status: string; table: string }>('/api/v1/tables/register', {
    method: 'POST', body: JSON.stringify({ path, name, format }),
  })
export const deregisterTable = (name: string) =>
  request<{ status: string }>(`/api/v1/tables/${name}`, { method: 'DELETE' })
export const getTableDescription = (name: string) =>
  request<{ table_description: string; column_descriptions: Record<string, string> }>(`/api/v1/tables/${name}/description`)
export const updateTableDescription = (name: string, body: { table_description?: string; column_descriptions?: Record<string, string> }) =>
  request<{ status: string }>(`/api/v1/tables/${name}/description`, { method: 'PUT', body: JSON.stringify(body) })

// Query History
export const getQueryHistory = (limit = 100) =>
  request<{ history: QueryHistoryEntry[] }>(`/api/v1/query/history?limit=${limit}`)

// Transforms
export const getTransforms = () => request<{ transforms: UserTransform[] }>('/api/v1/transforms')
export const createTransform = (t: Partial<UserTransform>) =>
  request<{ status: string }>('/api/v1/transforms', { method: 'POST', body: JSON.stringify(t) })
export const deleteTransform = (name: string) =>
  request<{ status: string }>(`/api/v1/transforms/${name}`, { method: 'DELETE' })
export const runTransform = (name: string) =>
  request<TransformRunResponse>(`/api/v1/transforms/${name}/run`, { method: 'POST' })

// Lineage
export const getLineage = () => request<LineageResponse>('/api/v1/lineage')

// Vector
export const vectorSearch = (query: string, k = 10) =>
  request<VectorSearchResponse>('/api/v1/vector/search', { method: 'POST', body: JSON.stringify({ query, k }) })
export const vectorIndex = (documents: Array<{ id: string; text: string; metadata?: Record<string, unknown> }>) =>
  request<{ status: string }>('/api/v1/vector/index', { method: 'POST', body: JSON.stringify({ documents }) })
export const getVectorStatus = () => request<VectorStatusResponse>('/api/v1/vector/status')

// Streaming
export const getStreamStatus = () => request<StreamStatusResponse>('/api/v1/stream/status')
export const getStreamEvents = (limit = 50) =>
  request<{ events: StreamEvent[] }>(`/api/v1/stream/events?limit=${limit}`)
export const ingestStream = (count = 100) =>
  request<{ status: string; events_generated: number }>('/api/v1/stream/ingest', { method: 'POST', body: JSON.stringify({ count }) })

// Streaming Pipelines
export const getPipelines = () => request<{ pipelines: StreamingPipeline[] }>('/api/v1/streaming/pipelines')
export const createPipeline = (p: Partial<StreamingPipeline>) =>
  request<{ status: string }>('/api/v1/streaming/pipelines', { method: 'POST', body: JSON.stringify(p) })
export const deletePipeline = (id: string) =>
  request<{ status: string }>(`/api/v1/streaming/pipelines/${id}`, { method: 'DELETE' })
export const startPipeline = (id: string) =>
  request<{ status: string; id: string; source_type: string }>(`/api/v1/streaming/pipelines/${id}/start`, { method: 'POST' })
export const stopPipeline = (id: string) =>
  request<{ status: string; id: string }>(`/api/v1/streaming/pipelines/${id}/stop`, { method: 'POST' })

// Scheduler
export const getSchedules = () => request<{ schedules: ScheduledJob[] }>('/api/v1/schedules')
export const createSchedule = (s: Partial<ScheduledJob>) =>
  request<{ status: string }>('/api/v1/schedules', { method: 'POST', body: JSON.stringify(s) })
export const updateSchedule = (id: string, s: Partial<ScheduledJob>) =>
  request<{ status: string }>(`/api/v1/schedules/${id}`, { method: 'PUT', body: JSON.stringify(s) })
export const deleteSchedule = (id: string) =>
  request<{ status: string }>(`/api/v1/schedules/${id}`, { method: 'DELETE' })
export const runSchedule = (id: string) =>
  request<{ status: string }>(`/api/v1/schedules/${id}/run`, { method: 'POST' })
export const getScheduleRuns = () => request<{ runs: JobRun[] }>('/api/v1/schedules/runs')
export const getClusters = () => request<ClusterInfo>('/api/v1/clusters')

// Connections
export const getConnections = () => request<{ connections: ConnectionEntry[] }>('/api/v1/connections')
export const addConnection = (c: {
  name: string; conn_type?: string; host: string; port: number; database: string; username: string; password?: string;
  auth_method?: string; connection_string?: string; aws_access_key?: string; aws_secret_key?: string; aws_session_token?: string; aws_region?: string;
}) =>
  request<{ status: string; sync_status: string; id: string; name: string; tables: string[] }>('/api/v1/connections', { method: 'POST', body: JSON.stringify(c) })
export const updateConnection = (id: string, c: Parameters<typeof addConnection>[0]) =>
  request<{ status: string; sync_status: string; id: string; name: string; tables: string[] }>(`/api/v1/connections/${id}`, { method: 'PUT', body: JSON.stringify(c) })
export const deleteConnection = (id: string) =>
  request<{ status: string }>(`/api/v1/connections/${id}`, { method: 'DELETE' })
export const getConnectionStatus = (id: string) =>
  request<{ id: string; sync_status: string; sync_error: string | null; tables: string[]; table_count: number }>(`/api/v1/connections/${id}/status`)

// Upload
export const uploadFile = (file: File) => {
  const fd = new FormData()
  fd.append('file', file)
  return fetch('/api/v1/upload', { method: 'POST', body: fd }).then(r => r.json())
}

// S3 Storage
export const getS3Configs = () => request<{ configs: S3Config[] }>('/api/v1/storage/s3')
export const addS3Config = (c: Partial<S3Config> & { secret_key: string }) =>
  request<{ status: string }>('/api/v1/storage/s3', { method: 'POST', body: JSON.stringify(c) })
export const deleteS3Config = (name: string) =>
  request<{ status: string }>(`/api/v1/storage/s3/${name}`, { method: 'DELETE' })

// EXPLAIN
export const explainSql = (sql: string) =>
  request<ExplainResponse>('/api/v1/sql/explain', { method: 'POST', body: JSON.stringify({ sql }) })

// Compare SQL across all engines
export interface SqlCompareResponse {
  query_id: string
  sql: string
  datafusion: { duration_ms: number; row_count: number; status: string; error?: string }
  duckdb: { duration_ms: number; row_count: number; status: string; error?: string }
  polars: { duration_ms: number; row_count: number; status: string; error?: string }
  speedup: number
  winner: string
}
export const compareSql = (sql: string) =>
  request<SqlCompareResponse>('/api/v1/sql/compare', { method: 'POST', body: JSON.stringify({ sql }) })

// Quality
export const getQualityChecks = () => request<QualityChecksResponse>('/api/v1/quality/checks')
export const getQualityRules = () => request<{ rules: QualityRule[] }>('/api/v1/quality/rules')
export const createQualityRule = (rule: Partial<QualityRule>) =>
  request<{ status: string; rule: QualityRule }>('/api/v1/quality/rules', { method: 'POST', body: JSON.stringify(rule) })
export const deleteQualityRule = (id: string) =>
  request<{ status: string }>(`/api/v1/quality/rules/${id}`, { method: 'DELETE' })

// Scheduler DAG
export const getSchedulerDag = () => request<SchedulerDagResponse>('/api/v1/schedules/dag')

// dbt
export const uploadDbtProject = (project: DbtProject) =>
  request<{ status: string; models: number; sources: number }>('/api/v1/dbt/upload', { method: 'POST', body: JSON.stringify(project) })
export const getDbtProject = () => request<{ name: string; version: string; model_count: number; source_count: number; uploaded_at: string }>('/api/v1/dbt/project')
export const getDbtModels = () => request<{ models: DbtModel[]; sources: DbtSource[] }>('/api/v1/dbt/models')
export const runDbtModel = (name: string) =>
  request<DbtRunResponse>(`/api/v1/dbt/run/${name}`, { method: 'POST' })
export const runAllDbtModels = () =>
  request<DbtRunAllResponse>('/api/v1/dbt/run-all', { method: 'POST' })

// System Metrics (real-time)
export const getSystemMetrics = () => request<SystemMetricsResponse>('/api/v1/system/metrics')

// Query Cost Estimation
export const estimateQuery = (sql: string) =>
  request<QueryEstimateResponse>('/api/v1/sql/estimate', { method: 'POST', body: JSON.stringify({ sql }) })

// Connection Test
export const testConnection = (params: ConnectionTestRequest) =>
  request<ConnectionTestResponse>('/api/v1/connections/test', { method: 'POST', body: JSON.stringify(params) })

// Bootstrap
export const getBootstrapStatus = () => request<BootstrapStatus>('/api/v1/bootstrap/status')
export const runBootstrap = () => request<Record<string, unknown>>('/api/v1/bootstrap', { method: 'POST' })

// Benchmarks
export const getBenchmarkQueries = () => request<BenchmarkQueriesResponse>('/api/v1/benchmarks/queries')
export const runBenchmark = (queryId: string) =>
  request<BenchmarkRunResponse>('/api/v1/benchmarks/run', { method: 'POST', body: JSON.stringify({ query_id: queryId }) })
export const getBenchmarkResults = () => request<{ results: BenchmarkResult[] }>('/api/v1/benchmarks/results')
export const compareBenchmark = (queryId: string) =>
  request<BenchmarkCompareResponse>('/api/v1/benchmarks/compare', { method: 'POST', body: JSON.stringify({ query_id: queryId }) })

// Engines
export const getEngines = () => request<EnginesResponse>('/api/v1/engines')

// Providers
export const getProviders = () => request<{ providers: Array<{ name: string; enabled: boolean; connections: number; tables: number }> }>('/api/v1/providers')

// Connections Import/Export
export const importConnections = (payload: { connections?: any[]; s3_configs?: any[] }) =>
  request<{ imported: { connections: any[]; s3_configs: any[] }; total: number; errors: string[] }>('/api/v1/connections/import', { method: 'POST', body: JSON.stringify(payload) })

export const exportConnections = () =>
  request<{ connections: any[]; s3_configs: any[] }>('/api/v1/connections/export')

// Trino (DuckDB-cached catalog browsing)
export const trinoBrowse = (connId: string) =>
  request<{ catalogs: Array<{ name: string; schemas: Array<{ name: string; tables: string[] }> }>; cached_at: string | null; total_tables: number }>(`/api/v1/trino/${connId}/browse`)
export const trinoColumns = (connId: string, catalog: string, schema: string, table: string) =>
  request<{ columns: Array<{ name: string; data_type: string; nullable: boolean; ordinal: number }>; table: string }>(`/api/v1/trino/${connId}/columns?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`)
export const trinoPreview = (connId: string, catalog: string, schema: string, table: string) =>
  request<{ columns: string[]; rows: Record<string, unknown>[]; row_count: number; duration_ms: number; engine: string }>(`/api/v1/trino/${connId}/preview?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`)
export const trinoQuery = (connId: string, sql: string, catalog: string) =>
  request<{ columns: string[]; rows: Record<string, unknown>[]; row_count: number; duration_ms: number; engine: string }>(`/api/v1/trino/${connId}/query`, { method: 'POST', body: JSON.stringify({ sql, catalog }) })
export const trinoRefresh = (connId: string) =>
  request<{ status: string; tables_cached: number }>(`/api/v1/trino/${connId}/refresh`, { method: 'POST' })
export const trinoStats = (connId: string) =>
  request<{ schemas_cached: number; tables_cached: number; columns_cached: number; last_refresh: string | null }>(`/api/v1/trino/${connId}/stats`)

// Migration - Iceberg focused
export interface MigrationTable {
  conn_id: string; catalog: string; schema_name: string; table_name: string
  format: string; location: string | null; metastore_uri: string | null
  column_count: number; row_count: number | null
  registered_in_rake: boolean; rake_table_name: string | null
  status: string; error: string | null
}
export interface EngineResultM { engine: string; duration_ms: number; row_count: number; status: string; error?: string; path?: string }
export interface MigrationComparison {
  id: string; sql: string; results: EngineResultM[]; winner: string; speedup: number; data_match: boolean; timestamp: string
}
export interface MigrationWarehouse {
  catalog: string; warehouse: string; bucket: string
}
export const migrationDiscover = (connId: string) =>
  request<{ tables: MigrationTable[]; table_count: number; iceberg_catalogs: string[]; warehouses: MigrationWarehouse[]; required_buckets: string[]; phase: string }>(`/api/v1/migration/${connId}/discover`, { method: 'POST' })
export const migrationRegister = (connId: string, tables?: string[]) =>
  request<{ registered: number; total: number; errors: Array<{ table: string; error: string }> }>(`/api/v1/migration/${connId}/register`, { method: 'POST', body: JSON.stringify({ tables }) })
export const migrationCompare = (connId: string, sql: string, localSql?: string, useNativeS3?: boolean) =>
  request<MigrationComparison>('/api/v1/migration/compare', { method: 'POST', body: JSON.stringify({ conn_id: connId, sql, local_sql: localSql, use_native_s3: useNativeS3 }) })
export const migrationCredentials = (bucket: string, accessKey: string, secretKey: string, region?: string) =>
  request<{ status: string }>('/api/v1/migration/credentials', { method: 'POST', body: JSON.stringify({ bucket, access_key: accessKey, secret_key: secretKey, region: region || 'us-east-1' }) })
export const getMigrationTables = (connId: string) =>
  request<{ tables: MigrationTable[] }>(`/api/v1/migration/${connId}/tables`)
export const getMigrationComparisons = () =>
  request<{ comparisons: MigrationComparison[] }>('/api/v1/migration/comparisons')
