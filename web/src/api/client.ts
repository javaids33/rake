import type {
  SqlResponse, SqlRequest, TableInfo, TableSchemaResponse, TableStatsResponse,
  TablePreviewResponse, QueryHistoryEntry, HealthResponse, SystemInfoResponse,
  SystemResourcesResponse, FlightInfoResponse, FlightStatusResponse, UserTransform, TransformRunResponse,
  LineageResponse, VectorSearchResponse, VectorStatusResponse, StreamStatusResponse,
  StreamingPipeline, ScheduledJob, JobRun, ConnectionEntry, S3Config,
  ClusterInfo, StreamEvent, ExplainResponse, QualityChecksResponse, QualityRule,
  SchedulerDagResponse, DbtProject, DbtModel, DbtSource, DbtRunResponse, DbtRunAllResponse,
  SystemMetricsResponse, QueryEstimateResponse, ConnectionTestRequest, ConnectionTestResponse,
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
export const executeSql = (sql: string) =>
  request<SqlResponse>('/api/v1/sql', { method: 'POST', body: JSON.stringify({ sql } as SqlRequest) })

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
export const addConnection = (c: { name: string; host: string; port: number; database: string; username: string; password?: string }) =>
  request<{ status: string }>('/api/v1/connections', { method: 'POST', body: JSON.stringify(c) })
export const deleteConnection = (id: string) =>
  request<{ status: string }>(`/api/v1/connections/${id}`, { method: 'DELETE' })

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
