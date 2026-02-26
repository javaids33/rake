import { useState, useEffect } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { StatusDot } from '../components/ui/StatusDot'
import { Tooltip as UiTooltip } from '../components/ui/Tooltip'
import { cn, formatDuration, formatNumber, formatBytes, formatRelativeTime, inferFormat } from '../lib/utils'
import {
  getSystemInfo, getSystemResources, getFlightInfo, getQueryHistory,
  getStreamStatus, getPipelines, getSchedules, getScheduleRuns,
  getTables, getConnections, getVectorStatus, getSystemMetrics,
} from '../api/client'
import type {
  SystemInfoResponse, SystemResourcesResponse, FlightInfoResponse,
  QueryHistoryEntry, StreamStatusResponse, StreamingPipeline,
  ScheduledJob, JobRun, ConnectionEntry, VectorStatusResponse, TableInfo,
  SystemMetricsResponse,
} from '../types'
import {
  BarChart, Bar, PieChart, Pie, Cell, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import {
  Gauge, Activity, BarChart3, Radio, Clock, Database,
  Cpu, HardDrive, Zap, Server, Layers, Network,
  CheckCircle2, XCircle, Timer, ArrowUpRight,
} from 'lucide-react'

// ─────────────────────────────────────────────────
// Theme constants
// ─────────────────────────────────────────────────
const COLORS = ['#fbbf24', '#22d3ee', '#10b981', '#f43f5e', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16']

const tooltipStyle = {
  contentStyle: { background: '#0d1730', border: '1px solid rgba(251,191,36,0.1)', borderRadius: 10, fontSize: 12, backdropFilter: 'blur(12px)' },
  itemStyle: { color: '#94a3b8' },
  labelStyle: { color: '#e2e8f0', fontWeight: 600 as const },
}

const axisStyle = { fontSize: 10, fill: '#475569' }
const gridStroke = 'rgba(251,191,36,0.04)'

// ─────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────
function bucketLatency(ms: number): string {
  if (ms < 100) return '<100ms'
  if (ms < 500) return '100-500ms'
  if (ms < 1000) return '500ms-1s'
  if (ms < 5000) return '1-5s'
  return '5s+'
}

function percentile(sorted: number[], p: number): number {
  if (!sorted.length) return 0
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)]
}

// ─────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────
export function EngineMetrics() {
  const [tab, setTab] = useState('overview')

  // Data stores
  const [systemInfo, setSystemInfo] = useState<SystemInfoResponse | null>(null)
  const [resources, setResources] = useState<SystemResourcesResponse | null>(null)
  const [flightInfo, setFlightInfo] = useState<FlightInfoResponse | null>(null)
  const [queryHistory, setQueryHistory] = useState<QueryHistoryEntry[]>([])
  const [streamStatus, setStreamStatus] = useState<StreamStatusResponse | null>(null)
  const [pipelines, setPipelines] = useState<StreamingPipeline[]>([])
  const [schedules, setSchedules] = useState<ScheduledJob[]>([])
  const [scheduleRuns, setScheduleRuns] = useState<JobRun[]>([])
  const [tables, setTables] = useState<TableInfo[]>([])
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [vectorStatus, setVectorStatus] = useState<VectorStatusResponse | null>(null)
  const [metrics, setMetrics] = useState<SystemMetricsResponse | null>(null)

  useEffect(() => {
    const load = () => getSystemMetrics().then(setMetrics).catch(() => {})
    load()
    const interval = setInterval(load, 5000)
    return () => clearInterval(interval)
  }, [])

  useEffect(() => {
    const calls = [
      getSystemInfo().then(setSystemInfo).catch(() => {}),
      getSystemResources().then(setResources).catch(() => {}),
      getFlightInfo().then(setFlightInfo).catch(() => {}),
      getQueryHistory(200).then(r => setQueryHistory(r.history || [])).catch(() => {}),
      getStreamStatus().then(setStreamStatus).catch(() => {}),
      getPipelines().then(r => setPipelines(r.pipelines || [])).catch(() => {}),
      getSchedules().then(r => setSchedules(r.schedules || [])).catch(() => {}),
      getScheduleRuns().then(r => setScheduleRuns(r.runs || [])).catch(() => {}),
      getTables().then(r => setTables(r.tables || [])).catch(() => {}),
      getConnections().then(r => setConnections(r.connections || [])).catch(() => {}),
      getVectorStatus().then(setVectorStatus).catch(() => {}),
    ]
    Promise.allSettled(calls)
  }, [])

  // ── Derived data ────────────────────────────────
  const uptime = systemInfo ? formatDuration(systemInfo.uptime_seconds * 1000) : '--'
  const totalQueries = systemInfo?.query_count ?? queryHistory.length
  const durations = queryHistory.map(q => q.duration_ms).sort((a, b) => a - b)
  const avgDuration = durations.length ? durations.reduce((s, d) => s + d, 0) / durations.length : 0
  const p50 = percentile(durations, 50)
  const p95 = percentile(durations, 95)

  // Latency bucket data
  const bucketCounts: Record<string, number> = { '<100ms': 0, '100-500ms': 0, '500ms-1s': 0, '1-5s': 0, '5s+': 0 }
  queryHistory.forEach(q => { bucketCounts[bucketLatency(q.duration_ms)]++ })
  const latencyData = Object.entries(bucketCounts).map(([name, count]) => ({ name, count }))

  // Query type breakdown
  const typeCounts: Record<string, number> = {}
  queryHistory.forEach(q => { typeCounts[q.query_type] = (typeCounts[q.query_type] || 0) + 1 })
  const typeData = Object.entries(typeCounts).map(([name, count]) => ({ name, count }))

  // Success / failure
  const successCount = queryHistory.filter(q => q.status === 'success').length
  const errorCount = queryHistory.filter(q => q.status === 'error').length
  const successPieData = [
    { name: 'Success', value: successCount },
    { name: 'Error', value: errorCount },
  ]

  // Top 10 slowest
  const slowest = [...queryHistory].sort((a, b) => b.duration_ms - a.duration_ms).slice(0, 10)

  // Schedule run stats
  const runSuccessCount = scheduleRuns.filter(r => r.status === 'success').length
  const runFailedCount = scheduleRuns.filter(r => r.status === 'failed').length
  const runRunningCount = scheduleRuns.filter(r => r.status === 'running').length
  const runPieData = [
    { name: 'Success', value: runSuccessCount },
    { name: 'Failed', value: runFailedCount },
    { name: 'Running', value: runRunningCount },
  ].filter(d => d.value > 0)

  const jobTypeCounts: Record<string, number> = {}
  schedules.forEach(s => { jobTypeCounts[s.job_type] = (jobTypeCounts[s.job_type] || 0) + 1 })
  const jobTypeData = Object.entries(jobTypeCounts).map(([name, count]) => ({ name, count }))

  const avgRunDuration = scheduleRuns.length
    ? scheduleRuns.filter(r => r.duration_ms).reduce((s, r) => s + (r.duration_ms || 0), 0) / scheduleRuns.filter(r => r.duration_ms).length || 0
    : 0
  const runSuccessRate = scheduleRuns.length ? ((runSuccessCount / scheduleRuns.length) * 100).toFixed(1) : '0'

  // Format distribution
  const formatCounts: Record<string, number> = {}
  tables.forEach(t => {
    const { format } = inferFormat(t.name)
    formatCounts[format] = (formatCounts[format] || 0) + 1
  })
  const formatPieData = Object.entries(formatCounts).map(([name, value]) => ({ name, value }))

  // Connection grouped tables
  const formatGroups: Record<string, string[]> = {}
  tables.forEach(t => {
    const { format } = inferFormat(t.name)
    if (!formatGroups[format]) formatGroups[format] = []
    formatGroups[format].push(t.name)
  })

  // ── Tabs config ─────────────────────────────────
  const tabConfig = [
    { id: 'overview', label: 'Overview', icon: <Activity className="w-3.5 h-3.5" /> },
    { id: 'queries', label: 'Query Performance', icon: <BarChart3 className="w-3.5 h-3.5" /> },
    { id: 'streaming', label: 'Streaming & CDC', icon: <Radio className="w-3.5 h-3.5" /> },
    { id: 'scheduler', label: 'Scheduler & Jobs', icon: <Clock className="w-3.5 h-3.5" /> },
    { id: 'storage', label: 'Storage & Catalog', icon: <Database className="w-3.5 h-3.5" /> },
  ]

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6 animate-fade-in">
      {/* ── Header ────────────────────────────────── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-emerald-400/10 border border-emerald-400/20 flex items-center justify-center">
            <Gauge className="w-4.5 h-4.5 text-emerald-400" />
          </div>
          <div>
            <h1 className="text-base font-display font-bold text-zinc-100">Engine Metrics</h1>
            <p className="text-2xs text-zinc-500">Cross-engine performance, resource utilization, and system health</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Badge dot dotColor={resources?.distributed_mode ? 'bg-cyan-400' : 'bg-amber-400'}>
            {resources?.distributed_mode ? 'Distributed' : 'Single Node'}
          </Badge>
          {resources && (
            <Badge className="font-mono">
              {resources.cpu_cores} cores / {formatBytes(resources.total_memory_bytes)}
            </Badge>
          )}
        </div>
      </div>

      {/* ── Tabs ──────────────────────────────────── */}
      <Tabs tabs={tabConfig} active={tab} onChange={setTab} />

      {/* ── Tab content ───────────────────────────── */}
      {tab === 'overview' && <OverviewTab
        resources={resources} systemInfo={systemInfo} streamStatus={streamStatus}
        vectorStatus={vectorStatus} flightInfo={flightInfo} tables={tables}
        connections={connections} totalQueries={totalQueries} uptime={uptime}
        metrics={metrics}
      />}
      {tab === 'queries' && <QueryPerformanceTab
        history={queryHistory} latencyData={latencyData} typeData={typeData}
        successPieData={successPieData} slowest={slowest}
        avgDuration={avgDuration} p50={p50} p95={p95}
      />}
      {tab === 'streaming' && <StreamingTab streamStatus={streamStatus} pipelines={pipelines} />}
      {tab === 'scheduler' && <SchedulerTab
        schedules={schedules} scheduleRuns={scheduleRuns}
        runPieData={runPieData} jobTypeData={jobTypeData}
        avgRunDuration={avgRunDuration} runSuccessRate={runSuccessRate}
        runSuccessCount={runSuccessCount} runFailedCount={runFailedCount}
      />}
      {tab === 'storage' && <StorageTab
        tables={tables} connections={connections}
        formatPieData={formatPieData} formatGroups={formatGroups}
      />}
    </div>
  )
}

// ─────────────────────────────────────────────────
// Tab 1: Overview
// ─────────────────────────────────────────────────
function OverviewTab({ resources, systemInfo, streamStatus, vectorStatus, flightInfo, tables, connections, totalQueries, uptime, metrics }: {
  resources: SystemResourcesResponse | null
  systemInfo: SystemInfoResponse | null
  streamStatus: StreamStatusResponse | null
  vectorStatus: VectorStatusResponse | null
  flightInfo: FlightInfoResponse | null
  tables: TableInfo[]
  connections: ConnectionEntry[]
  totalQueries: number
  uptime: string
  metrics: SystemMetricsResponse | null
}) {
  // Gauge helper
  const gaugeColor = (pct: number) => pct > 85 ? '#ef4444' : pct > 60 ? '#f59e0b' : '#22c55e'
  const circumference = Math.PI * 64 // 2 * PI * r where r = 32 (diameter 80 => r = 40 - strokeWidth/2)

  const renderGauge = (label: string, value: number) => {
    const offset = circumference - (value / 100) * circumference
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6 }}>
        <svg width={80} height={80} viewBox="0 0 80 80">
          <circle
            cx={40} cy={40} r={32}
            fill="none"
            stroke="rgba(30,41,59,0.5)"
            strokeWidth={8}
          />
          <circle
            cx={40} cy={40} r={32}
            fill="none"
            stroke={gaugeColor(value)}
            strokeWidth={8}
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            transform="rotate(-90 40 40)"
            style={{ transition: 'stroke-dashoffset 0.6s ease, stroke 0.6s ease' }}
          />
          <text
            x={40} y={40}
            textAnchor="middle"
            dominantBaseline="central"
            fill="#e2e8f0"
            fontSize={16}
            fontWeight={700}
            fontFamily="'DM Sans', sans-serif"
          >
            {Math.round(value)}%
          </text>
        </svg>
        <span style={{ fontSize: 11, color: '#94a3b8', fontFamily: "'DM Sans', sans-serif", letterSpacing: '0.03em' }}>{label}</span>
      </div>
    )
  }

  // Format uptime from seconds to "Xh Ym"
  const formatUptimeShort = (seconds: number) => {
    const h = Math.floor(seconds / 3600)
    const m = Math.floor((seconds % 3600) / 60)
    return `${h}h ${m}m`
  }

  return (
    <div className="space-y-6 animate-slide-up">
      {/* System Gauges */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Gauge className="w-4 h-4 text-amber-400" />
          System Gauges
        </h3>
        <div style={{ display: 'flex', gap: 32, justifyContent: 'center', padding: '20px 0', marginBottom: 20 }}>
          {renderGauge('CPU Usage', metrics?.cpu_usage_percent ?? 0)}
          {renderGauge('Memory Usage', metrics?.memory_usage_percent ?? 0)}
          {renderGauge('Disk Usage', metrics?.disk_usage_percent ?? 0)}
        </div>
      </div>

      {/* Live Stats */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
        <div style={{ background: 'rgba(15,23,42,0.6)', border: '1px solid rgba(30,58,138,0.3)', borderRadius: 8, padding: '12px 16px', textAlign: 'center' }}>
          <div style={{ fontSize: 20, fontWeight: 700, color: '#f59e0b', fontFamily: "'DM Sans', sans-serif" }}>
            {metrics?.queries_per_second ?? 0}
          </div>
          <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2, fontFamily: "'DM Sans', sans-serif" }}>QPS</div>
        </div>
        <div style={{ background: 'rgba(15,23,42,0.6)', border: '1px solid rgba(30,58,138,0.3)', borderRadius: 8, padding: '12px 16px', textAlign: 'center' }}>
          <div style={{ fontSize: 20, fontWeight: 700, color: '#e2e8f0', fontFamily: "'DM Sans', sans-serif" }}>
            {formatNumber(metrics?.total_queries ?? 0)}
          </div>
          <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2, fontFamily: "'DM Sans', sans-serif" }}>Total Queries</div>
        </div>
        <div style={{ background: 'rgba(15,23,42,0.6)', border: '1px solid rgba(30,58,138,0.3)', borderRadius: 8, padding: '12px 16px', textAlign: 'center' }}>
          <div style={{ fontSize: 20, fontWeight: 700, color: '#e2e8f0', fontFamily: "'DM Sans', sans-serif" }}>
            {metrics?.load_avg_1m?.toFixed(2) ?? '0.00'}
          </div>
          <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2, fontFamily: "'DM Sans', sans-serif" }}>Load Avg</div>
        </div>
        <div style={{ background: 'rgba(15,23,42,0.6)', border: '1px solid rgba(30,58,138,0.3)', borderRadius: 8, padding: '12px 16px', textAlign: 'center' }}>
          <div style={{ fontSize: 20, fontWeight: 700, color: '#e2e8f0', fontFamily: "'DM Sans', sans-serif" }}>
            {metrics?.uptime_seconds ? formatUptimeShort(metrics.uptime_seconds) : '0h 0m'}
          </div>
          <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 2, fontFamily: "'DM Sans', sans-serif" }}>Uptime</div>
        </div>
      </div>

      {/* Machine Resources */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Server className="w-4 h-4 text-amber-400" />
          Machine Resources
        </h3>
        <div className="grid grid-cols-2 gap-4">
          <Card>
            <div className="flex items-center gap-3 mb-2">
              <div className="w-8 h-8 rounded-lg bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
                <Cpu className="w-4 h-4 text-amber-400" />
              </div>
              <div>
                <p className="text-2xs text-zinc-500 tracking-wide">CPU Cores</p>
                <span className="text-xl font-display font-bold text-zinc-50 readout tracking-tight">
                  {resources?.cpu_cores ?? '--'}
                </span>
              </div>
            </div>
            <p className="text-2xs text-zinc-500 tracking-wide">
              Target partitions: {resources?.target_partitions ?? '--'}
            </p>
          </Card>
          <Card>
            <div className="flex items-center gap-3 mb-2">
              <div className="w-8 h-8 rounded-lg bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center">
                <HardDrive className="w-4 h-4 text-cyan-400" />
              </div>
              <div>
                <p className="text-2xs text-zinc-500 tracking-wide">Total Memory</p>
                <span className="text-xl font-display font-bold text-zinc-50 readout tracking-tight">
                  {resources ? formatBytes(resources.total_memory_bytes) : '--'}
                </span>
              </div>
            </div>
            <p className="text-2xs text-zinc-500 tracking-wide">
              Engine limit: {resources?.engine_memory_limit ? formatBytes(resources.engine_memory_limit) : 'Unlimited'}
            </p>
          </Card>
        </div>
      </div>

      {/* Engine Configuration */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Zap className="w-4 h-4 text-amber-400" />
          Engine Configuration
        </h3>
        <Card>
          <div className="grid grid-cols-3 divide-x divide-white/[0.04]">
            {[
              { label: 'Batch Size', value: resources?.batch_size ? formatNumber(resources.batch_size) : '--' },
              { label: 'Target Partitions', value: resources?.target_partitions ?? '--' },
              { label: 'Tokio Workers', value: resources?.tokio_workers ?? '--' },
            ].map(s => (
              <div key={s.label} className="px-4 py-2 text-center">
                <span className="text-xl font-display font-bold text-zinc-50 readout tracking-tight">{s.value}</span>
                <p className="text-2xs text-zinc-500 tracking-wide mt-1">{s.label}</p>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* Distributed Status */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Network className="w-4 h-4 text-amber-400" />
          Distributed Status
        </h3>
        <Card>
          <div className="flex items-center gap-6">
            <div>
              <p className="text-2xs text-zinc-500 tracking-wide">Mode</p>
              <Badge className={resources?.distributed_mode ? 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20' : 'bg-amber-400/10 text-amber-400 border-amber-400/20'}>
                {resources?.distributed_mode ? 'Distributed' : 'Standalone'}
              </Badge>
            </div>
            <div>
              <p className="text-2xs text-zinc-500 tracking-wide">Flight Status</p>
              <Badge className="bg-white/[0.04] text-zinc-400">{resources?.flight_status ?? 'unknown'}</Badge>
            </div>
            <div>
              <p className="text-2xs text-zinc-500 tracking-wide">Node Role</p>
              <Badge className="bg-white/[0.04] text-zinc-400">{resources?.node_role ?? 'unknown'}</Badge>
            </div>
          </div>
        </Card>
      </div>

      {/* Engine Health */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Activity className="w-4 h-4 text-amber-400" />
          Engine Health
        </h3>
        <div className="grid grid-cols-4 gap-4">
          {[
            { name: 'SQL Engine', status: systemInfo ? 'healthy' as const : 'idle' as const, desc: 'DataFusion query execution', tooltip: 'DataFusion 51 \u2014 columnar OLAP execution with vectorized operators' },
            { name: 'Streaming', status: (streamStatus?.metrics?.events_ingested ?? 0) > 0 ? 'healthy' as const : 'idle' as const, desc: 'Kafka / CDC ingestion', tooltip: 'Kafka/CDC ingestion engine \u2014 materializes streams into Iceberg tables' },
            { name: 'Vector', status: (vectorStatus?.document_count ?? 0) > 0 ? 'healthy' as const : 'idle' as const, desc: 'Lance / LanceDB search', tooltip: 'Lance-backed vector search \u2014 IVF-PQ/HNSW indexes for AI workloads' },
            { name: 'Flight', status: flightInfo?.status === 'running' ? 'healthy' as const : 'idle' as const, desc: `Arrow Flight RPC${flightInfo?.active_clients ? ` \u2014 ${flightInfo.active_clients} client${flightInfo.active_clients !== 1 ? 's' : ''}` : ''}`, tooltip: 'Arrow Flight gRPC server \u2014 distributed query execution and JDBC/ODBC gateway' },
          ].map(e => (
            <UiTooltip key={e.name} content={e.tooltip} position="bottom">
              <Card hover>
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-medium text-zinc-300">{e.name}</span>
                  <StatusDot status={e.status} pulse={e.status === 'healthy'} />
                </div>
                <p className="text-2xs text-zinc-600">{e.desc}</p>
              </Card>
            </UiTooltip>
          ))}
        </div>
      </div>

      {/* Quick Stats */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <ArrowUpRight className="w-4 h-4 text-amber-400" />
          Quick Stats
        </h3>
        <div className="grid grid-cols-4 gap-4">
          {[
            { label: 'Uptime', value: uptime, icon: Timer, color: 'text-emerald-400' },
            { label: 'Total Queries', value: formatNumber(totalQueries), icon: BarChart3, color: 'text-amber-400' },
            { label: 'Tables', value: String(tables.length), icon: Database, color: 'text-cyan-400' },
            { label: 'Connections', value: String(connections.length), icon: Layers, color: 'text-violet-400' },
          ].map(s => (
            <Card key={s.label}>
              <div className="flex items-center gap-3">
                <s.icon className={cn('w-4 h-4', s.color)} />
                <div>
                  <span className="text-xl font-display font-bold text-zinc-50 readout tracking-tight">{s.value}</span>
                  <p className="text-2xs text-zinc-500 tracking-wide">{s.label}</p>
                </div>
              </div>
            </Card>
          ))}
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────
// Tab 2: Query Performance
// ─────────────────────────────────────────────────
function QueryPerformanceTab({ history, latencyData, typeData, successPieData, slowest, avgDuration, p50, p95 }: {
  history: QueryHistoryEntry[]
  latencyData: { name: string; count: number }[]
  typeData: { name: string; count: number }[]
  successPieData: { name: string; value: number }[]
  slowest: QueryHistoryEntry[]
  avgDuration: number
  p50: number
  p95: number
}) {
  return (
    <div className="space-y-6 animate-slide-up">
      {/* Summary Stats */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Avg Duration', value: formatDuration(avgDuration), color: 'text-amber-400' },
          { label: 'P50 (Median)', value: formatDuration(p50), color: 'text-cyan-400' },
          { label: 'P95', value: formatDuration(p95), color: 'text-rose-400' },
          { label: 'Total Queries', value: formatNumber(history.length), color: 'text-emerald-400' },
        ].map(s => (
          <Card key={s.label}>
            <p className="text-2xs text-zinc-500 tracking-wide mb-1">{s.label}</p>
            <span className={cn('text-xl font-display font-bold readout tracking-tight', s.color)}>{s.value}</span>
          </Card>
        ))}
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-2 gap-4">
        {/* Latency Distribution */}
        <Card>
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <Timer className="w-4 h-4 text-amber-400" />
            Latency Distribution
          </h3>
          {history.length ? (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={latencyData} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
                <XAxis dataKey="name" tick={axisStyle} />
                <YAxis tick={axisStyle} />
                <Tooltip {...tooltipStyle} />
                <Bar dataKey="count" name="Queries" radius={[4, 4, 0, 0]}>
                  {latencyData.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex flex-col items-center justify-center h-[280px] text-zinc-600">
              <BarChart3 className="w-8 h-8 text-zinc-700 mb-2" />
              <p className="text-xs">No query data available</p>
            </div>
          )}
        </Card>

        {/* Query Type Breakdown */}
        <Card>
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <Layers className="w-4 h-4 text-cyan-400" />
            Query Type Breakdown
          </h3>
          {typeData.length ? (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={typeData} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
                <XAxis dataKey="name" tick={axisStyle} />
                <YAxis tick={axisStyle} />
                <Tooltip {...tooltipStyle} />
                <Bar dataKey="count" name="Count" radius={[4, 4, 0, 0]}>
                  {typeData.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex flex-col items-center justify-center h-[280px] text-zinc-600">
              <BarChart3 className="w-8 h-8 text-zinc-700 mb-2" />
              <p className="text-xs">No query type data</p>
            </div>
          )}
        </Card>
      </div>

      {/* Success vs Failure pie */}
      <Card>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <CheckCircle2 className="w-4 h-4 text-emerald-400" />
          Success vs Failure
        </h3>
        {history.length ? (
          <div className="flex items-center gap-8">
            <ResponsiveContainer width="50%" height={280}>
              <PieChart>
                <Pie
                  data={successPieData}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={4}
                  dataKey="value"
                  stroke="none"
                >
                  <Cell fill="#10b981" />
                  <Cell fill="#f43f5e" />
                </Pie>
                <Tooltip {...tooltipStyle} />
                <Legend
                  verticalAlign="bottom"
                  formatter={(value: string) => <span className="text-xs text-zinc-400">{value}</span>}
                />
              </PieChart>
            </ResponsiveContainer>
            <div className="space-y-3">
              <div className="flex items-center gap-3">
                <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                <div>
                  <span className="text-lg font-display font-bold text-zinc-50 readout">{successPieData[0].value}</span>
                  <p className="text-2xs text-zinc-500">Successful</p>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <XCircle className="w-4 h-4 text-rose-400" />
                <div>
                  <span className="text-lg font-display font-bold text-zinc-50 readout">{successPieData[1].value}</span>
                  <p className="text-2xs text-zinc-500">Failed</p>
                </div>
              </div>
              <div>
                <p className="text-2xs text-zinc-500">Success Rate</p>
                <span className="text-sm font-bold text-emerald-400 readout">
                  {history.length ? ((successPieData[0].value / history.length) * 100).toFixed(1) : 0}%
                </span>
              </div>
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-[280px] text-zinc-600">
            <CheckCircle2 className="w-8 h-8 text-zinc-700 mb-2" />
            <p className="text-xs">No queries recorded yet</p>
          </div>
        )}
      </Card>

      {/* Top 10 Slowest Queries */}
      <Card padding="none">
        <div className="p-5 pb-0">
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <Clock className="w-4 h-4 text-rose-400" />
            Top 10 Slowest Queries
          </h3>
        </div>
        {slowest.length ? (
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-white/[0.04]">
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">SQL</th>
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">Duration</th>
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">Type</th>
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">Time</th>
                </tr>
              </thead>
              <tbody>
                {slowest.map((q, i) => (
                  <tr key={`${q.query_id}-${i}`} className="border-b border-white/[0.02] hover:bg-white/[0.02] transition-colors">
                    <td className="px-5 py-2.5 text-xs text-zinc-400 font-mono max-w-xs truncate">{q.sql.slice(0, 80)}{q.sql.length > 80 ? '...' : ''}</td>
                    <td className="px-5 py-2.5 text-xs text-amber-400 font-mono">{formatDuration(q.duration_ms)}</td>
                    <td className="px-5 py-2.5">
                      <Badge className="bg-white/[0.04] text-zinc-400">{q.query_type}</Badge>
                    </td>
                    <td className="px-5 py-2.5 text-xs text-zinc-500">{formatRelativeTime(q.timestamp)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-12 text-zinc-600">
            <Clock className="w-8 h-8 text-zinc-700 mb-2" />
            <p className="text-xs">No queries recorded yet</p>
          </div>
        )}
      </Card>
    </div>
  )
}

// ─────────────────────────────────────────────────
// Tab 3: Streaming & CDC
// ─────────────────────────────────────────────────
function StreamingTab({ streamStatus, pipelines }: {
  streamStatus: StreamStatusResponse | null
  pipelines: StreamingPipeline[]
}) {
  const metrics = streamStatus?.metrics
  const hasData = metrics && metrics.events_ingested > 0

  return (
    <div className="space-y-6 animate-slide-up">
      {/* Stats cards */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Events Ingested', value: hasData ? formatNumber(metrics.events_ingested) : '0', color: 'text-amber-400' },
          { label: 'Bytes Processed', value: hasData ? formatBytes(metrics.bytes_ingested) : '0 B', color: 'text-cyan-400' },
          { label: 'Throughput', value: hasData ? `${formatNumber(metrics.events_per_sec)}/s` : '0/s', color: 'text-emerald-400' },
          { label: 'Avg Latency', value: hasData ? formatDuration(metrics.avg_latency_ms) : '--', color: 'text-violet-400' },
        ].map(s => (
          <Card key={s.label}>
            <p className="text-2xs text-zinc-500 tracking-wide mb-1">{s.label}</p>
            <span className={cn('text-xl font-display font-bold readout tracking-tight', s.color)}>{s.value}</span>
          </Card>
        ))}
      </div>

      {/* Buffer utilization */}
      {streamStatus && (
        <Card>
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <Gauge className="w-4 h-4 text-amber-400" />
            Buffer Utilization
          </h3>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-2xs text-zinc-400">
              <span>Buffer Size</span>
              <span className="font-mono">{formatNumber(streamStatus.buffer_size)} events</span>
            </div>
            <div className="w-full h-2 rounded-full bg-white/[0.04] overflow-hidden">
              <div
                className="h-full rounded-full bg-gradient-to-r from-amber-400 to-amber-500 transition-all duration-500"
                style={{ width: `${Math.min((streamStatus.buffer_size / 10000) * 100, 100)}%` }}
              />
            </div>
            <p className="text-2xs text-zinc-600">Capacity: 10,000 events (Tokio mpsc bounded channel)</p>
          </div>
        </Card>
      )}

      {/* Pipeline status list */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Radio className="w-4 h-4 text-cyan-400" />
          Pipeline Status
        </h3>
        {pipelines.length ? (
          <div className="space-y-2">
            {pipelines.map(p => (
              <Card key={p.id} hover>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <StatusDot status={p.status === 'running' ? 'healthy' : p.status === 'error' ? 'error' : 'idle'} />
                    <div>
                      <span className="text-sm font-medium text-zinc-200">{p.name}</span>
                      <p className="text-2xs text-zinc-500">Sink: {p.sink_table}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    <Badge className="bg-white/[0.04] text-zinc-400">{p.source_type}</Badge>
                    <span className="text-xs text-zinc-400 font-mono">{formatNumber(p.events_processed)} events</span>
                  </div>
                </div>
              </Card>
            ))}
          </div>
        ) : (
          <Card>
            <div className="flex flex-col items-center justify-center py-8 text-zinc-600">
              <Radio className="w-8 h-8 text-zinc-700 mb-2" />
              <p className="text-xs">No streaming pipelines configured</p>
            </div>
          </Card>
        )}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────
// Tab 4: Scheduler & Jobs
// ─────────────────────────────────────────────────
function SchedulerTab({ schedules, scheduleRuns, runPieData, jobTypeData, avgRunDuration, runSuccessRate, runSuccessCount, runFailedCount }: {
  schedules: ScheduledJob[]
  scheduleRuns: JobRun[]
  runPieData: { name: string; value: number }[]
  jobTypeData: { name: string; count: number }[]
  avgRunDuration: number
  runSuccessRate: string
  runSuccessCount: number
  runFailedCount: number
}) {
  const recentRuns = [...scheduleRuns].sort((a, b) => new Date(b.started_at).getTime() - new Date(a.started_at).getTime()).slice(0, 15)

  return (
    <div className="space-y-6 animate-slide-up">
      {/* Stats row */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Active Jobs', value: String(schedules.filter(s => s.enabled).length), color: 'text-amber-400' },
          { label: 'Total Runs', value: String(scheduleRuns.length), color: 'text-cyan-400' },
          { label: 'Success Rate', value: `${runSuccessRate}%`, color: 'text-emerald-400' },
          { label: 'Avg Run Duration', value: formatDuration(avgRunDuration), color: 'text-violet-400' },
        ].map(s => (
          <Card key={s.label}>
            <p className="text-2xs text-zinc-500 tracking-wide mb-1">{s.label}</p>
            <span className={cn('text-xl font-display font-bold readout tracking-tight', s.color)}>{s.value}</span>
          </Card>
        ))}
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-2 gap-4">
        {/* Job Run PieChart */}
        <Card>
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <CheckCircle2 className="w-4 h-4 text-emerald-400" />
            Run Results
          </h3>
          {runPieData.length ? (
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie
                  data={runPieData}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={4}
                  dataKey="value"
                  stroke="none"
                >
                  {runPieData.map((_, i) => (
                    <Cell key={i} fill={['#10b981', '#f43f5e', '#fbbf24'][i % 3]} />
                  ))}
                </Pie>
                <Tooltip {...tooltipStyle} />
                <Legend
                  verticalAlign="bottom"
                  formatter={(value: string) => <span className="text-xs text-zinc-400">{value}</span>}
                />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex flex-col items-center justify-center h-[280px] text-zinc-600">
              <Clock className="w-8 h-8 text-zinc-700 mb-2" />
              <p className="text-xs">No job runs recorded</p>
            </div>
          )}
        </Card>

        {/* Job Type Distribution */}
        <Card>
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <Layers className="w-4 h-4 text-violet-400" />
            Job Type Distribution
          </h3>
          {jobTypeData.length ? (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={jobTypeData} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
                <XAxis dataKey="name" tick={axisStyle} />
                <YAxis tick={axisStyle} />
                <Tooltip {...tooltipStyle} />
                <Bar dataKey="count" name="Jobs" radius={[4, 4, 0, 0]}>
                  {jobTypeData.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex flex-col items-center justify-center h-[280px] text-zinc-600">
              <Clock className="w-8 h-8 text-zinc-700 mb-2" />
              <p className="text-xs">No scheduled jobs</p>
            </div>
          )}
        </Card>
      </div>

      {/* Recent Runs Table */}
      <Card padding="none">
        <div className="p-5 pb-0">
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <Clock className="w-4 h-4 text-amber-400" />
            Recent Runs
          </h3>
        </div>
        {recentRuns.length ? (
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-white/[0.04]">
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">Job</th>
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">Status</th>
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">Started</th>
                  <th className="px-5 py-2 text-2xs font-medium text-zinc-500 uppercase tracking-wider">Duration</th>
                </tr>
              </thead>
              <tbody>
                {recentRuns.map((r, i) => (
                  <tr key={`${r.id}-${i}`} className="border-b border-white/[0.02] hover:bg-white/[0.02] transition-colors">
                    <td className="px-5 py-2.5 text-xs text-zinc-300">{r.job_name}</td>
                    <td className="px-5 py-2.5">
                      <Badge className={cn(
                        r.status === 'success' ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' :
                        r.status === 'failed' ? 'bg-rose-400/10 text-rose-400 border-rose-400/20' :
                        'bg-amber-400/10 text-amber-400 border-amber-400/20'
                      )}>
                        {r.status}
                      </Badge>
                    </td>
                    <td className="px-5 py-2.5 text-xs text-zinc-500">{formatRelativeTime(r.started_at)}</td>
                    <td className="px-5 py-2.5 text-xs text-zinc-400 font-mono">{r.duration_ms ? formatDuration(r.duration_ms) : '--'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-12 text-zinc-600">
            <Clock className="w-8 h-8 text-zinc-700 mb-2" />
            <p className="text-xs">No runs recorded yet</p>
          </div>
        )}
      </Card>
    </div>
  )
}

// ─────────────────────────────────────────────────
// Tab 5: Storage & Catalog
// ─────────────────────────────────────────────────
function StorageTab({ tables, connections, formatPieData, formatGroups }: {
  tables: TableInfo[]
  connections: ConnectionEntry[]
  formatPieData: { name: string; value: number }[]
  formatGroups: Record<string, string[]>
}) {
  return (
    <div className="space-y-6 animate-slide-up">
      {/* Stats cards */}
      <div className="grid grid-cols-2 gap-4">
        <Card>
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
              <Database className="w-4 h-4 text-amber-400" />
            </div>
            <div>
              <span className="text-xl font-display font-bold text-zinc-50 readout tracking-tight">{tables.length}</span>
              <p className="text-2xs text-zinc-500 tracking-wide">Total Tables</p>
            </div>
          </div>
        </Card>
        <Card>
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center">
              <Network className="w-4 h-4 text-cyan-400" />
            </div>
            <div>
              <span className="text-xl font-display font-bold text-zinc-50 readout tracking-tight">{connections.length}</span>
              <p className="text-2xs text-zinc-500 tracking-wide">Total Connections</p>
            </div>
          </div>
        </Card>
      </div>

      {/* Format distribution */}
      <Card>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Layers className="w-4 h-4 text-violet-400" />
          Format Distribution
        </h3>
        {formatPieData.length ? (
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie
                data={formatPieData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={100}
                paddingAngle={4}
                dataKey="value"
                stroke="none"
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
              >
                {formatPieData.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip {...tooltipStyle} />
              <Legend
                verticalAlign="bottom"
                formatter={(value: string) => <span className="text-xs text-zinc-400">{value}</span>}
              />
            </PieChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex flex-col items-center justify-center h-[280px] text-zinc-600">
            <Database className="w-8 h-8 text-zinc-700 mb-2" />
            <p className="text-xs">No tables registered</p>
          </div>
        )}
      </Card>

      {/* Connection Health */}
      <div>
        <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
          <Server className="w-4 h-4 text-cyan-400" />
          Connection Health
        </h3>
        {connections.length ? (
          <div className="space-y-2">
            {connections.map(c => (
              <Card key={c.id} hover>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <StatusDot status={c.status === 'connected' ? 'healthy' : c.status === 'error' ? 'error' : 'idle'} />
                    <div>
                      <span className="text-sm font-medium text-zinc-200">{c.name}</span>
                      <p className="text-2xs text-zinc-500">{c.host}:{c.port}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    <Badge className="bg-white/[0.04] text-zinc-400">{c.conn_type}</Badge>
                    <StatusDot
                      status={c.status === 'connected' ? 'healthy' : c.status === 'error' ? 'error' : 'idle'}
                      label={c.status}
                    />
                  </div>
                </div>
              </Card>
            ))}
          </div>
        ) : (
          <Card>
            <div className="flex flex-col items-center justify-center py-8 text-zinc-600">
              <Server className="w-8 h-8 text-zinc-700 mb-2" />
              <p className="text-xs">No connections configured</p>
            </div>
          </Card>
        )}
      </div>

      {/* Tables by format */}
      {Object.keys(formatGroups).length > 0 && (
        <div>
          <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
            <Database className="w-4 h-4 text-amber-400" />
            Tables by Format
          </h3>
          <div className="grid grid-cols-2 gap-4">
            {Object.entries(formatGroups).map(([format, names]) => (
              <Card key={format}>
                <div className="flex items-center justify-between mb-2">
                  <Badge className="bg-white/[0.04] text-zinc-300">{format}</Badge>
                  <span className="text-2xs text-zinc-500 font-mono">{names.length} table{names.length !== 1 ? 's' : ''}</span>
                </div>
                <div className="space-y-1 max-h-40 overflow-y-auto">
                  {names.map(n => (
                    <p key={n} className="text-xs text-zinc-400 font-mono truncate">{n}</p>
                  ))}
                </div>
              </Card>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
