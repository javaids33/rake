import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { StatusDot } from '../components/ui/StatusDot'
import { Tooltip } from '../components/ui/Tooltip'
import { cn, formatDuration, formatNumber, formatRelativeTime, inferFormat, FORMAT_COLORS } from '../lib/utils'
import {
  getSystemInfo, getSystemResources, getTables, getQueryHistory,
  getStreamStatus, getVectorStatus, getTransforms, getSchedules,
  getScheduleRuns, getPipelines, getConnections,
} from '../api/client'
import type {
  SystemInfoResponse, SystemResourcesResponse, QueryHistoryEntry,
  StreamingMetrics, VectorStatusResponse, JobRun,
} from '../types'
import {
  Database, Terminal, Radio, Search, Clock, Activity, Layers,
  ArrowRight, Zap, Server, ArrowUpRight, Gauge, Cpu,
  FolderInput, AlertTriangle, CheckCircle2,
} from 'lucide-react'

export function Home() {
  const navigate = useNavigate()
  const [system, setSystem] = useState<SystemInfoResponse | null>(null)
  const [_resources, setResources] = useState<SystemResourcesResponse | null>(null)
  const [tableNames, setTableNames] = useState<string[]>([])
  const [recentQueries, setRecentQueries] = useState<QueryHistoryEntry[]>([])
  const [streamMetrics, setStreamMetrics] = useState<StreamingMetrics | null>(null)
  const [vectorStatus, setVectorStatus] = useState<VectorStatusResponse | null>(null)
  const [transformCount, setTransformCount] = useState(0)
  const [jobCount, setJobCount] = useState(0)
  const [jobRuns, setJobRuns] = useState<JobRun[]>([])
  const [pipelineCount, setPipelineCount] = useState(0)
  const [connectionCount, setConnectionCount] = useState(0)

  useEffect(() => {
    getSystemInfo().then(setSystem).catch(() => {})
    getSystemResources().then(setResources).catch(() => {})
    getTables().then(r => {
      const names = (r.tables || []).map((t: { name: string }) => t.name)
      setTableNames(names)
    }).catch(() => {})
    getQueryHistory(8).then(r => setRecentQueries(r.history || [])).catch(() => {})
    getStreamStatus().then(r => setStreamMetrics(r.metrics)).catch(() => {})
    getVectorStatus().then(setVectorStatus).catch(() => {})
    getTransforms().then(r => setTransformCount(r.transforms?.length || 0)).catch(() => {})
    getSchedules().then(r => setJobCount(r.schedules?.length || 0)).catch(() => {})
    getScheduleRuns().then(r => setJobRuns(r.runs || [])).catch(() => {})
    getPipelines().then(r => setPipelineCount(r.pipelines?.length || 0)).catch(() => {})
    getConnections().then(r => setConnectionCount(r.connections?.length || 0)).catch(() => {})
  }, [])

  // Compute alerts
  const failedRuns = jobRuns.filter(r => r.status === 'failed')
  const failedQueries = recentQueries.filter(q => q.status !== 'success')
  const alerts: Array<{ color: string; dotColor: string; message: string; to: string }> = []
  if (failedRuns.length > 0) alerts.push({ color: 'text-rose-400', dotColor: 'bg-rose-400', message: `${failedRuns.length} failed job run${failedRuns.length !== 1 ? 's' : ''}`, to: '/scheduler' })
  if (failedQueries.length > 0) alerts.push({ color: 'text-rose-400', dotColor: 'bg-rose-400', message: `${failedQueries.length} failed quer${failedQueries.length !== 1 ? 'ies' : 'y'}`, to: '/history' })
  if (pipelineCount === 0) alerts.push({ color: 'text-amber-400', dotColor: 'bg-amber-400', message: 'No active pipelines', to: '/streaming' })
  if (vectorStatus && vectorStatus.document_count === 0) alerts.push({ color: 'text-amber-400', dotColor: 'bg-amber-400', message: 'Vector index empty', to: '/vector' })

  const tableCount = tableNames.length

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      {/* Hero */}
      <div className="relative overflow-hidden rounded-2xl border border-amber-400/[0.06] bg-gradient-to-br from-navy-900/80 via-navy-900/60 to-navy-850/40 p-8 backdrop-blur-md animate-fade-in">
        <div className="absolute top-0 right-0 w-[400px] h-[400px] bg-amber-400/[0.03] rounded-full blur-[100px]" />
        <div className="absolute bottom-0 left-[30%] w-[300px] h-[300px] bg-cyan-400/[0.02] rounded-full blur-[80px]" />
        <div className="absolute inset-0 dot-grid opacity-50" />

        <div className="relative z-10">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-4">
              <div className="relative">
                <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-amber-400 to-amber-600 flex items-center justify-center shadow-xl shadow-amber-500/25">
                  <span className="text-navy-950 font-display font-extrabold text-xl">R</span>
                </div>
                <div className="absolute -inset-2 rounded-2xl bg-amber-400/10 blur-xl -z-10" />
              </div>
              <div>
                <h1 className="text-2xl font-display font-bold text-zinc-50 tracking-tight">RustLake Platform</h1>
                <p className="text-sm text-zinc-400 mt-0.5 font-sans">Arrow-native composable data platform</p>
              </div>
            </div>
            <Badge dot dotColor="bg-emerald-400" className="bg-emerald-400/[0.06] text-emerald-400 border-emerald-400/15">All Systems Operational</Badge>
          </div>

          <div className="flex items-center gap-2 mt-5">
            {[
              { icon: Cpu, label: 'DataFusion 51', tip: 'Query engine — SQL parsing, 30+ optimizer rules, vectorized execution' },
              { icon: Server, label: 'Arrow 57', tip: 'In-memory columnar format — zero-copy data exchange between all crates' },
              { icon: Database, label: 'Iceberg', tip: 'Primary table format — ACID transactions, time travel, schema evolution' },
              { icon: Search, label: 'Lance', tip: 'Vector storage — 100x faster random access, IVF-PQ/HNSW indexes' },
              { icon: Radio, label: 'Kafka CDC', tip: 'Streaming ingestion — Kafka consumer, MongoDB/Postgres CDC' },
            ].map(b => (
              <Tooltip key={b.label} content={b.tip} position="bottom">
                <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-white/[0.03] border border-white/[0.04] text-2xs text-zinc-400 cursor-help">
                  <b.icon className="w-3 h-3 text-amber-400/50" />
                  <span className="font-mono tracking-wide">{b.label}</span>
                </div>
              </Tooltip>
            ))}
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-4 gap-4 stagger">
        {[
          { label: 'Write SQL', desc: 'Open the query editor', to: '/sql', icon: Terminal, color: 'text-amber-400', border: 'border-amber-400/10', glow: 'bg-amber-400/5' },
          { label: 'Add Data Source', desc: 'Connect or upload data', to: '/sources', icon: FolderInput, color: 'text-cyan-400', border: 'border-cyan-400/10', glow: 'bg-cyan-400/5' },
          { label: 'Browse Catalog', desc: 'Explore registered tables', to: '/catalog', icon: Database, color: 'text-cyan-400', border: 'border-cyan-400/10', glow: 'bg-cyan-400/5' },
          { label: 'View Metrics', desc: 'Engine performance & health', to: '/metrics', icon: Gauge, color: 'text-emerald-400', border: 'border-emerald-400/10', glow: 'bg-emerald-400/5' },
        ].map(a => (
          <Card key={a.label} hover onClick={() => navigate(a.to)} className={`relative overflow-hidden group ${a.border}`}>
            <div className={`absolute top-0 right-0 w-20 h-20 rounded-full ${a.glow} blur-2xl`} />
            <div className="relative flex items-center gap-3">
              <div className={`w-10 h-10 rounded-xl bg-white/[0.03] border border-white/[0.05] flex items-center justify-center ${a.color}`}>
                <a.icon className="w-5 h-5" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-[13px] font-display font-semibold text-zinc-200 group-hover:text-zinc-50 transition-colors">{a.label}</p>
                <p className="text-2xs text-zinc-500">{a.desc}</p>
              </div>
              <ArrowRight className="w-4 h-4 text-zinc-600 group-hover:text-zinc-400 transition-colors" />
            </div>
          </Card>
        ))}
      </div>

      {/* Main content: 2/3 + 1/3 */}
      <div className="grid grid-cols-3 gap-6">
        {/* Left column */}
        <div className="col-span-2 space-y-6">
          {/* Recent Queries Notebook */}
          <Card className="animate-slide-up" padding="none">
            <div className="flex items-center justify-between px-5 pt-4 pb-3 border-b border-amber-400/[0.04]">
              <h2 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2">
                <Layers className="w-4 h-4 text-amber-400/60" /> Recent Queries
              </h2>
              <Link to="/history" className="text-2xs text-amber-400/70 hover:text-amber-400 flex items-center gap-1 transition-colors">
                View all <ArrowUpRight className="w-3 h-3" />
              </Link>
            </div>
            <div className="divide-y divide-white/[0.02]">
              {recentQueries.length === 0 ? (
                <div className="px-5 py-10 text-center">
                  <Terminal className="w-6 h-6 text-zinc-700 mx-auto mb-2" />
                  <p className="text-xs text-zinc-600">No queries yet</p>
                  <Link to="/sql" className="text-2xs text-amber-400/60 hover:text-amber-400 mt-1 inline-flex items-center gap-1">
                    Open SQL Editor <ArrowRight className="w-3 h-3" />
                  </Link>
                </div>
              ) : recentQueries.map(q => (
                <Link key={q.query_id} to="/sql" className="group px-5 py-2.5 flex items-center gap-3 hover:bg-white/[0.01] transition-colors">
                  <div className={cn('w-1.5 h-1.5 rounded-full flex-shrink-0', q.status === 'success' ? 'bg-emerald-400' : 'bg-rose-400')} />
                  <code className="text-xs font-mono text-zinc-400 truncate flex-1 group-hover:text-zinc-300 transition-colors">{q.sql}</code>
                  <span className="text-2xs font-mono text-zinc-600 flex items-center gap-1 flex-shrink-0 readout">
                    <Zap className="w-3 h-3 text-amber-400/30" /> {formatDuration(q.duration_ms)}
                  </span>
                  <span className="text-2xs text-zinc-700 flex-shrink-0">{formatRelativeTime(q.timestamp)}</span>
                  <ArrowRight className="w-3 h-3 text-zinc-700 group-hover:text-amber-400/60 transition-colors flex-shrink-0" />
                </Link>
              ))}
            </div>
          </Card>

          {/* Dataset Overview */}
          <Card className="animate-slide-up [animation-delay:0.05s]" padding="none">
            <div className="flex items-center justify-between px-5 pt-4 pb-3 border-b border-amber-400/[0.04]">
              <h2 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2">
                <Database className="w-4 h-4 text-cyan-400/60" /> Dataset Overview
              </h2>
              <Link to="/catalog" className="text-2xs text-amber-400/70 hover:text-amber-400 flex items-center gap-1 transition-colors">
                View catalog <ArrowUpRight className="w-3 h-3" />
              </Link>
            </div>
            <div className="divide-y divide-white/[0.02]">
              {tableNames.length === 0 ? (
                <div className="px-5 py-10 text-center">
                  <Database className="w-6 h-6 text-zinc-700 mx-auto mb-2" />
                  <p className="text-xs text-zinc-600">No tables registered</p>
                  <Link to="/sources" className="text-2xs text-amber-400/60 hover:text-amber-400 mt-1 inline-flex items-center gap-1">
                    Add data source <ArrowRight className="w-3 h-3" />
                  </Link>
                </div>
              ) : tableNames.slice(0, 8).map(name => {
                const fmt = inferFormat(name)
                const isNew = name.startsWith('uploads_')
                return (
                  <Link key={name} to="/catalog" className="group px-5 py-2.5 flex items-center gap-3 hover:bg-white/[0.01] transition-colors">
                    <Badge className={cn('text-2xs', FORMAT_COLORS[fmt.format] || 'bg-white/[0.04] text-zinc-400')}>{fmt.format}</Badge>
                    <span className="text-xs font-mono text-zinc-400 truncate flex-1 group-hover:text-zinc-300 transition-colors">{name}</span>
                    {isNew && <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20 text-2xs">New</Badge>}
                  </Link>
                )
              })}
              {tableNames.length > 8 && (
                <div className="px-5 py-2 text-center">
                  <Link to="/catalog" className="text-2xs text-zinc-500 hover:text-amber-400 transition-colors">
                    +{tableNames.length - 8} more tables
                  </Link>
                </div>
              )}
            </div>
          </Card>
        </div>

        {/* Right column */}
        <div className="space-y-6 animate-slide-up" style={{ animationDelay: '0.1s' }}>
          {/* Health Alerts */}
          <Card padding="none">
            <div className="flex items-center justify-between px-5 pt-4 pb-3 border-b border-amber-400/[0.04]">
              <h2 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2">
                <AlertTriangle className="w-4 h-4 text-amber-400/60" /> Health Alerts
              </h2>
              {alerts.length > 0 && (
                <Badge className="bg-rose-400/10 text-rose-400 border-rose-400/20">{alerts.length}</Badge>
              )}
            </div>
            <div className="divide-y divide-white/[0.02]">
              {alerts.length === 0 ? (
                <div className="px-5 py-8 text-center">
                  <CheckCircle2 className="w-6 h-6 text-emerald-400/60 mx-auto mb-2" />
                  <p className="text-xs text-emerald-400/80">All clear — no issues detected</p>
                </div>
              ) : alerts.map((a, i) => (
                <Link key={i} to={a.to} className="group px-5 py-3 flex items-center gap-3 hover:bg-white/[0.01] transition-colors">
                  <div className={cn('w-2 h-2 rounded-full flex-shrink-0', a.dotColor)} />
                  <span className={cn('text-xs flex-1', a.color)}>{a.message}</span>
                  <span className="text-2xs text-zinc-600 group-hover:text-amber-400/60 flex items-center gap-1 transition-colors">
                    View <ArrowRight className="w-3 h-3" />
                  </span>
                </Link>
              ))}
            </div>
          </Card>

          {/* Quick Stats */}
          <Card>
            <h2 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-4">
              <Activity className="w-4 h-4 text-emerald-400/60" /> Quick Stats
            </h2>
            <div className="space-y-3">
              {[
                { icon: Database, label: 'Tables registered', value: String(tableCount), color: 'text-cyan-400' },
                { icon: Terminal, label: 'Queries executed', value: formatNumber(system?.query_count || recentQueries.length), color: 'text-amber-400' },
                { icon: Radio, label: 'Active streams', value: String(streamMetrics?.active_streams ?? 0), color: 'text-cyan-400' },
                { icon: Clock, label: 'Scheduled jobs', value: String(jobCount), color: 'text-amber-400' },
                { icon: Server, label: 'Connections', value: String(connectionCount), color: 'text-cyan-400' },
                { icon: Activity, label: 'Uptime', value: system ? formatDuration(system.uptime_seconds * 1000) : '-', color: 'text-emerald-400' },
              ].map(s => (
                <div key={s.label} className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <s.icon className={cn('w-3.5 h-3.5', s.color)} />
                    <span className="text-xs text-zinc-500">{s.label}</span>
                  </div>
                  <span className="text-xs font-display font-bold text-zinc-200 readout">{s.value}</span>
                </div>
              ))}
            </div>
          </Card>

          {/* Platform Status */}
          <div>
            <h2 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2 mb-3">
              <Gauge className="w-4 h-4 text-emerald-400/60" /> Engine Status
            </h2>
            {[
              { label: 'SQL Engine', status: system ? 'healthy' as const : 'idle' as const, icon: Terminal, to: '/metrics', color: 'text-amber-400', tip: 'DataFusion OLAP engine — handles SQL queries, aggregations, joins' },
              { label: 'Streaming', status: streamMetrics ? 'healthy' as const : 'idle' as const, icon: Radio, to: '/streaming', color: 'text-cyan-400', tip: 'Kafka/CDC ingestion engine — materializes streams into Iceberg tables' },
              { label: 'Vector Index', status: vectorStatus?.document_count ? 'healthy' as const : 'idle' as const, icon: Search, to: '/vector', color: 'text-rose-400', tip: 'LanceDB vector search — IVF-PQ, HNSW indexes for AI workloads' },
              { label: 'Transforms', status: transformCount > 0 ? 'healthy' as const : 'idle' as const, icon: Activity, to: '/transforms', color: 'text-violet-400', tip: 'dbt-compatible SQL transformations — ref() resolution, lineage tracking' },
            ].map(s => (
              <Tooltip key={s.label} content={s.tip} position="left">
                <Link to={s.to}>
                  <Card hover className="flex items-center gap-3 mb-2 group">
                    <div className={cn('w-8 h-8 rounded-lg bg-white/[0.03] border border-white/[0.05] flex items-center justify-center transition-colors', s.color)}>
                      <s.icon className="w-4 h-4" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-[13px] font-medium text-zinc-300 group-hover:text-zinc-100 transition-colors">{s.label}</p>
                    </div>
                    <StatusDot status={s.status} pulse={s.status === 'healthy'} />
                  </Card>
                </Link>
              </Tooltip>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
