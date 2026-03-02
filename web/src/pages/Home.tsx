import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { StatusDot } from '../components/ui/StatusDot'
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
  Database, Terminal, Radio, Search, Clock, Activity,
  Layers, ArrowRight, Zap, Server, ArrowUpRight,
  AlertTriangle, CheckCircle2, Cpu,
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
      const raw = r.tables || []
      const names = raw.map((t: string | { name: string }) => typeof t === 'string' ? t : t.name)
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
  const alerts: Array<{ severity: 'error' | 'warning'; message: string; to: string }> = []
  if (failedRuns.length > 0) alerts.push({ severity: 'error', message: `${failedRuns.length} failed job run${failedRuns.length !== 1 ? 's' : ''}`, to: '/scheduler' })
  if (failedQueries.length > 0) alerts.push({ severity: 'error', message: `${failedQueries.length} failed quer${failedQueries.length !== 1 ? 'ies' : 'y'}`, to: '/history' })
  if (pipelineCount === 0) alerts.push({ severity: 'warning', message: 'No active pipelines', to: '/streaming' })
  if (vectorStatus && vectorStatus.document_count === 0) alerts.push({ severity: 'warning', message: 'Vector index empty', to: '/vector' })

  const tableCount = tableNames.length
  const hasAlerts = alerts.length > 0
  const errorCount = alerts.filter(a => a.severity === 'error').length

  // Engine statuses
  const engines = [
    { label: 'SQL Engine', up: !!system, icon: Terminal, color: 'text-amber-400' },
    { label: 'Streaming', up: !!streamMetrics, icon: Radio, color: 'text-cyan-400' },
    { label: 'Vector', up: (vectorStatus?.document_count ?? 0) > 0, icon: Search, color: 'text-rose-400' },
    { label: 'Transforms', up: transformCount > 0, icon: Activity, color: 'text-violet-400' },
  ]
  const enginesUp = engines.filter(e => e.up).length

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-5">

      {/* ── Status Bar ── top-level system status, always visible */}
      <div className="relative overflow-hidden rounded-xl border border-white/[0.04] bg-navy-900/60 backdrop-blur-md animate-fade-in">
        <div className="absolute inset-0 dot-grid opacity-30" />
        <div className="relative z-10 flex items-center justify-between px-5 py-3">
          {/* Left: platform identity + engine dots */}
          <div className="flex items-center gap-4">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-amber-400 to-amber-600 flex items-center justify-center shadow-lg shadow-amber-500/20">
              <span className="text-navy-950 font-display font-extrabold text-sm">R</span>
            </div>
            <div className="flex items-center gap-3">
              {engines.map(e => (
                <div key={e.label} className="flex items-center gap-1.5">
                  <div className={cn('w-1.5 h-1.5 rounded-full', e.up ? 'bg-emerald-400' : 'bg-zinc-600')} />
                  <span className="text-2xs text-zinc-500 font-mono">{e.label}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Right: status summary */}
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-3 text-2xs font-mono text-zinc-500">
              <span>{enginesUp}/4 engines</span>
              <span className="text-zinc-700">|</span>
              <span>{tableCount} tables</span>
              <span className="text-zinc-700">|</span>
              <span>{connectionCount} conn</span>
              {system && (
                <>
                  <span className="text-zinc-700">|</span>
                  <span>{formatDuration(system.uptime_seconds * 1000)} up</span>
                </>
              )}
            </div>
            {hasAlerts ? (
              <Badge dot dotColor={errorCount > 0 ? 'bg-rose-400' : 'bg-amber-400'} className={errorCount > 0 ? 'bg-rose-400/[0.08] text-rose-400 border-rose-400/15' : 'bg-amber-400/[0.08] text-amber-400 border-amber-400/15'}>
                {alerts.length} alert{alerts.length !== 1 ? 's' : ''}
              </Badge>
            ) : (
              <Badge dot dotColor="bg-emerald-400" className="bg-emerald-400/[0.06] text-emerald-400 border-emerald-400/15">
                All systems go
              </Badge>
            )}
          </div>
        </div>
      </div>

      {/* ── Alerts ── only shows when there are issues */}
      {hasAlerts && (
        <div className="grid gap-2 animate-slide-up">
          {alerts.map((a, i) => (
            <Link key={i} to={a.to} className="group flex items-center gap-3 px-4 py-2.5 rounded-lg border transition-colors bg-navy-900/40 hover:bg-navy-900/60"
              style={{ borderColor: a.severity === 'error' ? 'rgba(251,113,133,0.12)' : 'rgba(251,191,36,0.12)' }}>
              <AlertTriangle className={cn('w-3.5 h-3.5 flex-shrink-0', a.severity === 'error' ? 'text-rose-400' : 'text-amber-400')} />
              <span className={cn('text-xs flex-1', a.severity === 'error' ? 'text-rose-300' : 'text-amber-300')}>{a.message}</span>
              <span className="text-2xs text-zinc-600 group-hover:text-zinc-400 flex items-center gap-1 transition-colors">
                View <ArrowRight className="w-3 h-3" />
              </span>
            </Link>
          ))}
        </div>
      )}

      {/* ── Live Stats Bar ── real-time counters */}
      <div className="grid grid-cols-6 gap-3 animate-slide-up" style={{ animationDelay: '0.05s' }}>
        {[
          { label: 'Tables', value: String(tableCount), icon: Database, color: 'text-cyan-400', to: '/catalog' },
          { label: 'Queries', value: formatNumber(system?.query_count || 0), icon: Terminal, color: 'text-amber-400', to: '/history' },
          { label: 'Connections', value: String(connectionCount), icon: Server, color: 'text-blue-400', to: '/sources' },
          { label: 'Pipelines', value: String(pipelineCount), icon: Radio, color: 'text-emerald-400', to: '/streaming' },
          { label: 'Jobs', value: String(jobCount), icon: Clock, color: 'text-orange-400', to: '/scheduler' },
          { label: 'Transforms', value: String(transformCount), icon: Activity, color: 'text-violet-400', to: '/transforms' },
        ].map(s => (
          <Link key={s.label} to={s.to} className="group">
            <Card hover className="flex items-center gap-3 cursor-pointer">
              <s.icon className={cn('w-4 h-4 flex-shrink-0', s.color)} />
              <div className="min-w-0">
                <p className="text-lg font-mono font-black text-zinc-100 tabular-nums leading-none">{s.value}</p>
                <p className="text-2xs text-zinc-600 mt-0.5">{s.label}</p>
              </div>
            </Card>
          </Link>
        ))}
      </div>

      {/* ── Main Content: Recent Queries (2/3) + Tables (1/3) ── */}
      <div className="grid grid-cols-3 gap-5">

        {/* Recent Queries — primary workspace content */}
        <Card className="col-span-2 animate-slide-up [animation-delay:0.1s]" padding="none">
          <div className="flex items-center justify-between px-5 pt-4 pb-3 border-b border-amber-400/[0.04]">
            <h2 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2">
              <Layers className="w-4 h-4 text-amber-400/60" /> Recent Queries
            </h2>
            <div className="flex items-center gap-3">
              <Link to="/sql" className="text-2xs text-amber-400/70 hover:text-amber-400 flex items-center gap-1 transition-colors">
                Open editor <ArrowUpRight className="w-3 h-3" />
              </Link>
              <Link to="/history" className="text-2xs text-zinc-500 hover:text-zinc-300 flex items-center gap-1 transition-colors">
                All history <ArrowUpRight className="w-3 h-3" />
              </Link>
            </div>
          </div>
          <div className="divide-y divide-white/[0.02]">
            {recentQueries.length === 0 ? (
              <div className="px-5 py-6 text-center">
                <Terminal className="w-5 h-5 text-zinc-700 mx-auto mb-1.5" />
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
              </Link>
            ))}
          </div>
        </Card>

        {/* Registered Tables — compact list */}
        <Card className="animate-slide-up [animation-delay:0.12s]" padding="none">
          <div className="flex items-center justify-between px-5 pt-4 pb-3 border-b border-amber-400/[0.04]">
            <h2 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2">
              <Database className="w-4 h-4 text-cyan-400/60" /> Tables
              <span className="text-2xs font-mono text-zinc-600">{tableCount}</span>
            </h2>
            <Link to="/catalog" className="text-2xs text-amber-400/70 hover:text-amber-400 flex items-center gap-1 transition-colors">
              Catalog <ArrowUpRight className="w-3 h-3" />
            </Link>
          </div>
          <div className="divide-y divide-white/[0.02]">
            {tableNames.length === 0 ? (
              <div className="px-5 py-6 text-center">
                <Database className="w-5 h-5 text-zinc-700 mx-auto mb-1.5" />
                <p className="text-xs text-zinc-600">No tables registered</p>
                <Link to="/sources" className="text-2xs text-amber-400/60 hover:text-amber-400 mt-1 inline-flex items-center gap-1">
                  Add data source <ArrowRight className="w-3 h-3" />
                </Link>
              </div>
            ) : (
              <>
                {tableNames.slice(0, 8).map(name => {
                  const fmt = inferFormat(name)
                  return (
                    <Link key={name} to="/catalog" className="group px-4 py-1.5 flex items-center gap-2.5 hover:bg-white/[0.01] transition-colors">
                      <Badge className={cn('text-2xs px-1.5 py-0', FORMAT_COLORS[fmt.format] || 'bg-white/[0.04] text-zinc-400')}>{fmt.format}</Badge>
                      <span className="text-xs font-mono text-zinc-400 truncate flex-1 group-hover:text-zinc-300 transition-colors">{name}</span>
                    </Link>
                  )
                })}
                {tableNames.length > 8 && (
                  <Link to="/catalog" className="block px-4 py-2 text-center text-2xs text-zinc-500 hover:text-amber-400 transition-colors">
                    +{tableNames.length - 8} more in catalog <ArrowRight className="w-3 h-3 inline ml-1" />
                  </Link>
                )}
              </>
            )}
          </div>
        </Card>
      </div>

      {/* ── Bottom row: Engine status + Streaming activity ── */}
      <div className="grid grid-cols-2 gap-5 animate-slide-up" style={{ animationDelay: '0.15s' }}>

        {/* Engine Status — compact horizontal */}
        <Card padding="none">
          <div className="px-5 pt-3 pb-2 border-b border-amber-400/[0.04]">
            <h2 className="text-xs font-display font-semibold text-zinc-300 flex items-center gap-2">
              <Cpu className="w-3.5 h-3.5 text-amber-400/50" /> Engines
              <span className="text-2xs font-mono text-zinc-600 ml-auto">{enginesUp}/4 active</span>
            </h2>
          </div>
          <div className="px-5 py-3 grid grid-cols-2 gap-x-6 gap-y-2">
            {engines.map(e => (
              <div key={e.label} className="flex items-center gap-2">
                <StatusDot status={e.up ? 'healthy' : 'idle'} pulse={e.up} />
                <e.icon className={cn('w-3 h-3', e.up ? e.color : 'text-zinc-700')} />
                <span className={cn('text-xs', e.up ? 'text-zinc-300' : 'text-zinc-600')}>{e.label}</span>
              </div>
            ))}
          </div>
        </Card>

        {/* Streaming / Activity — live counters */}
        <Card padding="none">
          <div className="px-5 pt-3 pb-2 border-b border-amber-400/[0.04]">
            <h2 className="text-xs font-display font-semibold text-zinc-300 flex items-center gap-2">
              <Radio className="w-3.5 h-3.5 text-cyan-400/50" /> Real-time Activity
            </h2>
          </div>
          <div className="px-5 py-3 grid grid-cols-2 gap-x-6 gap-y-2">
            {[
              { label: 'Events ingested', value: streamMetrics ? formatNumber(streamMetrics.events_ingested) : '0' },
              { label: 'Events/sec', value: String(streamMetrics?.events_per_sec ?? 0) },
              { label: 'Active streams', value: String(streamMetrics?.active_streams ?? 0) },
              { label: 'Avg latency', value: streamMetrics?.avg_latency_ms != null ? `${streamMetrics.avg_latency_ms}ms` : '-' },
              { label: 'Vector docs', value: String(vectorStatus?.document_count ?? 0) },
              { label: 'Total queries', value: formatNumber(system?.query_count ?? 0) },
            ].map(s => (
              <div key={s.label} className="flex items-center justify-between">
                <span className="text-2xs text-zinc-600">{s.label}</span>
                <span className="text-xs font-mono font-semibold text-zinc-300 readout tabular-nums">{s.value}</span>
              </div>
            ))}
          </div>
        </Card>
      </div>
    </div>
  )
}
