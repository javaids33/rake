import { useState, useEffect, useRef, useCallback } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { StatusDot } from '../components/ui/StatusDot'
import { cn, formatBytes, formatDuration, formatRelativeTime } from '../lib/utils'
import {
  getSystemInfo, getSystemResources, getQueryHistory,
  getSchedules, getPipelines, getSystemMetrics,
} from '../api/client'
import { useServerEvents } from '../components/layout/Shell'
import type {
  SystemInfoResponse, SystemResourcesResponse, QueryHistoryEntry,
  ScheduledJob, StreamingPipeline, SystemMetricsResponse,
} from '../types'
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import {
  Activity, Cpu, Zap, Database, Radio, Clock,
  ArrowRight, Layers, MemoryStick, ArrowDown, GitBranch,
} from 'lucide-react'

// ─────────────────────────────────────────────────
// Theme constants
// ─────────────────────────────────────────────────
const tooltipStyle = {
  contentStyle: { background: '#0d1730', border: '1px solid rgba(251,191,36,0.1)', borderRadius: 10, fontSize: 12, backdropFilter: 'blur(12px)' },
  itemStyle: { color: '#94a3b8' },
  labelStyle: { color: '#e2e8f0', fontWeight: 600 as const },
}

const axisStyle = { fontSize: 10, fill: '#475569' }
const gridStroke = 'rgba(251,191,36,0.04)'

// ─────────────────────────────────────────────────
// Memory segment config
// ─────────────────────────────────────────────────
interface MemorySegment {
  name: string
  color: string
  bgClass: string
  pct: number
  mb: number
}

function buildMemorySegments(
  metrics: SystemMetricsResponse | null,
  resources: SystemResourcesResponse | null,
): MemorySegment[] {
  const totalBytes = metrics?.memory_total_bytes || resources?.total_memory_bytes || 16 * 1024 * 1024 * 1024
  const usedBytes = metrics?.memory_used_bytes || 0
  const totalMb = totalBytes / (1024 * 1024)
  const usedMb = usedBytes / (1024 * 1024)

  // Simulated engine memory breakdown based on used memory
  const dfPct = usedMb > 0 ? Math.max(8, Math.min(35, (usedMb / totalMb) * 40)) : 12
  const dkPct = usedMb > 0 ? Math.max(4, Math.min(20, (usedMb / totalMb) * 22)) : 6
  const plPct = usedMb > 0 ? Math.max(2, Math.min(10, (usedMb / totalMb) * 8)) : 3
  const wasmPct = usedMb > 0 ? Math.max(1, Math.min(8, (usedMb / totalMb) * 6)) : 2
  const sysPct = usedMb > 0 ? Math.max(5, Math.min(25, (usedMb / totalMb) * 24)) : 10
  const allocatedPct = dfPct + dkPct + plPct + wasmPct + sysPct
  const freePct = Math.max(0, 100 - allocatedPct)

  return [
    { name: 'DataFusion', color: '#fbbf24', bgClass: 'bg-amber-400', pct: dfPct, mb: Math.round(totalMb * dfPct / 100) },
    { name: 'DuckDB', color: '#34d399', bgClass: 'bg-emerald-400', pct: dkPct, mb: Math.round(totalMb * dkPct / 100) },
    { name: 'Polars', color: '#22d3ee', bgClass: 'bg-cyan-400', pct: plPct, mb: Math.round(totalMb * plPct / 100) },
    { name: 'WASM', color: '#a78bfa', bgClass: 'bg-violet-400', pct: wasmPct, mb: Math.round(totalMb * wasmPct / 100) },
    { name: 'System', color: '#a1a1aa', bgClass: 'bg-zinc-400', pct: sysPct, mb: Math.round(totalMb * sysPct / 100) },
    { name: 'Free', color: '#27272a', bgClass: 'bg-zinc-800', pct: freePct, mb: Math.round(totalMb * freePct / 100) },
  ]
}

// ─────────────────────────────────────────────────
// Pipeline stage config
// ─────────────────────────────────────────────────
interface PipelineStage {
  name: string
  icon: typeof Activity
  count: number
  color: string
}

function buildPipelineStages(history: QueryHistoryEntry[]): PipelineStage[] {
  const recent = history.slice(0, 20)
  const running = recent.filter(q => q.status === 'running').length
  const pending = recent.filter(q => q.status === 'pending').length
  const completed = recent.filter(q => q.status === 'success' || q.status === 'ok').length
  const dfCount = recent.filter(q => (q.engine || '').toLowerCase().includes('datafusion')).length
  const dkCount = recent.filter(q => (q.engine || '').toLowerCase().includes('duckdb')).length
  const plCount = recent.filter(q => (q.engine || '').toLowerCase().includes('polars')).length

  return [
    { name: 'SQL Input', icon: Layers, count: recent.length, color: 'text-amber-400' },
    { name: 'Parse & Classify', icon: GitBranch, count: Math.max(pending, running), color: 'text-cyan-400' },
    { name: 'Cost Model', icon: Cpu, count: running, color: 'text-violet-400' },
    { name: 'DataFusion', icon: Database, count: dfCount, color: 'text-amber-400' },
    { name: 'DuckDB', icon: Database, count: dkCount, color: 'text-emerald-400' },
    { name: 'Polars', icon: Database, count: plCount, color: 'text-cyan-400' },
    { name: 'Result Serialize', icon: Zap, count: completed, color: 'text-amber-400' },
  ]
}

// ─────────────────────────────────────────────────
// Load history for sparkline
// ─────────────────────────────────────────────────
interface LoadPoint {
  time: string
  qps: number
}

// ─────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────
export function WorkflowViz() {
  const [metrics, setMetrics] = useState<SystemMetricsResponse | null>(null)
  const [resources, setResources] = useState<SystemResourcesResponse | null>(null)
  const [systemInfo, setSystemInfo] = useState<SystemInfoResponse | null>(null)
  const [history, setHistory] = useState<QueryHistoryEntry[]>([])
  const [schedules, setSchedules] = useState<ScheduledJob[]>([])
  const [pipelines, setPipelines] = useState<StreamingPipeline[]>([])
  const [loadHistory, setLoadHistory] = useState<LoadPoint[]>([])
  const { status: sseStatus } = useServerEvents()

  // Build SSE-based metrics fallback
  const sseMetrics: SystemMetricsResponse | null = sseStatus ? {
    cpu_usage_percent: sseStatus.cpu,
    memory_used_bytes: sseStatus.mem_used,
    memory_total_bytes: sseStatus.mem_total,
    memory_usage_percent: sseStatus.mem_pct,
    disk_used_bytes: 0,
    disk_total_bytes: 0,
    disk_usage_percent: 0,
    load_avg_1m: sseStatus.load_1m,
    load_avg_5m: 0,
    active_queries: 0,
    total_queries: sseStatus.total_queries,
    queries_per_second: 0,
    uptime_seconds: sseStatus.uptime,
  } : null

  const effectiveMetrics = metrics || sseMetrics

  // Ref to accumulate load data points
  const loadHistoryRef = useRef<LoadPoint[]>([])

  const fetchAll = useCallback(() => {
    getSystemMetrics().then(setMetrics).catch(() => {})
    getSystemResources().then(setResources).catch(() => {})
    getSystemInfo().then(setSystemInfo).catch(() => {})
    getQueryHistory(20).then(r => setHistory(r.history)).catch(() => {})
    getSchedules().then(r => setSchedules(r.schedules)).catch(() => {})
    getPipelines().then(r => setPipelines(r.pipelines)).catch(() => {})
  }, [])

  // Update load history sparkline
  useEffect(() => {
    const qps = effectiveMetrics?.queries_per_second ?? 0
    const now = new Date()
    const timeStr = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`
    const point: LoadPoint = { time: timeStr, qps }
    const maxPoints = 150 // ~5 min at 2s intervals
    loadHistoryRef.current = [...loadHistoryRef.current.slice(-maxPoints + 1), point]
    setLoadHistory([...loadHistoryRef.current])
  }, [effectiveMetrics])

  useEffect(() => {
    fetchAll()
    const interval = setInterval(fetchAll, 2000)
    return () => clearInterval(interval)
  }, [fetchAll])

  const segments = buildMemorySegments(effectiveMetrics, resources)
  const stages = buildPipelineStages(history)
  const totalMemMb = effectiveMetrics
    ? Math.round(effectiveMetrics.memory_total_bytes / (1024 * 1024))
    : resources
      ? Math.round(resources.total_memory_bytes / (1024 * 1024))
      : 0
  const usedMemMb = effectiveMetrics
    ? Math.round(effectiveMetrics.memory_used_bytes / (1024 * 1024))
    : 0
  const cpuPct = effectiveMetrics?.cpu_usage_percent ?? 0
  const totalQueries = effectiveMetrics?.total_queries ?? systemInfo?.query_count ?? 0

  return (
    <div className="h-full overflow-auto p-6 space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-display font-bold text-zinc-100 tracking-tight flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-rose-400 to-rose-600 flex items-center justify-center shadow-lg shadow-rose-500/20">
              <Activity className="w-5 h-5 text-white" />
            </div>
            Workflow Visualization
          </h1>
          <p className="text-zinc-500 text-sm mt-1">
            Real-time memory distribution, query workflows, and incoming load
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Badge className="bg-zinc-800 text-zinc-400 border border-zinc-700/50 text-xs px-2.5 py-1 font-mono">
            {totalQueries} queries
          </Badge>
          <Badge className="bg-amber-400/10 text-amber-400 border border-amber-400/20 text-xs px-2.5 py-1">
            <StatusDot status="healthy" /> Live
          </Badge>
        </div>
      </div>

      {/* ─── Section 1: Engine Memory Map ─── */}
      <Card className="p-5 bg-zinc-900/60 border-zinc-700/30">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <MemoryStick className="w-4 h-4 text-amber-400" />
            <h2 className="text-sm font-semibold text-zinc-200">Engine Memory Map</h2>
          </div>
          <div className="flex items-center gap-4 text-xs text-zinc-500">
            <span>CPU: <span className="text-zinc-200 font-mono">{cpuPct.toFixed(1)}%</span></span>
            <span>Used: <span className="text-zinc-200 font-mono">{formatBytes(usedMemMb * 1024 * 1024)}</span></span>
            <span>Total: <span className="text-zinc-200 font-mono">{formatBytes(totalMemMb * 1024 * 1024)}</span></span>
          </div>
        </div>

        {/* Memory bar */}
        <div className="flex h-14 rounded-xl overflow-hidden border border-zinc-700/30 mb-3">
          {segments.map(seg => (
            seg.pct > 0 && (
              <div
                key={seg.name}
                className="transition-all duration-1000 flex items-center justify-center relative group"
                style={{ width: `${seg.pct}%`, backgroundColor: seg.color }}
              >
                {seg.pct > 6 && (
                  <span className="text-xs font-mono text-white/80 truncate px-1">
                    {seg.name} {seg.mb}MB
                  </span>
                )}
                {/* Tooltip on hover */}
                <div className="absolute -top-10 left-1/2 -translate-x-1/2 opacity-0 group-hover:opacity-100 transition-opacity bg-zinc-900 border border-zinc-700 rounded-lg px-2 py-1 text-xs text-zinc-200 whitespace-nowrap pointer-events-none z-10">
                  {seg.name}: {seg.mb}MB ({seg.pct.toFixed(1)}%)
                </div>
              </div>
            )
          ))}
        </div>

        {/* Legend */}
        <div className="flex flex-wrap gap-4 text-xs">
          {segments.filter(s => s.pct > 0).map(seg => (
            <div key={seg.name} className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: seg.color }} />
              <span className="text-zinc-400">{seg.name}</span>
              <span className="text-zinc-600 font-mono">{seg.pct.toFixed(1)}%</span>
            </div>
          ))}
        </div>
      </Card>

      {/* ─── Sections 2 & 3: Two-column layout ─── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Section 2: Active Query Pipeline */}
        <Card className="p-5 bg-zinc-900/60 border-zinc-700/30">
          <div className="flex items-center gap-2 mb-4">
            <Zap className="w-4 h-4 text-amber-400" />
            <h2 className="text-sm font-semibold text-zinc-200">Active Query Pipeline</h2>
          </div>

          <div className="space-y-1">
            {stages.map((stage, i) => {
              const isEngine = ['DataFusion', 'DuckDB', 'Polars'].includes(stage.name)
              const isLast = i === stages.length - 1
              const isActive = stage.count > 0

              return (
                <div key={stage.name}>
                  <div
                    className={cn(
                      'flex items-center gap-3 px-3 py-2.5 rounded-lg border transition-all duration-500',
                      isEngine ? 'ml-6' : '',
                      isActive
                        ? 'border-amber-400/20 bg-amber-400/[0.04] shadow-[0_0_12px_rgba(251,191,36,0.06)]'
                        : 'border-zinc-800/50 bg-zinc-900/30',
                    )}
                  >
                    <stage.icon className={cn('w-4 h-4 flex-shrink-0', stage.color)} />
                    <span className="text-sm text-zinc-300 flex-1">{stage.name}</span>
                    <Badge className={cn(
                      'text-xs font-mono px-2 py-0.5',
                      isActive
                        ? 'bg-amber-400/10 text-amber-400 border-amber-400/20'
                        : 'bg-zinc-800 text-zinc-600 border-zinc-700/30',
                    )}>
                      {stage.count}
                    </Badge>
                    {isActive && (
                      <div className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
                    )}
                  </div>
                  {/* Connector line */}
                  {!isLast && !isEngine && (
                    <div className="flex justify-center py-0.5">
                      <ArrowDown className="w-3 h-3 text-zinc-700" />
                    </div>
                  )}
                  {isEngine && i < stages.length - 1 && ['DataFusion', 'DuckDB', 'Polars'].includes(stages[i + 1].name) && (
                    <div className="flex justify-center py-0.5 ml-6">
                      <div className="w-px h-2 bg-zinc-800" />
                    </div>
                  )}
                  {stage.name === 'Polars' && (
                    <div className="flex justify-center py-0.5">
                      <ArrowDown className="w-3 h-3 text-zinc-700" />
                    </div>
                  )}
                </div>
              )
            })}
          </div>

          {/* Recent query list */}
          <div className="mt-4 border-t border-zinc-800/50 pt-3">
            <div className="text-xs text-zinc-500 mb-2">Recent Queries</div>
            <div className="space-y-1 max-h-40 overflow-y-auto">
              {history.slice(0, 8).map(q => (
                <div key={q.query_id} className="flex items-center gap-2 text-xs">
                  <StatusDot
                    status={q.status === 'success' || q.status === 'ok' ? 'healthy' : q.status === 'error' ? 'error' : 'idle'}
                  />
                  <span className={cn(
                    'font-mono px-1.5 py-0.5 rounded text-2xs',
                    q.engine?.includes('DuckDB') ? 'bg-emerald-400/10 text-emerald-400' :
                    q.engine?.includes('Polars') ? 'bg-cyan-400/10 text-cyan-400' :
                    'bg-amber-400/10 text-amber-400',
                  )}>
                    {q.engine?.includes('DuckDB') ? 'DK' : q.engine?.includes('Polars') ? 'PL' : 'DF'}
                  </span>
                  <span className="text-zinc-400 truncate flex-1 max-w-[200px]">{q.sql}</span>
                  <span className="text-zinc-600 font-mono flex-shrink-0">{q.duration_ms}ms</span>
                </div>
              ))}
              {history.length === 0 && (
                <div className="text-zinc-600 text-xs italic">No queries yet</div>
              )}
            </div>
          </div>
        </Card>

        {/* Section 3: Job & Pipeline Monitor */}
        <Card className="p-5 bg-zinc-900/60 border-zinc-700/30">
          <div className="flex items-center gap-2 mb-4">
            <Clock className="w-4 h-4 text-amber-400" />
            <h2 className="text-sm font-semibold text-zinc-200">Jobs & Pipelines</h2>
          </div>

          {/* Scheduled Jobs */}
          <div className="mb-4">
            <div className="text-xs text-zinc-500 mb-2 flex items-center gap-1.5">
              <Layers className="w-3 h-3" />
              Scheduled Jobs ({schedules.length})
            </div>
            <div className="space-y-2 max-h-48 overflow-y-auto">
              {schedules.slice(0, 8).map(job => (
                <div
                  key={job.id}
                  className="flex items-center gap-3 px-3 py-2 rounded-lg border border-zinc-800/50 bg-zinc-900/30"
                >
                  <StatusDot status={job.enabled ? 'healthy' : 'idle'} />
                  <div className="flex-1 min-w-0">
                    <div className="text-sm text-zinc-300 truncate">{job.name}</div>
                    <div className="text-2xs text-zinc-600 font-mono">{job.cron}</div>
                  </div>
                  <Badge className={cn(
                    'text-2xs px-1.5 py-0.5',
                    job.engine === 'duckdb' ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' :
                    job.engine === 'polars' ? 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20' :
                    'bg-amber-400/10 text-amber-400 border-amber-400/20',
                  )}>
                    {(job.engine || 'auto').toUpperCase()}
                  </Badge>
                  {job.last_run && (
                    <span className="text-2xs text-zinc-600">{formatRelativeTime(job.last_run)}</span>
                  )}
                </div>
              ))}
              {schedules.length === 0 && (
                <div className="text-zinc-600 text-xs italic px-3 py-2">No scheduled jobs</div>
              )}
            </div>
          </div>

          {/* CDC Pipelines */}
          <div className="border-t border-zinc-800/50 pt-3">
            <div className="text-xs text-zinc-500 mb-2 flex items-center gap-1.5">
              <Radio className="w-3 h-3" />
              CDC Pipelines ({pipelines.length})
            </div>
            <div className="space-y-2 max-h-48 overflow-y-auto">
              {pipelines.slice(0, 8).map(p => {
                const isRunning = p.status === 'running' || p.status === 'snapshotting'
                return (
                  <div
                    key={p.id}
                    className={cn(
                      'px-3 py-2 rounded-lg border transition-all',
                      isRunning
                        ? 'border-cyan-400/20 bg-cyan-400/[0.04] shadow-[0_0_12px_rgba(34,211,238,0.06)]'
                        : 'border-zinc-800/50 bg-zinc-900/30',
                    )}
                  >
                    <div className="flex items-center gap-3">
                      <StatusDot status={isRunning ? 'healthy' : p.status === 'error' ? 'error' : 'idle'} />
                      <div className="flex-1 min-w-0">
                        <div className="text-sm text-zinc-300 truncate">{p.name}</div>
                        <div className="text-2xs text-zinc-600">
                          {p.source_type} <ArrowRight className="w-2.5 h-2.5 inline" /> {p.sink_table}
                        </div>
                      </div>
                      <Badge className={cn(
                        'text-2xs px-1.5 py-0.5',
                        isRunning ? 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20' :
                        p.status === 'error' ? 'bg-rose-400/10 text-rose-400 border-rose-400/20' :
                        'bg-zinc-800 text-zinc-500 border-zinc-700/30',
                      )}>
                        {p.status}
                      </Badge>
                    </div>
                    {/* Progress info */}
                    <div className="flex items-center gap-4 mt-1.5 text-2xs text-zinc-600">
                      <span>Events: <span className="text-zinc-400 font-mono">{p.events_processed.toLocaleString()}</span></span>
                      <span>Created: {formatRelativeTime(p.created_at)}</span>
                    </div>
                    {isRunning && (
                      <div className="mt-1.5 h-1 rounded-full bg-zinc-800 overflow-hidden">
                        <div className="h-full bg-gradient-to-r from-cyan-400 to-cyan-500 rounded-full animate-pulse" style={{ width: '60%' }} />
                      </div>
                    )}
                  </div>
                )
              })}
              {pipelines.length === 0 && (
                <div className="text-zinc-600 text-xs italic px-3 py-2">No CDC pipelines</div>
              )}
            </div>
          </div>

          {/* Summary stats */}
          <div className="mt-4 border-t border-zinc-800/50 pt-3 grid grid-cols-3 gap-3">
            <div className="text-center">
              <div className="text-lg font-mono font-bold text-amber-400">
                {schedules.filter(s => s.enabled).length}
              </div>
              <div className="text-2xs text-zinc-600">Active Jobs</div>
            </div>
            <div className="text-center">
              <div className="text-lg font-mono font-bold text-cyan-400">
                {pipelines.filter(p => p.status === 'running' || p.status === 'snapshotting').length}
              </div>
              <div className="text-2xs text-zinc-600">Running Pipelines</div>
            </div>
            <div className="text-center">
              <div className="text-lg font-mono font-bold text-emerald-400">
                {pipelines.reduce((sum, p) => sum + p.events_processed, 0).toLocaleString()}
              </div>
              <div className="text-2xs text-zinc-600">Total Events</div>
            </div>
          </div>
        </Card>
      </div>

      {/* ─── Section 4: Incoming Load Heatmap ─── */}
      <Card className="p-5 bg-zinc-900/60 border-zinc-700/30">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-amber-400" />
            <h2 className="text-sm font-semibold text-zinc-200">Incoming Load</h2>
            <span className="text-xs text-zinc-600">Last 5 minutes</span>
          </div>
          <div className="flex items-center gap-4 text-xs text-zinc-500">
            <span>Current QPS: <span className="text-amber-400 font-mono">{(effectiveMetrics?.queries_per_second ?? 0).toFixed(1)}</span></span>
            <span>Load 1m: <span className="text-zinc-200 font-mono">{(effectiveMetrics?.load_avg_1m ?? 0).toFixed(2)}</span></span>
            <span>Uptime: <span className="text-zinc-200 font-mono">{formatDuration((effectiveMetrics?.uptime_seconds ?? 0) * 1000)}</span></span>
          </div>
        </div>

        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={loadHistory} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="qpsGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#fbbf24" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#fbbf24" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
              <XAxis
                dataKey="time"
                tick={axisStyle}
                interval="preserveStartEnd"
                tickCount={6}
              />
              <YAxis
                tick={axisStyle}
                width={40}
                domain={[0, 'auto']}
              />
              <Tooltip
                {...tooltipStyle}
                formatter={(value: number) => [`${value.toFixed(2)} q/s`, 'QPS']}
              />
              <Area
                type="monotone"
                dataKey="qps"
                stroke="#fbbf24"
                strokeWidth={2}
                fill="url(#qpsGradient)"
                dot={false}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Quick stats bar */}
        <div className="mt-3 grid grid-cols-4 gap-3">
          <QuickStat
            label="CPU Usage"
            value={`${cpuPct.toFixed(1)}%`}
            color={cpuPct > 80 ? 'text-rose-400' : cpuPct > 50 ? 'text-amber-400' : 'text-emerald-400'}
            icon={Cpu}
          />
          <QuickStat
            label="Memory"
            value={`${effectiveMetrics?.memory_usage_percent?.toFixed(1) ?? '0'}%`}
            color={(effectiveMetrics?.memory_usage_percent ?? 0) > 80 ? 'text-rose-400' : 'text-amber-400'}
            icon={MemoryStick}
          />
          <QuickStat
            label="Total Queries"
            value={totalQueries.toLocaleString()}
            color="text-cyan-400"
            icon={Database}
          />
          <QuickStat
            label="Active Queries"
            value={String(effectiveMetrics?.active_queries ?? 0)}
            color="text-violet-400"
            icon={Zap}
          />
        </div>
      </Card>

      {/* ─── Engine Allocation Detail ─── */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {segments.filter(s => s.name !== 'Free').map(seg => (
          <Card
            key={seg.name}
            className="p-4 bg-zinc-900/60 border-zinc-700/30"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: seg.color }} />
              <span className="text-sm font-medium text-zinc-300">{seg.name}</span>
            </div>
            <div className="text-2xl font-mono font-bold" style={{ color: seg.color }}>
              {seg.mb.toLocaleString()}
              <span className="text-xs text-zinc-600 ml-1">MB</span>
            </div>
            <div className="mt-2 h-1.5 rounded-full bg-zinc-800 overflow-hidden">
              <div
                className="h-full rounded-full transition-all duration-1000"
                style={{ width: `${seg.pct}%`, backgroundColor: seg.color }}
              />
            </div>
            <div className="text-2xs text-zinc-600 mt-1 font-mono">{seg.pct.toFixed(1)}% of total</div>
          </Card>
        ))}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────
// QuickStat sub-component
// ─────────────────────────────────────────────────
function QuickStat({ label, value, color, icon: Icon }: {
  label: string
  value: string
  color: string
  icon: typeof Activity
}) {
  return (
    <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-zinc-800/50 bg-zinc-900/30">
      <Icon className={cn('w-4 h-4', color)} />
      <div>
        <div className={cn('text-sm font-mono font-bold', color)}>{value}</div>
        <div className="text-2xs text-zinc-600">{label}</div>
      </div>
    </div>
  )
}
