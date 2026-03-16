import { useEffect, useState, memo } from 'react'
import { getEngines } from '../../api/client'
import { useAppStore } from '../../stores/app'
import { cn } from '../../lib/utils'
import type { EngineInfo } from '../../types'
import type { ServerStatus } from '../../hooks/useEventStream'
import { Cpu, HardDrive, Database, Zap, Server, Timer } from 'lucide-react'

function formatBytes(bytes: number): string {
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(0)} MB`
  return `${(bytes / 1e3).toFixed(0)} KB`
}

function formatUptime(s: number): string {
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  return `${h}h ${m}m`
}

interface HeaderProps {
  serverStatus: ServerStatus | null
  sseConnected: boolean
}

export const Header = memo(function Header({ serverStatus, sseConnected }: HeaderProps) {
  const [engines, setEngines] = useState<EngineInfo[]>([])
  const { darkMode } = useAppStore()

  // Fetch full engine info once on mount (SSE gives us a lighter version)
  useEffect(() => {
    getEngines().then(r => setEngines(r.engines)).catch(() => {})
  }, [])

  // Derive metrics from SSE status
  const healthy = sseConnected && serverStatus?.health === 'ok'
  const metrics = serverStatus ? {
    cpu: serverStatus.cpu,
    memUsed: serverStatus.mem_used,
    memTotal: serverStatus.mem_total,
    memPct: serverStatus.mem_pct,
    totalQueries: serverStatus.total_queries,
    uptime: serverStatus.uptime,
  } : null
  const tables = serverStatus?.tables ?? 0

  // Merge SSE engine data with the full engine info fetched once
  const displayEngines = engines.length > 0 ? engines : (serverStatus?.engines ?? []).map(e => ({
    name: e.name,
    version: e.version,
    status: e.status,
    role: 'general',
  }))

  const runningEngines = displayEngines.filter(e => e.status === 'running')

  const dim = darkMode ? 'text-zinc-600' : 'text-slate-400'
  const label = darkMode ? 'text-zinc-500' : 'text-slate-500'
  const value = darkMode ? 'text-zinc-300' : 'text-slate-700'
  const accent = darkMode ? 'text-amber-400/60' : 'text-amber-600/70'

  return (
    <header className={cn(
      'h-11 flex items-center justify-between px-5 relative z-10 backdrop-blur-md',
      darkMode
        ? 'border-b border-amber-500/[0.04] bg-navy-950/50'
        : 'border-b border-slate-200 bg-white/80'
    )}>
      {darkMode && <div className="absolute bottom-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-amber-400/8 to-transparent" />}

      {/* Left: system metrics */}
      <div className="flex items-center gap-5">
        {/* Memory */}
        {metrics && (
          <div className="flex items-center gap-1.5">
            <HardDrive className={cn('w-3 h-3', accent)} />
            <span className={cn('text-2xs font-mono', value)}>
              {formatBytes(metrics.memUsed)}
            </span>
            <span className={cn('text-2xs', dim)}>/ {formatBytes(metrics.memTotal)}</span>
            <div className={cn('w-16 h-1.5 rounded-full overflow-hidden', darkMode ? 'bg-white/[0.06]' : 'bg-slate-200')}>
              <div
                className={cn('h-full rounded-full transition-all duration-500', {
                  'bg-emerald-400': metrics.memPct < 70,
                  'bg-amber-400': metrics.memPct >= 70 && metrics.memPct < 90,
                  'bg-rose-400': metrics.memPct >= 90,
                })}
                style={{ width: `${metrics.memPct}%` }}
              />
            </div>
          </div>
        )}

        {/* CPU */}
        {metrics && (
          <div className="flex items-center gap-1.5">
            <Cpu className={cn('w-3 h-3', accent)} />
            <span className={cn('text-2xs font-mono', value)}>{metrics.cpu.toFixed(0)}%</span>
            <span className={cn('text-2xs', label)}>CPU</span>
          </div>
        )}

        {/* Engines */}
        <div className="flex items-center gap-1.5">
          <Server className={cn('w-3 h-3', accent)} />
          <div className="flex items-center gap-1">
            {displayEngines.map(e => (
              <div
                key={e.name}
                title={`${e.name} ${e.version} — ${e.status}`}
                className={cn(
                  'px-1.5 py-0.5 rounded text-2xs font-mono font-semibold',
                  e.status === 'running'
                    ? e.name === 'DuckDB'
                      ? darkMode ? 'bg-emerald-400/10 text-emerald-400' : 'bg-emerald-50 text-emerald-700'
                      : e.name === 'Polars'
                        ? darkMode ? 'bg-cyan-400/10 text-cyan-400' : 'bg-cyan-50 text-cyan-700'
                        : darkMode ? 'bg-amber-400/10 text-amber-400' : 'bg-amber-50 text-amber-700'
                    : darkMode ? 'bg-zinc-800 text-zinc-600' : 'bg-slate-100 text-slate-400'
                )}
              >
                {e.name === 'DataFusion' ? 'DF' : e.name === 'DuckDB' ? 'DK' : e.name === 'Polars' ? 'PL' : e.name.substring(0, 2).toUpperCase()}
              </div>
            ))}
          </div>
          <span className={cn('text-2xs', label)}>{runningEngines.length}/{displayEngines.length}</span>
        </div>

        {/* Tables */}
        {tables > 0 && (
          <div className="flex items-center gap-1.5">
            <Database className={cn('w-3 h-3', accent)} />
            <span className={cn('text-2xs font-mono', value)}>{tables}</span>
            <span className={cn('text-2xs', label)}>tables</span>
          </div>
        )}

        {/* Queries */}
        {metrics && (
          <div className="flex items-center gap-1.5">
            <Zap className={cn('w-3 h-3', accent)} />
            <span className={cn('text-2xs font-mono', value)}>{metrics.totalQueries}</span>
            <span className={cn('text-2xs', label)}>queries</span>
          </div>
        )}
      </div>

      {/* Right: latency + status */}
      <div className="flex items-center gap-5">
        {metrics && (
          <div className={cn('flex items-center gap-1.5 text-2xs', dim)}>
            <Timer className={cn('w-3 h-3', accent)} />
            <span className={cn('font-mono', label)}>{formatUptime(metrics.uptime)}</span>
          </div>
        )}

        <div className="flex items-center gap-2">
          <div className="relative">
            <div className={`w-2 h-2 rounded-full ${
              serverStatus === null ? 'bg-zinc-500' : healthy ? 'bg-emerald-400' : 'bg-rose-400'
            }`} />
            {healthy && (
              <div className="absolute -inset-1 rounded-full bg-emerald-400/10 blur-sm" />
            )}
          </div>
          <span className={cn('text-2xs font-medium tracking-wide', {
            'text-zinc-500': serverStatus === null && darkMode,
            'text-slate-400': serverStatus === null && !darkMode,
            'text-emerald-400/80': healthy && darkMode,
            'text-emerald-600': healthy && !darkMode,
            'text-rose-400': !healthy && serverStatus !== null && darkMode,
            'text-rose-600': !healthy && serverStatus !== null && !darkMode,
          })}>
            {serverStatus === null ? 'CONNECTING' : healthy ? 'ONLINE' : 'OFFLINE'}
          </span>
        </div>
      </div>
    </header>
  )
})
