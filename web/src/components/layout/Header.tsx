import { useEffect, useState } from 'react'
import { getHealth, getSystemMetrics, getEngines, getTables } from '../../api/client'
import { useAppStore } from '../../stores/app'
import { cn } from '../../lib/utils'
import type { EngineInfo } from '../../types'
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

export function Header() {
  const [healthy, setHealthy] = useState<boolean | null>(null)
  const [latency, setLatency] = useState<number | null>(null)
  const [metrics, setMetrics] = useState<{
    cpu: number; memUsed: number; memTotal: number; memPct: number
    totalQueries: number; uptime: number
  } | null>(null)
  const [engines, setEngines] = useState<EngineInfo[]>([])
  const [tables, setTables] = useState<number>(0)
  const { darkMode } = useAppStore()

  useEffect(() => {
    const check = async () => {
      const start = performance.now()
      try {
        await getHealth()
        setLatency(Math.round(performance.now() - start))
        setHealthy(true)
      } catch {
        setHealthy(false)
        setLatency(null)
      }
    }

    const fetchMetrics = async () => {
      try {
        const m = await getSystemMetrics()
        setMetrics({
          cpu: m.cpu_usage_percent,
          memUsed: m.memory_used_bytes,
          memTotal: m.memory_total_bytes,
          memPct: m.memory_usage_percent,
          totalQueries: m.total_queries,
          uptime: m.uptime_seconds,
        })
      } catch {}
    }

    const fetchEngines = async () => {
      try {
        const r = await getEngines()
        setEngines(r.engines)
      } catch {}
    }

    const fetchTables = async () => {
      try {
        const r = await getTables()
        setTables(r.tables?.length ?? 0)
      } catch {}
    }

    check()
    fetchMetrics()
    fetchEngines()
    fetchTables()
    const hId = setInterval(check, 15000)
    const mId = setInterval(() => { fetchMetrics(); fetchTables() }, 10000)
    return () => { clearInterval(hId); clearInterval(mId) }
  }, [])

  const runningEngines = engines.filter(e => e.status === 'running')

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
            {engines.map(e => (
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
          <span className={cn('text-2xs', label)}>{runningEngines.length}/{engines.length}</span>
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

        {latency !== null && (
          <div className={cn('flex items-center gap-1.5 text-2xs', dim)}>
            <span className={cn('font-mono readout', label)}>{latency}ms</span>
          </div>
        )}

        <div className="flex items-center gap-2">
          <div className="relative">
            <div className={`w-2 h-2 rounded-full ${
              healthy === null ? 'bg-zinc-500' : healthy ? 'bg-emerald-400' : 'bg-rose-400'
            }`} />
            {healthy && (
              <>
                <div className="absolute inset-0 rounded-full bg-emerald-400 animate-ping opacity-30" />
                <div className="absolute -inset-1 rounded-full bg-emerald-400/10 blur-sm" />
              </>
            )}
          </div>
          <span className={cn('text-2xs font-medium tracking-wide', {
            'text-zinc-500': healthy === null && darkMode,
            'text-slate-400': healthy === null && !darkMode,
            'text-emerald-400/80': healthy && darkMode,
            'text-emerald-600': healthy && !darkMode,
            'text-rose-400': healthy === false && darkMode,
            'text-rose-600': healthy === false && !darkMode,
          })}>
            {healthy === null ? 'CHECKING' : healthy ? 'ONLINE' : 'OFFLINE'}
          </span>
        </div>
      </div>
    </header>
  )
}
